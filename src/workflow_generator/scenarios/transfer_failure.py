"""Transfer failure scenario — a job runs but doesn't create declared output files.

Purpose: Generates a workflow where a job succeeds (exit 0) but fails to
produce one or more output files it declared. Pegasus's post-script or
HTCondor's file transfer mechanism detects the missing output and marks
the job as failed.

Training signal: The workflow-monitor should detect transfer/staging errors
from post-script failure or HTCondor hold reasons related to missing output.
This is distinct from a bad exit code — the process "succeeded" but the
data contract was violated.
"""

from __future__ import annotations

from pathlib import Path
import stat
import textwrap

from workflow_generator.scenarios.base import (
    FailureScenario, ScenarioMetadata,
    generate_config_block, generate_properties_block, generate_site_catalog_block,
)


class TransferFailureScenario(FailureScenario):
    """Linear pipeline where the compute job doesn't create its declared output."""

    def get_metadata(self) -> ScenarioMetadata:
        return ScenarioMetadata(
            scenario_id="transfer_failure",
            display_name="Missing Output (Transfer Failure)",
            failure_category="data",
            expected_exit_code=0,
            expected_job_state="JOB_FAILURE",
            affected_jobs=["compute", "collect"],
            description=(
                "A 3-job pipeline where 'compute' exits 0 but never creates "
                "its declared output file ('compute_output.txt'). Pegasus "
                "detects the missing output during the post-script check and "
                "marks the job as failed. This simulates a subtle bug where "
                "a program runs without error but writes to the wrong path "
                "or skips output generation."
            ),
        )

    def generate_executables(self, bin_dir: Path) -> list[Path]:
        bin_dir.mkdir(parents=True, exist_ok=True)
        scripts = []

        good = bin_dir / "good_job.sh"
        if not good.exists():
            good.write_text(textwrap.dedent("""\
                #!/bin/bash
                set -e
                echo "GOOD_JOB: running at $(date)"
                pegasus-keg -T 2 "$@"
                exit 0
            """))
            good.chmod(good.stat().st_mode | stat.S_IEXEC)
        scripts.append(good)

        no_output = bin_dir / "no_output_job.sh"
        no_output.write_text(textwrap.dedent("""\
            #!/bin/bash
            # --- Transfer Failure Scenario ---
            # Purpose: This job runs "successfully" (exit 0) but deliberately
            # does NOT create the output file it declared in the workflow.
            #
            # This simulates a real-world bug where a program:
            # - Writes to the wrong path
            # - Silently skips output due to an edge case
            # - Produces empty output that gets cleaned up
            #
            # Pegasus's post-script (pegasus-exitcode) checks that all declared
            # output files exist. When it finds them missing, it marks the job
            # as failed even though the process exit code was 0.
            echo "NO_OUTPUT: running successfully but producing no output"
            echo "NO_OUTPUT: declared output file will NOT be created"
            echo "NO_OUTPUT: this simulates a write-to-wrong-path bug"
            # Deliberately do NOT run pegasus-keg or create any output files
            exit 0
        """))
        no_output.chmod(no_output.stat().st_mode | stat.S_IEXEC)
        scripts.append(no_output)

        return scripts

    def generate_workflow_script(self, output_dir: Path, bin_dir: Path, data_config: str = "condorio") -> Path:
        script_path = output_dir / "workflow_transfer_failure.py"
        abs_out = output_dir.resolve()
        config = textwrap.indent(generate_config_block(str(abs_out)), " " * 12)
        props = textwrap.indent(generate_properties_block(data_config), " " * 12)
        site_cat = textwrap.indent(generate_site_catalog_block(data_config), " " * 12)

        script_path.write_text(textwrap.dedent(f"""\
            #!/usr/bin/env python3
            # --- Workflow: Missing Output (Transfer Failure) ---
            #
            # Purpose: A 3-job pipeline where 'compute' exits 0 but never
            # creates its declared output file. Pegasus detects the missing
            # output and fails the job.
            #
            # Topology:
            #   setup --> compute (FAILS — output not created) --> collect (never runs)
            #
            # Failure mechanism:
            #   The compute executable runs and exits 0, but it never calls
            #   pegasus-keg or creates 'compute_output.txt'. Pegasus's
            #   post-script (pegasus-exitcode) verifies output existence and
            #   marks the job as failed when the file is missing.
            #
            # Why this matters for ML training:
            #   This is a SUBTLE failure — the exit code is 0, which normally
            #   indicates success. The model must learn to look beyond exit
            #   codes and check file transfer / post-script status.

            import sys
            import os
            from pathlib import Path
            import logging

            logging.basicConfig(level=logging.INFO)

{config}
{props}

            # --- Section: Replica Catalog ---
            input_file = File("input.txt")
            input_path = WORK_DIR / "input.txt"
            input_path.write_text("input data for transfer failure scenario\\n")

            rc = ReplicaCatalog()
            rc.add_replica("local", input_file, str(input_path))

            # --- Section: Transformation Catalog ---
            setup_tx = Transformation("setup",
                site="condorpool", pfn=str(BIN_DIR / "good_job.sh"), is_stageable=True)
            compute_tx = Transformation("compute",
                site="condorpool", pfn=str(BIN_DIR / "no_output_job.sh"), is_stageable=True)
            collect_tx = Transformation("collect",
                site="condorpool", pfn=str(BIN_DIR / "good_job.sh"), is_stageable=True)

            tc = TransformationCatalog()
            tc.add_transformations(setup_tx, compute_tx, collect_tx)

{site_cat}

            # --- Section: Workflow DAG ---
            wf = Workflow("transfer-failure-workflow")

            f_setup_out = File("setup_output.txt")
            f_compute_out = File("compute_output.txt")
            f_final = File("final_output.txt")

            j_setup = Job("setup") \\
                .add_args("-a", "setup", "-T", "2", "-i", input_file, "-o", f_setup_out) \\
                .add_inputs(input_file) \\
                .add_outputs(f_setup_out)

            # This job will FAIL — no_output_job.sh exits 0 but never creates
            # compute_output.txt. Pegasus post-script catches the discrepancy.
            j_compute = Job("compute") \\
                .add_args("-i", f_setup_out, "-o", f_compute_out) \\
                .add_inputs(f_setup_out) \\
                .add_outputs(f_compute_out)

            j_collect = Job("collect") \\
                .add_args("-a", "collect", "-T", "2", "-i", f_compute_out, "-o", f_final) \\
                .add_inputs(f_compute_out) \\
                .add_outputs(f_final, stage_out=True, register_replica=True)

            wf.add_jobs(j_setup, j_compute, j_collect)

            # --- Section: Write Catalogs ---
            wf.add_replica_catalog(rc)
            wf.add_transformation_catalog(tc)
            wf.add_site_catalog(sc)

            wf.write(str(WORK_DIR / "workflow.yml"))
            print(f"Workflow written to {{WORK_DIR / 'workflow.yml'}}")

            # --- Section: Plan and Submit ---
            # submit=True tells pegasus-plan to also submit to HTCondor immediately.
            try:
                wf.plan(
                    conf=str(WORK_DIR / "pegasus.properties"),
                    sites=["condorpool"],
                    output_sites=["local"],
                    dir=str(SUBMIT_DIR),
                    submit=True,
                )
                print(f"Submit dir: {{wf.braindump.submit_dir}}")
            except Exception as e:
                print(f"Planning/submission error: {{e}}")
                raise

            # --- Section: Monitor Workflow ---
            # wf.status() queries Condor and returns a dict with job counts.
            status = wf.status()
            print(status)

            # Uncomment the next line to block until the workflow completes:
            # wf.wait(delay=5)

            # --- Section: Post-Execution Analysis ---
            # Uncomment once the workflow has finished running.

            # Analyze failures (if any):
            # wf.analyze(verbose=1)

            # Gather runtime statistics:
            # wf.statistics()
        """))
        script_path.chmod(script_path.stat().st_mode | stat.S_IEXEC)
        return script_path
