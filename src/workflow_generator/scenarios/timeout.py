"""Timeout scenario — a job exceeds its wall time limit.

Purpose: Generates a workflow where a job runs longer than the configured
maximum wall time. HTCondor's periodic_hold expression detects that the
job has been running too long and holds it.

Training signal: The workflow-monitor should detect hold reasons containing
"exceeded wall time" or "time limit" patterns.
"""

from __future__ import annotations

from pathlib import Path
import stat
import textwrap

from workflow_generator.scenarios.base import (
    FailureScenario, ScenarioMetadata,
    generate_config_block, generate_properties_block, generate_site_catalog_block,
)


class TimeoutScenario(FailureScenario):
    """Linear pipeline where the compute job exceeds its wall time limit."""

    def get_metadata(self) -> ScenarioMetadata:
        return ScenarioMetadata(
            scenario_id="timeout",
            display_name="Wall Time Exceeded (Timeout)",
            failure_category="resource",
            expected_exit_code=None,
            expected_job_state="JOB_HELD",
            affected_jobs=["compute"],
            description=(
                "A 3-job linear pipeline where 'compute' sleeps for 300 seconds "
                "but its wall time limit is set to 30 seconds. HTCondor's "
                "periodic_hold expression detects the overshoot and holds the job."
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

        slow = bin_dir / "slow_job.sh"
        slow.write_text(textwrap.dedent("""\
            #!/bin/bash
            # --- Timeout Scenario ---
            # Purpose: This job sleeps for 300 seconds, far exceeding the
            # 30-second wall time limit configured via periodic_hold.
            #
            # HTCondor checks (CurrentTime - JobCurrentStartDate) every
            # STARTER_UPDATE_INTERVAL seconds. When the elapsed time exceeds
            # the limit, the job is held with a descriptive reason string.
            #
            # On a real cluster, this simulates a computation that hangs or
            # takes much longer than expected (e.g., infinite loop, deadlock).
            echo "SLOW_JOB: starting at $(date)"
            echo "SLOW_JOB: will sleep 300s (limit is 30s)"
            sleep 300
            echo "SLOW_JOB: if you see this, the timeout didn't trigger"
            exit 0
        """))
        slow.chmod(slow.stat().st_mode | stat.S_IEXEC)
        scripts.append(slow)

        return scripts

    def generate_workflow_script(self, output_dir: Path, bin_dir: Path, data_config: str = "condorio") -> Path:
        script_path = output_dir / "workflow_timeout.py"
        abs_out = output_dir.resolve()
        config = textwrap.indent(generate_config_block(str(abs_out)), " " * 12)
        props = textwrap.indent(generate_properties_block(data_config), " " * 12)
        site_cat = textwrap.indent(generate_site_catalog_block(data_config), " " * 12)

        script_path.write_text(textwrap.dedent(f"""\
            #!/usr/bin/env python3
            # --- Workflow: Wall Time Exceeded (Timeout) ---
            #
            # Purpose: A 3-job pipeline where 'compute' sleeps for 300 seconds
            # but has a 30-second wall time limit, triggering HTCondor's
            # periodic_hold.
            #
            # Topology:
            #   setup --> compute (HELD — wall time exceeded) --> collect (never runs)
            #
            # Failure mechanism:
            #   The compute job's executable sleeps for 300 seconds.
            #   HTCondor's periodic_hold checks elapsed time every update interval
            #   and holds the job when (CurrentTime - JobCurrentStartDate) > 30.
            #
            # Training signal: workflow-monitor should detect hold reason
            # containing "wall time" or "time limit".

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
            input_path.write_text("input data for timeout scenario\\n")

            rc = ReplicaCatalog()
            rc.add_replica("local", input_file, str(input_path))

            # --- Section: Transformation Catalog ---
            setup_tx = Transformation("setup",
                site="condorpool", pfn=str(BIN_DIR / "good_job.sh"), is_stageable=True)
            compute_tx = Transformation("compute",
                site="condorpool", pfn=str(BIN_DIR / "slow_job.sh"), is_stageable=True)
            collect_tx = Transformation("collect",
                site="condorpool", pfn=str(BIN_DIR / "good_job.sh"), is_stageable=True)

            tc = TransformationCatalog()
            tc.add_transformations(setup_tx, compute_tx, collect_tx)

{site_cat}

            # --- Section: Workflow DAG ---
            wf = Workflow("timeout-workflow")

            f_setup_out = File("setup_output.txt")
            f_compute_out = File("compute_output.txt")
            f_final = File("final_output.txt")

            j_setup = Job("setup") \\
                .add_args("-a", "setup", "-T", "2", "-i", input_file, "-o", f_setup_out) \\
                .add_inputs(input_file) \\
                .add_outputs(f_setup_out)

            # This job will be HELD — slow_job.sh sleeps for 300s but
            # the periodic_hold triggers after 30s of elapsed time.
            j_compute = Job("compute") \\
                .add_args("-i", f_setup_out, "-o", f_compute_out) \\
                .add_inputs(f_setup_out) \\
                .add_outputs(f_compute_out)
            # Set wall time limit via periodic_hold
            j_compute.add_profiles(Namespace.CONDOR,
                key="periodic_hold",
                value="(JobStatus == 2) && ((CurrentTime - JobCurrentStartDate) > 30)")
            j_compute.add_profiles(Namespace.CONDOR,
                key="periodic_hold_reason",
                value='"Job exceeded wall time limit (30s)"')

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
