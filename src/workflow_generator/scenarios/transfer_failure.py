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

from workflow_generator.scenarios.base import FailureScenario, ScenarioMetadata


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

    def generate_workflow_script(self, output_dir: Path, bin_dir: Path) -> Path:
        script_path = output_dir / "workflow_transfer_failure.py"
        abs_out = output_dir.resolve()

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

            # --- Section: Configuration ---
            # Environment-specific paths. Edit these to match your local installation.
            PEGASUS_PYTHON_LIB = "/opt/homebrew/opt/pegasus/lib/pegasus/python"
            PEGASUS_HOME = "/opt/homebrew"
            CONDOR_HOME = "/Users/stealey/condor"
            CONDOR_CONFIG = os.path.join(CONDOR_HOME, "etc", "condor_config")

            # Derived paths
            WORK_DIR = Path("{abs_out}")
            BIN_DIR = WORK_DIR / "bin"
            SCRATCH_DIR = WORK_DIR / "scratch"
            OUTPUT_DIR = WORK_DIR / "output"
            SUBMIT_DIR = WORK_DIR / "submit"

            sys.path.insert(0, PEGASUS_PYTHON_LIB)
            os.environ["CONDOR_CONFIG"] = CONDOR_CONFIG
            os.environ["PATH"] = os.path.join(CONDOR_HOME, "bin") + os.pathsep + os.path.join(PEGASUS_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")
            os.chdir(WORK_DIR)

            from Pegasus.api import *

            # --- Section: Properties ---
            props = Properties()
            props["pegasus.data.configuration"] = "sharedfs"
            props["pegasus.monitord.encoding"] = "json"
            props["dagman.retry"] = "0"
            props.write(str(WORK_DIR / "pegasus.properties"))

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

            # --- Section: Site Catalog ---
            scratch_path = str(SCRATCH_DIR)
            output_path = str(OUTPUT_DIR)
            scratch_url = f"file://{{scratch_path}}"
            output_url = f"file://{{output_path}}"

            local_site = Site("local")
            local_site.add_directories(
                Directory(Directory.SHARED_SCRATCH, scratch_path)
                    .add_file_servers(FileServer(scratch_url, Operation.ALL)),
                Directory(Directory.SHARED_STORAGE, output_path)
                    .add_file_servers(FileServer(output_url, Operation.ALL)),
            )

            condorpool = Site("condorpool")
            condorpool.add_directories(
                Directory(Directory.SHARED_SCRATCH, scratch_path)
                    .add_file_servers(FileServer(scratch_url, Operation.ALL)),
            )
            condorpool.add_profiles(Namespace.CONDOR, key="universe", value="vanilla")
            condorpool.add_profiles(Namespace.CONDOR, key="getenv", value="True")
            condorpool.add_profiles(Namespace.PEGASUS, key="style", value="condor")
            condorpool.add_profiles(Namespace.ENV, key="PEGASUS_HOME", value=PEGASUS_HOME)
            condorpool.add_profiles(Namespace.ENV, key="CONDOR_HOME", value=CONDOR_HOME)

            sc = SiteCatalog()
            sc.add_sites(local_site, condorpool)

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
