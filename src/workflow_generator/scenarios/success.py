"""Success scenario — a clean baseline workflow that completes without errors.

Purpose: Provides positive training examples for the workflow-monitor ML model.
Without success cases, the model cannot learn the boundary between healthy
and failing workflows.
"""

from __future__ import annotations

from pathlib import Path
import stat
import textwrap

from workflow_generator.scenarios.base import FailureScenario, ScenarioMetadata


class SuccessScenario(FailureScenario):
    """Diamond-shaped workflow where all jobs succeed using pegasus-keg."""

    def get_metadata(self) -> ScenarioMetadata:
        return ScenarioMetadata(
            scenario_id="success",
            display_name="Clean Success (Baseline)",
            failure_category="none",
            expected_exit_code=0,
            expected_job_state="JOB_SUCCESS",
            affected_jobs=[],
            description=(
                "A diamond-shaped workflow (preprocess -> 2x findrange -> analyze) "
                "where every job completes successfully. All jobs use pegasus-keg to "
                "produce synthetic output files. This scenario provides the positive "
                "baseline that the workflow-monitor model needs for training."
            ),
        )

    def generate_executables(self, bin_dir: Path) -> list[Path]:
        """Generate a wrapper script that runs pegasus-keg successfully."""
        bin_dir.mkdir(parents=True, exist_ok=True)
        script = bin_dir / "success_job.sh"
        script.write_text(textwrap.dedent("""\
            #!/bin/bash
            # --- Success Scenario Job ---
            # This script wraps pegasus-keg to produce a valid output file.
            # pegasus-keg is a Pegasus utility that generates output files of a
            # specified size, simulating real computational work.
            #
            # Expected behavior: exit 0, output file created.
            set -e
            echo "SUCCESS_JOB: starting at $(date)"
            echo "SUCCESS_JOB: hostname=$(hostname), pid=$$"

            # Generate output data — pegasus-keg creates files based on -o args
            # The -T flag sets runtime in seconds (simulates computation)
            pegasus-keg -T 2 "$@"

            echo "SUCCESS_JOB: completed successfully at $(date)"
            exit 0
        """))
        script.chmod(script.stat().st_mode | stat.S_IEXEC)
        return [script]

    def generate_workflow_script(self, output_dir: Path, bin_dir: Path) -> Path:
        """Generate a Pegasus workflow script for the success scenario."""
        script_path = output_dir / "workflow_success.py"
        abs_out = output_dir.resolve()

        script_path.write_text(textwrap.dedent(f"""\
            #!/usr/bin/env python3
            # --- Workflow: Clean Success (Baseline) ---
            #
            # Purpose: A diamond-shaped workflow where every job succeeds.
            # This provides positive training examples for the workflow-monitor
            # ML model, establishing what "normal" execution looks like.
            #
            # Topology:
            #   preprocess --> findrange_1 --> analyze
            #                  findrange_2 ----^
            #
            # All jobs use pegasus-keg via a wrapper script to produce synthetic
            # output files. No failures are injected.

            import sys
            import os
            from pathlib import Path
            import logging

            logging.basicConfig(level=logging.INFO)

            # --- Section: Configuration ---
            # Environment-specific paths. Edit these to match your local installation.
            # All other paths are derived relative to WORK_DIR (the directory
            # containing this script/notebook).
            PEGASUS_PYTHON_LIB = "/opt/homebrew/opt/pegasus/lib/pegasus/python"
            PEGASUS_HOME = "/opt/homebrew"
            CONDOR_HOME = "/Users/stealey/condor"
            CONDOR_CONFIG = os.path.join(CONDOR_HOME, "etc", "condor_config")

            # Derived paths — no need to edit these
            WORK_DIR = Path("{abs_out}")
            BIN_DIR = WORK_DIR / "bin"
            SCRATCH_DIR = WORK_DIR / "scratch"
            OUTPUT_DIR = WORK_DIR / "output"
            SUBMIT_DIR = WORK_DIR / "submit"

            # Set up environment for Pegasus and HTCondor tools.
            # Jupyter kernels may not inherit shell PATH or CONDOR_CONFIG.
            sys.path.insert(0, PEGASUS_PYTHON_LIB)
            os.environ["CONDOR_CONFIG"] = CONDOR_CONFIG
            os.environ["PATH"] = os.path.join(CONDOR_HOME, "bin") + os.pathsep + os.path.join(PEGASUS_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")
            os.chdir(WORK_DIR)

            from Pegasus.api import *

            # --- Section: Properties ---
            # Configure Pegasus for local shared-filesystem execution.
            # sharedfs mode means jobs read/write from a common directory,
            # appropriate for single-machine or NFS-backed setups.
            props = Properties()
            props["pegasus.data.configuration"] = "sharedfs"
            props["pegasus.monitord.encoding"] = "json"
            props["dagman.retry"] = "0"  # No retries — we want clean pass/fail signals
            props.write(str(WORK_DIR / "pegasus.properties"))

            # --- Section: Replica Catalog ---
            # Register the initial input file that preprocess will consume.
            # In a real workflow this would point to actual data; here we create
            # a small synthetic file.
            input_file = File("input.txt")
            input_path = WORK_DIR / "input.txt"
            input_path.write_text("synthetic input data for success scenario\\n")

            rc = ReplicaCatalog()
            rc.add_replica("local", input_file, str(input_path))

            # --- Section: Transformation Catalog ---
            # Register the success_job.sh wrapper as the executable for all jobs.
            # is_stageable=True means Pegasus will transfer the script to workers.
            keg_path = str(BIN_DIR / "success_job.sh")

            preprocess_tx = Transformation("preprocess",
                site="condorpool", pfn=keg_path, is_stageable=True)
            findrange_tx = Transformation("findrange",
                site="condorpool", pfn=keg_path, is_stageable=True)
            analyze_tx = Transformation("analyze",
                site="condorpool", pfn=keg_path, is_stageable=True)

            tc = TransformationCatalog()
            tc.add_transformations(preprocess_tx, findrange_tx, analyze_tx)

            # --- Section: Site Catalog ---
            # Define local + condorpool sites for shared-filesystem execution.
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
            # Build the diamond DAG: preprocess -> (findrange_1, findrange_2) -> analyze
            wf = Workflow("success-workflow")

            fb1 = File("intermediate_1.txt")
            fb2 = File("intermediate_2.txt")
            fc1 = File("result_1.txt")
            fc2 = File("result_2.txt")
            fd = File("final_output.txt")

            # Job 1: preprocess — reads input, produces two intermediate files
            j1 = Job("preprocess") \\
                .add_args("-a", "preprocess", "-T", "2", "-i", input_file, "-o", fb1, fb2) \\
                .add_inputs(input_file) \\
                .add_outputs(fb1, fb2)

            # Job 2: findrange_1 — processes first intermediate file
            j2 = Job("findrange") \\
                .add_args("-a", "findrange", "-T", "2", "-i", fb1, "-o", fc1) \\
                .add_inputs(fb1) \\
                .add_outputs(fc1)

            # Job 3: findrange_2 — processes second intermediate file
            j3 = Job("findrange") \\
                .add_args("-a", "findrange", "-T", "2", "-i", fb2, "-o", fc2) \\
                .add_inputs(fb2) \\
                .add_outputs(fc2)

            # Job 4: analyze — merges results into final output
            j4 = Job("analyze") \\
                .add_args("-a", "analyze", "-T", "2", "-i", fc1, fc2, "-o", fd) \\
                .add_inputs(fc1, fc2) \\
                .add_outputs(fd, stage_out=True, register_replica=True)

            wf.add_jobs(j1, j2, j3, j4)

            # --- Section: Write Catalogs ---
            wf.add_replica_catalog(rc)
            wf.add_transformation_catalog(tc)
            wf.add_site_catalog(sc)

            # Write the workflow YAML for inspection
            wf.write(str(WORK_DIR / "workflow.yml"))
            print(f"Workflow written to {{WORK_DIR / 'workflow.yml'}}")

            # --- Section: Plan and Submit ---
            # Use the Pegasus Python API to plan and submit the workflow directly.
            # This replaces the CLI invocation of pegasus-plan + pegasus-run.
            # The plan() call maps the abstract workflow to an executable one;
            # submit=True tells it to also submit to HTCondor immediately.
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
            # Check workflow status and optionally wait for completion.
            # wf.status() queries Condor and returns a dict with job counts.
            # wf.wait() blocks with a progress bar until the workflow finishes.
            status = wf.status()
            print(status)

            # Uncomment the next line to block until the workflow completes:
            # wf.wait(delay=5)

            # --- Section: Post-Execution Analysis ---
            # After the workflow completes, use these methods to inspect results.
            # Uncomment once the workflow has finished running.

            # Analyze failures (if any):
            # wf.analyze(verbose=1)

            # Gather runtime statistics:
            # wf.statistics()
        """))
        script_path.chmod(script_path.stat().st_mode | stat.S_IEXEC)
        return script_path
