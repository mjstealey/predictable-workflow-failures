"""Missing input file scenario — a job references data that doesn't exist.

Purpose: Generates a workflow where a job declares an input file that has
no entry in the Replica Catalog. Pegasus's stage-in transfer will fail
because it cannot locate the physical file.

Training signal: The workflow-monitor should detect transfer/staging failures
from HTCondor hold reasons containing "Transfer input files failure" or
from Pegasus's pegasus-transfer error logs.
"""

from __future__ import annotations

from pathlib import Path
import stat
import textwrap

from workflow_generator.scenarios.base import FailureScenario, ScenarioMetadata


class MissingInputScenario(FailureScenario):
    """Diamond workflow where a branch job references a non-existent input file."""

    def get_metadata(self) -> ScenarioMetadata:
        return ScenarioMetadata(
            scenario_id="missing_input",
            display_name="Missing Input File",
            failure_category="data",
            expected_exit_code=None,
            expected_job_state="JOB_FAILURE",
            affected_jobs=["findrange_bad", "analyze"],
            description=(
                "A diamond workflow where 'findrange_bad' declares an input file "
                "('phantom_data.txt') whose Replica Catalog entry points to a "
                "non-existent path. The job fails at runtime when the file cannot "
                "be found on disk. The downstream 'analyze' job also fails because "
                "its dependency is unmet."
            ),
        )

    def generate_executables(self, bin_dir: Path) -> list[Path]:
        bin_dir.mkdir(parents=True, exist_ok=True)
        script = bin_dir / "keg_wrapper.sh"
        script.write_text(textwrap.dedent("""\
            #!/bin/bash
            # --- Keg Wrapper (generic success job) ---
            # Wraps pegasus-keg for use in scenarios where this particular
            # job should succeed. The failure is injected elsewhere (e.g.,
            # in the catalog configuration, not in the executable).
            set -e
            echo "KEG_WRAPPER: running at $(date)"
            pegasus-keg -T 2 "$@"
            echo "KEG_WRAPPER: completed at $(date)"
            exit 0
        """))
        script.chmod(script.stat().st_mode | stat.S_IEXEC)
        return [script]

    def generate_workflow_script(self, output_dir: Path, bin_dir: Path) -> Path:
        script_path = output_dir / "workflow_missing_input.py"
        abs_out = output_dir.resolve()

        script_path.write_text(textwrap.dedent(f"""\
            #!/usr/bin/env python3
            # --- Workflow: Missing Input File ---
            #
            # Purpose: One branch of a diamond workflow references an input file
            # whose Replica Catalog entry points to a non-existent path. The
            # planner accepts it, but the job fails at runtime when the file
            # cannot be found on disk.
            #
            # Topology:
            #   preprocess --> findrange_ok  --> analyze (FAILS — missing dependency)
            #                  findrange_bad -^
            #                  (FAILS — phantom_data.txt path does not exist)
            #
            # The failure is injected at the CATALOG level, not in the executable.
            # The 'findrange_bad' job declares 'phantom_data.txt' as an input,
            # but its RC entry points to a path that does not exist on disk.
            #
            # Training signal: workflow-monitor should detect staging/transfer
            # errors from the HTCondor hold reason or Pegasus transfer logs.

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
            # Register input.txt normally. For phantom_data.txt, register a PFN
            # that points to a file that does NOT exist on disk.
            # This is the failure injection point — the planner accepts it, but
            # the job fails at runtime when the file cannot be found.
            input_file = File("input.txt")
            input_path = WORK_DIR / "input.txt"
            input_path.write_text("real input data for missing input scenario\\n")

            phantom = File("phantom_data.txt")

            rc = ReplicaCatalog()
            rc.add_replica("local", input_file, str(input_path))
            rc.add_replica("local", phantom, str(WORK_DIR / "DOES_NOT_EXIST" / "phantom_data.txt"))

            # --- Section: Transformation Catalog ---
            keg_path = str(BIN_DIR / "keg_wrapper.sh")

            preprocess_tx = Transformation("preprocess",
                site="condorpool", pfn=keg_path, is_stageable=True)
            findrange_tx = Transformation("findrange",
                site="condorpool", pfn=keg_path, is_stageable=True)
            analyze_tx = Transformation("analyze",
                site="condorpool", pfn=keg_path, is_stageable=True)

            tc = TransformationCatalog()
            tc.add_transformations(preprocess_tx, findrange_tx, analyze_tx)

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
            wf = Workflow("missing-input-workflow")

            fb1 = File("intermediate_ok.txt")
            fc1 = File("result_ok.txt")
            fc2 = File("result_bad.txt")
            fd = File("final_output.txt")

            # Job 1: preprocess — succeeds normally
            j_pre = Job("preprocess") \\
                .add_args("-a", "preprocess", "-T", "2", "-i", input_file, "-o", fb1) \\
                .add_inputs(input_file) \\
                .add_outputs(fb1)

            # Job 2: findrange_ok — succeeds, processes the real intermediate file
            j_ok = Job("findrange") \\
                .add_args("-a", "findrange", "-T", "2", "-i", fb1, "-o", fc1) \\
                .add_inputs(fb1) \\
                .add_outputs(fc1)

            # Job 3: findrange_bad — FAILS because phantom_data.txt doesn't exist
            # This job declares phantom_data.txt as input, but it was never
            # registered in the Replica Catalog above.
            j_bad = Job("findrange") \\
                .add_args("-a", "findrange", "-T", "2", "-i", phantom, "-o", fc2) \\
                .add_inputs(phantom) \\
                .add_outputs(fc2)

            # Job 4: analyze — FAILS because j_bad's output is unavailable
            j_analyze = Job("analyze") \\
                .add_args("-a", "analyze", "-T", "2", "-i", fc1, fc2, "-o", fd) \\
                .add_inputs(fc1, fc2) \\
                .add_outputs(fd, stage_out=True, register_replica=True)

            wf.add_jobs(j_pre, j_ok, j_bad, j_analyze)

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
