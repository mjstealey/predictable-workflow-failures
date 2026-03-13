"""Dependency (cascade) failure scenario — a mid-DAG job fails, blocking downstream.

Purpose: Generates a workflow where a job in the middle of the DAG fails,
causing all downstream jobs to never execute. This tests the workflow-monitor's
ability to detect cascading failures where multiple jobs are affected but
only one is the root cause.

Training signal: The workflow-monitor should identify the root-cause job
(the one that actually failed) vs. the collateral-damage jobs (unstarted
due to unmet dependencies).
"""

from __future__ import annotations

from pathlib import Path
import stat
import textwrap

from workflow_generator.scenarios.base import FailureScenario, ScenarioMetadata


class DependencyFailureScenario(FailureScenario):
    """Fan-out/fan-in DAG where one branch fails, blocking the merge job."""

    def get_metadata(self) -> ScenarioMetadata:
        return ScenarioMetadata(
            scenario_id="dependency_failure",
            display_name="Cascading Dependency Failure",
            failure_category="cascade",
            expected_exit_code=1,
            expected_job_state="JOB_FAILURE",
            affected_jobs=["branch_bad", "merge"],
            description=(
                "A fan-out/fan-in workflow with 5 jobs: split -> (branch_ok, "
                "branch_bad) -> merge -> finalize. The 'branch_bad' job exits "
                "with code 1, preventing 'merge' from running (unmet dependency). "
                "'finalize' also never runs. The 'branch_ok' job succeeds but "
                "its output is stranded. This tests cascade detection — the "
                "workflow-monitor must distinguish root cause from collateral damage."
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

        bad = bin_dir / "cascade_fail.sh"
        bad.write_text(textwrap.dedent("""\
            #!/bin/bash
            # --- Cascade Failure Scenario ---
            # Purpose: This job intentionally fails (exit 1) to create a
            # cascading failure. Downstream jobs that depend on this job's
            # output will never execute, even though parallel branches
            # may complete successfully.
            #
            # This simulates a real-world scenario where one pipeline stage
            # crashes (e.g., corrupt data, unhandled edge case) and the
            # workflow-monitor must trace the failure back to this root cause.
            echo "CASCADE_FAIL: simulating mid-pipeline crash"
            echo "CASCADE_FAIL: downstream jobs will be blocked"
            exit 1
        """))
        bad.chmod(bad.stat().st_mode | stat.S_IEXEC)
        scripts.append(bad)

        return scripts

    def generate_workflow_script(self, output_dir: Path, bin_dir: Path) -> Path:
        script_path = output_dir / "workflow_dependency_failure.py"
        abs_out = output_dir.resolve()

        script_path.write_text(textwrap.dedent(f"""\
            #!/usr/bin/env python3
            # --- Workflow: Cascading Dependency Failure ---
            #
            # Purpose: A fan-out/fan-in DAG where one branch fails, blocking
            # all downstream jobs. Tests the workflow-monitor's ability to
            # distinguish root-cause failures from collateral damage.
            #
            # Topology:
            #   split --> branch_ok  --> merge (BLOCKED — dependency unmet)
            #         --> branch_bad -^         --> finalize (BLOCKED)
            #             (FAILS exit 1)
            #
            # Key insight for ML training: Only 'branch_bad' is the root cause.
            # 'merge' and 'finalize' show as "unsubmitted" or "unstarted" in
            # the event log — they were never attempted because DAGMan saw
            # the upstream failure and refused to submit them.

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
            input_path.write_text("input data for dependency failure scenario\\n")

            rc = ReplicaCatalog()
            rc.add_replica("local", input_file, str(input_path))

            # --- Section: Transformation Catalog ---
            split_tx = Transformation("split",
                site="condorpool", pfn=str(BIN_DIR / "good_job.sh"), is_stageable=True)
            branch_ok_tx = Transformation("branch_ok",
                site="condorpool", pfn=str(BIN_DIR / "good_job.sh"), is_stageable=True)
            branch_bad_tx = Transformation("branch_bad",
                site="condorpool", pfn=str(BIN_DIR / "cascade_fail.sh"), is_stageable=True)
            merge_tx = Transformation("merge",
                site="condorpool", pfn=str(BIN_DIR / "good_job.sh"), is_stageable=True)
            finalize_tx = Transformation("finalize",
                site="condorpool", pfn=str(BIN_DIR / "good_job.sh"), is_stageable=True)

            tc = TransformationCatalog()
            tc.add_transformations(split_tx, branch_ok_tx, branch_bad_tx, merge_tx, finalize_tx)

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
            wf = Workflow("dependency-failure-workflow")

            f_split_a = File("split_a.txt")
            f_split_b = File("split_b.txt")
            f_branch_ok_out = File("branch_ok_result.txt")
            f_branch_bad_out = File("branch_bad_result.txt")
            f_merged = File("merged_output.txt")
            f_final = File("final_output.txt")

            # Job 1: split — succeeds, produces two outputs for parallel branches
            j_split = Job("split") \\
                .add_args("-a", "split", "-T", "2", "-i", input_file, "-o", f_split_a, f_split_b) \\
                .add_inputs(input_file) \\
                .add_outputs(f_split_a, f_split_b)

            # Job 2: branch_ok — succeeds normally
            j_branch_ok = Job("branch_ok") \\
                .add_args("-a", "branch_ok", "-T", "2", "-i", f_split_a, "-o", f_branch_ok_out) \\
                .add_inputs(f_split_a) \\
                .add_outputs(f_branch_ok_out)

            # Job 3: branch_bad — FAILS with exit code 1 (ROOT CAUSE)
            j_branch_bad = Job("branch_bad") \\
                .add_args("-a", "branch_bad", "-i", f_split_b, "-o", f_branch_bad_out) \\
                .add_inputs(f_split_b) \\
                .add_outputs(f_branch_bad_out)

            # Job 4: merge — BLOCKED because branch_bad fails
            j_merge = Job("merge") \\
                .add_args("-a", "merge", "-T", "2", "-i", f_branch_ok_out, f_branch_bad_out, "-o", f_merged) \\
                .add_inputs(f_branch_ok_out, f_branch_bad_out) \\
                .add_outputs(f_merged)

            # Job 5: finalize — BLOCKED because merge is blocked
            j_finalize = Job("finalize") \\
                .add_args("-a", "finalize", "-T", "2", "-i", f_merged, "-o", f_final) \\
                .add_inputs(f_merged) \\
                .add_outputs(f_final, stage_out=True, register_replica=True)

            wf.add_jobs(j_split, j_branch_ok, j_branch_bad, j_merge, j_finalize)

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
