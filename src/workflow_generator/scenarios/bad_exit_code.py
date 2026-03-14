"""Bad exit code scenario — jobs exit with specific non-zero codes.

Purpose: Generates workflows where a job terminates with a known exit code.
The workflow-monitor model must learn to distinguish different exit codes:
  - Exit 1: generic failure
  - Exit 2: misuse of shell command
  - Exit 126: command not executable (permission denied)
  - Exit 127: command not found
  - Exit 137: killed by SIGKILL (OOM killer or manual kill)
  - Exit 139: segmentation fault (SIGSEGV)

Each exit code produces a different diagnostic signature in HTCondor logs.
"""

from __future__ import annotations

from pathlib import Path
import stat
import textwrap

from workflow_generator.scenarios.base import (
    FailureScenario, ScenarioMetadata,
    generate_config_block, generate_properties_block, generate_site_catalog_block,
)

# Default exit code; can be overridden at instantiation
DEFAULT_EXIT_CODE = 1


class BadExitCodeScenario(FailureScenario):
    """A linear workflow where a mid-pipeline job exits with a specified code."""

    def __init__(self, exit_code: int = DEFAULT_EXIT_CODE):
        self.exit_code = exit_code

    def get_metadata(self) -> ScenarioMetadata:
        return ScenarioMetadata(
            scenario_id=f"bad_exit_code_{self.exit_code}",
            display_name=f"Bad Exit Code ({self.exit_code})",
            failure_category="code",
            expected_exit_code=self.exit_code,
            expected_job_state="JOB_FAILURE",
            affected_jobs=["compute"],
            description=(
                f"A 3-job linear pipeline (setup -> compute -> collect) where the "
                f"'compute' job exits with code {self.exit_code}. "
                f"Exit code meanings: 1=generic failure, 2=misuse, 126=not executable, "
                f"127=not found, 137=SIGKILL, 139=SIGSEGV. "
                f"The 'collect' job never runs due to the upstream failure."
            ),
        )

    def generate_executables(self, bin_dir: Path) -> list[Path]:
        bin_dir.mkdir(parents=True, exist_ok=True)
        scripts = []

        # Good job script (for setup and collect)
        good = bin_dir / "good_job.sh"
        good.write_text(textwrap.dedent("""\
            #!/bin/bash
            # --- Good Job (succeeds) ---
            # Used for non-failing jobs in failure scenarios.
            set -e
            echo "GOOD_JOB: running at $(date)"
            pegasus-keg -T 2 "$@"
            exit 0
        """))
        good.chmod(good.stat().st_mode | stat.S_IEXEC)
        scripts.append(good)

        # Bad exit code script
        bad = bin_dir / f"bad_exit_{self.exit_code}.sh"
        if self.exit_code == 139:
            # Simulate SIGSEGV by sending signal to self
            bad.write_text(textwrap.dedent(f"""\
                #!/bin/bash
                # --- Bad Exit Code Scenario (exit {self.exit_code}) ---
                # Purpose: Simulate a segmentation fault (SIGSEGV).
                # The workflow-monitor should detect this as a crash with code 139.
                echo "BAD_EXIT: simulating SIGSEGV"
                kill -SEGV $$
            """))
        elif self.exit_code == 137:
            # Simulate SIGKILL
            bad.write_text(textwrap.dedent(f"""\
                #!/bin/bash
                # --- Bad Exit Code Scenario (exit {self.exit_code}) ---
                # Purpose: Simulate SIGKILL (as from OOM killer).
                # The workflow-monitor should detect this as killed with code 137.
                echo "BAD_EXIT: simulating SIGKILL"
                kill -KILL $$
            """))
        else:
            bad.write_text(textwrap.dedent(f"""\
                #!/bin/bash
                # --- Bad Exit Code Scenario (exit {self.exit_code}) ---
                # Purpose: Job exits with code {self.exit_code} to simulate a
                # specific failure type. The workflow-monitor should detect this
                # exit code in HTCondor job event logs.
                echo "BAD_EXIT: intentionally exiting with code {self.exit_code}"
                exit {self.exit_code}
            """))
        bad.chmod(bad.stat().st_mode | stat.S_IEXEC)
        scripts.append(bad)

        return scripts

    def generate_workflow_script(self, output_dir: Path, bin_dir: Path, data_config: str = "condorio") -> Path:
        script_path = output_dir / f"workflow_bad_exit_{self.exit_code}.py"
        abs_out = output_dir.resolve()
        config = textwrap.indent(generate_config_block(str(abs_out)), " " * 12)
        props = textwrap.indent(generate_properties_block(data_config), " " * 12)
        site_cat = textwrap.indent(generate_site_catalog_block(data_config), " " * 12)

        script_path.write_text(textwrap.dedent(f"""\
            #!/usr/bin/env python3
            # --- Workflow: Bad Exit Code ({self.exit_code}) ---
            #
            # Purpose: A 3-job linear pipeline where the middle job ('compute')
            # exits with code {self.exit_code}, causing a predictable failure.
            #
            # Topology:
            #   setup --> compute (FAILS with exit {self.exit_code}) --> collect (never runs)
            #
            # Exit code {self.exit_code} meaning:
            #   1=generic failure, 2=misuse, 126=not executable,
            #   127=not found, 137=SIGKILL, 139=SIGSEGV
            #
            # Training signal: The workflow-monitor model should learn to
            # classify this as a "code" failure based on the exit code pattern
            # in the HTCondor event log.

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
            input_path.write_text("input data for bad exit code scenario\\n")

            rc = ReplicaCatalog()
            rc.add_replica("local", input_file, str(input_path))

            # --- Section: Transformation Catalog ---
            # 'setup' and 'collect' use the good job script (will succeed).
            # 'compute' uses the bad exit script (will fail with code {self.exit_code}).
            setup_tx = Transformation("setup",
                site="condorpool", pfn=str(BIN_DIR / "good_job.sh"), is_stageable=True)
            compute_tx = Transformation("compute",
                site="condorpool", pfn=str(BIN_DIR / "bad_exit_{self.exit_code}.sh"), is_stageable=True)
            collect_tx = Transformation("collect",
                site="condorpool", pfn=str(BIN_DIR / "good_job.sh"), is_stageable=True)

            tc = TransformationCatalog()
            tc.add_transformations(setup_tx, compute_tx, collect_tx)

{site_cat}

            # --- Section: Workflow DAG ---
            # Linear pipeline: setup -> compute -> collect
            wf = Workflow("bad-exit-{self.exit_code}-workflow")

            f_setup_out = File("setup_output.txt")
            f_compute_out = File("compute_output.txt")
            f_final = File("final_output.txt")

            j_setup = Job("setup") \\
                .add_args("-a", "setup", "-T", "2", "-i", input_file, "-o", f_setup_out) \\
                .add_inputs(input_file) \\
                .add_outputs(f_setup_out)

            # This job will FAIL — the executable exits with code {self.exit_code}
            j_compute = Job("compute") \\
                .add_args("-a", "compute", "-T", "2", "-i", f_setup_out, "-o", f_compute_out) \\
                .add_inputs(f_setup_out) \\
                .add_outputs(f_compute_out)

            # This job will NEVER RUN because compute fails first
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
            # Use the Pegasus Python API to plan and submit the workflow directly.
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
