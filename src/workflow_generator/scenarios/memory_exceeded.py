"""Memory exceeded scenario — a job allocates more memory than allowed.

Purpose: Generates a workflow where a job deliberately consumes excessive
memory, triggering HTCondor's periodic_hold policy. On Linux this may
trigger the OOM killer (exit 137); on macOS with HTCondor, the periodic_hold
expression detects high ResidentSetSize and holds the job.

Training signal: The workflow-monitor should detect memory-related hold
reasons ("memory usage exceeded" or similar) and/or exit code 137.
"""

from __future__ import annotations

from pathlib import Path
import stat
import textwrap

from workflow_generator.scenarios.base import (
    FailureScenario, ScenarioMetadata,
    generate_config_block, generate_properties_block, generate_site_catalog_block,
)


class MemoryExceededScenario(FailureScenario):
    """Linear pipeline where the compute job exceeds its memory limit."""

    def get_metadata(self) -> ScenarioMetadata:
        return ScenarioMetadata(
            scenario_id="memory_exceeded",
            display_name="Memory Exceeded (OOM)",
            failure_category="resource",
            expected_exit_code=137,
            expected_job_state="JOB_HELD",
            affected_jobs=["compute"],
            description=(
                "A 3-job linear pipeline where 'compute' allocates ~200MB of memory "
                "but its HTCondor job requests only 50MB. The periodic_hold expression "
                "detects that ResidentSetSize exceeds the request and holds the job. "
                "On Linux, the OOM killer may terminate it with SIGKILL (exit 137) "
                "before the hold triggers."
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

        oom = bin_dir / "memory_hog.sh"
        oom.write_text(textwrap.dedent("""\
            #!/bin/bash
            # --- Memory Exceeded Scenario ---
            # Purpose: Allocate ~200MB of memory to exceed the 50MB request_memory
            # set on this job. HTCondor's periodic_hold expression will detect
            # the RSS overshoot and hold the job, or the OOM killer will SIGKILL it.
            #
            # The Python one-liner creates a bytearray that forces real memory
            # allocation (not just virtual address space).
            echo "MEMORY_HOG: starting allocation at $(date)"
            echo "MEMORY_HOG: requested=50MB, actual=~200MB"
            python3 -c "
            import time
            # Allocate 200MB — well over the 50MB request_memory
            data = bytearray(200 * 1024 * 1024)
            # Touch the memory to ensure it's resident, not just mapped
            for i in range(0, len(data), 4096):
                data[i] = 0xFF
            print('MEMORY_HOG: allocation complete, sleeping to let periodic_hold detect it')
            time.sleep(120)  # Hold long enough for HTCondor to check
            "
            echo "MEMORY_HOG: if you see this, the hold policy didn't trigger"
            exit 0
        """))
        oom.chmod(oom.stat().st_mode | stat.S_IEXEC)
        scripts.append(oom)

        return scripts

    def generate_workflow_script(self, output_dir: Path, bin_dir: Path, data_config: str = "condorio") -> Path:
        script_path = output_dir / "workflow_memory_exceeded.py"
        abs_out = output_dir.resolve()
        config = textwrap.indent(generate_config_block(str(abs_out)), " " * 12)
        props = textwrap.indent(generate_properties_block(data_config), " " * 12)
        site_cat = textwrap.indent(generate_site_catalog_block(data_config), " " * 12)

        script_path.write_text(textwrap.dedent(f"""\
            #!/usr/bin/env python3
            # --- Workflow: Memory Exceeded (OOM) ---
            #
            # Purpose: A 3-job pipeline where 'compute' requests 50MB of memory
            # but actually allocates ~200MB, triggering HTCondor's periodic_hold.
            #
            # Topology:
            #   setup --> compute (HELD — memory exceeded) --> collect (never runs)
            #
            # Failure mechanism:
            #   The compute job's executable allocates 200MB via Python bytearray.
            #   HTCondor's periodic_hold expression checks ResidentSetSize every
            #   STARTER_UPDATE_INTERVAL seconds and holds the job when RSS > request.
            #
            # Training signal: workflow-monitor should detect hold reason containing
            # "memory" and/or exit code 137 if OOM killer acts first.

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
            input_path.write_text("input data for memory exceeded scenario\\n")

            rc = ReplicaCatalog()
            rc.add_replica("local", input_file, str(input_path))

            # --- Section: Transformation Catalog ---
            setup_tx = Transformation("setup",
                site="condorpool", pfn=str(BIN_DIR / "good_job.sh"), is_stageable=True)
            compute_tx = Transformation("compute",
                site="condorpool", pfn=str(BIN_DIR / "memory_hog.sh"), is_stageable=True)
            collect_tx = Transformation("collect",
                site="condorpool", pfn=str(BIN_DIR / "good_job.sh"), is_stageable=True)

            tc = TransformationCatalog()
            tc.add_transformations(setup_tx, compute_tx, collect_tx)

{site_cat}

            # --- Section: Workflow DAG ---
            wf = Workflow("memory-exceeded-workflow")

            f_setup_out = File("setup_output.txt")
            f_compute_out = File("compute_output.txt")
            f_final = File("final_output.txt")

            j_setup = Job("setup") \\
                .add_args("-a", "setup", "-T", "2", "-i", input_file, "-o", f_setup_out) \\
                .add_inputs(input_file) \\
                .add_outputs(f_setup_out)

            # This job will be HELD — memory_hog.sh allocates 200MB but
            # request_memory is only 50MB.
            j_compute = Job("compute") \\
                .add_args("-i", f_setup_out, "-o", f_compute_out) \\
                .add_inputs(f_setup_out) \\
                .add_outputs(f_compute_out)
            # Set a low memory request so periodic_hold triggers
            j_compute.add_condor_profile(request_memory="50")
            # periodic_hold checks RSS against the memory request
            j_compute.add_profiles(Namespace.CONDOR,
                key="periodic_hold",
                value="(JobStatus == 2) && (ResidentSetSize > 50000)")
            j_compute.add_profiles(Namespace.CONDOR,
                key="periodic_hold_reason",
                value='"Job exceeded memory limit (RSS > 50MB)"')

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
