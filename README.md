# predictable-workflow-failures

Generate [Pegasus WMS](https://pegasus.isi.edu/) workflows with predictable, labeled failure modes. Each scenario produces a self-contained workflow that triggers a specific failure category — exit codes, data staging, resource limits, cascading dependencies, or transfer errors — for testing monitoring tools and training failure-detection models.

Part of the **pegasusai** project. The sibling [`workflow-monitor`](https://github.com/pegasusai/workflow-monitor) consumes these workflows to validate its diagnostics across all failure categories and to verify parity between its live TUI, SSH client, and replay modes.

Each scenario produces:

- A self-contained Python workflow script using `Pegasus.api`
- Shell executables that simulate specific failure behaviors
- A `metadata.json` sidecar with ground-truth labels
- An optional Jupyter notebook (`.ipynb`) for interactive testing

## Prerequisites

| Dependency | Version | Installation |
|------------|---------|-------------|
| [uv](https://docs.astral.sh/uv/) | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| [Pegasus WMS](https://pegasus.isi.edu/) | 5.1.2 | `brew install pegasus` (macOS) or [download](https://pegasus.isi.edu/downloads) |
| [HTCondor](https://htcondor.org/) | 24.x+ | [Install guide](https://htcondor.readthedocs.io/en/latest/getting-htcondor/) |
| Python | 3.11+ | Managed by uv |

Optional for notebook testing:

| Dependency | Installation |
|------------|-------------|
| [JupyterLab](https://jupyter.org/) | `uv tool install jupyterlab` |

Verify your local setup:

```bash
pegasus-version          # should print 5.1.2 or later
condor_version           # should print 24.x or 25.x
pegasus-keg --help       # Pegasus synthetic job generator (used by scenarios)
condor_q                 # HTCondor queue (requires running daemons)
```

## Installation

```bash
git clone https://github.com/pegasusai/predictable-workflow-failures.git
cd predictable-workflow-failures
uv sync
```

This installs all runtime dependencies (`click`, `pyyaml`, `nbformat`) and registers the `workflow-generator` CLI.

For development (adds `pytest` and `ruff`):

```bash
uv sync --group dev
```

## Quick Start

```bash
# List all available failure scenarios
uv run workflow-generator list

# Generate a single scenario
uv run workflow-generator generate success -o ./generated

# Generate all 7 scenarios at once
uv run workflow-generator generate-all -o ./generated

# Run the generated workflow script to produce Pegasus catalogs and submit
cd generated/success/
python3 workflow_success.py

# Or plan and submit manually
pegasus-plan --conf pegasus.properties --sites condorpool \
  --output-sites local --dir submit --submit workflow.yml

# Monitor execution with workflow-monitor
uv run workflow-monitor /path/to/generated/success
```

## CLI Reference

### `workflow-generator list`

Display all available failure scenarios with their metadata:

```
$ uv run workflow-generator list
  success                   Clean Success (Baseline)
                            category=none, exit_code=0, state=JOB_SUCCESS

  bad_exit_code_1           Bad Exit Code (1)
                            category=code, exit_code=1, state=JOB_FAILURE

  missing_input             Missing Input File
                            category=data, exit_code=None, state=JOB_FAILURE
  ...
```

### `workflow-generator generate <scenario_id>`

Generate a single scenario. Output goes to `--output-dir/<scenario_id>/`.

```bash
uv run workflow-generator generate bad_exit_code -o ./generated --exit-code 137
uv run workflow-generator generate timeout -o ./generated
uv run workflow-generator generate success -o ./generated --no-notebook
```

| Option | Default | Description |
|--------|---------|-------------|
| `-o, --output-dir` | `./generated` | Root output directory |
| `--exit-code` | `1` | Exit code for the `bad_exit_code` scenario (1, 2, 126, 127, 137, 139) |
| `--notebook / --no-notebook` | `--notebook` | Generate a `.ipynb` alongside the `.py` script |
| `--data-config` | `condorio` | Pegasus data configuration: `condorio`, `sharedfs`, or `nonsharedfs` |

### `workflow-generator generate-all`

Generate all 7 scenarios into separate subdirectories:

```bash
uv run workflow-generator generate-all -o ./generated
```

### `workflow-generator convert <workflow.py>`

Convert any workflow Python script to a Jupyter notebook:

```bash
uv run workflow-generator convert generated/success/workflow_success.py
uv run workflow-generator convert generated/timeout/workflow_timeout.py -o custom_name.ipynb
```

## Failure Scenarios

### `success` — Clean Baseline

A diamond-shaped DAG (preprocess → 2x findrange → analyze) where every job succeeds using `pegasus-keg`. Provides positive examples so that monitoring tools and ML models can learn the boundary between healthy and failing workflows.

- **Category:** none
- **Expected state:** `JOB_SUCCESS`
- **DAG shape:** Diamond (4 jobs)
- **workflow-monitor diagnostics:** No diagnostics panel shown

### `bad_exit_code` — Non-Zero Exit Codes

A 3-job linear pipeline (setup → compute → collect) where `compute` exits with a specific code. Parameterized to generate different exit codes:

| Exit Code | Meaning | Injection |
|-----------|---------|-----------|
| 1 | Generic failure | `exit 1` |
| 2 | Misuse of shell command | `exit 2` |
| 126 | Command not executable (permission denied) | `exit 126` |
| 127 | Command not found | `exit 127` |
| 137 | Killed by SIGKILL (OOM killer) | `kill -KILL $$` |
| 139 | Segmentation fault (SIGSEGV) | `kill -SEGV $$` |

- **Category:** code
- **Expected state:** `JOB_FAILURE`
- **workflow-monitor diagnostics:** Exit-code-based suggestions (e.g., "Killed by SIGKILL — likely OOM, increase request_memory")

### `missing_input` — Data Staging Failure

A diamond DAG where one branch references `phantom_data.txt` — a file deliberately omitted from the Replica Catalog. Pegasus's data staging fails when it cannot locate the physical file.

- **Category:** data
- **Expected state:** `JOB_FAILURE`
- **Injection:** Replica Catalog PFN points to non-existent path
- **workflow-monitor diagnostics:** Transfer input failure pattern detected from HTCondor hold reason

### `memory_exceeded` — OOM / Periodic Hold

A 3-job pipeline where `compute` allocates ~200MB of memory but requests only 50MB. HTCondor's `periodic_hold` expression detects the RSS overshoot and holds the job with a descriptive hold reason.

- **Category:** resource
- **Expected state:** `JOB_HELD`
- **Injection:** Executable allocates memory; `periodic_hold` on `ResidentSetSize`
- **HTCondor profile:** `periodic_hold_reason = "Job exceeded memory limit (RSS > 50MB)"`
- **workflow-monitor diagnostics:** Memory limit pattern matched from hold reason; suggests increasing `request_memory`

### `timeout` — Wall Time Exceeded

A 3-job pipeline where `compute` sleeps for 300 seconds but has a 30-second wall time limit. HTCondor's `periodic_hold` detects the elapsed time and holds the job.

- **Category:** resource
- **Expected state:** `JOB_HELD`
- **Injection:** Executable sleeps; `periodic_hold` on `(CurrentTime - JobCurrentStartDate)`
- **HTCondor profile:** `periodic_hold_reason = "Job exceeded wall time limit (30s)"`
- **workflow-monitor diagnostics:** Wall time limit pattern matched from hold reason; suggests increasing time limit or optimizing job

### `dependency_failure` — Cascading DAG Failure

A fan-out/fan-in DAG (split → branch_ok + branch_bad → merge → finalize) where `branch_bad` crashes, blocking `merge` and `finalize`. Tests the ability to distinguish root cause from collateral damage.

- **Category:** cascade
- **Expected state:** `JOB_FAILURE`
- **Injection:** Executable exits 1; downstream jobs never submitted by DAGMan
- **workflow-monitor diagnostics:** Exit-code diagnostics for `branch_bad`; `merge` and `finalize` show as `UNSUBMITTED`

### `transfer_failure` — Missing Output

A 3-job pipeline where `compute` exits 0 but never creates its declared output file. Pegasus's post-script detects the missing output and fails the job despite the clean exit code. This is a subtle failure — exit codes alone are not sufficient to detect it.

- **Category:** data
- **Expected state:** `JOB_FAILURE`
- **Injection:** Executable skips output creation
- **workflow-monitor diagnostics:** "Output file transfer failed — expected file not created" from HTCondor hold reason

## Generated Output Structure

Each scenario produces the following directory layout:

```
generated/<scenario_id>/
  workflow_<scenario_id>.py      # Self-contained Pegasus workflow script
  workflow_<scenario_id>.ipynb   # Jupyter notebook (if --notebook)
  metadata.json                  # Ground-truth labels
  bin/
    good_job.sh                  # Wrapper around pegasus-keg (succeeds)
    <failure_script>.sh          # Scenario-specific failure executable
```

Running the `.py` script generates additional Pegasus artifacts:

```
generated/<scenario_id>/
  workflow.yml                   # Abstract workflow (Pegasus YAML)
  pegasus.properties             # Pegasus configuration
  input.txt                      # Synthetic input data
  scratch/                       # Shared scratch directory
  output/                        # Final output directory
  submit/                        # Pegasus submit directory
    <user>/pegasus/<wf>/run000N/
      braindump.yml              # Workflow metadata (used by workflow-monitor)
      <dag>.stampede.db          # SQLite database (used by workflow-monitor)
```

## Metadata Format

Each `metadata.json` contains a `WorkflowManifest` with labeled scenario metadata:

```json
{
  "workflow_name": "timeout",
  "scenarios": [
    {
      "scenario_id": "timeout",
      "display_name": "Wall Time Exceeded (Timeout)",
      "failure_category": "resource",
      "expected_exit_code": null,
      "expected_job_state": "JOB_HELD",
      "affected_jobs": ["compute"],
      "description": "A 3-job linear pipeline where ..."
    }
  ],
  "generation_timestamp": "2026-03-12T18:00:00+00:00",
  "generator_version": "0.1.0",
  "total_scenarios": 1,
  "expected_failures": 1,
  "expected_successes": 0
}
```

### Metadata Fields

| Field | Type | Description |
|-------|------|-------------|
| `scenario_id` | string | Unique identifier (e.g., `bad_exit_code_137`) |
| `failure_category` | string | One of: `none`, `data`, `resource`, `code`, `cascade` |
| `expected_exit_code` | int or null | Exit code the failing job should produce |
| `expected_job_state` | string | `JOB_SUCCESS`, `JOB_FAILURE`, or `JOB_HELD` |
| `affected_jobs` | list[string] | Job names that fail or are blocked |

## Using with workflow-monitor

The primary use case for this project is testing [`workflow-monitor`](https://github.com/pegasusai/workflow-monitor) across all failure categories. The monitor provides a real-time Rich TUI dashboard with diagnostics that should correctly identify and surface each failure mode.

### Testing on a local machine (sharedfs)

```bash
# Generate and run a scenario
uv run workflow-generator generate memory_exceeded -o ./generated --data-config sharedfs
cd generated/memory_exceeded && python3 workflow_memory_exceeded.py

# Monitor with the live TUI
uv run workflow-monitor /path/to/generated/memory_exceeded
```

### Testing on a multi-node cluster (condorio)

```bash
# On the submit node: generate and run
uv run workflow-generator generate memory_exceeded -o ./generated --data-config condorio
cd generated/memory_exceeded && python3 workflow_memory_exceeded.py

# Start the server daemon (writes JSONL with HTCondor ClassAd data)
uv run workflow-monitor --serve /path/to/submit/run0001

# From your local machine: monitor via SSH
uv run workflow-monitor \
  --remote user@submit-node:/path/to/submit/run0001/workflow-events.jsonl
```

### Comparing LIVE vs SSH modes

Each scenario should produce equivalent output in both the live TUI (running directly on the submit node) and the SSH client (running locally, fetching JSONL via SSH). Key things to compare:

| Panel | What to check |
|-------|---------------|
| **Workflow Status** | Same state, elapsed time, job counts, progress percentage |
| **Diagnostics** | Same hold reasons, exit codes, and remediation suggestions |
| **Compute Jobs** | Same states, exit codes, durations for all jobs |
| **Auxiliary Jobs** | Same infrastructure job states |
| **Recent Events** | Same sequence of job-state transitions |

### Expected diagnostics by scenario

| Scenario | Diagnostic trigger | Expected pattern |
|----------|-------------------|------------------|
| `success` | *(none)* | No diagnostics panel |
| `bad_exit_code` | Failed job with non-zero exit | Exit code suggestions (OOM, segfault, etc.) |
| `missing_input` | HTCondor hold — transfer input failure | "Input file transfer failed" |
| `memory_exceeded` | HTCondor hold — RSS exceeded | "Job exceeded memory limit" |
| `timeout` | HTCondor hold — wall time exceeded | "Job exceeded wall time limit" |
| `dependency_failure` | Failed job with exit 1 | Exit code 1 suggestions; downstream jobs UNSUBMITTED |
| `transfer_failure` | HTCondor hold — transfer output failure | "Output file transfer failed — expected file not created" |

## Configuration

### Data Configuration Mode

Generated workflows use `--data-config` to control how files are transferred between submit host and workers:

| Mode | Use When | Description |
|------|----------|-------------|
| `condorio` (default) | Multi-node pool, no shared FS | HTCondor handles all file transfers |
| `sharedfs` | Single machine or NFS-backed | Jobs read/write from a common directory |
| `nonsharedfs` | Multi-node, Pegasus-managed | Pegasus handles transfers via `pegasus-transfer` |

```bash
# Multi-node pool (default — works everywhere)
uv run workflow-generator generate success -o ./generated

# Single machine with minicondor
uv run workflow-generator generate success -o ./generated --data-config sharedfs
```

`dagman.retry = 0` is set in all modes to disable automatic retries, producing clean pass/fail signals.

### Site Catalog

Generated workflows define two sites:

- **`local`** — The submit host. Has `SHARED_SCRATCH` and `SHARED_STORAGE` directories under the scenario output directory.
- **`condorpool`** — The execution site. Uses `vanilla` universe with `getenv=True`. In `sharedfs` mode, it declares a `SHARED_SCRATCH` directory; in `condorio`/`nonsharedfs` modes, it relies on HTCondor/Pegasus for file transfer.

### HTCondor Profiles

Scenarios that need HTCondor-level failure injection set profiles directly on the job:

```python
# Memory exceeded: hold when RSS > 50MB
job.add_profiles(Namespace.CONDOR, key="periodic_hold",
    value="(JobStatus == 2) && (ResidentSetSize > 50000)")
job.add_profiles(Namespace.CONDOR, key="periodic_hold_reason",
    value='"Job exceeded memory limit (RSS > 50MB)"')

# Timeout: hold when elapsed time > 30 seconds
job.add_profiles(Namespace.CONDOR, key="periodic_hold",
    value="(JobStatus == 2) && ((CurrentTime - JobCurrentStartDate) > 30)")
```

### Platform Auto-Detection

Generated workflow scripts auto-detect Pegasus and HTCondor installation paths at runtime using `shutil.which()` and filesystem probes. This supports:

- **macOS (Homebrew):** Pegasus Python lib at `/opt/homebrew/opt/pegasus/lib/pegasus/python`
- **Linux (system packages):** Pegasus Python lib in `/usr/lib/python3*/dist-packages`
- **Custom installations:** Override via environment variables

```bash
# Override any auto-detected path
export PEGASUS_HOME=/opt/pegasus
export PEGASUS_PYTHON_LIB=/opt/pegasus/lib/pegasus/python
export CONDOR_HOME=/opt/condor
export CONDOR_CONFIG=/opt/condor/etc/condor_config
```

## Notebook Workflow

Generated `.ipynb` notebooks split the workflow script into interactive cells using `# --- Section: Name ---` comment markers in the source. Each section becomes a markdown heading cell followed by a code cell:

```
[Markdown] Workflow description (from header comments)
[Code]     import statements
[Markdown] ## Properties — explanation
[Code]     props = Properties(); ...
[Markdown] ## Replica Catalog — explanation
[Code]     rc = ReplicaCatalog(); ...
...
```

Open notebooks in JupyterLab for step-by-step execution and inspection:

```bash
jupyter lab generated/success/workflow_success.ipynb
```

## Development

```bash
uv sync --group dev
uv run pytest -v              # run tests
uv run ruff check src/ tests/ # lint
uv run ruff format src/ tests/ # format
```

### Adding a New Scenario

1. Create `src/workflow_generator/scenarios/my_scenario.py`
2. Subclass `FailureScenario` from `scenarios/base.py`
3. Implement `get_metadata()`, `generate_executables()`, and `generate_workflow_script()`
4. Register in `scenarios/__init__.py` by adding to the `SCENARIOS` dict
5. Add a test case in `tests/test_scenarios.py`

The `ScenarioMetadata` must include a `failure_category` from the set `{none, data, resource, code, cascade}`.

## Tested Platforms

| Platform | Pegasus | HTCondor | Data Config | Pool |
|----------|---------|----------|-------------|------|
| macOS arm64 (Homebrew) | 5.1.2 | 25.6.1 | `sharedfs` | Single-node `minicondor` |
| Ubuntu 24.04 x86_64 (FABRIC) | 5.1.2 | 24.12.17 | `condorio` | Multi-node (submit + 2 workers) |

## Limitations

Behavior may differ on untested setups:

- **HTCondor policy expressions** — `periodic_hold` expressions for `memory_exceeded` and `timeout` scenarios depend on ClassAd attributes (`ResidentSetSize`, `JobCurrentStartDate`) that may behave differently under cgroup v1 vs. v2, or when slot partitioning is configured.
- **Signal handling** — Exit codes 137 (SIGKILL) and 139 (SIGSEGV) are generated via `kill -SIG $$` in shell scripts; signal delivery semantics can vary across shells and container runtimes.
- **Failure mode differences across data configs** — Some failure scenarios (e.g., `missing_input`, `transfer_failure`) may manifest differently under `condorio` vs. `sharedfs` because the file staging mechanism changes.

## License

Apache-2.0. See [LICENSE](LICENSE) for details.
