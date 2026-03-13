# workflow-generator

Generate [Pegasus WMS](https://pegasus.isi.edu/) workflows with predictable failure modes for ML training data. Part of the **pegasusai** project — the sibling [`workflow-monitor`](../workflow-monitor) consumes the generated workflows and their labeled metadata to train failure-detection models.

Each scenario produces:

- A self-contained Python workflow script using `Pegasus.api`
- Shell executables that simulate specific failure behaviors
- A `metadata.json` sidecar with ground-truth labels for supervised learning
- An optional Jupyter notebook (`.ipynb`) for interactive testing in JupyterLab

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

# Run the generated workflow script to produce Pegasus catalogs
cd generated/success/
python3 workflow_success.py

# Plan and submit to local HTCondor
pegasus-plan --conf pegasus.properties --sites condorpool \
  --output-sites local --dir submit --submit workflow.yml

# Monitor execution
pegasus-status submit/<run-dir>
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

A diamond-shaped DAG (preprocess -> 2x findrange -> analyze) where every job succeeds using `pegasus-keg`. Provides positive training examples so the ML model learns the boundary between healthy and failing workflows.

- **Category:** none
- **Expected state:** `JOB_SUCCESS`
- **DAG shape:** Diamond (4 jobs)

### `bad_exit_code` — Non-Zero Exit Codes

A 3-job linear pipeline (setup -> compute -> collect) where `compute` exits with a specific code. Parameterized to generate different exit codes:

| Exit Code | Meaning |
|-----------|---------|
| 1 | Generic failure |
| 2 | Misuse of shell command |
| 126 | Command not executable (permission denied) |
| 127 | Command not found |
| 137 | Killed by SIGKILL (OOM killer) |
| 139 | Segmentation fault (SIGSEGV) |

- **Category:** code
- **Expected state:** `JOB_FAILURE`
- **Injection:** Executable runs `exit N` or `kill -SIG $$`

### `missing_input` — Data Staging Failure

A diamond DAG where one branch references `phantom_data.txt` — a file deliberately omitted from the Replica Catalog. Pegasus's data staging fails when it cannot locate the physical file.

- **Category:** data
- **Expected state:** `JOB_FAILURE`
- **Injection:** Replica Catalog (PFN entry omitted)

### `memory_exceeded` — OOM / Periodic Hold

A 3-job pipeline where `compute` allocates ~200MB of memory but requests only 50MB. HTCondor's `periodic_hold` expression detects the RSS overshoot and holds the job.

- **Category:** resource
- **Expected state:** `JOB_HELD`
- **Injection:** Executable allocates memory; `periodic_hold` on `ResidentSetSize`

### `timeout` — Wall Time Exceeded

A 3-job pipeline where `compute` sleeps for 300 seconds but has a 30-second wall time limit. HTCondor's `periodic_hold` detects the elapsed time and holds the job.

- **Category:** resource
- **Expected state:** `JOB_HELD`
- **Injection:** Executable sleeps; `periodic_hold` on `(CurrentTime - JobCurrentStartDate)`

### `dependency_failure` — Cascading DAG Failure

A fan-out/fan-in DAG (split -> branch_ok + branch_bad -> merge -> finalize) where `branch_bad` crashes, blocking `merge` and `finalize`. Tests the model's ability to distinguish root cause from collateral damage.

- **Category:** cascade
- **Expected state:** `JOB_FAILURE`
- **Injection:** Executable exits 1; downstream jobs never submitted by DAGMan

### `transfer_failure` — Missing Output

A 3-job pipeline where `compute` exits 0 but never creates its declared output file. Pegasus's post-script detects the missing output and fails the job despite the clean exit code. This is a subtle failure — the model must learn to look beyond exit codes.

- **Category:** data
- **Expected state:** `JOB_FAILURE`
- **Injection:** Executable skips output creation

## Generated Output Structure

Each scenario produces the following directory layout:

```
generated/<scenario_id>/
  workflow_<scenario_id>.py      # Self-contained Pegasus workflow script
  workflow_<scenario_id>.ipynb   # Jupyter notebook (if --notebook)
  metadata.json                  # Ground-truth labels for ML training
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

## Configuration

### Data Configuration Mode

All generated workflows use `sharedfs` (shared filesystem) mode, appropriate for single-machine or NFS-backed setups. This is set in the generated `pegasus.properties`:

```properties
pegasus.data.configuration = sharedfs
pegasus.monitord.encoding = json
dagman.retry = 0
```

`dagman.retry = 0` disables automatic retries so that failures produce clean, unambiguous signals for ML training.

### Site Catalog

Generated workflows define two sites:

- **`local`** — The submit host. Has `SHARED_SCRATCH` and `SHARED_STORAGE` directories under the scenario output directory.
- **`condorpool`** — The execution site. Uses `vanilla` universe with `getenv=True` so jobs inherit the submitter's environment (including `PATH` to `pegasus-keg`).

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

### Pegasus Python API Path

The Pegasus Python modules are not pip-installed — they live under the Homebrew prefix. Generated workflow scripts add the path at runtime:

```python
import sys
sys.path.insert(0, "/opt/homebrew/opt/pegasus/lib/pegasus/python")
from Pegasus.api import *
```

If your Pegasus installation is elsewhere, update this path in the generated scripts or set `PYTHONPATH`:

```bash
export PYTHONPATH=/path/to/pegasus/lib/pegasus/python:$PYTHONPATH
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

The `ScenarioMetadata` must include a `failure_category` from the set `{none, data, resource, code, cascade}` so that `workflow-monitor` can use it as a classification label.

## Limitations

This project has been developed and tested on a single-host setup: **macOS arm64** running Pegasus 5.1.2 (Homebrew) and HTCondor 25.6.1 with a `minicondor` personal pool. All generated workflows use `sharedfs` data configuration and target a local `condorpool` execution site.

Behavior may differ on other setups:

- **Linux clusters / HPC** — Shared filesystem paths, scratch directories, and `getenv=True` assumptions may not apply. Multi-node pools with separate submit and execute hosts will need site catalog adjustments.
- **Non-shared filesystems** — Workflows assume `sharedfs` mode. Deployments using `condorio` or `nonsharedfs` require changes to the data configuration and may alter how staging failures (e.g., `missing_input`) manifest.
- **Pegasus Python path** — Generated scripts hardcode `/opt/homebrew/opt/pegasus/lib/pegasus/python` for the Pegasus API. Other installations (pip, RPM, tarball) will need this path updated or `PYTHONPATH` set.
- **HTCondor policy expressions** — `periodic_hold` expressions for `memory_exceeded` and `timeout` scenarios depend on ClassAd attributes (`ResidentSetSize`, `JobCurrentStartDate`) that may behave differently under cgroup v1 vs. v2, or when slot partitioning is configured.
- **Signal handling** — Exit codes 137 (SIGKILL) and 139 (SIGSEGV) are generated via `kill -SIG $$` in shell scripts; signal delivery semantics can vary across shells and container runtimes.
