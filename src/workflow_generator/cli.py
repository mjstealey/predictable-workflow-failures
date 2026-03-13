"""CLI entry point for workflow-generator.

Commands:
  list        — Show available failure scenarios
  generate    — Generate a single scenario workflow
  generate-all — Generate all scenarios
  convert     — Convert a workflow .py to a Jupyter notebook
"""

from __future__ import annotations

from pathlib import Path

import click

from workflow_generator.scenarios import list_scenarios, get_scenario
from workflow_generator.scenarios.bad_exit_code import BadExitCodeScenario
from workflow_generator.converter import python_to_notebook
from workflow_generator.metadata import WorkflowManifest


@click.group()
@click.version_option(version="0.1.0")
def main():
    """Generate Pegasus WMS workflows with predictable failure modes."""


@main.command("list")
def list_cmd():
    """List all available failure scenarios."""
    scenarios = list_scenarios()
    for scenario_id, cls in scenarios.items():
        instance = cls() if cls is not BadExitCodeScenario else cls()
        meta = instance.get_metadata()
        click.echo(f"  {meta.scenario_id:<25} {meta.display_name}")
        click.echo(f"  {'':25} category={meta.failure_category}, "
                    f"exit_code={meta.expected_exit_code}, "
                    f"state={meta.expected_job_state}")
        click.echo()


@main.command()
@click.argument("scenario_id")
@click.option("--output-dir", "-o", type=click.Path(), default="./generated",
              help="Output directory for generated workflow files.")
@click.option("--exit-code", type=int, default=1,
              help="Exit code for bad_exit_code scenario (default: 1).")
@click.option("--notebook/--no-notebook", default=True,
              help="Also generate a Jupyter notebook (.ipynb) from the workflow script.")
def generate(scenario_id: str, output_dir: str, exit_code: int, notebook: bool):
    """Generate a single failure scenario workflow.

    SCENARIO_ID is one of: success, bad_exit_code, missing_input,
    memory_exceeded, timeout, dependency_failure, transfer_failure.
    """
    out = Path(output_dir) / scenario_id
    out.mkdir(parents=True, exist_ok=True)
    bin_dir = out / "bin"

    # Instantiate scenario
    cls = get_scenario(scenario_id)
    if cls is BadExitCodeScenario:
        instance = cls(exit_code=exit_code)
    else:
        instance = cls()

    meta = instance.get_metadata()
    click.echo(f"Generating scenario: {meta.display_name}")

    # Generate executables
    scripts = instance.generate_executables(bin_dir)
    click.echo(f"  Executables: {len(scripts)} scripts in {bin_dir}/")

    # Generate workflow script
    wf_script = instance.generate_workflow_script(out, bin_dir)
    click.echo(f"  Workflow script: {wf_script}")

    # Write metadata
    manifest = WorkflowManifest(
        workflow_name=meta.scenario_id,
        scenarios=[meta],
    )
    manifest.write(out / "metadata.json")
    click.echo(f"  Metadata: {out}/metadata.json")

    # Convert to notebook
    if notebook:
        nb_path = python_to_notebook(wf_script)
        click.echo(f"  Notebook: {nb_path}")

    click.echo()
    click.echo("Next steps:")
    click.echo(f"  cd {out.resolve()}")
    click.echo(f"  python3 {wf_script.name}  # generates workflow.yml + catalogs")
    click.echo("  pegasus-plan --conf pegasus.properties --sites condorpool \\")
    click.echo("    --output-sites local --dir submit --submit workflow.yml")


@main.command("generate-all")
@click.option("--output-dir", "-o", type=click.Path(), default="./generated",
              help="Output directory for generated workflow files.")
@click.option("--notebook/--no-notebook", default=True,
              help="Also generate Jupyter notebooks.")
def generate_all(output_dir: str, notebook: bool):
    """Generate all failure scenarios into separate subdirectories."""
    scenarios = list_scenarios()
    for scenario_id, cls in scenarios.items():
        out = Path(output_dir) / scenario_id
        out.mkdir(parents=True, exist_ok=True)
        bin_dir = out / "bin"

        instance = cls()
        meta = instance.get_metadata()
        click.echo(f"Generating: {meta.display_name}")

        instance.generate_executables(bin_dir)
        wf_script = instance.generate_workflow_script(out, bin_dir)

        manifest = WorkflowManifest(
            workflow_name=meta.scenario_id,
            scenarios=[meta],
        )
        manifest.write(out / "metadata.json")

        if notebook:
            python_to_notebook(wf_script)

    click.echo(f"\nAll {len(scenarios)} scenarios generated in {output_dir}/")


@main.command()
@click.argument("workflow_py", type=click.Path(exists=True))
@click.option("--output", "-o", type=click.Path(), default=None,
              help="Output .ipynb path (default: same name with .ipynb extension).")
def convert(workflow_py: str, output: str | None):
    """Convert a workflow Python script to a Jupyter notebook."""
    source = Path(workflow_py)
    out = Path(output) if output else None
    nb_path = python_to_notebook(source, out)
    click.echo(f"Notebook written to {nb_path}")


if __name__ == "__main__":
    main()
