"""Abstract base class for failure scenarios."""

from __future__ import annotations

import abc
from dataclasses import dataclass, field, asdict
from pathlib import Path
import json


@dataclass
class ScenarioMetadata:
    """Metadata describing a failure scenario for ML labeling.

    The workflow-monitor project uses these fields to build supervised
    training pairs: (event_sequence, ground_truth_label).
    """

    scenario_id: str
    display_name: str
    failure_category: str  # "none", "data", "resource", "code", "infra", "cascade"
    expected_exit_code: int | None
    expected_job_state: str  # "JOB_SUCCESS", "JOB_FAILURE", "JOB_HELD"
    affected_jobs: list[str] = field(default_factory=list)
    description: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    def write(self, path: Path) -> None:
        path.write_text(json.dumps(self.to_dict(), indent=2) + "\n")


class FailureScenario(abc.ABC):
    """Base class that every scenario must implement."""

    @abc.abstractmethod
    def get_metadata(self) -> ScenarioMetadata:
        """Return metadata describing this failure scenario."""

    @abc.abstractmethod
    def generate_executables(self, bin_dir: Path) -> list[Path]:
        """Create shell scripts in bin_dir that jobs will run.

        Returns paths to the created scripts.
        """

    @abc.abstractmethod
    def generate_workflow_script(self, output_dir: Path, bin_dir: Path) -> Path:
        """Generate a self-contained Python workflow script.

        The script uses Pegasus.api to define the workflow, write catalogs,
        and optionally plan+submit. Returns path to the .py file.
        """
