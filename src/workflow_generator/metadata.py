"""Workflow manifest — combines scenario metadata for ML training labels.

The manifest is written alongside the generated workflow as metadata.json.
The workflow-monitor project can load this to create supervised training
pairs: (event_sequence, ground_truth_label).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path

from workflow_generator.scenarios.base import ScenarioMetadata


@dataclass
class WorkflowManifest:
    """Top-level metadata for a generated workflow."""

    workflow_name: str
    scenarios: list[ScenarioMetadata] = field(default_factory=list)
    generation_timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    generator_version: str = "0.1.0"

    @property
    def total_scenarios(self) -> int:
        return len(self.scenarios)

    @property
    def expected_failures(self) -> int:
        return sum(1 for s in self.scenarios if s.failure_category != "none")

    @property
    def expected_successes(self) -> int:
        return sum(1 for s in self.scenarios if s.failure_category == "none")

    def to_dict(self) -> dict:
        d = asdict(self)
        d["total_scenarios"] = self.total_scenarios
        d["expected_failures"] = self.expected_failures
        d["expected_successes"] = self.expected_successes
        return d

    def write(self, path: Path) -> None:
        path.write_text(json.dumps(self.to_dict(), indent=2) + "\n")

    @classmethod
    def from_file(cls, path: Path) -> WorkflowManifest:
        data = json.loads(path.read_text())
        scenarios = [ScenarioMetadata(**s) for s in data.get("scenarios", [])]
        return cls(
            workflow_name=data["workflow_name"],
            scenarios=scenarios,
            generation_timestamp=data.get("generation_timestamp", ""),
            generator_version=data.get("generator_version", "0.1.0"),
        )
