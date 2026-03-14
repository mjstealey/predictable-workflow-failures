"""Failure scenario registry.

Each scenario generates a Pegasus workflow that fails in a specific,
predictable way, producing labeled training data for workflow-monitor.
"""

from workflow_generator.scenarios.base import (
    FailureScenario,
    ScenarioMetadata as ScenarioMetadata,
    generate_config_block as generate_config_block,
    generate_properties_block as generate_properties_block,
    generate_site_catalog_block as generate_site_catalog_block,
)
from workflow_generator.scenarios.success import SuccessScenario
from workflow_generator.scenarios.bad_exit_code import BadExitCodeScenario
from workflow_generator.scenarios.missing_input import MissingInputScenario
from workflow_generator.scenarios.memory_exceeded import MemoryExceededScenario
from workflow_generator.scenarios.timeout import TimeoutScenario
from workflow_generator.scenarios.dependency_failure import DependencyFailureScenario
from workflow_generator.scenarios.transfer_failure import TransferFailureScenario

# Registry mapping scenario_id -> scenario class
SCENARIOS: dict[str, type[FailureScenario]] = {
    "success": SuccessScenario,
    "bad_exit_code": BadExitCodeScenario,
    "missing_input": MissingInputScenario,
    "memory_exceeded": MemoryExceededScenario,
    "timeout": TimeoutScenario,
    "dependency_failure": DependencyFailureScenario,
    "transfer_failure": TransferFailureScenario,
}


def list_scenarios() -> dict[str, type[FailureScenario]]:
    return SCENARIOS


def get_scenario(scenario_id: str) -> type[FailureScenario]:
    if scenario_id not in SCENARIOS:
        raise KeyError(f"Unknown scenario: {scenario_id}. Available: {list(SCENARIOS.keys())}")
    return SCENARIOS[scenario_id]
