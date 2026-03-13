"""Tests for failure scenario generation."""


import pytest

from workflow_generator.scenarios import get_scenario, SCENARIOS
from workflow_generator.scenarios.bad_exit_code import BadExitCodeScenario


def test_registry_has_all_scenarios():
    assert len(SCENARIOS) == 7
    assert "success" in SCENARIOS
    assert "bad_exit_code" in SCENARIOS
    assert "missing_input" in SCENARIOS
    assert "memory_exceeded" in SCENARIOS
    assert "timeout" in SCENARIOS
    assert "dependency_failure" in SCENARIOS
    assert "transfer_failure" in SCENARIOS


def test_get_unknown_scenario():
    with pytest.raises(KeyError, match="Unknown scenario"):
        get_scenario("nonexistent")


def test_all_scenarios_have_metadata():
    for scenario_id, cls in SCENARIOS.items():
        instance = cls()
        meta = instance.get_metadata()
        assert meta.scenario_id
        assert meta.display_name
        assert meta.failure_category in ("none", "data", "resource", "code", "cascade")
        assert meta.expected_job_state in ("JOB_SUCCESS", "JOB_FAILURE", "JOB_HELD")
        assert meta.description


def test_all_scenarios_generate_executables(tmp_path):
    for scenario_id, cls in SCENARIOS.items():
        instance = cls()
        bin_dir = tmp_path / scenario_id / "bin"
        scripts = instance.generate_executables(bin_dir)
        assert len(scripts) >= 1
        for script in scripts:
            assert script.exists()
            assert script.stat().st_mode & 0o100  # executable bit


def test_all_scenarios_generate_workflow_script(tmp_path):
    for scenario_id, cls in SCENARIOS.items():
        instance = cls()
        out_dir = tmp_path / scenario_id
        out_dir.mkdir()
        bin_dir = out_dir / "bin"
        instance.generate_executables(bin_dir)
        wf_script = instance.generate_workflow_script(out_dir, bin_dir)
        assert wf_script.exists()
        assert wf_script.suffix == ".py"
        content = wf_script.read_text()
        assert "Pegasus.api" in content
        assert "Workflow" in content


def test_bad_exit_code_parameterized(tmp_path):
    for code in [1, 2, 126, 127, 137, 139]:
        instance = BadExitCodeScenario(exit_code=code)
        meta = instance.get_metadata()
        assert meta.scenario_id == f"bad_exit_code_{code}"
        assert meta.expected_exit_code == code

        bin_dir = tmp_path / f"exit_{code}" / "bin"
        scripts = instance.generate_executables(bin_dir)
        bad_script = [s for s in scripts if "bad_exit" in s.name][0]
        content = bad_script.read_text()
        assert str(code) in content


def test_success_scenario_metadata():
    instance = SCENARIOS["success"]()
    meta = instance.get_metadata()
    assert meta.failure_category == "none"
    assert meta.expected_exit_code == 0
    assert meta.expected_job_state == "JOB_SUCCESS"
    assert meta.affected_jobs == []
