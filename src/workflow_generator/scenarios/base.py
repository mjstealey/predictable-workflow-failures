"""Abstract base class for failure scenarios."""

from __future__ import annotations

import abc
from dataclasses import dataclass, field, asdict
from pathlib import Path
import json
import textwrap


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


def generate_config_block(work_dir: str) -> str:
    """Generate the Configuration section for workflow scripts.

    Returns Python source code that auto-detects Pegasus and HTCondor
    installation paths at runtime, supporting macOS (Homebrew) and
    Linux (system packages like Debian/Ubuntu).

    Paths can be overridden via environment variables:
      PEGASUS_HOME, PEGASUS_PYTHON_LIB, CONDOR_HOME, CONDOR_CONFIG
    """
    return textwrap.dedent(f"""\
        # --- Section: Configuration ---
        # Auto-detect Pegasus and HTCondor installation paths at runtime.
        # To override, set environment variables before running this script:
        #   PEGASUS_HOME, PEGASUS_PYTHON_LIB, CONDOR_HOME, CONDOR_CONFIG
        import platform as _platform
        import shutil as _shutil
        import glob as _glob

        _system = _platform.system()

        # Detect PEGASUS_HOME from pegasus-version binary location
        if os.environ.get("PEGASUS_HOME"):
            PEGASUS_HOME = os.environ["PEGASUS_HOME"]
        else:
            _pbin = _shutil.which("pegasus-version")
            PEGASUS_HOME = str(Path(_pbin).resolve().parent.parent) if _pbin else (
                "/opt/homebrew" if _system == "Darwin" else "/usr"
            )

        # Detect Pegasus Python library path
        if os.environ.get("PEGASUS_PYTHON_LIB"):
            PEGASUS_PYTHON_LIB = os.environ["PEGASUS_PYTHON_LIB"]
        else:
            PEGASUS_PYTHON_LIB = ""
            _lib_candidates = [
                Path(PEGASUS_HOME) / "opt" / "pegasus" / "lib" / "pegasus" / "python",
                Path(PEGASUS_HOME) / "lib" / "pegasus" / "python",
            ]
            for _dist in sorted(_glob.glob("/usr/lib/python3*/dist-packages")):
                _lib_candidates.append(Path(_dist))
            for _lp in _lib_candidates:
                if (_lp / "Pegasus" / "__init__.py").exists():
                    PEGASUS_PYTHON_LIB = str(_lp)
                    break

        # Detect CONDOR_HOME from condor_version binary location
        if os.environ.get("CONDOR_HOME"):
            CONDOR_HOME = os.environ["CONDOR_HOME"]
        else:
            _cbin = _shutil.which("condor_version")
            CONDOR_HOME = str(Path(_cbin).resolve().parent.parent) if _cbin else ""

        # Detect CONDOR_CONFIG
        if os.environ.get("CONDOR_CONFIG"):
            CONDOR_CONFIG = os.environ["CONDOR_CONFIG"]
        else:
            CONDOR_CONFIG = ""
            _cfg_candidates = []
            if CONDOR_HOME:
                _cfg_candidates.append(Path(CONDOR_HOME) / "etc" / "condor_config")
            _cfg_candidates.append(Path("/etc/condor/condor_config"))
            for _cfg in _cfg_candidates:
                if _cfg.exists():
                    CONDOR_CONFIG = str(_cfg)
                    break

        # Derived paths — no need to edit these
        WORK_DIR = Path("{work_dir}")
        BIN_DIR = WORK_DIR / "bin"
        SCRATCH_DIR = WORK_DIR / "scratch"
        OUTPUT_DIR = WORK_DIR / "output"
        SUBMIT_DIR = WORK_DIR / "submit"

        # Set up environment for Pegasus and HTCondor tools.
        # Jupyter kernels may not inherit shell PATH or CONDOR_CONFIG.
        if PEGASUS_PYTHON_LIB:
            sys.path.insert(0, PEGASUS_PYTHON_LIB)
        if CONDOR_CONFIG:
            os.environ["CONDOR_CONFIG"] = CONDOR_CONFIG
        _extra = []
        if CONDOR_HOME:
            _extra.append(os.path.join(CONDOR_HOME, "bin"))
        if PEGASUS_HOME:
            _extra.append(os.path.join(PEGASUS_HOME, "bin"))
        os.environ["PATH"] = os.pathsep.join(_extra + [os.environ.get("PATH", "")])
        os.chdir(WORK_DIR)

        from Pegasus.api import *
    """)


def generate_properties_block(data_config: str = "condorio") -> str:
    """Generate the Properties section for workflow scripts."""
    return textwrap.dedent(f"""\
        # --- Section: Properties ---
        # Configure Pegasus execution mode.
        # sharedfs: submit + worker share a filesystem (single machine / NFS)
        # condorio: HTCondor handles all file transfers (multi-node, most portable)
        # nonsharedfs: Pegasus handles transfers via pegasus-transfer
        props = Properties()
        props["pegasus.data.configuration"] = "{data_config}"
        props["pegasus.monitord.encoding"] = "json"
        props["dagman.retry"] = "0"  # No retries — we want clean pass/fail signals
        props.write(str(WORK_DIR / "pegasus.properties"))
    """)


def generate_site_catalog_block(data_config: str = "condorio") -> str:
    """Generate the Site Catalog section for workflow scripts.

    For sharedfs: both local and condorpool declare SHARED_SCRATCH.
    For condorio/nonsharedfs: condorpool gets no scratch directory since
    HTCondor or Pegasus handles transfers.
    """
    if data_config == "sharedfs":
        return textwrap.dedent("""\
            # --- Section: Site Catalog ---
            # Define local + condorpool sites for shared-filesystem execution.
            scratch_path = str(SCRATCH_DIR)
            output_path = str(OUTPUT_DIR)
            scratch_url = f"file://{scratch_path}"
            output_url = f"file://{output_path}"

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
        """)
    else:
        # condorio or nonsharedfs — no shared scratch on condorpool
        return textwrap.dedent("""\
            # --- Section: Site Catalog ---
            # Define local + condorpool sites. HTCondor handles file transfers
            # (condorio mode) so condorpool needs no shared scratch directory.
            scratch_path = str(SCRATCH_DIR)
            output_path = str(OUTPUT_DIR)
            scratch_url = f"file://{scratch_path}"
            output_url = f"file://{output_path}"

            local_site = Site("local")
            local_site.add_directories(
                Directory(Directory.SHARED_SCRATCH, scratch_path)
                    .add_file_servers(FileServer(scratch_url, Operation.ALL)),
                Directory(Directory.SHARED_STORAGE, output_path)
                    .add_file_servers(FileServer(output_url, Operation.ALL)),
            )

            condorpool = Site("condorpool")
            condorpool.add_profiles(Namespace.CONDOR, key="universe", value="vanilla")
            condorpool.add_profiles(Namespace.CONDOR, key="getenv", value="True")
            condorpool.add_profiles(Namespace.PEGASUS, key="style", value="condor")
            condorpool.add_profiles(Namespace.ENV, key="PEGASUS_HOME", value=PEGASUS_HOME)
            condorpool.add_profiles(Namespace.ENV, key="CONDOR_HOME", value=CONDOR_HOME)

            sc = SiteCatalog()
            sc.add_sites(local_site, condorpool)
        """)


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
    def generate_workflow_script(
        self, output_dir: Path, bin_dir: Path, data_config: str = "condorio",
    ) -> Path:
        """Generate a self-contained Python workflow script.

        The script uses Pegasus.api to define the workflow, write catalogs,
        and optionally plan+submit. Returns path to the .py file.

        Args:
            data_config: Pegasus data configuration mode. One of:
                - "condorio": HTCondor handles transfers (most portable)
                - "sharedfs": shared filesystem between submit + worker
                - "nonsharedfs": Pegasus handles transfers
        """
