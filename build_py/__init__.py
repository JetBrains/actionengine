"""
A fully custom PEP 517 build backend that manually builds a .whl file
without setuptools or other build backends.
"""

import sys

if sys.version_info < (3, 11):
    import tomli as tomllib
else:
    import tomllib

import base64
import hashlib
import os
import platform
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

NAME = "actionengine"
NAME_WITH_HYPHEN = "action-engine"
REPO_ROOT = Path(__file__).parent.parent.resolve()
BUILD_TYPE = "Debug"

CLANG_RESOURCES = """- https://clang.llvm.org/get_started.html
- https://launchpad.net/ubuntu/noble/+package/clang-19
- https://formulae.brew.sh/formula/llvm"""


def get_arch_tag():
    """Return the architecture tag for this build."""
    machine = platform.machine().lower()
    if "arm" in machine or "aarch64" in machine:
        if sys.platform.startswith("linux"):
            return "aarch64"
        elif sys.platform == "darwin":
            return "arm64"
        return "arm64"

    return "x86_64"


def get_platform_tag():
    """Return the platform tag for this build."""
    arch = get_arch_tag()

    if sys.platform.startswith("linux"):
        return f"manylinux_2_17_{arch}"
    elif sys.platform == "darwin":
        return f"macosx_15_0_{arch}"
    elif sys.platform == "win32":
        return "win_amd64" if arch == "x86_64" else "win_arm64"
    else:
        raise RuntimeError(f"Unsupported platform: {sys.platform}")


def get_tag():
    """Return the wheel tag for this build."""
    py_version = f"cp{sys.version_info.major}{sys.version_info.minor}"
    abi_tag = "abi3"
    return f"{py_version}-{abi_tag}-{get_platform_tag()}"


def get_requires_for_build_wheel(config_settings=None):
    return []


def _run_build_process(is_editable=False):
    """Common logic for compiling C++ extensions and generating stubs."""
    print(
        f">>> Running build process (editable={is_editable}, type={BUILD_TYPE})"
    )

    path_env = os.environ.get("PATH")
    if path_env is None:
        raise RuntimeError("PATH is not set.")

    sep = ";" if sys.platform == "win32" else ":"
    path_dirs = path_env.split(sep)

    exe_suffix = ".exe" if sys.platform == "win32" else ""
    cc = os.environ.get("CC")
    cxx = os.environ.get("CXX")
    if cc is None or cxx is None:
        for directory in path_dirs:
            if cc is not None and cxx is not None:
                break

            directory = Path(directory)
            cc_path = directory / f"clang{exe_suffix}"
            cxx_path = directory / f"clang++{exe_suffix}"
            if cc_path.exists() and cxx_path.exists():
                cc = str(cc_path)
                cxx = str(cxx_path)
                break

    if cc is None:
        raise RuntimeError(
            "Could not find 'clang' compiler in PATH. "
            "Please install a clang build appropriate for your platform, "
            "or set the CC environment variable if you are sure you have "
            "one installed. You may find these resources helpful:\n"
            f"{CLANG_RESOURCES}"
        )

    if cxx is None:
        raise RuntimeError(
            "Could not find 'clang++' compiler in PATH. "
            "Please install a clang build appropriate for your platform, "
            "or set the CXX environment variable if you are sure you have "
            "one installed. You may find these resources helpful:\n"
            f"{CLANG_RESOURCES}"
        )

    os.environ["CC"] = cc
    os.environ["CXX"] = cxx

    if os.environ.get("ACTIONENGINE_KEEP_BUILD_DIR", None) is None:
        shutil.rmtree(REPO_ROOT / "build", ignore_errors=True)

    print("Cleaning previous builds...")
    for so_file in (REPO_ROOT / "py" / "actionengine").glob("*.so"):
        print(f"Removing {so_file}")
        os.remove(so_file)

    stub_path = REPO_ROOT / "py" / "actionengine" / "_C"
    if stub_path.exists():
        shutil.rmtree(stub_path)

    os.chdir(REPO_ROOT)
    # Build the C++ extensions:
    env = os.environ.copy()
    env["CMAKE_BUILD_TYPE"] = BUILD_TYPE
    if (
        subprocess.Popen(
            [str(REPO_ROOT / "scripts" / "configure.sh")],
            env=env,
        )
    ).wait() != 0:
        raise RuntimeError("Build failed during configure step.")

    if (
        subprocess.Popen(
            [
                str(REPO_ROOT / "scripts" / "build_python.sh"),
                "--only-rebuild-pybind11",
            ],
            env=env,
        ).wait()
        != 0
    ):
        raise RuntimeError("Build failed during cmake build step.")


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())

    print(">>> Custom build_wheel(): building a pure wheel manually")

    _run_build_process(is_editable=False)

    dependencies = project.get("project", {}).get("dependencies", [])
    requires_dist = "\nRequires-Dist: ".join(dependencies)

    # Create a temporary directory for our wheel contents
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)

        # Copy package files into the temp directory
        pkg_target = temp_dir / NAME
        shutil.copytree((REPO_ROOT / "py" / "actionengine"), pkg_target)

        # make stubs: generate .pyi files from compiled .so files in current
        # environment (need to import the compiled extensions)
        pythonpath = os.environ.get("PYTHONPATH", "")
        pythonpaths = pythonpath.split(os.pathsep) if pythonpath else []
        pythonpaths = [pkg_target.parent] + pythonpaths
        env = os.environ.copy()
        env["PYTHONPATH"] = os.pathsep.join([str(p) for p in pythonpaths])

        if (
            subprocess.Popen(
                [
                    str(REPO_ROOT / "scripts" / "generate_stubs.sh"),
                    str(pkg_target.parent),
                ],
                env=env,
            ).wait()
            != 0
        ):
            raise RuntimeError("Build failed during stub generation step.")

        for cache_dir in pkg_target.rglob("__pycache__"):
            shutil.rmtree(cache_dir)

        version = project.get("project", {}).get("version")
        if version is None:
            raise RuntimeError("Version not found in pyproject.toml")

        dist_info = (
            temp_dir
            / f"{NAME_WITH_HYPHEN.replace('-', '_')}-{version}.dist-info"
        )
        dist_info.mkdir()

        project_scripts = project.get("project", {}).get("scripts", {})
        script_lines = []
        for script_name, entry_point in project_scripts.items():
            script_lines.append(f"{script_name}={entry_point}\n")

        # Create entry_points.txt for console_scripts
        (dist_info / "entry_points.txt").write_text(
            "[console_scripts]\n" + "".join(script_lines)
        )

        # Generate METADATA
        (dist_info / "METADATA").write_text(f"""Metadata-Version: 2.1
Name: {NAME_WITH_HYPHEN}
Version: {version}
Requires-Dist: {requires_dist}
""")

        # Generate WHEEL file
        (dist_info / "WHEEL").write_text(f"""Wheel-Version: 1.0
Generator: {NAME_WITH_HYPHEN}
Root-Is-Purelib: false
Tag: {get_tag()}
Entry-Points: console_scripts
""")

        # Write RECORD file (will list all files)
        record_lines = []
        for file_path in temp_dir.rglob("*"):
            if not file_path.is_file():
                continue
            with open(file_path, "rb") as f:
                data = f.read()

            hash_digest = hashlib.sha256(data).digest()
            hash_b64 = base64.urlsafe_b64encode(hash_digest).rstrip(b"=")
            hash_str = f"sha256={hash_b64.decode('utf-8')}"
            size = len(data)

            rel = file_path.relative_to(temp_dir)
            record_lines.append(f"{rel},{hash_str},{size}\n")
        record_lines.append(f"{dist_info.relative_to(temp_dir) / 'RECORD'},,\n")

        (dist_info / "RECORD").write_text("".join(record_lines))

        # Build wheel filename
        wheel_name = (
            f"{NAME_WITH_HYPHEN.replace('-', '_')}-{version}-{get_tag()}.whl"
        )
        wheel_path = Path(wheel_directory) / wheel_name

        # Zip it up
        with zipfile.ZipFile(wheel_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_path in temp_dir.rglob("*"):
                if file_path.is_file():
                    zf.write(file_path, file_path.relative_to(temp_dir))

        print(f"✅ Built wheel: {wheel_path}")
        return wheel_name


def build_editable(
    wheel_directory, config_settings=None, metadata_directory=None
):
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())

    print(">>> Custom build_editable(): building an editable wheel")

    # Reuse the same compilation logic
    _run_build_process(is_editable=True)

    # Generate stubs in the source tree
    if (
        subprocess.Popen(
            [
                str(REPO_ROOT / "scripts" / "generate_stubs.sh"),
                str(REPO_ROOT / "py"),
            ],
            env=os.environ,
        ).wait()
        != 0
    ):
        raise RuntimeError("Build failed during stub generation step.")

    dependencies = project.get("project", {}).get("dependencies", [])
    requires_dist = "\nRequires-Dist: ".join(dependencies)

    version = project.get("project", {}).get("version")
    if version is None:
        raise RuntimeError("Version not found in pyproject.toml")

    # Create a temporary directory for our editable wheel contents
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)

        dist_info = (
            temp_dir
            / f"{NAME_WITH_HYPHEN.replace('-', '_')}-{version}.dist-info"
        )
        dist_info.mkdir()

        # Create .pth file pointing to the 'py' directory
        (temp_dir / f"{NAME}.pth").write_text(str(REPO_ROOT / "py"))

        project_scripts = project.get("project", {}).get("scripts", {})
        script_lines = []
        for script_name, entry_point in project_scripts.items():
            script_lines.append(f"{script_name}={entry_point}\n")

        # Create entry_points.txt for console_scripts
        (dist_info / "entry_points.txt").write_text(
            "[console_scripts]\n" + "".join(script_lines)
        )

        # Generate METADATA
        (dist_info / "METADATA").write_text(
            f"Metadata-Version: 2.1\n"
            f"Name: {NAME_WITH_HYPHEN}\n"
            f"Version: {version}\n"
            f"Requires-Dist: {requires_dist}\n"
        )

        # Generate WHEEL file
        (dist_info / "WHEEL").write_text(
            f"Wheel-Version: 1.0\n"
            f"Generator: {NAME_WITH_HYPHEN}\n"
            f"Root-Is-Purelib: false\n"
            f"Tag: {get_tag()}\n"
            f"Entry-Points: console_scripts\n"
        )

        # Write RECORD file
        record_lines = []
        for file_path in temp_dir.rglob("*"):
            if not file_path.is_file():
                continue
            with open(file_path, "rb") as f:
                data = f.read()

            hash_digest = hashlib.sha256(data).digest()
            hash_b64 = base64.urlsafe_b64encode(hash_digest).rstrip(b"=")
            hash_str = f"sha256={hash_b64.decode('utf-8')}"
            size = len(data)

            rel = file_path.relative_to(temp_dir)
            record_lines.append(f"{rel},{hash_str},{size}\n")
        record_lines.append(f"{dist_info.relative_to(temp_dir) / 'RECORD'},,\n")

        (dist_info / "RECORD").write_text("".join(record_lines))

        # Build wheel filename
        wheel_name = (
            f"{NAME_WITH_HYPHEN.replace('-', '_')}-{version}-{get_tag()}.whl"
        )
        wheel_path = Path(wheel_directory) / wheel_name

        # Zip it up
        with zipfile.ZipFile(wheel_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_path in temp_dir.rglob("*"):
                if file_path.is_file():
                    zf.write(file_path, file_path.relative_to(temp_dir))

        print(f"✅ Built editable wheel: {wheel_path}")
        return wheel_name


def build_sdist(sdist_directory, config_settings=None):
    raise RuntimeError("SDist build is not supported in this custom backend.")


def get_requires_for_build_editable(config_settings=None):
    return []


def prepare_metadata_for_build_editable(
    metadata_directory, config_settings=None
):
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    version = project.get("project", {}).get("version")
    dist_info = (
        Path(metadata_directory)
        / f"{NAME_WITH_HYPHEN.replace('-', '_')}-{version}.dist-info"
    )
    dist_info.mkdir(exist_ok=True)

    dependencies = project.get("project", {}).get("dependencies", [])
    requires_dist = "\nRequires-Dist: ".join(dependencies)

    (dist_info / "METADATA").write_text(
        f"Metadata-Version: 2.1\n"
        f"Name: {NAME_WITH_HYPHEN}\n"
        f"Version: {version}\n"
        f"Requires-Dist: {requires_dist}\n"
    )

    return dist_info.name
