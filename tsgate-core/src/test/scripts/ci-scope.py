#!/usr/bin/env python3
"""Select database CI coverage from GitHub event data and a bounded Git diff."""

import argparse
import json
import os
from pathlib import Path
import re
import runpy
import subprocess

ROOT = Path(__file__).resolve().parents[4]
BACKENDS = ("iotdb", "influxdb1", "influxdb")
MODULES = {
    "tsgate-iotdb": {"iotdb"},
    "tsgate-influxdb1": {"influxdb1", "opengemini"},
    "tsgate-influxdb3": {"influxdb"},
    "tsgate-opengemini": {"opengemini"},
}


def coverage(paths, full=False):
    selected = set(BACKENDS) | {"opengemini"} if full else set()
    for path in paths:
        module = path.split("/", 1)[0]
        if (path in ("pom.xml", "LICENSE", "NOTICE", ".github/workflows/ci.yml")
                or path.startswith("scripts/") or path.endswith("/pom.xml")
                or module in ("tsgate-core", "tsgate-bom") or module.endswith("-spring-boot-starter")):
            selected.update((*BACKENDS, "opengemini"))
        elif module in MODULES:
            selected.update(MODULES[module])
        elif module.startswith("tsgate-"):
            # An unregistered module must not silently bypass database validation.
            selected.update((*BACKENDS, "opengemini"))
    return dict(docker=[backend for backend in BACKENDS if backend in selected],
                opengemini="opengemini" in selected, full=full)


def select(event_name, event, sha, ref, git=None):
    if event_name == "workflow_dispatch" or ref.startswith("refs/tags/v"):
        return coverage([], full=True) | {"reason": "Manual or version-tag validation"}
    base = event.get("pull_request", {}).get("base", {}).get("sha") if event_name == "pull_request" else event.get("before")
    if not all(isinstance(value, str) and re.fullmatch(r"[0-9a-fA-F]{40,64}", value)
               and int(value, 16) != 0 for value in (base, sha)):
        return coverage([], full=True) | {"reason": "Missing or invalid comparison commit; conservative full coverage"}
    git = git or subprocess.run
    try:
        result = git(["git", "-C", str(ROOT), "diff", "--name-only", "-z", base, sha, "--"],
                     check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=45)
        paths = result.stdout.decode("utf-8").split("\0")
        paths = [path for path in paths if path]
    except (OSError, subprocess.SubprocessError, UnicodeError):
        return coverage([], full=True) | {"reason": "Git comparison unavailable; conservative full coverage"}
    return coverage(paths) | {"reason": "Changed build paths", "paths": paths, "base": base, "sha": sha}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-file", type=Path, default=Path(os.environ.get("GITHUB_EVENT_PATH", "/dev/null")))
    parser.add_argument("--event-name", default=os.environ.get("GITHUB_EVENT_NAME", ""))
    parser.add_argument("--sha", default=os.environ.get("GITHUB_SHA", ""))
    parser.add_argument("--ref", default=os.environ.get("GITHUB_REF", ""))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--json", type=Path)
    args = parser.parse_args(argv)
    registration = runpy.run_path(str(Path(__file__).with_name("verify-backend-registration.py")))
    registration["verify"](ROOT)
    try:
        event = json.loads(args.event_file.read_text())
        if not isinstance(event, dict):
            event = {}
    except (OSError, ValueError):
        event = {}
    result = select(args.event_name, event, args.sha, args.ref)
    print(json.dumps(result, indent=2))
    if args.output:
        with args.output.open("a", encoding="utf-8") as output:
            for key in ("docker", "opengemini", "full"):
                output.write(key + "=" + json.dumps(result[key], separators=(",", ":")) + "\n")
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
