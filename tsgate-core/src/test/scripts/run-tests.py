#!/usr/bin/env python3
"""Run the same portable Maven checks locally and in GitHub Actions."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.request
import uuid
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[4]
SERVERS = Path(__file__).resolve().parent.parent / "resources/ci/docker-servers.json"


def capture(command, timeout=45):
    return subprocess.run(command, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=timeout, check=True).stdout


def logged(command, output, timeout=1500):
    """Kill Maven and its forked JVMs if a bounded test run is interrupted."""
    print("RUN:", " ".join(command), flush=True)
    with output.open("w") as log:
        process = subprocess.Popen(command, cwd=ROOT, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            return process.wait(timeout=timeout)
        except BaseException:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
            raise


def reports(output, report_type):
    totals = dict(tests=0, failures=0, errors=0, skipped=0)
    suites = []
    for report in sorted(ROOT.glob("*/target/" + report_type + "-reports/TEST-*.xml")):
        module = report.parents[2].name
        destination = output / "reports" / module / (report_type + "-reports")
        destination.mkdir(parents=True, exist_ok=True)
        shutil.copy2(report, destination / report.name)
        xml = ET.parse(report).getroot()
        suite = {key: int(xml.get(key, 0)) for key in totals}
        for key in totals:
            totals[key] += suite[key]
        suites.append(dict(module=module, name=xml.get("name"), **suite))
    return dict(totals=totals, suites=suites)


def source_manifest():
    """Identify the exact build inputs, including uncommitted local changes."""
    paths = {ROOT / path for path in ("pom.xml", "LICENSE", "NOTICE", ".github/workflows/ci.yml",
                                    "scripts/verify-release-artifacts.py")}
    paths.update(ROOT.glob("tsgate-*/pom.xml"))
    for module in ROOT.glob("tsgate-*"):
        paths.update(path for path in (module / "src").rglob("*") if path.is_file())
    return {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(paths) if path.is_file()}


def iotdb_cli(server, sql):
    return ["docker", "exec", server["name"], "/iotdb/sbin/start-cli.sh",
            "-h", "127.0.0.1", "-p", "6667", "-u", "root", "-pw", "root",
            "-sql_dialect", "table", "-e", sql]


def wait_ready(server):
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        try:
            if server["kind"] == "iotdb":
                logs = capture(["docker", "logs", "--tail", "300", server["name"]])
                ready = "DataNode is set up successfully" in logs
                if ready:
                    ready = "information_schema" in capture(iotdb_cli(server, "SHOW DATABASES"), 20)
            else:
                health = "/ping" if server["kind"] == "influxdb1" else "/health"
                with urllib.request.urlopen(server["url"] + health, timeout=2) as response:
                    ready = response.status in (200, 204)
            if ready:
                break
        except (OSError, subprocess.SubprocessError):
            pass
        time.sleep(1)
    else:
        raise RuntimeError("Database did not become ready: " + server["kind"])

    version_arguments = ["influxd", "version"] if server["kind"] == "influxdb1" else ["influxdb3", "--version"]
    command = iotdb_cli(server, "SHOW VERSION") if server["kind"] == "iotdb" else [
        "docker", "exec", server["name"], *version_arguments]
    version = capture(command)
    if not re.search(r"(?<![0-9.])" + re.escape(server["version"]) + r"(?![0-9.])", version):
        raise RuntimeError("Unexpected database version: " + version)
    server["actualVersion"] = version.strip()
    print("READY:", server["kind"], server["version"], flush=True)


def start_server(server, created, pull_policy, suffix):
    server["name"] = "tsgate-ci-" + suffix + "-" + server["kind"]
    # Each run owns its containers and dynamically assigned loopback-only ports.
    command = ["docker", "run", "--pull=" + pull_policy, "-d", "--name", server["name"],
               "--memory=2g", "--cpus=2", "-p", "127.0.0.1::" + str(server["port"])]
    if server.get("platform"):
        command += ["--platform", server["platform"]]
    for key, value in server["environment"].items():
        command += ["-e", key + "=" + value]
    command += [server["image"], *server["command"]]
    # Record ownership before starting so that cancellation still removes the container.
    created.append(server)
    server["containerId"] = capture(command, 600).strip()
    endpoint = capture(["docker", "port", server["name"], str(server["port"]) + "/tcp"]).strip()
    if not re.fullmatch(r"127\.0\.0\.1:[0-9]+", endpoint):
        raise RuntimeError("Unexpected published endpoint: " + endpoint)
    server["endpoint"] = endpoint
    server["url"] = "http://" + endpoint
    image = json.loads(capture(["docker", "image", "inspect", server["image"]]))[0]
    server["imageId"] = image["Id"]
    server["repoDigests"] = image.get("RepoDigests", [])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("unit", "docker"))
    parser.add_argument("--spring-boot", default="2.7.18")
    parser.add_argument("--maven-repo", type=Path)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--release-artifacts", action="store_true")
    parser.add_argument("--pull", choices=("missing", "never", "always"), default="missing")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    suffix = uuid.uuid4().hex[:10]
    output = args.output or ROOT / ".local-test/ci" / (args.mode + "-" + suffix)
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    summary = dict(mode=args.mode, springBoot=args.spring_boot, status="failed", servers=[], cleanup=[])
    created = []
    try:
        summary["java"] = capture(["java", "-version"])
        summary["maven"] = capture(["mvn", "-version"])
        summary["sourceManifest"] = source_manifest()
        command = ["mvn", "-B", "--no-transfer-progress", "-Dstyle.color=never",
                   "-Dspring-boot.version=" + args.spring_boot]
        if args.maven_repo:
            command += ["-Dmaven.repo.local=" + str(args.maven_repo.resolve())]
        if args.offline:
            command += ["-o"]
        profiles = ["release-artifacts"] if args.release_artifacts else []
        if args.mode == "docker":
            capture(["docker", "info", "--format", "{{.ServerVersion}}"])
            summary["servers"] = json.loads(SERVERS.read_text())
            for server in summary["servers"]:
                start_server(server, created, args.pull, suffix)
            for server in summary["servers"]:
                wait_ready(server)
                key = "endpoint" if server["kind"] == "iotdb" else "url"
                command += ["-Dtsdb.it." + server["kind"] + "." + key + "=" + server[key]]
            profiles.append("docker-it")
            command += ["-Dtsdb.it.iotdb.rpc-compression-enabled=true",
                        "-Dtsdb.it.iotdb.legacy-compression-rejection=false",
                        "-Dtsdb.it.influxdb.strict-cursor-sql=OR",
                        "-Dtsdb.it.influxdb.legacy-or-failure=false"]
        if profiles:
            command += ["-P" + ",".join(profiles)]
        command += ["clean", "verify"]
        summary["command"] = command
        summary["mavenExit"] = logged(command, output / "maven.log")
        for report_type in (["surefire", "failsafe"] if args.mode == "docker" else ["surefire"]):
            result = reports(output, report_type)
            summary[report_type] = result
            print(report_type.upper() + ":", result["totals"], flush=True)
            if result["totals"]["tests"] == 0 or any(result["totals"][key] for key in ("failures", "errors", "skipped")):
                raise RuntimeError("Missing, failing or skipped tests: " + report_type)
        if summary["mavenExit"] != 0:
            raise RuntimeError("Maven failed; see " + str(output / "maven.log"))
        if args.release_artifacts:
            audit = [sys.executable, str(ROOT / "scripts/verify-release-artifacts.py"),
                     "--project-root", str(ROOT), "--output", str(output / "release-artifacts.json")]
            summary["releaseAuditExit"] = logged(audit, output / "release-artifacts.log", 120)
            if summary["releaseAuditExit"] != 0:
                raise RuntimeError("Unsigned release artifact audit failed")
        summary["status"] = "passed"
    except (Exception, KeyboardInterrupt) as error:
        summary["error"] = str(error)
        print("FAILED:", error, file=sys.stderr, flush=True)
    finally:
        for server in reversed(created):
            try:
                logs = capture(["docker", "logs", "--tail", "1000", server["name"]])
                (output / (server["kind"] + ".log")).write_text(logs)
            except subprocess.SubprocessError:
                pass
            try:
                capture(["docker", "rm", "-fv", server["name"]])
                summary["cleanup"].append(dict(name=server["name"], removed=True))
            except subprocess.SubprocessError as error:
                summary["cleanup"].append(dict(name=server["name"], removed=False, error=str(error)))
                summary["status"] = "failed"
        summary["finishedAt"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print("RESULT:", summary["status"], "in", output, flush=True)
    return 0 if summary["status"] == "passed" else 1


def interrupted(_signal, _frame):
    """Let termination signals pass through the normal container cleanup path."""
    raise KeyboardInterrupt()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    sys.exit(main())
