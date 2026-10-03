#!/usr/bin/env python3
"""Run both openGemini releases in owned single-node and cluster Docker environments.

The environment fixture owns deployment details. This runner serializes all Maven
runs, retains only current openGemini Failsafe reports, and removes each deployment
before creating the next one. It does not publish artifacts or change JAVA_HOME.
"""

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
import urllib.parse
import uuid
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[4]
ENVIRONMENT = ROOT / "tsgate-opengemini/src/test/docker/environment.py"
MATRIX = tuple((version, mode) for version in ("1.4.1", "1.5.2") for mode in ("single", "cluster"))
MODULES = {
    "tsgate-opengemini": "com.alandevise.tsgate.integration.OpenGeminiDockerIT",
    "tsgate-opengemini-spring-boot-starter": "com.alandevise.tsgate.integration.OpenGeminiStarterDockerIT",
}
LABEL = "com.alandevise.tsgate.opengemini.fixture"
COUNTERS = ("tests", "failures", "errors", "skipped")


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def terminate_process_group(process):
    """Stop the command and forked JVMs on timeout or interruption."""
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass
    finally:
        # A terminated launcher can leave forked JVMs in the same process group.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=10)


def capture(command, timeout=45):
    process = subprocess.Popen(command, cwd=ROOT, text=True, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, start_new_session=True)
    try:
        output, _ = process.communicate(timeout=timeout)
    except BaseException:
        terminate_process_group(process)
        raise
    if process.returncode:
        raise subprocess.CalledProcessError(process.returncode, command, output=output)
    return output


def logged(command, output, timeout):
    print("RUN:", " ".join(command), flush=True)
    with output.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            return process.wait(timeout=timeout)
        except BaseException:
            terminate_process_group(process)
            raise


def source_manifest():
    """Identify reactor inputs, including uncommitted code and the deployment fixture."""
    paths = {ROOT / name for name in ("pom.xml", "LICENSE", "NOTICE", ".github/workflows/ci.yml",
                                     "scripts/verify-release-artifacts.py")}
    paths.update(ROOT.glob("tsgate-*/pom.xml"))
    for module in ROOT.glob("tsgate-*"):
        paths.update(path for path in (module / "src").rglob("*") if path.is_file()
                     and "__pycache__" not in path.parts and path.suffix != ".pyc")
    return {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(paths) if path.is_file()}


def java_home(properties):
    match = re.search(r"^\s*java\.home\s*=\s*(.+)$", properties, re.MULTILINE)
    if not match:
        raise RuntimeError("java -XshowSettings did not report java.home")
    return Path(match.group(1).strip()).resolve()


def toolchain():
    """Check the actual runtimes, including macOS's /usr/bin/java launcher."""
    java_path = shutil.which("java")
    maven_path = shutil.which("mvn")
    if not java_path or not maven_path:
        raise RuntimeError("Both java and mvn must be available on PATH")
    java = capture(["java", "-XshowSettings:properties", "-version"])
    home = java_home(java)
    specification = re.search(r"^\s*java\.specification\.version\s*=\s*(\d+)\s*$", java, re.MULTILINE)
    if not specification or int(specification.group(1)) < 17:
        raise RuntimeError("The openGemini reactor requires Java 17 or newer")
    configured_home = os.environ.get("JAVA_HOME")
    if configured_home:
        configured_java = Path(configured_home) / "bin/java"
        if not configured_java.is_file():
            raise RuntimeError("JAVA_HOME does not contain bin/java")
        if java_home(capture([str(configured_java), "-XshowSettings:properties", "-version"])) != home:
            raise RuntimeError("JAVA_HOME and PATH select different Java runtimes")
    maven = capture(["mvn", "-version"])
    runtime = re.search(r"^Java version:.*?runtime:\s*(.+)$", maven, re.MULTILINE)
    if not runtime or Path(runtime.group(1).strip()).resolve() != home:
        raise RuntimeError("Maven and PATH select different Java runtimes")
    return dict(javaPath=java_path, mavenPath=maven_path, javaHome=str(home),
                configuredJavaHome=configured_home, java=java, maven=maven)


def deployment(state_path, version, mode):
    state = json.loads(state_path.read_text(encoding="utf-8"))
    if state.get("ready") is not True:
        raise RuntimeError("Deployment is not ready; Maven was not started")
    if not isinstance(state.get("owner"), str) or not state["owner"]:
        raise RuntimeError("Deployment state lacks an ownership identifier")
    if state.get("actual_version") != version or state.get("mode") != mode:
        raise RuntimeError("Deployment does not prove the requested binary version: " + version)
    if type(state.get("replicas")) is not int or state["replicas"] != (1 if mode == "single" else 3):
        raise RuntimeError("Deployment replication count does not match " + mode)
    urls = state.get("urls")
    if (not isinstance(urls, list) or not urls or not all(isinstance(url, str) for url in urls)
            or state.get("url") not in urls or len(set(urls)) != len(urls)):
        raise RuntimeError("Deployment must provide a primary URL and distinct SQL entry URLs")
    for url in urls:
        if not isinstance(url, str):
            raise RuntimeError("Deployment SQL URLs must be strings")
        parsed = urllib.parse.urlsplit(url)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.query or parsed.fragment or "," in url):
            raise RuntimeError("Invalid SQL entry URL in deployment state")
        _ = parsed.port
    return state


def maven_command(args, state, version):
    command = ["mvn", "-B", "--no-transfer-progress", "-Dstyle.color=never",
               "-Dspring-boot.version=" + args.spring_boot, "-Pdocker-it",
               "-pl", ",".join(MODULES), "-am",
               "-Dit.test=OpenGeminiDockerIT,OpenGeminiStarterDockerIT",
               "-Dfailsafe.failIfNoSpecifiedTests=false",
               "-Dtsdb.it.opengemini.url=" + state["url"],
               "-Dtsdb.it.opengemini.urls=" + ",".join(state["urls"]),
               "-Dtsdb.it.opengemini.version=" + version,
               "-Dtsdb.it.opengemini.replicas=" + str(state["replicas"])]
    if args.maven_repo:
        command.append("-Dmaven.repo.local=" + str(args.maven_repo.resolve()))
    if args.offline:
        command.append("-o")
    return command + ["clean", "verify"]


def collect_reports(output, started_ns):
    """Never aggregate other modules or files predating this specific Maven invocation."""
    result = dict(totals=dict.fromkeys(COUNTERS, 0), suites=[], staleReports=[])
    for module in MODULES:
        source = ROOT / module / "target/failsafe-reports"
        if not source.is_dir():
            continue
        destination = output / "reports" / module / "failsafe-reports"
        for report in sorted(source.iterdir()):
            if not report.is_file():
                continue
            if report.stat().st_mtime_ns < started_ns:
                if report.name.startswith("TEST-") and report.suffix == ".xml":
                    result["staleReports"].append(str(report.relative_to(ROOT)))
                continue
            destination.mkdir(parents=True, exist_ok=True)
            shutil.copy2(report, destination / report.name)
            if not report.name.startswith("TEST-") or report.suffix != ".xml":
                continue
            xml = ET.parse(report).getroot()
            if xml.tag != "testsuite":
                raise RuntimeError("Unexpected Failsafe report envelope: " + str(report))
            suite = {key: int(xml.get(key, "0")) for key in COUNTERS}
            if any(value < 0 for value in suite.values()):
                raise RuntimeError("Negative test counter: " + str(report))
            for key in COUNTERS:
                result["totals"][key] += suite[key]
            result["suites"].append(dict(module=module, name=xml.get("name"),
                                          report=str(destination / report.name), **suite))
    return result


def validate_reports(result):
    if result["staleReports"]:
        raise RuntimeError("Rejected stale Failsafe reports: " + ", ".join(result["staleReports"]))
    suites = {(suite["module"], suite["name"]) for suite in result["suites"] if suite["tests"] > 0}
    if suites != set(MODULES.items()) or len(result["suites"]) != len(MODULES):
        raise RuntimeError("Both selected openGemini Docker test classes must produce current, nonempty reports")
    if result["totals"]["tests"] == 0 or any(result["totals"][key] for key in ("failures", "errors", "skipped")):
        raise RuntimeError("Failing, erroneous or skipped openGemini Docker tests")


def cleanup(state_path, output, timeout):
    evidence = dict(stateFile=str(state_path), status="failed")
    if not state_path.is_file():
        evidence.update(status="not-created", reason="Fixture did not create an ownership state file")
        return evidence
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        owner = state.get("owner")
        if not isinstance(owner, str) or not owner:
            raise RuntimeError("Cannot clean a state without a fixture ownership identifier")
        selection = "label=" + LABEL + "=" + owner
        # Cancellation can occur after Docker creates a container but before the
        # fixture records its name. Adopt only this state's labelled containers;
        # the stop CLI independently verifies every label before deletion.
        try:
            discovered = capture(["docker", "ps", "-a", "--filter", selection,
                                  "--format", "{{.Names}}"], 45).split()
            evidence["discoveredOwnedContainers"] = discovered
            state["containers"] = list(dict.fromkeys(state.get("containers", []) + discovered))
            state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
        except Exception as error:
            evidence["discoveryError"] = str(error)
        command = [sys.executable, str(ENVIRONMENT), "stop", "--state", str(state_path)]
        evidence["command"] = command
        evidence["log"] = str(output / "stop.log")
        evidence["exit"] = logged(command, output / "stop.log", timeout)
        if evidence["exit"]:
            raise RuntimeError("Environment stop failed")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        evidence["stopped"] = state.get("stopped") is True
        if not evidence["stopped"] or state.get("owner") != owner:
            raise RuntimeError("Stop did not prove removal of the owned environment")
        evidence["owner"] = owner
        evidence["remainingContainers"] = capture(["docker", "ps", "-a", "--filter", selection,
                                                    "--format", "{{.ID}}"], 45).split()
        evidence["remainingNetworks"] = capture(["docker", "network", "ls", "--filter", selection,
                                                  "--format", "{{.ID}}"], 45).split()
        if evidence["remainingContainers"] or evidence["remainingNetworks"]:
            raise RuntimeError("Owned Docker resources remain after stop")
        evidence["status"] = "passed"
    except (Exception, KeyboardInterrupt) as error:
        evidence["error"] = str(error) or type(error).__name__
    return evidence


def run_environment(args, output, version, mode, manifest):
    output.mkdir(parents=True, exist_ok=False)
    state_path = output / "state.json"
    result = dict(version=version, mode=mode, status="failed", startedAt=now(), output=str(output))
    interrupted_run = False
    try:
        start = [sys.executable, str(ENVIRONMENT), "start", "--version", version,
                 "--mode", mode, "--state", str(state_path)]
        result["startCommand"] = start
        result["startExit"] = logged(start, output / "start.log", args.environment_timeout)
        if result["startExit"]:
            raise RuntimeError("Environment start failed; see start.log and fixture evidence")
        result["environment"] = deployment(state_path, version, mode)
        inspect = [sys.executable, str(ENVIRONMENT), "inspect", "--state", str(state_path)]
        result["inspectCommand"] = inspect
        result["inspectExit"] = logged(inspect, output / "inspect.log", args.inspect_timeout)
        if result["inspectExit"]:
            raise RuntimeError("Environment inspection failed")
        result["environment"] = deployment(state_path, version, mode)
        if source_manifest() != manifest:
            raise RuntimeError("Build inputs changed before Maven; stop concurrent source edits")
        result["mavenCommand"] = maven_command(args, result["environment"], version)
        started_ns = time.time_ns()
        result["mavenStartedAtNs"] = started_ns
        try:
            result["mavenExit"] = logged(result["mavenCommand"], output / "maven.log", args.maven_timeout)
        finally:
            # Preserve fresh partial reports even when Maven is interrupted or times out.
            try:
                result["failsafe"] = collect_reports(output, started_ns)
            except Exception as error:
                result["reportCollectionError"] = str(error)
        if result.get("mavenExit") != 0:
            raise RuntimeError("Maven failed; see maven.log")
        if "reportCollectionError" in result:
            raise RuntimeError(result["reportCollectionError"])
        validate_reports(result["failsafe"])
        if source_manifest() != manifest:
            raise RuntimeError("Build inputs changed during Maven; results do not share one source manifest")
        result["status"] = "passed"
    except (Exception, KeyboardInterrupt) as error:
        result["error"] = str(error) or type(error).__name__
        interrupted_run = isinstance(error, KeyboardInterrupt)
    finally:
        # The fixture writes ownership before creating Docker resources, including failed starts.
        result["cleanup"] = cleanup(state_path, output, args.stop_timeout)
        if result["cleanup"]["status"] == "failed":
            result["status"] = "failed"
        result["finishedAt"] = now()
        result["interrupted"] = interrupted_run
        (output / "summary.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print("ENVIRONMENT:", version, mode, result["status"], flush=True)
    return result


def positive_timeout(value):
    timeout = int(value)
    if timeout <= 0:
        raise argparse.ArgumentTypeError("Timeouts must be positive seconds")
    return timeout


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="New directory for all logs, states, reports and summary.json")
    parser.add_argument("--spring-boot", default="2.7.18")
    parser.add_argument("--maven-repo", type=Path)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--maven-timeout", type=positive_timeout, default=1800)
    parser.add_argument("--environment-timeout", type=positive_timeout, default=1800)
    parser.add_argument("--inspect-timeout", type=positive_timeout, default=180)
    parser.add_argument("--stop-timeout", type=positive_timeout, default=180)
    args = parser.parse_args(argv)
    output = (args.output or ROOT / ".local-test/opengemini-matrix" / uuid.uuid4().hex[:10]).resolve()
    output.mkdir(parents=True, exist_ok=False)
    summary = dict(status="failed", startedAt=now(), springBoot=args.spring_boot, output=str(output),
                   environments=[dict(version=version, mode=mode, status="not-run") for version, mode in MATRIX])
    try:
        if not ENVIRONMENT.is_file():
            raise RuntimeError("Missing environment fixture: " + str(ENVIRONMENT))
        summary["toolchain"] = toolchain()
        summary["dockerVersion"] = capture(["docker", "info", "--format", "{{.ServerVersion}}"])
        summary["sourceManifest"] = source_manifest()
        for index, (version, mode) in enumerate(MATRIX):
            result = run_environment(args, output / (version + "-" + mode), version, mode, summary["sourceManifest"])
            summary["environments"][index] = result
            if result["interrupted"]:
                raise KeyboardInterrupt()
            if result["cleanup"]["status"] == "failed":
                raise RuntimeError("Cleanup failed; no further environments were created")
            if source_manifest() != summary["sourceManifest"]:
                raise RuntimeError("Build inputs changed; remaining matrix environments were not run")
        if all(environment["status"] == "passed" for environment in summary["environments"]):
            summary["status"] = "passed"
        else:
            summary["error"] = "One or more matrix environments failed; see individual summaries"
    except (Exception, KeyboardInterrupt) as error:
        summary["error"] = str(error) or type(error).__name__
        print("FAILED:", summary["error"], file=sys.stderr, flush=True)
    finally:
        summary["failsafeTotals"] = {key: sum(environment.get("failsafe", {}).get("totals", {}).get(key, 0)
                                             for environment in summary["environments"]) for key in COUNTERS}
        if "sourceManifest" in summary:
            summary["sourceManifestUnchanged"] = source_manifest() == summary["sourceManifest"]
            if not summary["sourceManifestUnchanged"]:
                summary["status"] = "failed"
        summary["finishedAt"] = now()
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        print("RESULT:", summary["status"], "in", output, flush=True)
    return 0 if summary["status"] == "passed" else 1


def interrupted(_signal, _frame):
    raise KeyboardInterrupt()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    sys.exit(main())
