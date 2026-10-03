"""Regression checks for selection, fresh evidence and owned-environment cleanup."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location("opengemini_matrix", Path(__file__).with_name("run-matrix.py"))
matrix = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(matrix)


class MatrixRunnerTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.root_patch = patch.object(matrix, "ROOT", self.root)
        self.root_patch.start()
        self.addCleanup(self.root_patch.stop)
        self.args = argparse.Namespace(spring_boot="2.7.18", maven_repo=None, offline=False,
                                       maven_timeout=30, environment_timeout=30, inspect_timeout=30, stop_timeout=30)

    def state(self, version="1.4.1", mode="single"):
        return dict(owner="tsgate-og-unit", ready=True, actual_version=version, mode=mode,
                    url="http://127.0.0.1:18086", urls=["http://127.0.0.1:18086"],
                    replicas=1 if mode == "single" else 3, containers=["recorded"])

    def write_state(self, path, state=None):
        path.write_text(json.dumps(state or self.state()))

    def report(self, module, name=None, tests=7, failures=0, errors=0, skipped=0, timestamp=None):
        directory = self.root / module / "target/failsafe-reports"
        directory.mkdir(parents=True, exist_ok=True)
        name = name or matrix.MODULES[module]
        report = directory / ("TEST-" + name + ".xml")
        report.write_text('<testsuite name="%s" tests="%d" failures="%d" errors="%d" skipped="%d"/>'
                          % (name, tests, failures, errors, skipped))
        if timestamp is not None:
            os.utime(report, ns=(timestamp, timestamp))
        return report

    def write_selected_reports(self, timestamp=None):
        for index, module in enumerate(matrix.MODULES):
            self.report(module, tests=13 + index, timestamp=timestamp)

    def test_maven_selects_only_both_new_it_classes_and_forwards_all_deployment_settings(self):
        self.args.maven_repo = self.root / "local-maven"
        self.args.offline = True
        state = self.state("1.5.2", "cluster")
        state["urls"].append("http://127.0.0.1:28086")
        command = matrix.maven_command(self.args, state, "1.5.2")
        self.assertEqual(["clean", "verify"], command[-2:])
        self.assertEqual(",".join(matrix.MODULES), command[command.index("-pl") + 1])
        for argument in ("-Pdocker-it", "-am", "-Dit.test=OpenGeminiDockerIT,OpenGeminiStarterDockerIT",
                         "-Dfailsafe.failIfNoSpecifiedTests=false", "-Dtsdb.it.opengemini.replicas=3",
                         "-Dtsdb.it.opengemini.url=" + state["url"],
                         "-Dtsdb.it.opengemini.urls=" + ",".join(state["urls"]),
                         "-Dtsdb.it.opengemini.version=1.5.2", "-o"):
            self.assertIn(argument, command)

    def test_reports_ignore_other_modules_and_read_actual_counts(self):
        self.write_selected_reports(timestamp=200)
        self.report("tsgate-influxdb1", name="old.Suite", tests=1000, failures=100, timestamp=200)
        output = self.root / "evidence"
        result = matrix.collect_reports(output, 100)
        matrix.validate_reports(result)
        self.assertEqual(dict(tests=27, failures=0, errors=0, skipped=0), result["totals"])
        self.assertEqual(set(matrix.MODULES), {suite["module"] for suite in result["suites"]})
        self.assertFalse((output / "reports/tsgate-influxdb1").exists())

    def test_stale_reports_are_not_copied_or_counted_and_fail_validation(self):
        self.write_selected_reports(timestamp=99)
        output = self.root / "evidence"
        result = matrix.collect_reports(output, 100)
        self.assertEqual(0, result["totals"]["tests"])
        self.assertEqual(2, len(result["staleReports"]))
        self.assertFalse((output / "reports").exists())
        with self.assertRaisesRegex(RuntimeError, "stale"):
            matrix.validate_reports(result)

    def test_missing_unexpected_empty_or_failing_test_suites_are_rejected(self):
        modules = list(matrix.MODULES)
        self.write_selected_reports(timestamp=200)
        for counter in ("failures", "errors", "skipped"):
            with self.subTest(counter=counter):
                self.report(modules[0], timestamp=200, **{counter: 1})
                with self.assertRaisesRegex(RuntimeError, "Failing"):
                    matrix.validate_reports(matrix.collect_reports(self.root / counter, 100))
        self.report(modules[0], tests=0, timestamp=200)
        with self.assertRaisesRegex(RuntimeError, "nonempty"):
            matrix.validate_reports(matrix.collect_reports(self.root / "empty", 100))
        self.report(modules[0], tests=7, timestamp=200)
        extra = self.report(modules[0], name="unexpected.Suite", timestamp=200)
        with self.assertRaisesRegex(RuntimeError, "Both selected"):
            matrix.validate_reports(matrix.collect_reports(self.root / "extra", 100))
        extra.unlink()
        for report in (self.root / modules[1] / "target/failsafe-reports").glob("TEST-*.xml"):
            report.unlink()
        with self.assertRaisesRegex(RuntimeError, "Both selected"):
            matrix.validate_reports(matrix.collect_reports(self.root / "missing", 100))

    def test_deployment_requires_ready_binary_version_replica_and_owned_url_evidence(self):
        path = self.root / "state.json"
        self.write_state(path)
        self.assertEqual("1.4.1", matrix.deployment(path, "1.4.1", "single")["actual_version"])
        invalid = ({"ready": False}, {"actual_version": "1.5.2"}, {"replicas": 3}, {"owner": ""},
                   {"urls": []}, {"urls": ["file:///tmp"]}, {"mode": "cluster"},
                   {"urls": ["http://127.0.0.1:18086", "http://127.0.0.1:18086"]})
        for changes in invalid:
            with self.subTest(changes=changes):
                state = self.state() | changes
                self.write_state(path, state)
                with self.assertRaises(RuntimeError):
                    matrix.deployment(path, "1.4.1", "single")

    def test_cleanup_adopts_unrecorded_owned_containers_and_proves_no_resources_remain(self):
        path = self.root / "state.json"
        self.write_state(path)

        def stop(command, output, timeout):
            self.assertEqual("stop", command[2])
            state = json.loads(path.read_text())
            self.assertEqual(["recorded", "unrecorded"], state["containers"])
            self.write_state(path, state | {"stopped": True})
            return 0

        with patch.object(matrix, "logged", side_effect=stop), patch.object(matrix, "capture",
                side_effect=["recorded\nunrecorded\n", "", ""]) as capture:
            result = matrix.cleanup(path, self.root, 30)
        self.assertEqual("passed", result["status"])
        self.assertEqual([], result["remainingContainers"])
        self.assertEqual([], result["remainingNetworks"])
        for call in capture.call_args_list:
            self.assertIn("label=" + matrix.LABEL + "=tsgate-og-unit", call.args[0])

    def test_cleanup_reports_remaining_owned_resources_as_failure(self):
        path = self.root / "state.json"
        self.write_state(path)

        def stop(*args):
            self.write_state(path, self.state() | {"stopped": True})
            return 0

        with patch.object(matrix, "logged", side_effect=stop), patch.object(matrix, "capture",
                side_effect=["recorded", "leftover-container", ""]):
            result = matrix.cleanup(path, self.root, 30)
        self.assertEqual("failed", result["status"])
        self.assertEqual(["leftover-container"], result["remainingContainers"])

    def test_failed_start_still_cleans_owned_environment_and_never_starts_maven(self):
        output = self.root / "environment"

        def start(command, log, timeout):
            self.write_state(output / "state.json", self.state() | {"ready": False})
            return 1

        with patch.object(matrix, "logged", side_effect=start) as logged, patch.object(matrix, "cleanup",
                return_value={"status": "passed"}) as cleanup:
            result = matrix.run_environment(self.args, output, "1.4.1", "single", {})
        self.assertEqual("failed", result["status"])
        self.assertEqual(1, logged.call_count)
        cleanup.assert_called_once()
        self.assertNotIn("mavenCommand", result)
        self.assertTrue((output / "summary.json").is_file())

    def test_maven_failure_or_timeout_retains_fresh_partial_reports_and_cleans(self):
        for timeout in (False, True):
            with self.subTest(timeout=timeout):
                output = self.root / ("timeout" if timeout else "failure")

                def execute(command, log, deadline):
                    if command[0] != "mvn":
                        self.write_state(output / "state.json")
                        return 0
                    self.write_selected_reports()
                    if timeout:
                        raise subprocess.TimeoutExpired(command, deadline)
                    return 1

                with patch.object(matrix, "logged", side_effect=execute), patch.object(matrix, "cleanup",
                        return_value={"status": "passed"}) as cleanup, patch.object(matrix, "source_manifest", return_value={}):
                    result = matrix.run_environment(self.args, output, "1.4.1", "single", {})
                self.assertEqual("failed", result["status"])
                self.assertEqual(27, result["failsafe"]["totals"]["tests"])
                cleanup.assert_called_once()
                self.assertEqual(2, len(list((output / "reports").rglob("TEST-*.xml"))))

    def test_logged_timeout_signals_the_whole_group_even_when_launcher_exits_first(self):
        process = Mock(pid=9123)
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired(["mvn"], 1), 0, 0]
        with patch.object(matrix.subprocess, "Popen", return_value=process) as popen, patch.object(matrix.os, "killpg") as kill:
            with self.assertRaises(subprocess.TimeoutExpired):
                matrix.logged(["mvn"], self.root / "maven.log", 1)
        self.assertTrue(popen.call_args.kwargs["start_new_session"])
        self.assertEqual([(9123, signal.SIGTERM), (9123, signal.SIGKILL)], [call.args for call in kill.call_args_list])

    def test_default_java_home_path_and_maven_must_select_the_same_runtime(self):
        home = self.root / "jdk17"
        (home / "bin").mkdir(parents=True)
        (home / "bin/java").touch()
        properties = "    java.home = " + str(home) + "\n    java.specification.version = 17\n"
        maven_version = "Apache Maven 3.9.9\nJava version: 17.0.14, vendor: Eclipse Adoptium, runtime: " + str(home) + "\n"
        with patch.dict(os.environ, {"JAVA_HOME": str(home)}), patch.object(matrix.shutil, "which", return_value="tool"):
            with patch.object(matrix, "capture", side_effect=[properties, properties, maven_version]):
                self.assertEqual(str(home.resolve()), matrix.toolchain()["javaHome"])
            with patch.object(matrix, "capture", side_effect=[properties, properties.replace("jdk17", "jdk25")]):
                with self.assertRaisesRegex(RuntimeError, "JAVA_HOME and PATH"):
                    matrix.toolchain()
            with patch.object(matrix, "capture", side_effect=[properties, properties, maven_version.replace("jdk17", "jdk25")]):
                with self.assertRaisesRegex(RuntimeError, "Maven and PATH"):
                    matrix.toolchain()

    def test_matrix_runs_serially_all_four_environments_without_hardcoded_test_counts(self):
        fixture = self.root / "environment.py"
        fixture.touch()
        visited = []

        def environment(args, output, version, mode, manifest):
            visited.append((version, mode))
            return dict(version=version, mode=mode, status="passed", interrupted=False,
                        cleanup={"status": "passed"}, failsafe={"totals": dict(tests=27, failures=0, errors=0, skipped=0)})

        output = self.root / "matrix"
        with patch.object(matrix, "ENVIRONMENT", fixture), patch.object(matrix, "toolchain", return_value={}), \
                patch.object(matrix, "capture", return_value="Docker test version"), \
                patch.object(matrix, "source_manifest", return_value={"pom.xml": "digest"}), \
                patch.object(matrix, "run_environment", side_effect=environment):
            self.assertEqual(0, matrix.main(["--output", str(output)]))
        self.assertEqual(list(matrix.MATRIX), visited)
        summary = json.loads((output / "summary.json").read_text())
        self.assertEqual(108, summary["failsafeTotals"]["tests"])
        self.assertTrue(summary["sourceManifestUnchanged"])

    def test_failed_cleanup_prevents_creating_another_environment(self):
        fixture = self.root / "environment.py"
        fixture.touch()
        failed = dict(status="failed", interrupted=False, cleanup={"status": "failed"})
        with patch.object(matrix, "ENVIRONMENT", fixture), patch.object(matrix, "toolchain", return_value={}), \
                patch.object(matrix, "capture", return_value="version"), \
                patch.object(matrix, "source_manifest", return_value={}), \
                patch.object(matrix, "run_environment", return_value=failed) as run:
            self.assertEqual(1, matrix.main(["--output", str(self.root / "failed")]))
        self.assertEqual(1, run.call_count)


if __name__ == "__main__":
    unittest.main()
