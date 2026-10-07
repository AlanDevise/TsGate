#!/usr/bin/env python3
"""Regression checks for explicit external openGemini coverage selection; no database tools required."""

import importlib.util
import json
import subprocess
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch


SPEC = importlib.util.spec_from_file_location("tsgate_test_runner", Path(__file__).with_name("run-tests.py"))
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class OpenGeminiSelectionTest(unittest.TestCase):
    def test_default_three_server_run_explicitly_excludes_open_gemini_tests(self):
        parameters, metadata = RUNNER.opengemini_options()
        self.assertEqual(["-Dfailsafe.excludes=**/OpenGemini*DockerIT.java"], parameters)
        self.assertEqual("excluded", metadata["status"])
        self.assertFalse(metadata["ownedByRunner"])
        self.assertIn("separate deployment matrix", metadata["reason"])

    def test_external_endpoint_includes_tests_and_normalizes_the_base_url(self):
        parameters, metadata = RUNNER.opengemini_options(" http://127.0.0.1:8086/ ")
        self.assertEqual(["-Dtsdb.it.opengemini.url=http://127.0.0.1:8086",
                          "-Dtsdb.it.opengemini.urls=http://127.0.0.1:8086",
                          "-Dtsdb.it.opengemini.replicas=1"], parameters)
        self.assertEqual("included", metadata["status"])
        self.assertFalse(metadata["ownedByRunner"])
        self.assertNotIn("expectedVersion", metadata)
        self.assertTrue(all("failsafe.excludes" not in parameter for parameter in parameters))

    def test_cluster_urls_and_expected_version_are_forwarded_without_treating_them_as_verified(self):
        parameters, metadata = RUNNER.opengemini_options(
            "http://127.0.0.1:8086", "1.5.2",
            "http://127.0.0.1:8087/,http://127.0.0.1:8086,http://127.0.0.1:8087", replicas=3)
        self.assertIn("-Dtsdb.it.opengemini.version=1.5.2", parameters)
        self.assertIn("-Dtsdb.it.opengemini.urls=http://127.0.0.1:8087,http://127.0.0.1:8086", parameters)
        self.assertEqual("1.5.2", metadata["expectedVersion"])
        self.assertEqual(3, metadata["replicas"])
        self.assertIn("-Dtsdb.it.opengemini.replicas=3", parameters)
        self.assertEqual(2, len(metadata["urls"]))
        self.assertNotIn("actualVersion", metadata)

    def test_primary_endpoint_is_included_when_only_additional_cluster_urls_are_supplied(self):
        parameters, metadata = RUNNER.opengemini_options(
            "http://127.0.0.1:8086", urls="http://127.0.0.1:8087")
        self.assertEqual(["http://127.0.0.1:8086", "http://127.0.0.1:8087"], metadata["urls"])
        self.assertIn("-Dtsdb.it.opengemini.urls=http://127.0.0.1:8086,http://127.0.0.1:8087", parameters)

    def test_matrix_metadata_cannot_silently_run_without_a_primary_endpoint(self):
        for settings in ({"version": "1.4.1"}, {"urls": "http://127.0.0.1:8086"}, {"replicas": 3}):
            with self.subTest(settings=settings), self.assertRaisesRegex(ValueError, "require --opengemini-url"):
                RUNNER.opengemini_options(**settings)

    def test_invalid_endpoints_fail_before_any_container_is_started(self):
        for endpoint in ("", "127.0.0.1:8086", "ftp://localhost", "http://", "http://localhost:invalid",
                         "http://user:password@localhost:8086", "http://localhost:8086?db=test",
                         "http://localhost:8086#fragment"):
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                RUNNER.opengemini_options(endpoint)
        with self.assertRaises(ValueError):
            RUNNER.opengemini_options("http://localhost:8086", urls="http://localhost:8087,")
        with self.assertRaisesRegex(ValueError, "greater than zero"):
            RUNNER.opengemini_options("http://localhost:8086", replicas=0)
        with self.assertRaisesRegex(ValueError, "must not be blank"):
            RUNNER.opengemini_options("http://localhost:8086", version="  ")


class SourceManifestTest(unittest.TestCase):
    def test_python_cache_is_excluded_but_real_source_is_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = root / "tsgate-example/src/scripts"
            cache = scripts / "__pycache__"
            cache.mkdir(parents=True)
            (scripts / "x.py").write_text("print('source')\n")
            (cache / "x.pyc").write_bytes(b"cached bytecode")
            with patch.object(RUNNER, "ROOT", root):
                manifest = RUNNER.source_manifest()
            self.assertEqual(["tsgate-example/src/scripts/x.py"], list(manifest))


class BackendSelectionTest(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(RUNNER.os.environ, {"GITHUB_SHA": ""})
        environment.start()
        self.addCleanup(environment.stop)

    def test_default_selection_keeps_all_servers_and_no_test_filter(self):
        servers = json.loads(RUNNER.SERVERS.read_text())
        self.assertEqual(list(RUNNER.BACKEND_TESTS), [server["kind"] for server in RUNNER.backend_selection(servers)])
        self.assertEqual([], RUNNER.backend_parameters())

    def test_selected_backend_only_starts_its_server_and_uses_unambiguous_test_classes(self):
        servers = json.loads(RUNNER.SERVERS.read_text())
        for backend in RUNNER.BACKEND_TESTS:
            with self.subTest(backend=backend):
                selected = RUNNER.backend_selection(servers, backend)
                self.assertEqual([backend], [server["kind"] for server in selected])
                self.assertIsNot(selected[0], next(server for server in servers if server["kind"] == backend))
                parameters = RUNNER.backend_parameters(backend)
                self.assertEqual("-Dit.test=" + RUNNER.BACKEND_TESTS[backend], parameters[0])
                self.assertIn("-Dfailsafe.failIfNoSpecifiedTests=false", parameters)
                self.assertNotIn("DatabaseContractIT", parameters[0])
                self.assertNotIn("OpenGemini", parameters[0])
        with self.assertRaises(ValueError):
            RUNNER.backend_selection(servers, "unknown")

    def test_backend_and_external_endpoints_are_mutually_exclusive_before_docker_io(self):
        with patch.object(RUNNER, "capture") as capture:
            for arguments in (["unit", "--backend", "iotdb"],
                              ["docker", "--backend", "iotdb", "--opengemini-url", "http://localhost:8086"]):
                with self.subTest(arguments=arguments), self.assertRaises(SystemExit):
                    RUNNER.main(arguments)
            capture.assert_not_called()

    def execute(self, backend=None, manifests=None, cleanup_error=False):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        output = Path(directory.name) / "result"
        created = []

        def start(server, owned, policy, suffix):
            server.update(name="owned-" + server["kind"], endpoint="127.0.0.1:18086", url="http://127.0.0.1:18086")
            owned.append(server)
            created.append(server["kind"])

        def capture(command, timeout=45):
            if cleanup_error and command[:3] == ["docker", "rm", "-fv"]:
                raise subprocess.CalledProcessError(1, command)
            return "test tool version"

        arguments = ["docker", "--output", str(output)] + (["--backend", backend] if backend else [])
        result = dict(totals=dict(tests=1, failures=0, errors=0, skipped=0), suites=[])
        with patch.object(RUNNER, "capture", side_effect=capture),                 patch.object(RUNNER, "git_commit", return_value="a" * 40),                 patch.object(RUNNER, "source_manifest", side_effect=manifests or [{}, {}, {}]),                 patch.object(RUNNER, "start_server", side_effect=start), patch.object(RUNNER, "wait_ready"),                 patch.object(RUNNER, "logged", return_value=0), patch.object(RUNNER, "reports", return_value=result):
            code = RUNNER.main(arguments)
        return code, json.loads((output / "summary.json").read_text()), created

    def test_subset_reports_explicitly_mark_other_backends_not_run(self):
        code, summary, created = self.execute("influxdb1")
        self.assertEqual(0, code)
        self.assertEqual(["influxdb1"], created)
        self.assertEqual("a" * 40, summary["gitCommit"])
        self.assertTrue(summary["gitCommitUnchanged"])
        for backend, state in summary["backends"].items():
            self.assertEqual(backend == "influxdb1", state["selected"])
            self.assertEqual("passed" if backend == "influxdb1" else "not-run", state["status"])
        self.assertIn("-Dit.test=" + RUNNER.BACKEND_TESTS["influxdb1"], summary["command"])
        self.assertEqual("excluded", summary["opengemini"]["status"])

    def test_default_remains_full_and_cleanup_failure_makes_the_run_fail(self):
        code, summary, created = self.execute()
        self.assertEqual(0, code)
        self.assertEqual(list(RUNNER.BACKEND_TESTS), created)
        self.assertFalse(any(parameter.startswith("-Dit.test=") for parameter in summary["command"]))
        self.assertTrue(all(state["status"] == "passed" for state in summary["backends"].values()))
        code, summary, _ = self.execute("iotdb", cleanup_error=True)
        self.assertEqual(1, code)
        self.assertEqual("failed", summary["backends"]["iotdb"]["status"])
        self.assertFalse(summary["cleanup"][0]["removed"])

    def test_github_commit_mismatch_fails_before_starting_any_database(self):
        with patch.dict(RUNNER.os.environ, {"GITHUB_SHA": "b" * 40}):
            code, summary, created = self.execute("iotdb")
        self.assertEqual(1, code)
        self.assertEqual([], created)
        self.assertIn("does not match GITHUB_SHA", summary["error"])
        self.assertNotIn("command", summary)

    def test_source_change_during_validation_is_a_failure_even_if_tests_pass(self):
        code, summary, _ = self.execute("iotdb", manifests=[{"x": "before"}, {"x": "before"}, {"x": "after"}])
        self.assertEqual(1, code)
        self.assertFalse(summary["sourceManifestUnchanged"])
        self.assertIn("changed during validation", summary["error"])
        self.assertTrue(summary["cleanup"][0]["removed"])


if __name__ == "__main__":
    unittest.main()
