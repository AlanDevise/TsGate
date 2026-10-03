#!/usr/bin/env python3
"""Regression checks for explicit external openGemini coverage selection; no database tools required."""

import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


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


if __name__ == "__main__":
    unittest.main()
