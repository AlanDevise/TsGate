"""Regression checks for database change selection without network or shell evaluation."""

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock

SPEC = importlib.util.spec_from_file_location("ci_scope", Path(__file__).with_name("ci-scope.py"))
scope = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(scope)


class CoverageTest(unittest.TestCase):
    def test_backend_code_and_shared_protocol_map_to_affected_databases(self):
        cases = {
            "tsgate-iotdb/src/main/Adapter.java": (["iotdb"], False),
            "tsgate-influxdb1/src/main/Adapter.java": (["influxdb1"], True),
            "tsgate-influxdb3/src/test/Case.java": (["influxdb"], False),
            "tsgate-opengemini/src/test/docker/environment.py": ([], True),
            "README.md": ([], False),
        }
        for path, expected in cases.items():
            with self.subTest(path=path):
                result = scope.coverage([path])
                self.assertEqual(expected, (result["docker"], result["opengemini"]))
                self.assertFalse(result["full"])

    def test_core_dependency_workflow_and_mixed_starter_changes_cover_every_backend(self):
        for path in ("pom.xml", "tsgate-iotdb/pom.xml", "tsgate-core/src/main/X.java",
                     "tsgate-core/src/test/scripts/run-tests.py", "tsgate-bom/pom.xml", "LICENSE", "NOTICE",
                     ".github/workflows/ci.yml", "scripts/verify-release-artifacts.py",
                     "tsgate-influxdb1-spring-boot-starter/src/test/AllStartersIoTDBDockerIT.java",
                     "tsgate-iotdb-spring-boot-starter/src/main/Config.java", "tsgate-new/src/X.java"):
            with self.subTest(path=path):
                result = scope.coverage([path])
                self.assertEqual(list(scope.BACKENDS), result["docker"])
                self.assertTrue(result["opengemini"])

    def test_full_manual_and_version_tags_need_no_git_diff(self):
        git = Mock(side_effect=AssertionError("Git must not run"))
        for event, ref in (("workflow_dispatch", "refs/heads/main"), ("push", "refs/tags/v2.1.1")):
            self.assertTrue(scope.select(event, {}, "", ref, git)["full"])
        git.assert_not_called()

    def test_push_and_pull_request_use_argument_arrays_and_nul_delimited_paths(self):
        base, sha = "a" * 40, "b" * 40
        git = Mock(return_value=Mock(stdout=b"tsgate-influxdb1/src/quoted '$() name.java\0"))
        for event_name, event in (("push", {"before": base}),
                                  ("pull_request", {"pull_request": {"base": {"sha": base}}})):
            result = scope.select(event_name, event, sha, "refs/heads/main", git)
            self.assertEqual(["influxdb1"], result["docker"])
            self.assertTrue(result["opengemini"])
            command = git.call_args.args[0]
            self.assertEqual([base, sha, "--"], command[-3:])
            self.assertNotIn("shell", git.call_args.kwargs)

    def test_missing_zero_or_untrusted_commits_and_failed_diffs_fall_back_to_full(self):
        for base in (None, "", "0" * 40, "$(command)", "--output=/tmp/x"):
            with self.subTest(base=base):
                git = Mock()
                self.assertTrue(scope.select("push", {"before": base}, "b" * 40, "refs/heads/main", git)["full"])
                git.assert_not_called()
        git = Mock(side_effect=subprocess.CalledProcessError(1, ["git"]))
        self.assertTrue(scope.select("push", {"before": "a" * 40}, "b" * 40, "refs/heads/main", git)["full"])

    def test_output_only_contains_controlled_single_line_json(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            event = root / "event.json"
            event.write_text("{}")
            output, report = root / "outputs", root / "scope.json"
            self.assertEqual(0, scope.main(["--event-name", "workflow_dispatch", "--event-file", str(event),
                                           "--output", str(output), "--json", str(report)]))
            values = dict(line.split("=", 1) for line in output.read_text().splitlines())
            self.assertEqual(list(scope.BACKENDS), json.loads(values["docker"]))
            self.assertTrue(json.loads(values["full"]))
            self.assertTrue(json.loads(report.read_text())["opengemini"])


if __name__ == "__main__":
    unittest.main()
