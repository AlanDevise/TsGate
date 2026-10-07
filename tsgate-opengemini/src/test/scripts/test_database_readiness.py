# SPDX-License-Identifier: Apache-2.0
"""Read-only, exact-match data-Raft readiness checks for owned cluster fixtures."""

import importlib.util
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location(
    "opengemini_environment", Path(__file__).resolve().parents[1] / "docker/environment.py")
environment = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(environment)


class DatabaseReadinessTest(unittest.TestCase):
    def state(self):
        return dict(mode="cluster", replicas=3, ready=True, stopped=False,
                    owner="tsgate-og-unit", containers=["tsgate-og-unit-node" + str(i) for i in (1, 2, 3)])

    def record(self, partition, database="owned_db", message="try to transfer leadership finish"):
        return json.dumps(dict(msg=message, database=database, partition=partition))

    def docker(self, logs, label="tsgate-og-unit"):
        def run(*args, **kwargs):
            self.assertGreater(kwargs["timeout"], 0)
            self.assertLessEqual(kwargs["timeout"], 5)
            if args[0] == "inspect":
                return Mock(stdout=json.dumps([{"Config": {"Labels": {environment.LABEL: label}}}]))
            self.assertEqual(("exec", args[1], "cat", "/var/log/opengemini/store.log"), args)
            return Mock(stdout=logs[args[1]])
        return run

    def test_parser_requires_exact_database_message_and_integer_partition(self):
        lines = ["not JSON", "[]", self.record(0, database="owned_db_extra"),
                 self.record(0, message="try to transfer leadership finish extra"),
                 self.record(True), self.record(1.0), self.record(3), self.record("1"),
                 self.record(2), self.record(2)]
        self.assertEqual({2}, environment.database_ready_partitions("\n".join(lines), "owned_db"))

    def test_each_owned_node_may_have_any_distinct_partition_and_only_reads_are_issued(self):
        state = self.state()
        logs = {name: self.record(partition) for name, partition in zip(state["containers"], (2, 0, 1))}
        with patch.object(environment, "docker", side_effect=self.docker(logs)) as docker, \
                patch.object(environment, "request") as request, patch.object(environment, "save") as save:
            result = environment.wait_database(state, "owned_db")
        self.assertEqual("ready", result["status"])
        self.assertEqual({name: [partition] for name, partition in zip(state["containers"], (2, 0, 1))},
                         result["partitions"])
        self.assertEqual(6, docker.call_count)
        request.assert_not_called()
        save.assert_not_called()

    def test_different_owner_is_rejected_before_reading_any_log(self):
        with patch.object(environment, "docker", side_effect=self.docker({}, "another-owner")) as docker:
            with self.assertRaisesRegex(RuntimeError, "ownership label"):
                environment.wait_database(self.state(), "owned_db")
        self.assertEqual(1, docker.call_count)

    def test_invalid_cluster_states_fail_without_docker_or_writes(self):
        for change in ({"mode": "unknown"}, {"replicas": 1}, {"replicas": True}, {"owner": "foreign"},
                       {"containers": ["tsgate-og-unit-node1"] * 3}, {"ready": False}, {"stopped": True}):
            with self.subTest(change=change), patch.object(environment, "docker") as docker:
                with self.assertRaises(RuntimeError):
                    environment.wait_database(self.state() | change, "owned_db")
                docker.assert_not_called()

    def test_single_node_is_an_explicit_skip_without_docker(self):
        with patch.object(environment, "docker") as docker:
            result = environment.wait_database(self.state() | {"mode": "single", "replicas": 1}, "owned_db")
        self.assertEqual("skipped", result["status"])
        docker.assert_not_called()

    def test_duplicate_partition_across_nodes_cannot_pass(self):
        state = self.state()
        logs = {name: self.record(partition) for name, partition in zip(state["containers"], (0, 0, 2))}
        with patch.object(environment, "docker", side_effect=self.docker(logs)):
            with self.assertRaisesRegex(RuntimeError, "more than one owned node"):
                environment.wait_database(state, "owned_db")

    def test_multiple_partitions_on_one_node_are_ambiguous(self):
        state = self.state()
        logs = {state["containers"][0]: self.record(0) + "\n" + self.record(1)}
        with patch.object(environment, "docker", side_effect=self.docker(logs)):
            with self.assertRaisesRegex(RuntimeError, "Ambiguous"):
                environment.wait_database(state, "owned_db")

    def timeout(self, logs, read_timeout=False):
        clock = [0]
        def sleep(seconds):
            clock[0] += seconds
        run = self.docker(logs)
        if read_timeout:
            def run(*args, **kwargs):
                if args[0] == "inspect":
                    return self.docker(logs)(*args, **kwargs)
                raise subprocess.TimeoutExpired(args, kwargs["timeout"])
        with patch.object(environment.time, "monotonic", side_effect=lambda: clock[0]), \
                patch.object(environment.time, "sleep", side_effect=sleep), \
                patch.object(environment, "docker", side_effect=run) as docker, \
                patch.object(environment, "request") as request:
            with self.assertRaisesRegex(RuntimeError, "readiness timeout"):
                environment.wait_database(self.state(), "owned_db", timeout=1)
        self.assertEqual(1, clock[0])
        request.assert_not_called()
        self.assertTrue(all(call.args[0] in ("inspect", "exec") for call in docker.call_args_list))

    def test_missing_partition_times_out_without_writing(self):
        state = self.state()
        self.timeout({name: self.record(i) if i < 2 else "" for i, name in enumerate(state["containers"])})

    def test_wrong_database_records_cannot_satisfy_readiness(self):
        self.timeout({name: self.record(i, database="another_db")
                      for i, name in enumerate(self.state()["containers"])})

    def test_log_reads_have_bounded_timeouts(self):
        self.timeout({}, read_timeout=True)


if __name__ == "__main__":
    unittest.main()
