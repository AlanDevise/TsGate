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
        return dict(mode="cluster", replicas=3, version="1.5.2", ready=True, stopped=False,
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
        for change in ({"mode": "unknown"}, {"version": "unknown"}, {"replicas": 1}, {"replicas": True}, {"owner": "foreign"},
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


class Binary141DatabaseReadinessTest(unittest.TestCase):
    """Exercise the official binary's anonymous logs without attributing a finish."""

    state = DatabaseReadinessTest.state
    docker = DatabaseReadinessTest.docker

    def record141(self, message, **fields):
        sites = {
            '[ASSIGN]init and start node': 'engine/engine_replication.go:83',
            'Start TransferLeadership': 'handler/transfer_leader.go:44',
            'try to transfer leadership finish': 'raftconn/node.go:313',
        }
        return json.dumps(dict(level='info', msg=message, repeated=1,
                               location=sites.get(message, 'unrelated/source.go:1')) | fields)

    def startup(self, **fields):
        return json.dumps(dict(level='info', msg='TSStore starting', version='1.4.1',
                               commit=environment.BINARY_141_COMMIT) | fields)

    def legacy_log(self, partition=0, database='owned_db'):
        return '\n'.join((self.startup(),
                          self.record141('[ASSIGN]init and start node', db=database, pt=partition),
                          self.record141('try to transfer leadership finish'))) + '\n'

    def test_version141_complete_ledger_requires_two_stable_read_snapshots(self):
        state = self.state() | {'version': '1.4.1'}
        logs = {name: self.legacy_log(partition) for name, partition in zip(state['containers'], (2, 0, 1))}
        with patch.object(environment, 'docker', side_effect=self.docker(logs)) as docker, \
                patch.object(environment.time, 'sleep') as sleep, \
                patch.object(environment, 'request') as request, patch.object(environment, 'save') as save:
            result = environment.wait_database(state, 'owned_db')
        self.assertEqual(9, docker.call_count)
        self.assertEqual(2, result['stable_observations'])
        self.assertEqual({name: [partition] for name, partition in zip(state['containers'], (2, 0, 1))},
                         result['partitions'])
        self.assertTrue(all(value['pending_transfers'] == 0 for value in result['transfer_ledgers'].values()))
        sleep.assert_called_once_with(0.5)
        request.assert_not_called()
        save.assert_not_called()

    def test_other_database_pending_initialization_cannot_be_covered_by_target_finish(self):
        log = self.legacy_log() + self.record141('[ASSIGN]init and start node', db='another_db', pt=0)
        partitions, ledger = environment.database_readiness_141(log, 'owned_db')
        self.assertEqual(set(), partitions)
        self.assertEqual(1, ledger['pending_transfers'])

    def test_external_transfer_is_counted_before_anonymous_finish(self):
        log = self.legacy_log() + self.record141('Start TransferLeadership', db='older_db', oldPt=0, newPt=1)
        self.assertEqual(set(), environment.database_readiness_141(log, 'owned_db')[0])
        log += '\n' + self.record141('try to transfer leadership finish')
        partitions, ledger = environment.database_readiness_141(log, 'owned_db')
        self.assertEqual({0}, partitions)
        self.assertEqual(2, ledger['started_transfers'])
        self.assertEqual(2, ledger['finished_transfers'])

    def test_repeated_counts_are_summed_without_assigning_anonymous_finishes(self):
        log = '\n'.join((self.startup(),
                         self.record141('[ASSIGN]init and start node', db='older_db', pt=0, repeated=3),
                         self.record141('[ASSIGN]init and start node', db='owned_db', pt=0),
                         self.record141('try to transfer leadership finish', repeated=4)))
        partitions, ledger = environment.database_readiness_141(log, 'owned_db')
        self.assertEqual({0}, partitions)
        self.assertEqual(4, ledger['finished_transfers'])

    def test_target_database_requires_an_explicit_unambiguous_initialization(self):
        self.assertEqual(set(), environment.database_readiness_141(self.legacy_log(database='owned_db_extra'), 'owned_db')[0])
        for extra in (self.record141('[ASSIGN]init and start node', db='owned_db', pt=0),
                      self.record141('[ASSIGN]init and start node', db='owned_db', pt=1)):
            with self.subTest(extra=extra), self.assertRaisesRegex(RuntimeError, 'Ambiguous'):
                environment.database_readiness_141(self.legacy_log() + extra, 'owned_db')
        log = self.legacy_log().replace('"pt": 0', '"pt": 0, "repeated": 2')
        with self.assertRaisesRegex(RuntimeError, 'Ambiguous'):
            environment.database_readiness_141(log, 'owned_db')

    def test_missing_restart_wrong_version_or_commit_startup_cannot_pass(self):
        valid = self.legacy_log()
        for log in ('\n'.join(valid.splitlines()[1:]), valid + self.startup(),
                    valid.replace(environment.BINARY_141_COMMIT, '0' * 40),
                    valid.replace('"version": "1.4.1"', '"version": "1.5.2"')):
            with self.subTest(log=log), self.assertRaises(RuntimeError):
                environment.database_readiness_141(log, 'owned_db')

    def test_malformed_or_unbalanced_logs_cannot_pass(self):
        for log in (self.legacy_log() + '{"msg":', self.legacy_log() + '[]',
                    self.legacy_log() + self.record141('try to transfer leadership finish')):
            with self.subTest(log=log), self.assertRaises(RuntimeError):
                environment.database_readiness_141(log, 'owned_db')

    def test_known_initialization_or_transfer_failures_cannot_pass(self):
        for failure in ('[ASSIGN]InitAndStartNode failed', 'try to transfer leadership timeout',
                        'startRaftNode transferLeaderShip fail', 'TransferLeadership fail'):
            with self.subTest(failure=failure), self.assertRaisesRegex(RuntimeError, 'lifecycle failure'):
                environment.database_readiness_141(self.legacy_log() + self.record141(failure), 'owned_db')

    def test_event_site_level_and_positive_integer_repeated_are_required(self):
        for change in ({'location': 'unexpected/node.go:313'}, {'level': 'error'},
                       {'repeated': True}, {'repeated': 0}, {'repeated': 1.0}, {'repeated': '1'}):
            record = self.record141('try to transfer leadership finish', **change)
            log = '\n'.join(self.legacy_log().splitlines()[:-1]) + '\n' + record
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                environment.database_readiness_141(log, 'owned_db')

    def test_new_pending_ledger_resets_previous_stable_snapshot(self):
        state = self.state() | {'version': '1.4.1'}
        logs = {name: self.legacy_log(i) for i, name in enumerate(state['containers'])}
        read = self.docker(logs)
        read_count = [0]
        def changing(*args, **kwargs):
            answer = read(*args, **kwargs)
            if args[0] != 'exec':
                return answer
            cycle = read_count[0] // 3
            read_count[0] += 1
            if cycle in (1, 2, 3):
                answer.stdout += self.record141('Start TransferLeadership', db='other_db', oldPt=0, newPt=1) + '\n'
                if cycle >= 2:
                    answer.stdout += self.record141('try to transfer leadership finish') + '\n'
            return answer
        with patch.object(environment, 'docker', side_effect=changing), patch.object(environment.time, 'sleep'):
            result = environment.wait_database(state, 'owned_db')
        self.assertEqual(12, read_count[0])
        self.assertEqual(2, result['stable_observations'])
        self.assertTrue(all(value['started_transfers'] == 2 for value in result['transfer_ledgers'].values()))


if __name__ == "__main__":
    unittest.main()
