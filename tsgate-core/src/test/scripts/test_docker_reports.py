"""Current Docker report identity and capability evidence checks need no Maven or database."""

import copy
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

SPEC = importlib.util.spec_from_file_location("docker_reports", Path(__file__).with_name("docker-reports.py"))
REPORTS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORTS)


class DockerReportTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.adapter = dict(module="adapter", name="example.AdapterDockerIT", requiredCases=["capabilitySort"])
        self.starter = dict(module="starter", name="example.StarterDockerIT", requiredCases=[])
        self.plan = dict(requiredSuites=[self.adapter, self.starter],
                         allowedSuites=[dict(module=entry["module"], name=entry["name"])
                                        for entry in (self.adapter, self.starter)])

    def report(self, entry, cases=None, timestamp=200, state=None, filename=None, classname=None):
        names = cases if cases is not None else entry.get("requiredCases") or ["roundTrip"]
        counters = dict(tests=len(names), failures=int(state == "failure"), errors=int(state == "error"), skipped=int(state == "skipped"))
        suite = ET.Element("testsuite", name=entry["name"], **{key: str(value) for key, value in counters.items()})
        for index, name in enumerate(names):
            case = ET.SubElement(suite, "testcase", name=name, classname=classname or entry["name"])
            if index == 0 and state:
                ET.SubElement(case, state)
        path = self.root / entry["module"] / "target/failsafe-reports" / (filename or "TEST-" + entry["name"] + ".xml")
        path.parent.mkdir(parents=True, exist_ok=True)
        ET.ElementTree(suite).write(path, encoding="unicode")
        os.utime(path, ns=(timestamp, timestamp))
        return path

    def collect(self):
        return REPORTS.collect_reports(self.root, self.root / "result", 100, self.plan)

    def test_current_exact_suites_and_capability_cases_pass(self):
        self.report(self.adapter)
        self.report(self.starter)
        result = self.collect()
        REPORTS.validate_reports(result, self.plan)
        self.assertEqual(2, result["totals"]["tests"])
        self.assertEqual(["capabilitySort"], result["suites"][0]["cases"])

    def test_starter_or_adapter_alone_cannot_satisfy_the_other_required_suite(self):
        for entry in (self.adapter, self.starter):
            with self.subTest(entry=entry):
                report = self.report(entry)
                with self.assertRaisesRegex(RuntimeError, "Missing required Docker suite"):
                    REPORTS.validate_reports(self.collect(), self.plan)
                report.unlink()

    def test_old_reports_are_not_counted_and_cannot_be_used_with_a_fresh_suite(self):
        self.report(self.adapter, timestamp=99)
        self.report(self.starter)
        result = self.collect()
        self.assertEqual(1, result["totals"]["tests"])
        with self.assertRaisesRegex(RuntimeError, "stale"):
            REPORTS.validate_reports(result, self.plan)

    def test_other_backend_reports_never_supply_the_missing_adapter(self):
        self.report(self.starter)
        self.report(dict(module="other-backend", name=self.adapter["name"], requiredCases=["capabilitySort"]))
        result = self.collect()
        self.assertEqual(1, result["totals"]["tests"])
        self.assertEqual(1, len(result["ignoredReports"]))
        with self.assertRaisesRegex(RuntimeError, "Missing required"):
            REPORTS.validate_reports(result, self.plan)

    def test_module_class_filename_and_testcase_class_must_agree(self):
        self.report(self.starter)
        scenarios = (
            (dict(self.adapter, module="starter"), {}, "Unexpected"),
            (dict(self.adapter, name="example.OtherDockerIT"), {}, "Unexpected"),
            (self.adapter, dict(filename="TEST-renamed.xml"), "Unexpected"),
            (self.adapter, dict(classname="example.OtherDockerIT"), "class differs"),
        )
        for entry, options, message in scenarios:
            with self.subTest(options=options, entry=entry):
                report = self.report(entry, **options)
                with self.assertRaisesRegex(RuntimeError, message):
                    REPORTS.validate_reports(self.collect(), self.plan)
                report.unlink()

    def test_empty_failed_errored_and_skipped_suites_are_rejected(self):
        self.report(self.starter)
        for state in (None, "failure", "error", "skipped"):
            with self.subTest(state=state):
                self.report(self.adapter, cases=[] if state is None else None, state=state)
                with self.assertRaisesRegex(RuntimeError, "Empty, failing or skipped"):
                    REPORTS.validate_reports(self.collect(), self.plan)

    def test_nonempty_adapter_without_required_capability_case_fails(self):
        self.report(self.adapter, cases=["someOtherPassingTest"])
        self.report(self.starter)
        with self.assertRaisesRegex(RuntimeError, "Missing real capability cases"):
            REPORTS.validate_reports(self.collect(), self.plan)

    def test_duplicate_suites_and_counter_only_reports_are_rejected(self):
        self.report(self.adapter)
        self.report(self.starter)
        result = self.collect()
        result["suites"].append(copy.deepcopy(result["suites"][0]))
        with self.assertRaisesRegex(RuntimeError, "Duplicate"):
            REPORTS.validate_reports(result, self.plan)
        report = self.root / "adapter/target/failsafe-reports" / ("TEST-" + self.adapter["name"] + ".xml")
        report.write_text('<testsuite name="example.AdapterDockerIT" tests="1"/>')
        os.utime(report, ns=(200, 200))
        with self.assertRaisesRegex(RuntimeError, "counters do not match"):
            self.collect()

    def test_combining_backend_plans_deduplicates_shared_suites_and_keeps_required_cases(self):
        other = copy.deepcopy(self.plan)
        other["requiredSuites"][0]["requiredCases"].append("capabilityCursor")
        combined = REPORTS.combine_plans([self.plan, other])
        self.assertEqual(2, len(combined["requiredSuites"]))
        self.assertEqual(["capabilityCursor", "capabilitySort"], combined["requiredSuites"][0]["requiredCases"])
        self.assertEqual(["capabilitySort"], self.plan["requiredSuites"][0]["requiredCases"])


if __name__ == "__main__":
    unittest.main()
