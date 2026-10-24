"""Validate current Docker suites and capability cases against the registered source plan."""

from pathlib import Path
import re
import shutil
import xml.etree.ElementTree as ET

COUNTERS = ("tests", "failures", "errors", "skipped")


def combine_plans(plans):
    required, allowed = {}, {}
    for plan in plans:
        for entry in plan["allowedSuites"]:
            allowed[(entry["module"], entry["name"])] = entry
        for entry in plan["requiredSuites"]:
            identity = (entry["module"], entry["name"])
            previous = required.setdefault(identity, dict(entry, requiredCases=list(entry.get("requiredCases", []))))
            previous["requiredCases"] = sorted(set(previous["requiredCases"]) | set(entry.get("requiredCases", [])))
    return dict(requiredSuites=list(required.values()), allowedSuites=list(allowed.values()))


def collect_reports(root, output, started_ns, plan):
    root, output = Path(root), Path(output)
    allowed = {(entry["module"], entry["name"]) for entry in plan["allowedSuites"]}
    modules = {module for module, _ in allowed}
    result = dict(totals=dict.fromkeys(COUNTERS, 0), suites=[], staleReports=[], unexpectedReports=[], ignoredReports=[])
    for report in sorted(root.glob("*/target/failsafe-reports/TEST-*.xml")):
        module = report.parents[2].name
        relative = str(report.relative_to(root))
        if module not in modules:
            result["ignoredReports"].append(relative)
            continue
        if report.stat().st_mtime_ns < started_ns:
            result["staleReports"].append(relative)
            continue
        xml = ET.parse(report).getroot()
        name = xml.get("name")
        if (xml.tag != "testsuite" or (module, name) not in allowed
                or report.name != "TEST-" + str(name) + ".xml"):
            result["unexpectedReports"].append(relative)
            continue
        counters = {key: int(xml.get(key, "0")) for key in COUNTERS}
        if any(value < 0 for value in counters.values()):
            raise RuntimeError("Negative Docker test counter: " + relative)
        cases = xml.findall("testcase")
        actual = dict(tests=len(cases), failures=sum(case.find("failure") is not None for case in cases),
                      errors=sum(case.find("error") is not None for case in cases),
                      skipped=sum(case.find("skipped") is not None for case in cases))
        if actual != counters:
            raise RuntimeError("Docker test counters do not match testcase results: " + relative)
        if any(case.get("classname", name) != name for case in cases):
            raise RuntimeError("Docker testcase class differs from its suite: " + relative)
        destination = output / "reports" / module / "failsafe-reports"
        destination.mkdir(parents=True, exist_ok=True)
        shutil.copy2(report, destination / report.name)
        for key in COUNTERS:
            result["totals"][key] += counters[key]
        result["suites"].append(dict(module=module, name=name, report=str(destination / report.name),
                                    cases=[re.split(r"[\[(]", case.get("name", ""), maxsplit=1)[0] for case in cases],
                                    **counters))
    return result


def validate_reports(result, plan):
    if result["staleReports"]:
        raise RuntimeError("Rejected stale Docker reports: " + ", ".join(result["staleReports"]))
    if result["unexpectedReports"]:
        raise RuntimeError("Unexpected Docker report module or class: " + ", ".join(result["unexpectedReports"]))
    actual = {}
    allowed = {(entry["module"], entry["name"]) for entry in plan["allowedSuites"]}
    for suite in result["suites"]:
        identity = (suite["module"], suite["name"])
        if identity in actual or identity not in allowed:
            raise RuntimeError("Duplicate or unselected Docker suite: " + str(identity))
        if suite["tests"] <= 0 or any(suite[key] for key in ("failures", "errors", "skipped")):
            raise RuntimeError("Empty, failing or skipped Docker suite: " + str(identity))
        actual[identity] = suite
    for entry in plan["requiredSuites"]:
        identity = (entry["module"], entry["name"])
        if identity not in actual:
            raise RuntimeError("Missing required Docker suite: " + str(identity))
        missing = set(entry.get("requiredCases", [])) - set(actual[identity].get("cases", []))
        if missing:
            raise RuntimeError("Missing real capability cases in " + str(identity) + ": " + ", ".join(sorted(missing)))
    if result["totals"]["tests"] <= 0:
        raise RuntimeError("Missing Docker tests")
