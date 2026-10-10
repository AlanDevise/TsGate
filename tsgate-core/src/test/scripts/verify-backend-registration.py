#!/usr/bin/env python3
"""Cross-check backend integration registrations without building or starting databases.

The test-only manifest records identities and deployment entry points, not capability
values. XML/CSV/Spring resources and Java declarations are read structurally. CI
selection and the final workflow gate are exercised with in-memory inputs. This
checks registration completeness, not database compatibility or runtime behavior.
"""

import argparse
import ast
import contextlib
import csv
import fnmatch
import io
import json
import os
from pathlib import Path
import re
import textwrap
import types
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[4]
MANIFEST = "tsgate-core/src/test/resources/ci/backend-registration.json"
CAPABILITIES = "tsgate-core/src/test/resources/contracts/adapter-capabilities.csv"
CONDITION = "tsgate-core/src/main/java/com/alandevise/tsgate/config/TSDBAdapterEnabledCondition.java"
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
TOKEN = re.compile(r'//[^\n]*|/\*[\s\S]*?\*/|"""[\s\S]*?"""|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|[A-Za-z_$][\w$]*|\S')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def file_at(root, name):
    path = root / name
    require(not Path(name).is_absolute() and ".." not in Path(name).parts
            and path.resolve().is_relative_to(root.resolve()) and path.is_file(),
            "Missing or unsafe registration input: " + str(name))
    return path


def load_script(path):
    # Loading these trusted test scripts must not invoke their guarded CLI entry points.
    module = types.ModuleType("registration_probe")
    module.__file__ = str(path)
    exec(compile(path.read_text(encoding="utf-8"), str(path), "exec"), module.__dict__)
    return module


def tokens(source):
    return [token for token in TOKEN.findall(source) if not token.startswith(("//", "/*"))]


def body_end(items, start):
    depth = 1
    for index in range(start + 1, len(items)):
        if items[index] == "{":
            depth += 1
        elif items[index] == "}":
            depth -= 1
        if depth == 0:
            return index
    raise ValueError("Unbalanced Java declaration")


def declarations(path):
    items = tokens(path.read_text(encoding="utf-8"))
    package, imports, wildcard_imports, result = "", {}, set(), []
    index, prefix = 0, 0
    while index < len(items):
        token = items[index]
        if token in ("package", "import"):
            end = items.index(";", index)
            name = "".join(items[index + 1:end])
            if token == "package":
                package = name
            elif not name.startswith("static"):
                if name.endswith(".*"):
                    wildcard_imports.add(name[:-2])
                else:
                    imports[name.rsplit(".", 1)[-1]] = name
            index, prefix = end + 1, end + 1
            continue
        if token in ("class", "interface", "enum", "record") and index + 1 < len(items) and (index == 0 or items[index - 1] != "."):
            name = items[index + 1]
            opening = items.index("{", index)
            end = body_end(items, opening)
            result.append(dict(name=package + "." + name, simple=name, package=package,
                               imports=imports.copy(), wildcard_imports=tuple(wildcard_imports),
                               kind=token, signature=items[prefix:opening],
                               body=items[opening + 1:end], path=path))
            index, prefix = end + 1, end + 1
        else:
            index += 1
    return result


def resolve(declaration, name):
    return name if "." in name else declaration["imports"].get(name, declaration["package"] + "." + name)


def parents(declaration):
    signature = declaration["signature"]
    result = []
    for marker in ("extends", "implements"):
        if marker not in signature:
            continue
        index = signature.index(marker) + 1
        while index < len(signature) and signature[index] not in ("implements", "permits"):
            start = index
            while index < len(signature) and (re.fullmatch(r"[\w$]+", signature[index]) or signature[index] == "."):
                index += 1
            if index > start:
                result.append(resolve(declaration, "".join(signature[start:index])))
            if index < len(signature) and signature[index] == ",":
                index += 1
            else:
                break
    return result


def type_for(inventory, name, module=None):
    if (module, name) in inventory:
        return inventory[(module, name)]
    candidates = [value for (owner, identity), value in inventory.items() if identity == name]
    return candidates[0] if len(candidates) == 1 else None


def implements(name, target, inventory, module=None, seen=None):
    if name == target:
        return True
    declaration = type_for(inventory, name, module)
    seen = set() if seen is None else seen
    if declaration is None or (declaration["module"], name) in seen:
        return False
    seen.add((declaration["module"], name))
    return any(implements(parent, target, inventory, declaration["module"], seen)
               for parent in parents(declaration))


def has_test(declaration, inventory, seen=None):
    seen = set() if seen is None else seen
    identity = (declaration["module"], declaration["name"])
    if identity in seen:
        return False
    seen.add(identity)
    if any(has_sequence(method["header"], ["@", annotation]) for method in methods(declaration)
           for annotation in ("Test", "ParameterizedTest", "RepeatedTest", "TestFactory", "TestTemplate")):
        return True
    return any(parent is not None and has_test(parent, inventory, seen) for parent in
               (type_for(inventory, name, declaration["module"]) for name in parents(declaration)))


def methods(declaration):
    items, start, index, result = declaration["body"], 0, 0, []
    while index < len(items):
        if items[index] == "{":
            header, depth, openings = items[start:index], 0, []
            for position, token in enumerate(header):
                if token == "(":
                    if depth == 0:
                        openings.append(position)
                    depth += 1
                elif token == ")":
                    depth -= 1
            end = body_end(items, index)
            if openings and openings[-1] > 0:
                opening = openings[-1]
                result.append(dict(name=header[opening - 1], header=header,
                                   body=items[index + 1:end]))
            index, start = end + 1, end + 1
        else:
            index += 1
    return result


def has_concrete_count(name, inventory, module=None, seen=None):
    """Require the exact SPI operation on a class, including a source-visible superclass."""
    declaration = type_for(inventory, name, module)
    if declaration is None or declaration["kind"] != "class":
        return False
    identity = (declaration["module"], name)
    seen = set() if seen is None else seen
    if identity in seen:
        return False
    seen.add(identity)
    for method in methods(declaration):
        if method["name"] != "count":
            continue
        header = method["header"]
        signature = next((index for index in range(len(header) - 1)
                          if header[index:index + 2] == ["count", "("]), None)
        if signature is None or signature == 0 or header[signature - 1] != "long":
            continue
        if "public" not in header[:signature] or "static" in header[:signature]:
            continue
        end = header.index(")", signature + 2)
        parameters, start = [], signature + 2
        for index in range(start, end + 1):
            if index == end or header[index] == ",":
                parameter = [token for token in header[start:index] if token != "final"]
                parameters.append("".join(parameter[:-1]))
                start = index + 1
        if len(parameters) != 2 or parameters[0] not in ("String", "java.lang.String"):
            continue
        query_type = resolve(declaration, parameters[1])
        if parameters[1] == "TSDBQuery" and "TSDBQuery" not in declaration["imports"] and (
                "com.alandevise.tsgate.model" in declaration["wildcard_imports"]):
            query_type = "com.alandevise.tsgate.model.TSDBQuery"
        if query_type != "com.alandevise.tsgate.model.TSDBQuery":
            continue
        if has_sequence(method["body"], ["TSDBAdapter", ".", "super", ".", "count", "("]):
            return False
        return True
    return any(has_concrete_count(parent, inventory, declaration["module"], seen)
               for parent in parents(declaration))


def method_body(declaration, name):
    found = [method["body"] for method in methods(declaration) if method["name"] == name]
    require(len(found) == 1, "Missing or ambiguous Java method: " + name)
    return found[0]


def bean_returns(declaration, target):
    return any(has_sequence(method["header"], ["@", "Bean"])
               and any(method["header"][offset + 1] == method["name"]
                       and resolve(declaration, method["header"][offset]) == target
                       for offset in range(len(method["header"]) - 1))
               for method in methods(declaration))


def has_sequence(items, expected):
    return any(items[index:index + len(expected)] == expected for index in range(len(items)))


def java_inventory(root, modules, source):
    inventory = {}
    for module in modules:
        for path in sorted((root / module / "src" / source / "java").rglob("*.java")):
            for declaration in declarations(path):
                require((module, declaration["name"]) not in inventory, "Duplicate Java type in module: " + declaration["name"])
                declaration["module"] = module
                inventory[(module, declaration["name"])] = declaration
    return inventory


def workflow_jobs(path):
    # Parse the workflow's explicit, two-space job mapping. Unsupported structures fail closed.
    source = path.read_text(encoding="utf-8")
    require(re.search(r"^jobs:\s*$", source, re.M), "Workflow jobs mapping is missing")
    source = source.split("\njobs:", 1)[1]
    matches = list(re.finditer(r"^  ([A-Za-z_][\w-]*):\s*$", source, re.M))
    return {match.group(1): source[match.end():matches[index + 1].start() if index + 1 < len(matches) else len(source)]
            for index, match in enumerate(matches)}


def run_blocks(block):
    lines = block.splitlines()
    start = next((index for index, line in enumerate(lines) if line.strip() == "steps:" and line.startswith("    steps:")), None)
    if start is None:
        return []
    result, index = [], start + 1
    while index < len(lines):
        line = lines[index]
        if line.lstrip().startswith("#"):
            index += 1
            continue
        if line.strip() and len(line) - len(line.lstrip()) <= 4:
            break
        match = re.fullmatch(r"(?:      - |        )run:\s*(.*)", line)
        if not match:
            index += 1
            continue
        step_start = max((offset for offset in range(start + 1, index + 1)
                          if lines[offset].startswith("      - ")), default=index)
        step_end = next((offset for offset in range(index + 1, len(lines))
                         if lines[offset].startswith("      - ")), len(lines))
        conditional = any(re.match(r"^(?:      - |        )if:", item) for item in lines[step_start:step_end])
        value = match.group(1)
        if value in ("|", "|-", "|+", ">", ">-", ">+"):
            end = index + 1
            while end < len(lines) and (not lines[end].strip() or len(lines[end]) - len(lines[end].lstrip()) > 8):
                end += 1
            if not conditional:
                result.append(textwrap.dedent("\n".join(lines[index + 1:end])))
            index = end
        else:
            if not conditional:
                result.append(value)
            index += 1
    return result


def python_blocks(block):
    lines, result, index = block.splitlines(), [], 0
    while index < len(lines):
        line = lines[index]
        match = re.search(r"<<\s*['\"]?([A-Za-z_][\w]*)['\"]?\s*$", line)
        if match:
            end = next((end for end in range(index + 1, len(lines)) if lines[end].strip() == match.group(1)), None)
            require(end is not None, "Unclosed workflow heredoc")
            if re.match(r"^\s*python3?\s+-\s*<<", line):
                result.append(textwrap.dedent("\n".join(lines[index + 1:end])))
            index = end + 1
        else:
            index += 1
    return result


def run_scripts_in_step(block):
    """Recognize direct execution only; unused definitions and display heredocs do not count."""
    scripts, lines, index = set(), block.splitlines(), 0
    while index < len(lines):
        line = lines[index]
        match = re.match(r"^\s*python3?\s+([\w./-]+\.py)(?:\s|$)", line)
        if match:
            scripts.add(match.group(1))
        heredoc = re.search(r"<<-?\s*['\"]?([A-Za-z_][\w]*)['\"]?\s*$", line)
        if heredoc:
            end = next((end for end in range(index + 1, len(lines)) if lines[end].strip() == heredoc.group(1)), None)
            require(end is not None, "Unclosed workflow heredoc")
            index = end + 1
        else:
            index += 1
    for code in python_blocks(block):
        tree = ast.parse(code)
        assigned = {target.id: node.value for node in tree.body if isinstance(node, ast.Assign)
                    for target in node.targets if isinstance(target, ast.Name)}
        for statement in tree.body:
            node = statement.value if isinstance(statement, (ast.Expr, ast.Assign)) else None
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name) and node.func.value.id == "subprocess"
                    and node.func.attr in ("run", "check_call", "check_output") and node.args):
                continue
            argument = node.args[0]
            argument = assigned.get(argument.id, argument) if isinstance(argument, ast.Name) else argument
            if isinstance(argument, (ast.List, ast.Tuple)):
                scripts.update(item.value for item in argument.elts if isinstance(item, ast.Constant)
                               and isinstance(item.value, str) and item.value.endswith(".py"))
    return scripts


def run_scripts(block):
    return set().union(*(run_scripts_in_step(step) for step in run_blocks(block)))


def run_gate(code, checks, commit):
    previous = {key: os.environ.get(key) for key in ("CHECKS", "EXPECTED_SHA")}
    os.environ.update(CHECKS=json.dumps(checks), EXPECTED_SHA=commit)
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            exec(compile(code, "workflow-final-gate", "exec"), {"__name__": "registration_gate_probe"})
    except SystemExit as error:
        return error.code in (None, 0)
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    return True


def verify(root=ROOT):
    root = Path(root).resolve()
    registration = json.loads(file_at(root, MANIFEST).read_text(encoding="utf-8"))
    require(registration.get("schemaVersion") == 1, "Unsupported backend registration schema")
    rows = registration.get("backends", [])
    require(isinstance(rows, list) and rows, "Backend registrations must not be empty")
    ids = set()
    for row in rows:
        require(isinstance(row, dict) and all(isinstance(row.get(key), str) and row[key]
                for key in ("id", "module", "adapter", "enableId", "condition", "properties", "starter", "autoConfiguration")),
                "Incomplete backend identity")
        require(row["id"] not in ids, "Duplicate backend identity: " + row["id"])
        ids.add(row["id"])
    project = ET.parse(file_at(root, "pom.xml")).getroot()
    modules = [node.text.strip() for node in project.findall("m:modules/m:module", NS)]
    require(len(modules) == len(set(modules)), "Duplicate reactor module")
    poms = {module: ET.parse(file_at(root, module + "/pom.xml")).getroot() for module in modules}
    artifacts = {module: pom.findtext("m:artifactId", namespaces=NS) for module, pom in poms.items()}
    require(len(set(artifacts.values())) == len(artifacts), "Duplicate reactor artifact identity")
    jar_modules = {module for module, pom in poms.items() if pom.findtext("m:packaging", "jar", NS) == "jar"}
    production = java_inventory(root, modules, "main")
    tests = java_inventory(root, modules, "test")
    actual = {(declaration["module"], name) for (module, name), declaration in production.items()
              if declaration["kind"] == "class" and "abstract" not in declaration["signature"]
              and implements(name, "com.alandevise.tsgate.adapter.TSDBAdapter", production, module)}
    expected = {(row["module"], row["adapter"]) for row in rows}
    require(len(expected) == len(rows) and actual == expected,
            "Concrete SPI adapters differ from backend registrations: " + str(sorted(actual ^ expected)))
    with file_at(root, CAPABILITIES).open(encoding="utf-8", newline="") as source:
        capability_rows = list(csv.DictReader(source))
    require(len(capability_rows) == len(rows) and {row.get("backend") for row in capability_rows} == ids,
            "Capability matrix and backend registrations differ")
    capabilities = {row["backend"]: row for row in capability_rows}
    bom = ET.parse(file_at(root, "tsgate-bom/pom.xml")).getroot()
    managed = {(node.findtext("m:groupId", namespaces=NS), node.findtext("m:artifactId", namespaces=NS))
               for node in bom.findall("m:dependencyManagement/m:dependencies/m:dependency", NS)}
    root_group = project.findtext("m:groupId", namespaces=NS)
    require(all((root_group, artifacts[module]) in managed for module in jar_modules),
            "BOM does not manage every reactor JAR artifact")
    condition = declarations(file_at(root, CONDITION))[0]
    condition_body = condition["body"]
    activation = {}
    enabled_body = method_body(condition, "enabledAdapters")
    for index, token in enumerate(enabled_body):
        if token == "addIfEnabled" and enabled_body[index + 1:index + 5] == ["(", "context", ",", "enabled"]:
            arguments = enabled_body[index:index + 11]
            require(len(arguments) == 11 and arguments[5] == "," and arguments[7] == ","
                    and arguments[9:] == [")", ";"], "Unsupported adapter enablement call")
            enable, adapter = json.loads(arguments[6]), json.loads(arguments[8])
            require(enable not in activation, "Duplicate adapter enablement registration: " + enable)
            activation[enable] = adapter
    require(activation == {row["enableId"]: row["adapter"] for row in rows},
            "Production enablement inventory differs from backend registrations")
    scope = load_script(file_at(root, "tsgate-core/src/test/scripts/ci-scope.py"))
    runner = load_script(file_at(root, "tsgate-core/src/test/scripts/run-tests.py"))
    servers = json.loads(file_at(root, "tsgate-core/src/test/resources/ci/docker-servers.json").read_text())
    require(len(servers) == len({server["kind"] for server in servers}), "Duplicate Docker backend server")
    workflow = workflow_jobs(file_at(root, ".github/workflows/ci.yml"))
    require("release-gate" in workflow, "Final CI gate is missing")
    gate = workflow["release-gate"]
    needs = re.search(r"^    needs:\s*\[([^\]]+)\]\s*$", gate, re.M)
    require(needs is not None, "Unsupported final gate dependency mapping")
    gate_needs = {name.strip().strip("'\"") for name in needs.group(1).split(",")}
    gate_code = [code for step in run_blocks(gate) for code in python_blocks(step)]
    require(len(gate_code) == 1, "Final CI gate must expose one executable Python check")
    full = scope.coverage([], full=True)
    selected_jobs, representative, matrix_selectors = set(), set(), set()
    for row in rows:
        identity = row["id"]
        require(has_concrete_count(row["adapter"], production, row["module"]),
                identity + ": adapter must implement count(String, TSDBQuery) instead of the SPI row-materializing fallback")
        require(row["module"] in jar_modules and row["starter"] in jar_modules,
                identity + ": adapter/starter must be reactor JAR artifacts")
        profile = capabilities[identity]
        require(profile.get("module") == row["module"], identity + ": capability module differs")
        contract = profile.get("contractTest")
        contract_type = type_for(tests, contract, row["module"])
        require(contract_type is not None and contract_type["module"] == row["module"]
                and "abstract" not in contract_type["signature"]
                and implements(contract, "com.alandevise.tsgate.contract.SharedAdapterContract", tests, row["module"])
                and has_test(contract_type, tests),
                identity + ": shared contract is missing or not inherited")
        nested = row["condition"]
        require(has_sequence(condition_body, ["class", nested, "implements", "Condition", "{"]),
                identity + ": backend enablement condition is missing")
        start = next(index for index in range(len(condition_body))
                     if condition_body[index:index + 2] == ["class", nested])
        opening = condition_body.index("{", start)
        nested_body = condition_body[opening + 1:body_end(condition_body, opening)]
        matches_body = method_body({"body": nested_body}, "matches")
        require(has_sequence(matches_body, ["return", "enabledAdapters", "(", "context", ")", ".", "contains", "(", json.dumps(row["enableId"]), ")", ";"]),
                identity + ": enablement condition selects a different backend")
        properties = type_for(production, row["properties"], row["module"]) or {}
        require(has_sequence(properties.get("signature", []), ["@", "ConfigurationProperties", "(", "prefix", "=", json.dumps("tsdb." + row["enableId"]), ")"]),
                identity + ": configuration properties prefix differs")
        auto = type_for(production, row["autoConfiguration"], row["starter"]) or {}
        require(auto.get("module") == row["starter"] and auto.get("kind") == "class",
                identity + ": starter auto-configuration is missing")
        require(bean_returns(auto, row["adapter"]), identity + ": starter adapter bean is missing")
        require(bean_returns(auto, "com.alandevise.tsgate.core.TGTemplate"), identity + ": starter template bean is missing")
        require(bean_returns(auto, row["properties"]) or has_sequence(auto["signature"],
                ["@", "EnableConfigurationProperties", "(", "{", properties.get("simple", ""), ".", "class"]),
                identity + ": starter configuration properties are not bound")
        require(has_sequence(auto["signature"], ["@", "Conditional", "(", "TSDBAdapterEnabledCondition", ".", nested, ".", "class", ")"]),
                identity + ": starter does not use its backend enablement condition")
        imports = file_at(root, row["starter"] + "/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports")
        imported = [line.strip() for line in imports.read_text().splitlines() if line.strip() and not line.lstrip().startswith("#")]
        require(imported.count(row["autoConfiguration"]) == 1, identity + ": Spring auto-configuration import is missing or duplicated")
        dependencies = {(node.findtext("m:groupId", namespaces=NS), node.findtext("m:artifactId", namespaces=NS))
                        for node in poms[row["starter"]].findall("m:dependencies/m:dependency", NS)
                        if node.findtext("m:scope", "compile", NS) not in ("test", "provided")}
        require((root_group, artifacts[row["module"]]) in dependencies,
                identity + ": starter does not depend on its adapter at runtime")
        deployment = row.get("deployment", {})
        kind, job = deployment.get("kind"), deployment.get("job")
        require(kind in ("representative", "matrix") and job in workflow and job in gate_needs,
                identity + ": deployment CI job or final gate dependency is missing")
        selected_jobs.add(job)
        selected = scope.coverage([row["module"] + "/src/main/RegistrationProbe.java"])
        if kind == "representative":
            backend = deployment.get("backend")
            representative.add(backend)
            require(backend in runner.BACKEND_TESTS and backend in scope.BACKENDS
                    and backend in selected.get("docker", []) and backend in full.get("docker", []),
                    identity + ": representative runner or CI coverage is missing")
            server = runner.backend_selection(servers, backend)
            require(len(server) == 1, identity + ": representative Docker server is missing")
            patterns = runner.BACKEND_TESTS[backend].split(",")
            for module in (row["module"], row["starter"]):
                candidates = [declaration for declaration in tests.values() if declaration["module"] == module
                              and declaration["kind"] == "class" and "abstract" not in declaration["signature"]
                              and any(fnmatch.fnmatchcase(declaration["simple"], pattern) for pattern in patterns)
                              and has_test(declaration, tests)]
                require(candidates, identity + ": selected Docker test missing for " + module)
                if module == row["module"]:
                    require(any(implements(candidate["name"],
                                           "com.alandevise.tsgate.contract.BackendSemanticsContract", tests, module)
                                for candidate in candidates),
                            identity + ": selected adapter Docker test must inherit BackendSemanticsContract")
            require("tsgate-core/src/test/scripts/run-tests.py" in run_scripts(workflow[job])
                    and re.search(r"backend:\s*\$\{\{\s*fromJSON\(needs\.changes\.outputs\.docker\)", workflow[job]),
                    identity + ": CI does not execute the selected representative runner matrix")
        else:
            selector = deployment.get("selector")
            matrix_selectors.add(selector)
            require(selected.get(selector) is True and full.get(selector) is True,
                    identity + ": matrix CI selector is missing")
            entry = deployment.get("runner")
            matrix = load_script(file_at(root, entry))
            require(entry in run_scripts(workflow[job]), identity + ": CI does not invoke the matrix runner")
            require(matrix.MATRIX and all(isinstance(item, tuple) and item for item in matrix.MATRIX),
                    identity + ": deployment matrix is empty")
            for module in (row["module"], row["starter"]):
                test = matrix.MODULES.get(module)
                test_type = type_for(tests, test, module)
                require(test_type is not None and test_type["module"] == module
                        and "abstract" not in test_type["signature"] and has_test(test_type, tests),
                        identity + ": matrix Docker test missing for " + module)
                if module == row["module"]:
                    require(implements(test_type["name"],
                                       "com.alandevise.tsgate.contract.BackendSemanticsContract", tests, module),
                            identity + ": selected adapter Docker test must inherit BackendSemanticsContract")
            require(hasattr(matrix, "ENVIRONMENT") and Path(matrix.ENVIRONMENT).is_file(),
                    identity + ": deployment fixture is missing")
    require(representative == set(runner.BACKEND_TESTS) == set(scope.BACKENDS) == {server["kind"] for server in servers},
            "Representative Docker inventories differ from registered backends")
    require(set(full.get("docker", [])) == representative
            and {key for key, value in full.items() if value is True and key != "full"} == matrix_selectors,
            "Full CI selection differs from registered deployments")
    commit = "a" * 40
    checks = {"changes": {"result": "success", "outputs": {key: json.dumps(value) for key, value in full.items()}},
              **{job: {"result": "success", "outputs": {"commit": commit}} for job in selected_jobs | {"unit"}}}
    require(run_gate(gate_code[0], checks, commit), "Final CI gate rejects complete registered coverage")
    for job in selected_jobs:
        failed = json.loads(json.dumps(checks))
        failed[job]["result"] = "failure"
        require(not run_gate(gate_code[0], failed, commit), "Final CI gate ignores failed deployment job: " + job)
        wrong_sha = json.loads(json.dumps(checks))
        wrong_sha[job]["outputs"]["commit"] = "b" * 40
        require(not run_gate(gate_code[0], wrong_sha, commit), "Final CI gate ignores deployment source mismatch: " + job)
    return {"status": "passed", "backends": sorted(ids), "jarModules": sorted(jar_modules),
            "deploymentJobs": sorted(selected_jobs)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=ROOT)
    args = parser.parse_args(argv)
    try:
        print(json.dumps(verify(args.project_root), sort_keys=True))
    except (OSError, ValueError, KeyError, TypeError, AttributeError, ET.ParseError) as error:
        print("Backend registration check failed: " + str(error), file=__import__("sys").stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
