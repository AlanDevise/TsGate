"""Registration checks use source-only miniature reactors; no Maven, Docker or network."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("backend_registration", Path(__file__).with_name("verify-backend-registration.py"))
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)
RUNNER_SPEC = importlib.util.spec_from_file_location("registration_test_runner", Path(__file__).with_name("run-tests.py"))
RUNNER = importlib.util.module_from_spec(RUNNER_SPEC)
RUNNER_SPEC.loader.exec_module(RUNNER)


class RegistrationTest(unittest.TestCase):
    def write(self, root, path, text):
        destination = root / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(text, encoding="utf-8")
        return destination

    def fixture(self, root):
        """A complete, new backend with a different module ID, enable alias and SDK class."""
        adapter_module, starter = "tsgate-futuredb", "tsgate-futuredb-spring-boot-starter"
        self.write(root, "pom.xml", '''<project xmlns="http://maven.apache.org/POM/4.0.0">
          <groupId>io.github.alandevise</groupId><artifactId>tsgate</artifactId><version>1</version>
          <modules><module>tsgate-core</module><module>tsgate-bom</module>
          <module>tsgate-futuredb</module><module>tsgate-futuredb-spring-boot-starter</module></modules></project>''')
        for module in ("tsgate-core", adapter_module, starter):
            dependency = "<dependencies><dependency><groupId>io.github.alandevise</groupId><artifactId>tsgate-futuredb</artifactId></dependency></dependencies>" if module == starter else ""
            self.write(root, module + "/pom.xml", '<project xmlns="http://maven.apache.org/POM/4.0.0"><artifactId>' + module + '</artifactId>' + dependency + '</project>')
        self.write(root, "tsgate-bom/pom.xml", '<project xmlns="http://maven.apache.org/POM/4.0.0"><artifactId>tsgate-bom</artifactId><packaging>pom</packaging><dependencyManagement><dependencies>' + ''.join(
            '<dependency><groupId>io.github.alandevise</groupId><artifactId>' + module + '</artifactId></dependency>' for module in ("tsgate-core", adapter_module, starter)) + '</dependencies></dependencyManagement></project>')
        source = adapter_module + "/src/main/java/example/"
        self.write(root, source + "FutureAdapter.java", 'package example; import com.alandevise.tsgate.adapter.TSDBAdapter; public class FutureAdapter implements TSDBAdapter {}')
        self.write(root, source + "FutureProperties.java", 'package example; @ConfigurationProperties(prefix="tsdb.future") public class FutureProperties {}')
        self.write(root, CHECK.CONDITION, '''package com.alandevise.tsgate.config;
          public class TSDBAdapterEnabledCondition {
            private static List<String> enabledAdapters(ConditionContext context) {
              addIfEnabled(context, enabled, "future", "example.FutureAdapter"); return enabled;
            }
            public static final class Future implements Condition {
              public boolean matches(ConditionContext context, Metadata metadata) {
                return enabledAdapters(context).contains("future");
              }
            }
          }''')
        self.write(root, starter + "/src/main/java/example/FutureAutoConfiguration.java", '''package example;
          import com.alandevise.tsgate.config.TSDBAdapterEnabledCondition;
          import com.alandevise.tsgate.core.TGTemplate;
          @Configuration @Conditional(TSDBAdapterEnabledCondition.Future.class)
          public class FutureAutoConfiguration {
            @Bean public FutureProperties properties() { return new FutureProperties(); }
            @Bean public FutureAdapter adapter(FutureProperties p) { return new FutureAdapter(); }
            @Bean public TGTemplate template(FutureAdapter adapter) { return new TGTemplate(adapter); }
          }''')
        self.write(root, starter + "/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports", "example.FutureAutoConfiguration\n")
        self.write(root, "tsgate-core/src/test/java/com/alandevise/tsgate/contract/SharedAdapterContract.java", 'package com.alandevise.tsgate.contract; public interface SharedAdapterContract { @Test default void shared() {} }')
        self.write(root, adapter_module + "/src/test/java/example/FutureSharedTest.java", 'package example; import com.alandevise.tsgate.contract.SharedAdapterContract; class FutureSharedTest implements SharedAdapterContract {}')
        self.write(root, adapter_module + "/src/test/java/example/FutureDockerIT.java", 'package example; class FutureDockerIT { @Test void roundTrip() {} }')
        self.write(root, starter + "/src/test/java/example/FutureStarterDockerIT.java", 'package example; class FutureStarterDockerIT { @Test void startup() {} }')
        self.write(root, CHECK.CAPABILITIES, 'backend,module,contractTest,BATCH_WRITE\nfuturedb,tsgate-futuredb,example.FutureSharedTest,SUPPORTED\n')
        row = dict(id="futuredb", module=adapter_module, adapter="example.FutureAdapter", enableId="future",
                   condition="Future", properties="example.FutureProperties", starter=starter,
                   autoConfiguration="example.FutureAutoConfiguration",
                   deployment=dict(kind="representative", backend="future", job="docker"))
        self.write(root, CHECK.MANIFEST, json.dumps(dict(schemaVersion=1, backends=[row])))
        self.write(root, "tsgate-core/src/test/resources/ci/docker-servers.json", '[{"kind":"future","version":"1","image":"future:1"}]')
        self.write(root, "tsgate-core/src/test/scripts/run-tests.py", '''from pathlib import Path
BACKEND_TESTS={"future":"Future*DockerIT"}
def backend_selection(servers, backend):
    selected=[server for server in servers if server["kind"] == backend]
    if not selected: raise ValueError("missing server")
    return selected
''')
        self.write(root, "tsgate-core/src/test/scripts/ci-scope.py", '''BACKENDS=("future",)
def coverage(paths, full=False):
    selected=full or any(path.startswith("tsgate-futuredb/") for path in paths)
    return dict(docker=["future"] if selected else [], full=full)
''')
        self.write(root, ".github/workflows/ci.yml", '''name: CI
jobs:
  changes:
    runs-on: ubuntu-latest
  unit:
    runs-on: ubuntu-latest
  docker:
    strategy:
      matrix:
        backend: ${{ fromJSON(needs.changes.outputs.docker) }}
    steps:
      - run: >-
          python3 tsgate-core/src/test/scripts/run-tests.py docker
          --backend "$BACKEND"
  release-gate:
    needs: [changes, unit, docker]
    steps:
      - run: |
          python3 - <<'PYTHON'
          import json, os
          checks=json.loads(os.environ['CHECKS'])
          selected={'unit': True, 'docker': bool(json.loads(checks['changes']['outputs']['docker']))}
          for name, required in selected.items():
              if checks[name]['result'] != ('success' if required else 'skipped'):
                  raise SystemExit('job failed')
              if required and checks[name]['outputs']['commit'] != os.environ['EXPECTED_SHA']:
                  raise SystemExit('wrong source')
          PYTHON
''')

    def mutate(self, root, path, old, new):
        file = root / path
        text = file.read_text()
        self.assertIn(old, text, "Invalid negative fixture")
        file.write_text(text.replace(old, new), encoding="utf-8")

    def test_current_reactor_has_complete_registrations(self):
        result = CHECK.verify(CHECK.ROOT)
        self.assertEqual("passed", result["status"])
        self.assertEqual(["influxdb1", "influxdb3", "iotdb", "opengemini"], result["backends"])

    def test_complete_new_backend_with_nonmatching_alias_is_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            result = CHECK.verify(root)
            self.assertEqual(["futuredb"], result["backends"])
            self.assertEqual(["docker"], result["deploymentJobs"])

    def test_every_required_registration_is_checked(self):
        cases = [
            ("manifest", CHECK.MANIFEST, '"id": "futuredb"', '"id": "missing"'),
            ("reactor adapter", "pom.xml", '<module>tsgate-futuredb</module>', ''),
            ("reactor starter", "pom.xml", '<module>tsgate-futuredb-spring-boot-starter</module>', ''),
            ("adapter class", "tsgate-futuredb/src/main/java/example/FutureAdapter.java", 'implements TSDBAdapter', ''),
            ("capability", CHECK.CAPABILITIES, 'futuredb,tsgate-futuredb,example.FutureSharedTest,SUPPORTED', ''),
            ("enable mapping", CHECK.CONDITION, 'addIfEnabled(context, enabled, "future", "example.FutureAdapter");', ''),
            ("condition target", CHECK.CONDITION, 'contains("future")', 'contains("other")'),
            ("properties", "tsgate-futuredb/src/main/java/example/FutureProperties.java", '"tsdb.future"', '"tsdb.other"'),
            ("starter dependency", "tsgate-futuredb-spring-boot-starter/pom.xml", '<artifactId>tsgate-futuredb</artifactId>', '<artifactId>other</artifactId>'),
            ("BOM", "tsgate-bom/pom.xml", '<artifactId>tsgate-futuredb</artifactId>', '<artifactId>tsgate-other</artifactId>'),
            ("Spring import", "tsgate-futuredb-spring-boot-starter/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports", 'example.FutureAutoConfiguration', '# example.FutureAutoConfiguration'),
            ("starter condition", "tsgate-futuredb-spring-boot-starter/src/main/java/example/FutureAutoConfiguration.java", 'TSDBAdapterEnabledCondition.Future.class', 'TSDBAdapterEnabledCondition.Other.class'),
            ("adapter bean", "tsgate-futuredb-spring-boot-starter/src/main/java/example/FutureAutoConfiguration.java", '@Bean public FutureAdapter', 'public FutureAdapter'),
            ("template bean", "tsgate-futuredb-spring-boot-starter/src/main/java/example/FutureAutoConfiguration.java", '@Bean public TGTemplate', 'public TGTemplate'),
            ("properties binding", "tsgate-futuredb-spring-boot-starter/src/main/java/example/FutureAutoConfiguration.java", '@Bean public FutureProperties', 'public FutureProperties'),
            ("shared contract", "tsgate-futuredb/src/test/java/example/FutureSharedTest.java", 'implements SharedAdapterContract', '/* implements SharedAdapterContract */'),
            ("Docker adapter test", "tsgate-futuredb/src/test/java/example/FutureDockerIT.java", '@Test', '/* @Test */'),
            ("Docker starter test", "tsgate-futuredb-spring-boot-starter/src/test/java/example/FutureStarterDockerIT.java", '@Test', ''),
            ("Docker server", "tsgate-core/src/test/resources/ci/docker-servers.json", '"kind":"future"', '"kind":"other"'),
            ("runner", "tsgate-core/src/test/scripts/run-tests.py", '"future":"Future*DockerIT"', '"other":"OtherDockerIT"'),
            ("CI selection", "tsgate-core/src/test/scripts/ci-scope.py", 'return dict(docker=["future"] if selected else [], full=full)', 'return dict(docker=[], full=full)'),
            ("CI entry", ".github/workflows/ci.yml", 'python3 tsgate-core/src/test/scripts/run-tests.py docker', 'echo tsgate-core/src/test/scripts/run-tests.py docker'),
            ("gate needs", ".github/workflows/ci.yml", 'needs: [changes, unit, docker]', 'needs: [changes, unit]'),
            ("gate status", ".github/workflows/ci.yml", "checks[name]['result'] != ('success' if required else 'skipped')", 'False'),
            ("gate source", ".github/workflows/ci.yml", "required and checks[name]['outputs']['commit'] != os.environ['EXPECTED_SHA']", 'False'),
        ]
        for name, path, old, new in cases:
            with self.subTest(registration=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.fixture(root)
                self.mutate(root, path, old, new)
                with self.assertRaises((ValueError, KeyError, CHECK.ET.ParseError)):
                    CHECK.verify(root)

    def test_new_concrete_adapter_cannot_hide_without_a_manifest_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            self.write(root, "tsgate-futuredb/src/main/java/example/UnregisteredAdapter.java",
                       'package example; import com.alandevise.tsgate.adapter.TSDBAdapter; public class UnregisteredAdapter implements TSDBAdapter {}')
            with self.assertRaisesRegex(ValueError, "Concrete SPI adapters differ"):
                CHECK.verify(root)

    def test_abstract_base_and_inherited_shared_tests_are_supported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            self.write(root, "tsgate-futuredb/src/main/java/example/BaseAdapter.java", 'package example; import com.alandevise.tsgate.adapter.TSDBAdapter; public abstract class BaseAdapter implements TSDBAdapter {}')
            self.mutate(root, "tsgate-futuredb/src/main/java/example/FutureAdapter.java", 'implements TSDBAdapter', 'extends BaseAdapter')
            self.write(root, "tsgate-futuredb/src/test/java/example/FutureContract.java", 'package example; import com.alandevise.tsgate.contract.SharedAdapterContract; interface FutureContract extends SharedAdapterContract {}')
            self.mutate(root, "tsgate-futuredb/src/test/java/example/FutureSharedTest.java", 'implements SharedAdapterContract', 'implements FutureContract')
            self.assertEqual("passed", CHECK.verify(root)["status"])

    def test_unused_java_methods_do_not_count_as_registration(self):
        for method in ("enabledAdapters", "matches"):
            with self.subTest(method=method), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.fixture(root)
                if method == "enabledAdapters":
                    original = 'addIfEnabled(context, enabled, "future", "example.FutureAdapter");'
                    replacement = 'return enabled; } private static void unused(ConditionContext context, List<String> enabled) { ' + original
                    self.mutate(root, CHECK.CONDITION, original + ' return enabled;', replacement)
                else:
                    self.mutate(root, CHECK.CONDITION, 'return enabledAdapters(context).contains("future");',
                                'return false; } private boolean unused(ConditionContext context) { return enabledAdapters(context).contains("future");')
                with self.assertRaisesRegex(ValueError, "enablement"):
                    CHECK.verify(root)

    def test_yaml_environment_text_is_not_an_executed_runner_or_gate(self):
        for target in ("runner", "gate"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.fixture(root)
                if target == "runner":
                    self.mutate(root, ".github/workflows/ci.yml", "    steps:\n      - run: >-", "    env:\n      UNUSED: >-")
                else:
                    self.mutate(root, ".github/workflows/ci.yml", "    steps:\n      - run: |", "    env:\n      UNUSED: |")
                with self.assertRaisesRegex(ValueError, "CI|gate"):
                    CHECK.verify(root)

    def test_unreachable_nested_test_class_does_not_count_as_docker_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            self.write(root, "tsgate-futuredb/src/test/java/example/FutureDockerIT.java",
                       'package example; class FutureDockerIT { static class Unreachable { @Test void roundTrip() {} } }')
            with self.assertRaisesRegex(ValueError, "selected Docker test missing"):
                CHECK.verify(root)

    def test_display_heredocs_unused_python_and_conditional_steps_do_not_count_as_execution(self):
        replacements = (
            "cat <<'NOTE'\n          python3 tsgate-core/src/test/scripts/run-tests.py docker\n          NOTE\n          echo display-only",
            "python3 - <<'PYTHON'\n          import subprocess\n          def never_called():\n              subprocess.run(['python3', 'tsgate-core/src/test/scripts/run-tests.py', 'docker'])\n          PYTHON",
        )
        for replacement in replacements:
            with self.subTest(text=replacement), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.fixture(root)
                self.mutate(root, ".github/workflows/ci.yml", "python3 tsgate-core/src/test/scripts/run-tests.py docker", replacement)
                with self.assertRaisesRegex(ValueError, "CI does not execute"):
                    CHECK.verify(root)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            self.mutate(root, ".github/workflows/ci.yml", "      - run: >-", "      - if: false\n        run: >-")
            with self.assertRaisesRegex(ValueError, "CI does not execute"):
                CHECK.verify(root)

    def test_job_level_comments_do_not_hide_executable_steps(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            self.mutate(root, ".github/workflows/ci.yml", "    steps:\n      - run:",
                        "    steps:\n    # Ordinary YAML comment between steps.\n      - run:")
            self.assertEqual("passed", CHECK.verify(root)["status"])

    def test_dedicated_matrix_and_ci_selector_also_reject_missing_registration(self):
        matrix = CHECK.load_script(CHECK.ROOT / "tsgate-opengemini/src/test/scripts/run-matrix.py")
        scope = CHECK.load_script(CHECK.ROOT / "tsgate-core/src/test/scripts/ci-scope.py")
        failure = {"verify": lambda root: (_ for _ in ()).throw(ValueError("registration absent"))}
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "matrix"
            with patch.object(matrix.runpy, "run_path", return_value=failure), patch.object(matrix, "toolchain") as toolchain, patch.object(matrix, "capture") as capture, patch.object(matrix, "run_environment") as environment:
                self.assertEqual(1, matrix.main(["--output", str(output)]))
            toolchain.assert_not_called()
            capture.assert_not_called()
            environment.assert_not_called()
            selector_output = Path(directory) / "selection.json"
            with patch.object(scope.runpy, "run_path", return_value=failure), self.assertRaisesRegex(ValueError, "registration absent"):
                scope.main(["--json", str(selector_output)])
            self.assertFalse(selector_output.exists())

    def test_runner_registration_failure_precedes_tools_and_database_io(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result"
            with patch.object(RUNNER.runpy, "run_path", return_value={"verify": lambda root: (_ for _ in ()).throw(ValueError("registration absent"))}), patch.object(RUNNER, "capture") as capture, patch.object(RUNNER, "logged") as logged:
                code = RUNNER.main(["unit", "--output", str(output)])
            self.assertEqual(1, code)
            capture.assert_not_called()
            logged.assert_not_called()
            self.assertIn("registration absent", json.loads((output / "summary.json").read_text())["error"])


if __name__ == "__main__":
    unittest.main()
