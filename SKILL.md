---
name: tsgate
description: Integrate TsGate into Java applications or contribute to the TsGate adapter library. Use for TsGate dependencies, YAML activation, TG annotations and queries, backend compatibility, adapter fixes, new backends, tests, or release preparation. Apply only when the task uses or changes TsGate.
license: Apache-2.0
metadata:
  tsgate-version: "2.0.0"
  java-baseline: "17"
---

# TsGate integration and contribution

Use the workflow matching the request:

- **Application integration:** configure and call an existing TsGate release in a business application.
- **Library contribution:** change TsGate implementation, dependencies, tests, or documentation.

This guide describes TsGate 2.0.0. Check the application's resolved dependencies or the repository's current POM before generating version-specific code. Read only the Wiki topics needed for the task. If a different version is in use, inspect its source/Javadoc instead of assuming identical APIs.

The configuration-snapshot, strict-input validation and startup-failure-policy fixes described here are current source updates after the published 2.0.0 artifact; they have not been published as a new Maven release. Inspect the selected artifact before relying on those fixes.

The repository publishes this file as a portable skill. To install it, put it in a `tsgate` skill directory recognized by the chosen coding assistant, or explicitly ask the assistant to read this file. A file at the repository root is not a guarantee of automatic discovery by every tool. A standalone copy uses the public Wiki links below; contribution commands require a TsGate source checkout.

## Application integration

### Select the backend and dependencies

Establish the server product/version, Java version, Spring Boot version, and required operations from the application. Ask only for information that affects the implementation and cannot be inferred from its build/configuration.

- Maven groupId: `io.github.alandevise`; current published version: `2.0.0`.
- Java packages: `com.alandevise.tsdb.*`; Java 17 minimum, without preview features.
- Select `tsgate-iotdb-spring-boot-starter`, `tsgate-influxdb3-spring-boot-starter`, or `tsgate-influxdb1-spring-boot-starter`. Direct Java integration can use the matching adapter without a starter.
- Import `tsgate-bom` explicitly when client dependency alignment is needed. A starter cannot override application dependency management. With multiple imported BOMs, follow the precedence and Boot-parent examples in [Getting started](https://github.com/AlanDevise/TsGate/wiki/EN-Getting-Started).
- Select an IoTDB SDK through Maven/Gradle dependency management, not YAML. Inspect the resolved dependency tree and validate the SDK/server pair; do not equate client and server version numbers.
- InfluxDB 3's Arrow client requires `--add-opens=java.base/java.nio=ALL-UNNAMED` at application launch. A library or BOM cannot supply that launcher option. InfluxDB 1.x does not require it.

For example, an InfluxDB 1.x starter dependency is:

```xml
<dependency>
    <groupId>io.github.alandevise</groupId>
    <artifactId>tsgate-influxdb1-spring-boot-starter</artifactId>
    <version>2.0.0</version>
</dependency>
```

### Configure explicit activation

All backends default to disabled. Enable the selected backend explicitly; other `enable: false` entries and `spring.profiles.active` are unnecessary. At most one backend may be enabled in one Spring context. With none enabled, the starters create no adapter, `TGTemplate`, or official client bean.

Prefixes are `tsdb.iotdb`, `tsdb.influxdb` for InfluxDB 3, and `tsdb.influxdb1`. The default database is `tsdb`; configure `database` to use a business database. IoTDB and InfluxDB 1.x require it to exist beforehand. Use deployment-supplied credentials, for example:

```yaml
tsdb:
  influxdb1:
    enable: true
    url: ${TSGATE_INFLUXDB1_URL}
    username: ${TSGATE_INFLUXDB1_USERNAME}
    password: ${TSGATE_INFLUXDB1_PASSWORD}
    database: ${TSGATE_DATABASE:tsdb}
```

These environment-variable names are application choices, not additional TsGate properties. The username/password example assumes an authenticated server. Full connection, pool, and timeout options are in [Configuration](https://github.com/AlanDevise/TsGate/wiki/EN-Configuration).

All adapters capture a defensive configuration snapshot at construction, including nested settings and IoTDB node URLs. Finish binding and edits before construction; create a new adapter/context for changed or corrected settings. Retrying a transient initialization failure and replacing an IoTDB physical pool reuse the captured settings.

With `fail-fast=false`, tolerated client-resource initialization failure keeps the adapter/template while the native client is unavailable. Use optional injection or `ObjectProvider`; required native injection can still fail startup. After manual `init()` succeeds, get the borrowed client directly from the adapter. Spring does not automatically recreate an unavailable native-client bean. Required configuration remains mandatory.

### Use the actual public API

Inject `com.alandevise.tsdb.core.TGTemplate` by type; the default bean name is `tgTemplate`. Obtain a fresh `TGQueryBuilder<T>` with `template.query(Entity.class)` for each query. Do not share mutable query builders between requests.

The annotations are in `com.alandevise.tsdb.annotation`. A write POJO needs one `@TGTime` on a non-null `long`/`Long` epoch-millisecond value and at least one non-null `@TGField`. `@TGTag` values are converted to strings on write. Use a POJO with a no-argument constructor for ordinary result mapping. Shared SPI/model types include `TSDBAdapter`, `TSDBRecord`, `TSDBQuery`, and `TSDBException`; use their declared names.

This example receives an already configured template; the target table/measurement must match the backend schema requirements:

```java
import com.alandevise.tsdb.annotation.TGField;
import com.alandevise.tsdb.annotation.TGMeasurement;
import com.alandevise.tsdb.annotation.TGTag;
import com.alandevise.tsdb.annotation.TGTime;
import com.alandevise.tsdb.core.TGTemplate;
import java.util.List;

public final class TsGateExample {
    @TGMeasurement("telemetry")
    public static class Telemetry {
        @TGTime public Long time;
        @TGTag("device") public String device;
        @TGField("temperature") public Double temperature;
    }

    public static List<Telemetry> writeAndRead(TGTemplate template, long now) {
        Telemetry point = new Telemetry();
        point.time = now;
        point.device = "sensor-1";
        point.temperature = 21.5;
        template.write(point);
        return template.query(Telemetry.class)
                .whereTag("device", "sensor-1")
                .timeRange(now - 60_000L, now + 1L)
                .orderByTimeAsc()
                .limit(100)
                .list();
    }
}
```

Writes are synchronous. For batches where commitment matters, use `batchWriteDetailed(...)` and inspect `BatchWriteResult`; failures can carry it in `TSDBBatchWriteException`. A batch spanning requests is not one transaction. An `UNKNOWN` commit state cannot justify blindly replaying writes. Read [Writes and errors](https://github.com/AlanDevise/TsGate/wiki/EN-Writes-and-Errors) before implementing retries.

### Respect query and backend limits

- Structured `limit`/page size is `1..10000`. Native SQL has separate backend row limits, and InfluxDB responses also have byte limits. Do not use native SQL to bypass bounded-query behavior.
- A null/empty strict cursor means the first page; preserve all nonempty entries until validation. Null/blank keys, null values, additional keys and normalized duplicates must fail with `ARGUMENT_ERROR`, never be silently removed.
- Direct `TSDBAdapter.query` calls reject null queries, nonpositive limits, negative offsets and reversed time ranges before I/O. Count ignores pagination/cursors; backend limits and probe allowances still apply.
- Time-only cursors can skip rows sharing a boundary timestamp. Use strict composite cursors when supported and needed; pass the returned cursor unchanged, preserving column identity and sorting. InfluxDB 1.x rejects strict composite cursors. See [Pagination](https://github.com/AlanDevise/TsGate/wiki/EN-Pagination-and-Native-Queries).
- InfluxDB 3 Core 3.0.0/3.0.3 require `tsdb.influxdb.strict-cursor-sql: union-all` for the verified strict-cursor continuation queries. The default is `or`. UNION mode does not rewrite native SQL and rejects aggregate strict-cursor queries.
- IoTDB 2.0.2 requires `tsdb.iotdb.table.rpc-compression-enabled: false`. The default is `true`. This controls Tablet RPC encoding, not transport or disk compression. IoTDB table-model integration requires its session pool.
- IoTDB and InfluxDB 3 support documented regional-calendar day windows. InfluxDB 1.x does not. Preserve explicit time bounds and documented DST restrictions; do not replace regional boundaries with a fixed UTC offset.
- Preserve `TSDBException` error classification. Conversion or query failures must not become successful zero/false/empty results. Native clients are borrowed from the adapter; callers must respect ownership and shutdown.

Before claiming support, read the [compatibility matrix](https://github.com/AlanDevise/TsGate/wiki/EN-Compatibility-and-Validation). Evidence covers specific server, SDK, runtime, and configuration combinations; neither a major version range nor a passing mock test certifies untested servers.

For an integration deliverable, compile against the selected version, check dependency resolution, and verify configuration binding and representative reads/writes when a suitable server is available. State what was actually verified and what still needs server testing.

## Library contribution

### Locate the change

Work from the TsGate repository root. Inspect local repository instructions, the working tree, relevant source/Javadoc, and existing tests before editing. In a standalone skill installation, locate the source checkout first. The module layout is:

| Location | Responsibility |
|---|---|
| `tsgate-core/src/main/java/com/alandevise/tsdb/core/` | `TGTemplate`, fluent query building, result conversion, pagination |
| `tsgate-core/src/main/java/com/alandevise/tsdb/metadata/` | Annotation resolution and physical-column metadata |
| `tsgate-core/src/main/java/com/alandevise/tsdb/adapter/TSDBAdapter.java` | Backend SPI and shared operation contracts |
| `tsgate-core/src/main/java/com/alandevise/tsdb/model/` | Query, result, ordering, and batch commitment models |
| `tsgate-iotdb`, `tsgate-influxdb3`, `tsgate-influxdb1` | Backend SQL/protocol translation, clients, properties, lifecycle |
| `tsgate-*-spring-boot-starter` | Property binding, conditional activation, and native-client beans |
| `tsgate-bom/pom.xml` | Consumer dependency alignment |
| `pom.xml`, `.github/workflows/ci.yml`, `scripts/verify-release-artifacts.py` | Build, CI, and release verification |

Keep dialect-specific behavior in its adapter and database-client dependencies out of core. Change shared abstractions only when the behavior belongs to all affected backends. A new backend needs explicit capability/error semantics, a property model, lifecycle handling, tests, and starter/BOM integration where applicable; do not simulate unsupported operations with empty success responses.

### Preserve observable contracts

- Keep the Java 17 baseline without preview features. Check `pom.xml` before changing Java, Spring Boot, SDK, or server requirements; update documentation and verify the affected matrix for a baseline change.
- Reproduce a behavioral defect with a focused regression. Exercise shared-core changes across affected adapters and starter changes across backend activation combinations.
- Validate writes before network I/O where supported; preserve confirmed batch counts and distinguish rejection, partial commitment, and unknown commitment. Retryability and commit certainty are separate facts.
- Classify write failures by backend: InfluxDB 1.x HTTP 400 may partially persist and remains `UNKNOWN`; InfluxDB 3's definite-rejection rules depend on `accept_partial=false`. See [Writes and errors](https://github.com/AlanDevise/TsGate/wiki/EN-Writes-and-Errors). Distinguish application/adapter batch replay from the HTTP client's `retry-on-connection-failure` transport setting.
- Snapshot complete adapter configuration at construction, including nested settings and endpoint lists. Do not read caller-owned mutable Properties during operations or recovery; configuration changes require a new instance.
- Preserve idempotent initialization, retryable failed initialization, and terminal close. Close must coordinate with adapter operations; repeated IoTDB initialization must preserve the exposed native pool proxy. Native-client callers coordinate their own concurrent shutdown.
- Preserve exact backend column identity, complete cursor ordering, and pagination lookahead. Test equal timestamps, mixed sorts, case-distinct columns, missing cursor fields, and invalid time values when changing cursors.
- Preserve exact numeric range checks, invalid-value errors, bounded result consumption, and real timezone transitions. Test boundary inputs relevant to the change rather than only happy paths.
- Read existing error-code and resource-ownership contracts before replacing exceptions or exposing a native client. Keep public APIs and configuration behavior compatible unless the task explicitly calls for a breaking change.

### Select meaningful validation

Portable tests belong under each module's `src/test/` and are committed. Keep machine-specific fixtures, credentials, toolchains, and reports under ignored `.local-test/`. Use an isolated test database rather than production data.

Run the relevant existing tests while developing. The portable runner provides the complete unit suite and representative Docker regression from the repository root:

```bash
python3 tsgate-core/src/test/scripts/run-tests.py unit --spring-boot 2.7.18
python3 tsgate-core/src/test/scripts/run-tests.py docker --spring-boot 2.7.18
```

Select the runtime with `JAVA_HOME`; the runner needs Java 17+, Maven 3.9+, and Python 3. Docker mode also runs unit tests, starts its own loopback-only database containers, and removes them afterward. Use `--pull never` only when all required images are cached. Its representative versions come from `tsgate-core/src/test/resources/ci/docker-servers.json`; this command does not test every supported release.

For a protocol, dialect, client upgrade, lifecycle, or other server-visible change, run the appropriate real-server Docker regression. Add version-specific coverage for a new compatibility claim. For IoTDB Tablet encoding, include at least 10 rows in one actual Tablet and verify readback. For strict cursors, exercise continuation pages with equal timestamps and both ordering directions. Unit tests with mocked clients alone cannot establish database compatibility.

For shared API or runtime/dependency changes, verify the relevant JDK/Spring Boot combinations listed in the current workflow. Pure prose changes need documentation/example checks rather than an unrelated database regression. Report actual commands, versions, outcomes, and untested scope; do not reuse historical pass counts as new results.

### Synchronize documentation and delivery

- Write Javadoc and source comments in English. Preserve API contracts and useful examples.
- Every adapter code change updates the corresponding English and Chinese Wiki pages: behavior, configuration, limits, errors, compatibility, upgrade guidance, and examples must agree with the implementation.
- Keep `README.md` and `README.zh-CN.md` as synchronized introductions, architecture diagrams, and documentation navigation. Update both when the overview changes. Update this skill when its instructions or examples change.
- The source repository publishes those two READMEs and this `SKILL.md` as its Markdown files. Full bilingual guides live in the separate Wiki repository. Local `AGENTS.md` and reports stay ignored. If the Wiki cannot be updated, retain synchronized drafts under `.local-test/wiki-migration/pages/` and report the publication gap.
- Preserve Apache-2.0 for project-owned code, `NOTICE` attribution, and all applicable third-party notices. Generated Javadoc resources retain their own licenses.
- Inspect the final diff and staged file list. Follow the task's authorization for commits, pushes, and releases; this skill does not authorize changing repository visibility, rewriting history, or publishing packages by itself.

For release preparation, use PATCH for compatible fixes, MINOR for compatible additions, and MAJOR for breaking public contracts or supported baselines. Classify client upgrades by their actual effect on consumers. Keep module/BOM versions aligned, and never replace an already published Central version.

Build release artifacts with the documented Temurin 17 toolchain, then run the verifier:

```bash
mvn -Prelease-artifacts clean verify
python3 scripts/verify-release-artifacts.py --project-root . \
  --output .local-test/release-artifacts.json
```

This prepares unsigned artifacts. Signing and Central upload are separate authorized operations; use local settings/secrets outside Git. Read [Testing and publishing](https://github.com/AlanDevise/TsGate/wiki/EN-Testing-and-Publishing) for the exact release toolchain, signature requirements, and publication procedure.

## Topic references

Each Wiki page links to its Chinese counterpart. Use [the Chinese overview](https://github.com/AlanDevise/TsGate/wiki/ZH-Overview) when Chinese guidance is preferred.

- [Fluent queries and aggregation](https://github.com/AlanDevise/TsGate/wiki/EN-Queries): filtering, grouping, and windows.
- [Upgrades and API contracts](https://github.com/AlanDevise/TsGate/wiki/EN-Upgrades-and-Migration): SDK overrides, versioning, and backend workarounds.
- [Compatibility and validation](https://github.com/AlanDevise/TsGate/wiki/EN-Compatibility-and-Validation): the exact evidence behind support claims.
