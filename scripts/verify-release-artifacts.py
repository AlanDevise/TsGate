#!/usr/bin/env python3
"""Verify local TsGate release artifacts and notices without accessing a registry."""

import argparse
import json
import os
from pathlib import Path
import re
import struct
import sys
import xml.etree.ElementTree as ET
import zipfile


NS = {"m": "http://maven.apache.org/POM/4.0.0"}
PROPRIETARY_MARKERS = (
    (re.compile(rb"ORACLE\s+PROPRIETARY\s*/\s*CONFIDENTIAL", re.IGNORECASE),
     "Oracle proprietary/confidential marker"),
    (re.compile(rb"otnlicense", re.IGNORECASE), "Oracle OTN license reference"),
)


def verify_archive_notices(archive, archive_name, notices, classifier, require,
                           javadoc_legal=None):
    """Keep project and generated-resource licenses separate and intact."""
    contents = {name: archive.read(name) for name in archive.namelist()
                if not name.endswith("/")}
    for name, content in notices.items():
        resource = ("resources/" if classifier == "-javadoc" else "META-INF/") + name
        require(resource in contents, f"{archive_name}: missing {resource}")
        if resource in contents:
            require(contents[resource] == content,
                    f"{archive_name}: {resource} differs from repository")
    # Inspect every entry, including extensionless legal files and embedded metadata.
    for name, content in contents.items():
        for pattern, label in PROPRIETARY_MARKERS:
            require(not pattern.search(content), f"{archive_name}: {label} in {name}")
    if classifier != "-javadoc":
        return []

    def require_notice(name, phrases):
        require(name in contents, f"{archive_name}: missing third-party notice {name}")
        if name in contents:
            normalized = b" ".join(contents[name].lower().split())
            require(all(phrase in normalized for phrase in phrases),
                    f"{archive_name}: incomplete or unexpected third-party notice {name}")

    require_notice("legal/LICENSE", (b"gnu general public license", b"version 2",
                                     b'"classpath" exception',
                                     b"permission to link this library with independent modules"))
    require_notice("legal/ADDITIONAL_LICENSE_INFO", (b"gnu classpath exception",))
    require_notice("legal/ASSEMBLY_EXCEPTION", (b"openjdk assembly exception",))
    mit_terms = (b"copyright", b"permission is hereby granted, free of charge",
                 b"the above copyright notice and this permission notice shall be included",
                 b'the software is provided "as is"')
    resource_names = [Path(name).name.lower() for name in contents
                      if name.lower().endswith((".js", ".css"))]
    if any(name.startswith("jquery") and not name.startswith("jquery-ui")
           for name in resource_names):
        require_notice("legal/jquery.md", mit_terms)
    if any(name.startswith("jquery-ui") for name in resource_names):
        require_notice("legal/jqueryUI.md", mit_terms)
    if javadoc_legal is not None:
        for name, content in javadoc_legal.items():
            resource = "legal/" + name
            require(resource in contents,
                    f"{archive_name}: missing generator notice {resource}")
            if resource in contents:
                require(contents[resource] == content,
                        f"{archive_name}: {resource} differs from Javadoc generator")
    return sorted(name for name in contents if name.startswith("legal/"))


def verify(root, javadoc_java_home=None):
    project = ET.parse(root / "pom.xml").getroot()
    group = project.findtext("m:groupId", namespaces=NS)
    version = project.findtext("m:version", namespaces=NS)
    report = {"groupId": group, "version": version, "modules": [], "errors": []}

    def require(condition, message):
        if not condition:
            report["errors"].append(message)

    javadoc_legal = None
    if javadoc_java_home is not None:
        legal_directory = javadoc_java_home / "legal/jdk.javadoc"
        require(legal_directory.is_dir(),
                f"Missing Javadoc generator legal directory: {legal_directory}")
        javadoc_legal = {path.relative_to(legal_directory).as_posix(): path.read_bytes()
                         for path in legal_directory.rglob("*") if path.is_file()}
        require(bool(javadoc_legal), "No Javadoc generator notices found")
        report["javadocGeneratorNotices"] = sorted(javadoc_legal)

    notices = {name: (root / name).read_bytes() for name in ("LICENSE", "NOTICE")}
    for module in project.findall("m:modules/m:module", NS):
        directory = root / module.text
        pom = ET.parse(directory / "pom.xml").getroot()
        artifact = pom.findtext("m:artifactId", namespaces=NS)
        module_group = (pom.findtext("m:groupId", namespaces=NS)
                        or pom.findtext("m:parent/m:groupId", namespaces=NS))
        module_version = (pom.findtext("m:version", namespaces=NS)
                          or pom.findtext("m:parent/m:version", namespaces=NS))
        require(module_group == group, f"{artifact}: groupId differs from root project")
        require(module_version == version, f"{artifact}: version differs from root project")
        packaging = pom.findtext("m:packaging", "jar", NS)
        if packaging == "pom":
            continue
        entry = {"artifactId": artifact, "archives": [], "classes": 0}
        report["modules"].append(entry)
        sources = directory / "src/main/java"
        production_sources = {path.relative_to(sources).as_posix(): path
                              for path in sources.rglob("*.java")}
        classes_directory = directory / "target/classes"
        production_classes = {path.relative_to(classes_directory).as_posix(): path
                              for path in classes_directory.rglob("*.class")}
        tests_directory = directory / "target/test-classes"
        test_only_classes = {path.relative_to(tests_directory).as_posix()
                             for path in tests_directory.rglob("*.class")} - production_classes.keys()
        test_only_resources = set()
        for test_root in (directory / "src/test", directory / "src/test/resources"):
            for path in test_root.rglob("*"):
                if path.is_file() and path.suffix != ".java":
                    name = path.relative_to(test_root).as_posix()
                    if not (directory / "src/main/resources" / name).is_file():
                        test_only_resources.add(name)
        for classifier in ("", "-sources", "-javadoc"):
            path = directory / "target" / f"{artifact}-{version}{classifier}.jar"
            require(path.is_file(), f"Missing artifact: {path}")
            if not path.is_file():
                continue
            with zipfile.ZipFile(path) as archive:
                names = set(archive.namelist())
                entry["archives"].append(path.name)
                third_party_notices = verify_archive_notices(
                    archive, path.name, notices, classifier, require, javadoc_legal)
                if classifier == "-javadoc":
                    entry["javadocNotices"] = third_party_notices
                for name in names:
                    require(not name.startswith(("src/test/", ".local-test/", ".github/", "scripts/", "ci/"))
                            and "/src/test/" not in name and not name.endswith((".py", ".sh")),
                            f"{path.name}: contains development fixture or script: {name}")
                require(not names.intersection(test_only_resources),
                        f"{path.name}: contains resources exclusive to src/test")
                require(not names.intersection(test_only_classes),
                        f"{path.name}: contains classes exclusive to target/test-classes")
                if classifier == "":
                    metadata = f"META-INF/maven/{group}/{artifact}/pom.properties"
                    require(metadata in names, f"{path.name}: missing Maven coordinates {metadata}")
                    if metadata in names:
                        properties = dict(line.split("=", 1) for line in
                                          archive.read(metadata).decode("utf-8").splitlines()
                                          if "=" in line and not line.startswith("#"))
                        require(properties.get("groupId") == group
                                and properties.get("artifactId") == artifact
                                and properties.get("version") == version,
                                f"{path.name}: embedded Maven coordinates differ from project")
                    classes = {name for name in names if name.endswith(".class")}
                    entry["classes"] = len(classes)
                    require(bool(classes), f"{path.name}: contains no compiled classes")
                    require(classes == production_classes.keys(),
                            f"{path.name}: compiled class entries differ from target/classes")
                    for name in classes:
                        major = struct.unpack(">H", archive.read(name)[6:8])[0]
                        require(major == 61, f"{path.name}: {name} is not Java 17 bytecode")
                        if name in production_classes:
                            require(archive.read(name) == production_classes[name].read_bytes(),
                                    f"{path.name}: compiled class differs from target/classes: {name}")
                elif classifier == "-sources":
                    archive_sources = {name for name in names if name.endswith(".java")}
                    require(archive_sources == production_sources.keys(),
                            f"{path.name}: Java source entries differ from src/main/java")
                    require(not any(name.endswith(".class") for name in names),
                            f"{path.name}: source artifact contains compiled classes")
                    for name, source in production_sources.items():
                        require(name in names, f"{path.name}: missing source {name}")
                        if name in names:
                            require(archive.read(name) == source.read_bytes(),
                                    f"{path.name}: source differs from repository: {name}")
                else:
                    require(not any(name.endswith((".java", ".class")) for name in names),
                            f"{path.name}: Javadoc artifact contains Java sources or classes")
                    require("index.html" in names, f"{path.name}: missing Javadoc index")
                    if artifact == "tsgate-core":
                        document = "com/alandevise/tsgate/exception/TSDBException.html"
                        require(document in names, f"{path.name}: missing exception API docs")
                        if document in names:
                            require(b'id="getErrorCode()"' in archive.read(document),
                                    f"{path.name}: missing Lombok-generated public API docs")
    require(bool(report["modules"]), "No binary modules found")
    report["status"] = "passed" if not report["errors"] else "failed"
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, help="Optional JSON report path")
    parser.add_argument("--javadoc-java-home", type=Path,
                        default=Path(os.environ["JAVA_HOME"]) if os.environ.get("JAVA_HOME") else None,
                        help="Compare generated notices with this JDK (default: JAVA_HOME)")
    options = parser.parse_args()
    report = verify(options.project_root.resolve(), options.javadoc_java_home)
    result = json.dumps(report, indent=2)
    if options.output:
        options.output.parent.mkdir(parents=True, exist_ok=True)
        options.output.write_text(result + "\n", encoding="utf-8")
    print(result)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
