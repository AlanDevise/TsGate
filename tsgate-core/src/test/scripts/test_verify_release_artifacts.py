#!/usr/bin/env python3
"""Regression checks for the distribution notice gate; no external tools required."""

import importlib.util
import io
from pathlib import Path
import unittest
import zipfile


ROOT = Path(__file__).resolve().parents[4]
SPEC = importlib.util.spec_from_file_location(
    "verify_release_artifacts", ROOT / "scripts/verify-release-artifacts.py")
VERIFIER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFIER)

# These small fixtures exercise notice validation, not actual license grants.
PROJECT_NOTICES = {"LICENSE": b"Project Apache-2.0 license\n", "NOTICE": b"Project attribution\n"}
OPENJDK_NOTICES = {
    "LICENSE": (b"The GNU General Public License (GPL)\nVersion 2, June 1991\n"
                b'"CLASSPATH" EXCEPTION TO THE GPL\n'
                b"permission to link this library with independent modules\n"),
    "ADDITIONAL_LICENSE_INFO": b"GNU Classpath Exception\n",
    "ASSEMBLY_EXCEPTION": b"OPENJDK ASSEMBLY EXCEPTION\n",
}
MIT_NOTICE = (b"Copyright OpenJS Foundation and other contributors\n"
              b"Permission is hereby granted, free of charge\n"
              b"The above copyright notice and this permission notice shall be\n"
              b"included in all copies or substantial portions of the Software.\n"
              b'THE SOFTWARE IS PROVIDED "AS IS"\n')


class ArchiveNoticesTest(unittest.TestCase):
    def setUp(self):
        self.contents = {"resources/" + name: data for name, data in PROJECT_NOTICES.items()}
        self.contents.update({"legal/" + name: data for name, data in OPENJDK_NOTICES.items()})
        self.contents["script.js"] = (
            b"/* Copyright (c) Oracle and/or its affiliates.\n"
            b"GNU General Public License version 2 with the Classpath exception. */\n")

    def check_archive(self, classifier="-javadoc", generator=None):
        errors = []
        data = io.BytesIO()
        with zipfile.ZipFile(data, "w") as archive:
            for name, content in self.contents.items():
                archive.writestr(name, content)
        with zipfile.ZipFile(data) as archive:
            VERIFIER.verify_archive_notices(
                archive, "fixture.jar", PROJECT_NOTICES, classifier,
                lambda condition, message: errors.append(message) if not condition else None,
                generator)
        return errors

    def test_accepts_openjdk_notices_and_oracle_copyright(self):
        self.assertEqual([], self.check_archive(generator=OPENJDK_NOTICES))

    def test_rejects_proprietary_marker_in_arbitrary_entry(self):
        self.contents["assets/data"] = b"Oracle\nPROPRIETARY / CONFIDENTIAL"
        self.assertTrue(any("proprietary/confidential" in error for error in self.check_archive()))

    def test_rejects_otn_pointer_even_with_otherwise_valid_notices(self):
        self.contents["extra.txt"] = b"Please refer to https://java.com/OTNLICENSE"
        self.assertTrue(any("OTN license" in error for error in self.check_archive()))

    def test_rejects_proprietary_marker_in_binary_and_source_archives(self):
        for classifier in ("", "-sources"):
            with self.subTest(classifier=classifier):
                self.contents = {"META-INF/" + name: data for name, data in PROJECT_NOTICES.items()}
                self.contents["extra.txt"] = b"ORACLE PROPRIETARY/CONFIDENTIAL"
                self.assertTrue(any("proprietary/confidential" in error
                                    for error in self.check_archive(classifier)))

    def test_preserves_exact_project_notices_in_every_classifier(self):
        for classifier in ("", "-sources", "-javadoc"):
            with self.subTest(classifier=classifier):
                prefix = "resources/" if classifier == "-javadoc" else "META-INF/"
                self.contents.update({prefix + name: data for name, data in PROJECT_NOTICES.items()})
                self.contents[prefix + "NOTICE"] = b"Different project attribution\n"
                self.assertTrue(any("NOTICE differs from repository" in error
                                    for error in self.check_archive(classifier)))

    def test_rejects_missing_project_license(self):
        del self.contents["resources/LICENSE"]
        self.assertTrue(any("missing resources/LICENSE" in error for error in self.check_archive()))

    def test_rejects_missing_gpl_license(self):
        del self.contents["legal/LICENSE"]
        self.assertTrue(any("missing third-party notice legal/LICENSE" in error
                            for error in self.check_archive()))

    def test_rejects_missing_classpath_exception(self):
        self.contents["legal/LICENSE"] = b"The GNU General Public License (GPL) Version 2\n"
        self.assertTrue(any("unexpected third-party notice legal/LICENSE" in error
                            for error in self.check_archive()))

    def test_rejects_missing_additional_and_assembly_notices(self):
        del self.contents["legal/ADDITIONAL_LICENSE_INFO"]
        del self.contents["legal/ASSEMBLY_EXCEPTION"]
        self.assertEqual(2, len(self.check_archive()))

    def test_jquery_notice_required_only_when_resource_present(self):
        self.assertEqual([], self.check_archive())
        self.contents["script-dir/jquery-3.7.1.min.js"] = b"/* jQuery */"
        self.assertTrue(any("missing third-party notice legal/jquery.md" in error
                            for error in self.check_archive()))
        self.contents["legal/jquery.md"] = MIT_NOTICE
        self.assertEqual([], self.check_archive())

    def test_jquery_ui_requires_its_own_notice(self):
        self.contents["script-dir/jquery-ui.min.js"] = b"/* jQuery UI */"
        self.contents["legal/jquery.md"] = MIT_NOTICE
        self.assertTrue(any("missing third-party notice legal/jqueryUI.md" in error
                            for error in self.check_archive()))
        self.contents["legal/jqueryUI.md"] = MIT_NOTICE
        self.assertEqual([], self.check_archive())

    def test_rejects_truncated_jquery_notice(self):
        self.contents["script-dir/jquery-3.7.1.min.js"] = b"/* jQuery */"
        self.contents["legal/jquery.md"] = b"Copyright OpenJS Foundation"
        self.assertTrue(any("unexpected third-party notice legal/jquery.md" in error
                            for error in self.check_archive()))

    def test_rejects_generator_notice_changed_even_if_required_phrases_remain(self):
        self.contents["legal/LICENSE"] += b"Unauthorized license modification\n"
        self.assertTrue(any("LICENSE differs from Javadoc generator" in error
                            for error in self.check_archive(generator=OPENJDK_NOTICES)))

    def test_all_generator_notices_must_be_preserved(self):
        generator = dict(OPENJDK_NOTICES, **{"new-resource.md": b"Additional third-party terms\n"})
        self.assertTrue(any("missing generator notice legal/new-resource.md" in error
                            for error in self.check_archive(generator=generator)))
        self.contents["legal/new-resource.md"] = generator["new-resource.md"]
        self.assertEqual([], self.check_archive(generator=generator))


if __name__ == "__main__":
    unittest.main()
