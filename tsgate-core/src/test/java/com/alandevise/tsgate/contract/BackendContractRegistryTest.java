package com.alandevise.tsgate.contract;

import org.junit.jupiter.api.Test;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class BackendContractRegistryTest {
    @Test
    void everyAdapterModuleRegistersOneCompleteCapabilityProfileAndExecutableContract() throws Exception {
        Path root = repositoryRoot();
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        NodeList modules = factory.newDocumentBuilder().parse(root.resolve("pom.xml").toFile())
                .getElementsByTagName("module");
        Set<String> actualAdapters = new HashSet<>();
        for (int index = 0; index < modules.getLength(); index++) {
            String module = modules.item(index).getTextContent().trim();
            Path implementations = root.resolve(module).resolve("src/main/java/com/alandevise/tsgate/adapter/impl");
            if (Files.isDirectory(implementations)) {
                try (var sources = Files.list(implementations)) {
                    if (sources.anyMatch(path -> path.getFileName().toString().endsWith("Adapter.java")))
                        actualAdapters.add(module);
                }
            }
        }
        Set<String> registeredModules = new HashSet<>();
        Set<String> registeredTests = new HashSet<>();
        for (BackendContractRegistry.Profile profile : BackendContractRegistry.profiles()) {
            assertTrue(registeredModules.add(profile.module()), "Duplicate module: " + profile.module());
            assertTrue(registeredTests.add(profile.contractTest()), "Duplicate contract: " + profile.contractTest());
            assertEquals("tsgate-" + profile.id(), profile.module());
            assertEquals(Set.of(BackendCapability.values()), profile.capabilities().keySet());
            Path test = root.resolve(profile.module()).resolve("src/test/java")
                    .resolve(profile.contractTest().replace('.', '/') + ".java");
            assertTrue(Files.isRegularFile(test), "Missing executable contract: " + test);
            assertTrue(Files.readString(test).contains("implements SharedAdapterContract"),
                    "Registered test must execute the shared contract: " + test);
        }
        assertEquals(actualAdapters, registeredModules,
                "Every backend module must register its real supported and unsupported operations");
        assertFalse(registeredModules.isEmpty());
    }

    @Test
    void configurationPrerequisitesReferenceRealVersionRegressionMethods() throws Exception {
        Path root = repositoryRoot();
        for (BackendContractRegistry.Condition condition : BackendContractRegistry.conditions()) {
            String[] target = condition.regressionTest().split("#", -1);
            assertEquals(2, target.length, "A prerequisite must reference an executable test method");
            Path source = root.resolve(BackendContractRegistry.profile(condition.backend()).module())
                    .resolve("src/test/java").resolve(target[0].replace('.', '/') + ".java");
            assertTrue(Files.isRegularFile(source), "Missing version regression test: " + source);
            assertTrue(java.util.regex.Pattern.compile("\\bvoid\\s+" + java.util.regex.Pattern.quote(target[1]) + "\\s*\\(")
                    .matcher(Files.readString(source)).find(), "Missing version regression method: " + condition.regressionTest());
            assertFalse(condition.serverVersions().isEmpty());
            assertTrue(condition.property().startsWith("tsdb."));
        }
    }

    @Test
    void unknownBackendsFailInsteadOfInheritingAnOptimisticDefault() {
        assertThrows(IllegalArgumentException.class, () -> BackendContractRegistry.profile("unregistered-backend"));
        assertEquals(BackendContractRegistry.profiles().size(), BackendContractRegistry.profiles().stream()
                .map(BackendContractRegistry.Profile::id).collect(Collectors.toSet()).size());
    }

    private static Path repositoryRoot() {
        Path directory = Path.of("").toAbsolutePath().normalize();
        while (directory != null) {
            if (Files.isDirectory(directory.resolve("tsgate-core/src/main/java"))
                    && Files.isRegularFile(directory.resolve("pom.xml"))) return directory;
            directory = directory.getParent();
        }
        throw new AssertionError("Run the backend registry check from a TsGate source checkout");
    }
}
