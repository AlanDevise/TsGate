package com.alandevise.tsgate.contract;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Strict reader for the committed test capability table, without a production SPI dependency. */
public final class BackendContractRegistry {
    public static final String RESOURCE = "/contracts/adapter-capabilities.csv";
    public static final String CONDITIONS_RESOURCE = "/contracts/adapter-conditions.csv";

    public enum Status { SUPPORTED, UNSUPPORTED, CONFIG_REQUIRED }

    public record Condition(String backend, BackendCapability capability, List<String> serverVersions,
                            String property, String requiredValue, String regressionTest) {
        public Condition { serverVersions = List.copyOf(serverVersions); }
    }
    private static final Map<String, Profile> PROFILES = load();
    private static final List<Condition> CONDITIONS = loadConditions();

    private BackendContractRegistry() { }

    public record Profile(String id, String module, String contractTest,
                          Map<BackendCapability, Status> capabilities) {
        public Profile {
            capabilities = Collections.unmodifiableMap(new EnumMap<>(capabilities));
        }

        public boolean supports(BackendCapability capability) {
            Status supported = capabilities.get(capability);
            if (supported == null) throw new IllegalStateException("Missing capability: " + id + "/" + capability);
            return supported != Status.UNSUPPORTED;
        }
    }

    public static List<Profile> profiles() {
        return List.copyOf(PROFILES.values());
    }

    public static Profile profile(String id) {
        Profile profile = PROFILES.get(id);
        if (profile == null) throw new IllegalArgumentException("Backend is not registered: " + id);
        return profile;
    }

    public static List<Condition> conditions(String backend, BackendCapability capability) {
        profile(backend);
        return CONDITIONS.stream().filter(condition -> condition.backend().equals(backend)
                && condition.capability() == capability).toList();
    }

    public static List<Condition> conditions() { return CONDITIONS; }

    private static Map<String, Profile> load() {
        InputStream input = BackendContractRegistry.class.getResourceAsStream(RESOURCE);
        if (input == null) throw new IllegalStateException("Missing backend capability matrix: " + RESOURCE);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            String expectedHeader = "backend,module,contractTest," + String.join(",",
                    java.util.Arrays.stream(BackendCapability.values()).map(Enum::name).toList());
            if (!expectedHeader.equals(reader.readLine())) throw new IllegalStateException("Invalid capability matrix header");
            Map<String, Profile> profiles = new LinkedHashMap<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) throw new IllegalStateException("Blank backend capability matrix row");
                String[] cells = line.split(",", -1);
                if (cells.length != 3 + BackendCapability.values().length)
                    throw new IllegalStateException("Incomplete backend capability matrix row: " + line);
                for (String cell : cells) {
                    if (cell.isBlank() || !cell.equals(cell.trim()))
                        throw new IllegalStateException("Blank or padded capability cell: " + line);
                }
                Map<BackendCapability, Status> capabilities = new EnumMap<>(BackendCapability.class);
                for (int index = 0; index < BackendCapability.values().length; index++) {
                    String cell = cells[index + 3];
                    capabilities.put(BackendCapability.values()[index], Status.valueOf(cell));
                }
                Profile profile = new Profile(cells[0], cells[1], cells[2], capabilities);
                if (profiles.putIfAbsent(profile.id(), profile) != null)
                    throw new IllegalStateException("Duplicate backend registration: " + profile.id());
            }
            if (profiles.isEmpty()) throw new IllegalStateException("Backend capability matrix must not be empty");
            return Collections.unmodifiableMap(profiles);
        } catch (IOException error) {
            throw new IllegalStateException("Cannot read backend capability matrix", error);
        }
    }
    private static List<Condition> loadConditions() {
        InputStream input = BackendContractRegistry.class.getResourceAsStream(CONDITIONS_RESOURCE);
        if (input == null) throw new IllegalStateException("Missing configuration conditions: " + CONDITIONS_RESOURCE);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            if (!"backend,capability,serverVersions,property,requiredValue,regressionTest".equals(reader.readLine()))
                throw new IllegalStateException("Invalid configuration condition header");
            java.util.ArrayList<Condition> conditions = new java.util.ArrayList<>();
            java.util.HashSet<String> keys = new java.util.HashSet<>();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] cells = line.split(",", -1);
                if (cells.length != 6) throw new IllegalStateException("Incomplete configuration condition: " + line);
                for (String cell : cells) {
                    if (cell.isBlank() || !cell.equals(cell.trim()))
                        throw new IllegalStateException("Blank or padded configuration condition: " + line);
                }
                BackendCapability capability = BackendCapability.valueOf(cells[1]);
                if (profile(cells[0]).capabilities().get(capability) != Status.CONFIG_REQUIRED)
                    throw new IllegalStateException("Configuration condition requires CONFIG_REQUIRED status: " + line);
                List<String> versions = List.of(cells[2].split("\\|", -1));
                if (versions.stream().anyMatch(version -> !version.matches("[0-9]+\\.[0-9]+\\.[0-9]+")))
                    throw new IllegalStateException("Conditions must list exact server versions: " + line);
                Condition condition = new Condition(cells[0], capability, versions, cells[3], cells[4], cells[5]);
                if (!keys.add(cells[0] + "/" + cells[1] + "/" + cells[3]))
                    throw new IllegalStateException("Duplicate configuration condition: " + line);
                conditions.add(condition);
            }
            for (Profile profile : profiles()) {
                for (BackendCapability capability : BackendCapability.values()) {
                    if (profile.capabilities().get(capability) == Status.CONFIG_REQUIRED
                            && conditions.stream().noneMatch(condition -> condition.backend().equals(profile.id())
                            && condition.capability() == capability))
                        throw new IllegalStateException("Missing configuration prerequisite: " + profile.id() + "/" + capability);
                }
            }
            return List.copyOf(conditions);
        } catch (IOException error) {
            throw new IllegalStateException("Cannot read configuration conditions", error);
        }
    }

}
