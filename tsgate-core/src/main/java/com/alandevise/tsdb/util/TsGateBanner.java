package com.alandevise.tsdb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Prints the TsGate startup banner.
 * <p>After successful initialization, logs the banner, build version, and backend type through the application's logging implementation.
 * Maven writes the version into a resource during the build, so startup requires no access to an external version repository.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-21
 */
public final class TsGateBanner {

    private static final Logger LOGGER = LoggerFactory.getLogger(TsGateBanner.class);
    private static final String VERSION_RESOURCE = "META-INF/tsgate.properties";
    private static final String UNKNOWN_VERSION = "unknown";
    private static final String VERSION = resolveVersion();
    private static final String BANNER = """
            ___  __
             | _/__ _._|_ _
             |_>\\_|(_| |_(/_
            """;

    /**
     * Prevents instantiation of this stateless banner utility.
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    private TsGateBanner() {
    }

    /**
     * Logs the TsGate banner, adapter build version, and successfully initialized backend type.
     *
     * @param logger      logger to use, for example the auto-configuration class's SLF4J logger
     * @param adapterType initialized backend type, for example {@code "IoTDB"} or {@code "InfluxDB"}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    public static void print(Logger logger,
                             String adapterType) {
        logger.info("\n{}TsGate {} · {} initialized", BANNER, VERSION, adapterType);
    }

    /**
     * Reads the build version from a Maven-filtered resource, falling back to the JAR manifest when unavailable.
     *
     * @return current adapter build version, or {@code unknown} when unavailable
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    private static String resolveVersion() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader == null) {
            LOGGER.warn("Thread context ClassLoader is unavailable; "
                    + "falling back to the JAR Manifest for the TsGate version");
        } else {
            try (InputStream input = contextClassLoader.getResourceAsStream(VERSION_RESOURCE)) {
                if (input != null) {
                    Properties properties = new Properties();
                    properties.load(input);
                    String resourceVersion = normalizeVersion(properties.getProperty("version"));
                    if (resourceVersion != null) {
                        return resourceVersion;
                    }
                }
            } catch (IOException | RuntimeException exception) {
                LOGGER.warn("Failed to read the TsGate version resource; "
                        + "falling back to the JAR Manifest", exception);
            }
        }

        Package adapterPackage = TsGateBanner.class.getPackage();
        String manifestVersion = adapterPackage == null
                ? null : normalizeVersion(adapterPackage.getImplementationVersion());
        return manifestVersion == null ? UNKNOWN_VERSION : manifestVersion;
    }

    /**
     * Normalizes a version candidate, rejecting blanks and unresolved Maven resource placeholders.
     *
     * @param version candidate version
     * @return displayable version, or {@code null} when the candidate is invalid
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-21
     */
    private static String normalizeVersion(String version) {
        if (version == null || version.isBlank()
                || version.contains("${") || version.contains("@project.version@")) {
            return null;
        }
        return version.trim();
    }
}
