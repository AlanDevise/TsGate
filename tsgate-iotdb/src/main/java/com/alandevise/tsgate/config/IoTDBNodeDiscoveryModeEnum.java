package com.alandevise.tsgate.config;

/**
 * Node discovery and redirection policy for the IoTDB client.
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-23
 */
public enum IoTDBNodeDiscoveryModeEnum {

    /**
     * Choose automatically: disable discovery for one endpoint and enable it for multiple endpoints.
     */
    AUTO,

    /**
     * Always enable node discovery and redirection.
     */
    ENABLED,

    /**
     * Always disable node discovery and redirection.
     */
    DISABLED
}
