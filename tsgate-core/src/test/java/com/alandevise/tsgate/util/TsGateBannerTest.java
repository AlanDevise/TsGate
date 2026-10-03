package com.alandevise.tsgate.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import java.util.Properties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class TsGateBannerTest {
    @ParameterizedTest @ValueSource(strings = {"IoTDB", "InfluxDB", "InfluxDB1"})
    void logsNewBrandBuildVersionAndBackend(String backend) throws Exception {
        Properties properties = new Properties();
        try (var input = getClass().getClassLoader().getResourceAsStream("META-INF/tsgate.properties")) {
            assertThat(input).isNotNull();
            properties.load(input);
        }
        Logger logger = mock(Logger.class);
        TsGateBanner.print(logger, backend);
        ArgumentCaptor<String> art = ArgumentCaptor.forClass(String.class);
        verify(logger).info(eq("\n{}TsGate {} · {} initialized"), art.capture(),
                eq(properties.getProperty("version")), eq(backend));
        assertThat(art.getValue()).contains("\\_|").hasLineCount(3);
        assertThat(properties.getProperty("version")).doesNotContain("@", "${");
    }
}
