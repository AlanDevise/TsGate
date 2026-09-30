package com.alandevise.tsdb.integration;

import com.alandevise.tsdb.adapter.impl.IoTDBTableAdapter;
import com.alandevise.tsdb.annotation.*;
import com.alandevise.tsdb.core.TGTemplate;
import com.alandevise.tsdb.config.IoTDBProperties;
import com.alandevise.tsdb.exception.TSDBErrorCodeEnum;
import com.alandevise.tsdb.exception.TSDBException;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.session.TableSessionBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Configuration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class AllStartersIoTDBDockerIT {
    @ParameterizedTest @ValueSource(booleans = {false})
    void discoveredStarterBindsPropertiesAndExposesWorkingNativePoolAndTemplate(boolean omitDatabase) throws Exception {
        String db=omitDatabase ? "tsdb" : "it_all_iot_"+Long.toUnsignedString(System.nanoTime(),36);
        String endpoint=System.getProperty("tsdb.it.iotdb.endpoint","127.0.0.1:16667");
        Map<String, Object> settings = new HashMap<>(Map.of(
                "spring.main.banner-mode", "off", "tsdb.iotdb.username", "root", "tsdb.iotdb.password", "root",
                "tsdb.iotdb.pool.node-urls[0]", endpoint, "tsdb.query-log-enabled", "false",
                "tsdb.iotdb.pool.connection-timeout-in-ms", "30000", "tsdb.iotdb.pool.query-timeout-in-ms", "30000",
                "tsdb.iotdb.pool.wait-to-get-session-timeout-in-ms", "30000"));
        settings.put("tsdb.iotdb.table.rpc-compression-enabled", System.getProperty("tsdb.it.iotdb.rpc-compression-enabled", "true"));
        settings.put("tsdb.iotdb.enable", "true");
        settings.put("tsdb.influxdb.url", "${UNUSED_BACKEND_SETTING}");
        settings.put("tsdb.influxdb1.url", "${UNUSED_BACKEND_SETTING}");
        if (!omitDatabase) settings.put("tsdb.iotdb.database", db);
        try(ITableSession admin=new TableSessionBuilder().nodeUrls(List.of(endpoint)).username("root").password("root").enableAutoFetch(false).enableRedirection(false).connectionTimeoutInMs(30000).queryTimeoutInMs(30000).build()) {
            admin.executeNonQueryStatement("CREATE DATABASE "+db);
            try {
                admin.executeNonQueryStatement("CREATE TABLE "+db+".all_starters_metrics (device STRING TAG, value DOUBLE FIELD)");
                IoTDBTableAdapter adapter;
                try(var context=new SpringApplicationBuilder(App.class).web(WebApplicationType.NONE).properties(settings).run()) {
                    assertEquals(1,context.getBeansOfType(TGTemplate.class).size());
                    assertTrue(context.getBean(IoTDBProperties.class).isEnable());
                    assertEquals(1,context.getBeansOfType(com.alandevise.tsdb.adapter.TSDBAdapter.class).size());
                    assertTrue(context.getBeansOfType(com.alandevise.tsdb.adapter.impl.InfluxDB1Adapter.class).isEmpty());
                    assertEquals(db, context.getBean(IoTDBProperties.class).getDatabase());
                    adapter=context.getBean(IoTDBTableAdapter.class);
                    assertSame(adapter.getSessionPool(),context.getBean(ITableSessionPool.class));
                    TGTemplate template=context.getBean(TGTemplate.class);
                    Point point=new Point(); point.time=System.currentTimeMillis(); point.device="spring"; point.value=42d;
                    assertTrue(template.write(point)); assertEquals(42d,template.query(Point.class).list().get(0).value);
                    try(ITableSession session=context.getBean(ITableSessionPool.class).getSession();var result=session.executeQueryStatement("SELECT value FROM all_starters_metrics")) { assertTrue(result.hasNext()); }
                }
                assertEquals(TSDBErrorCodeEnum.ADAPTER_STATE_ERROR,
                        assertThrows(TSDBException.class, adapter::getSessionPool).getErrorCode());
            } finally { admin.executeNonQueryStatement("DROP DATABASE "+db); }
        }
    }
    @Configuration(proxyBeanMethods=false) @EnableAutoConfiguration static class App { }
    @TGMeasurement("all_starters_metrics") public static class Point {
        @TGTime public Long time; @TGTag public String device; @TGField public Double value;
        public Point() { }
    }
}
