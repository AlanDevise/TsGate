package com.alandevise.tsdb.integration;

import com.alandevise.tsdb.adapter.impl.InfluxDBAdapter;
import com.alandevise.tsdb.annotation.*;
import com.alandevise.tsdb.core.TGTemplate;
import com.alandevise.tsdb.config.InfluxDBProperties;
import com.influxdb.v3.client.InfluxDBClient;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Configuration;
import java.net.URI;
import java.net.http.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class InfluxDBStarterDockerIT {
    @ParameterizedTest @ValueSource(booleans = {true, false})
    void discoveredStarterBindsPropertiesAndExposesWorkingNativeClientAndTemplate(boolean omitDatabase) throws Exception {
        String db=omitDatabase ? "tsdb" : "it_spring_influx_"+Long.toUnsignedString(System.nanoTime(),36);
        String url=System.getProperty("tsdb.it.influxdb.url","http://127.0.0.1:18181");
        Map<String, Object> settings = new HashMap<>(Map.of(
                "spring.main.banner-mode", "off", "tsdb.influxdb.url", url,
                "tsdb.query-log-enabled", "false"));
        settings.put("tsdb.influxdb.enable", "true");
        if (!omitDatabase) settings.put("tsdb.influxdb.database", db);
        HttpClient admin=HttpClient.newHttpClient();
        {
            HttpResponse<String> created=admin.send(HttpRequest.newBuilder(URI.create(url+"/api/v3/configure/database")).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString("{\"db\":\""+db+"\"}")).build(),HttpResponse.BodyHandlers.ofString());
            assertEquals(200,created.statusCode(),created.body());
            try(var context=new SpringApplicationBuilder(App.class).web(WebApplicationType.NONE).properties(settings).run()) {
                assertEquals(1,context.getBeansOfType(TGTemplate.class).size());
                    assertTrue(context.getBean(InfluxDBProperties.class).isEnable());
                assertEquals(db, context.getBean(InfluxDBProperties.class).getDatabase());
                InfluxDBAdapter adapter=context.getBean(InfluxDBAdapter.class);
                assertSame(adapter.getNativeClient(),context.getBean(InfluxDBClient.class));
                TGTemplate template=context.getBean(TGTemplate.class);
                Point point=new Point(); point.time=System.currentTimeMillis(); point.device="spring"; point.value=42d;
                assertTrue(template.write(point)); assertEquals(42d,template.query(Point.class).list().get(0).value);
                try(var stream=context.getBean(InfluxDBClient.class).query("SELECT value FROM spring_metrics")) { assertEquals(1,stream.count()); }
            } finally {
                HttpResponse<String> deleted=admin.send(HttpRequest.newBuilder(URI.create(url+"/api/v3/configure/database?db="+db)).DELETE().build(),HttpResponse.BodyHandlers.ofString());
                assertEquals(200,deleted.statusCode(),deleted.body());
            }
        }
    }
    @Configuration(proxyBeanMethods=false) @EnableAutoConfiguration static class App { }
    @TGMeasurement("spring_metrics") public static class Point {
        @TGTime public Long time; @TGTag public String device; @TGField public Double value;
        public Point() { }
    }
}
