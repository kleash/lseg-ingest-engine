package com.lseg.ingest;

import com.lseg.ingest.config.IngestProperties;
import com.lseg.ingest.orchestrator.IngestOrchestrator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableConfigurationProperties(IngestProperties.class)
@EnableScheduling
public class IngestApplication {

    private static final Logger log = LoggerFactory.getLogger(IngestApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(IngestApplication.class, args);
    }

    @Bean
    public MeterRegistry meterRegistry() {
        return new SimpleMeterRegistry();
    }
}
