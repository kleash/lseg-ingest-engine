package com.lseg.ingest.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    @Bean
    @Primary
    public DataSource runtimeDataSource(
            @Value("${db.runtime.url}") String url,
            @Value("${db.runtime.user}") String user,
            @Value("${db.runtime.password}") String password,
            @Value("${db.runtime.pool.maxSize:16}") int maxSize,
            @Value("${db.runtime.pool.minIdle:4}") int minIdle,
            @Value("${db.runtime.pool.connectionTimeoutMs:30000}") long connTimeoutMs) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        cfg.setUsername(user);
        cfg.setPassword(password);
        cfg.setMaximumPoolSize(maxSize);
        cfg.setMinimumIdle(minIdle);
        cfg.setConnectionTimeout(connTimeoutMs);
        cfg.setPoolName("runtime");
        // Validate connections handed out from the pool; bounded test query for MariaDB.
        cfg.setConnectionTestQuery("SELECT 1");
        cfg.setValidationTimeout(5000);
        // Leak detection: fire warning if a connection is held longer than 10 minutes
        // (typical file ingestion is seconds; long holds indicate a stuck thread).
        cfg.setLeakDetectionThreshold(600000);
        cfg.setKeepaliveTime(120000);
        return new HikariDataSource(cfg);
    }
}
