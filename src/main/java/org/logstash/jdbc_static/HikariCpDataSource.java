package org.logstash.jdbc_static;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class HikariCpDataSource implements PoolDataSource {
    private final HikariDataSource pool;
    private final HikariConfig config;

    public HikariCpDataSource(final String driverClass, final String url, final String username, final String password) {
        // this.config.setDriverClassName(driverClass);
        config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setPoolName("logstash.plugin.jdbc_static");
        final int poolSize = Runtime.getRuntime().availableProcessors() + 1;
        config.setMaximumPoolSize(poolSize);
        if (username != null && password != null) {
            config.setUsername(username);
            config.setPassword(password);
        }
//        config.addDataSourceProperty("maximumPoolSize", String.valueOf(poolSize));
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        this.pool = new HikariDataSource(config);
    }

    @Override
    public final Connection getConnection() throws SQLException {
        return this.pool.getConnection();
    }

    @Override
    public final HikariConfig getConfig() {
        return config;
    }

    @Override
    public final void close() {
        this.pool.close();
    }
}
