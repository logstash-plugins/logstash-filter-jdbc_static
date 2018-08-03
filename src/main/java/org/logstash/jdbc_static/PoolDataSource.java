package org.logstash.jdbc_static;

import com.zaxxer.hikari.HikariConfig;

import java.sql.Connection;
import java.sql.SQLException;

public interface PoolDataSource {
    Connection getConnection() throws SQLException;

    HikariConfig getConfig();

    void close();
}
