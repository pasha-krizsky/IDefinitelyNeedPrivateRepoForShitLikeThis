package com.pasha.oracleToCsvDataMigration.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import oracle.jdbc.pool.OracleDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.SQLException;

@Configuration
@Import(BaseConfig.class)
public class DatabaseConfig {

    @Value("${oracle.url}")
    private String oracleUrl;

    @Value("${oracle.username}")
    private String oracleUsername;

    @Value("${oracle.password}")
    private String oraclePassword;

    @Bean
    public JdbcTemplate jdbcTemplate() throws SQLException {
        return new JdbcTemplate(hikariDataSource());
    }

    @Bean
    public HikariDataSource hikariDataSource() throws SQLException {
        return new HikariDataSource(hikariConfig());
    }

    @Bean
    public HikariConfig hikariConfig() throws SQLException {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDataSource(dataSource());
        return hikariConfig;
    }

    @Bean
    public DataSource dataSource() throws SQLException {
        OracleDataSource oracleDataSource = new OracleDataSource();
        oracleDataSource.setURL(oracleUrl);
        oracleDataSource.setUser(oracleUsername);
        oracleDataSource.setPassword(oraclePassword);
        return oracleDataSource;
    }
}
