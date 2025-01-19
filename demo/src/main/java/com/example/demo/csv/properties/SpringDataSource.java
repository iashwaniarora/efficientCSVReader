package com.example.demo.csv.properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;

@Component
public class SpringDataSource {

    @Autowired
    private DataSource dataSource;

    public Connection getCurrentConnection() {
        return DataSourceUtils.getConnection(dataSource);
    }
}
