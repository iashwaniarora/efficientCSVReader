package com.example.demo.csv.service;


import com.example.demo.csv.properties.Property;
import com.example.demo.csv.properties.SpringDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
public class CSVReader {

    private static final Logger log = LoggerFactory.getLogger(CSVReader.class);
    @Autowired
    Property properties;

    @Autowired
    SpringDataSource connectionDataSource;

    public void readData() {

        String folderLocation = properties.csvFolderLocation;

       log.info("Reading Folder" + folderLocation);
        File folder = new File(folderLocation);
        File[] listOfFiles = folder.listFiles();
       log.info("Files to be processed :-"+ Arrays.toString(listOfFiles));
        if (listOfFiles != null) {
            Arrays.stream(listOfFiles).filter(file -> file.getName().endsWith(".csv")).forEach(
                    file -> {
                        try {
                            readCSVFile(folderLocation, file.getName());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
        }
    }

    private void readCSVFile(String folderLocation, String fileName) throws IOException {

       log.info("Reading CSV file {}" , fileName);
        log.info("***************************************************");
        //Get Headers
        CSVParser headerParser = CSVParser.parse(new FileReader(folderLocation + FileSystems.getDefault().getSeparator() + fileName), CSVFormat.DEFAULT.builder().setSkipHeaderRecord(false).build());
        List<String> headers = null;
        int i = 0;
        for (CSVRecord csvRecord : headerParser) {
            if (i == 0) {
                headers = Arrays.stream(csvRecord.values()).toList();
               log.info("Headers initialized :-" + headers);
                break;
            }
        }

        String tableName = FilenameUtils.removeExtension(fileName);
        try {
            createTable(headers, tableName);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

       log.info("Starting reading and adding data to DB.");
        //Read Data
        try (CSVParser parser = new CSVParser(new FileReader(folderLocation + FileSystems.getDefault().getSeparator() + fileName), CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {
            int batchSize = properties.linesReadBatchLimit;
            List<CSVRecord> batch = new ArrayList<>(batchSize);
            int batchCount=1;
            for (CSVRecord record : parser) {

                batch.add(record);
                if (batch.size() == batchSize) {
                   log.info("Processing Batch Number :"+batchCount);
                    batchCount++;
                    processBatch(batch, headers, tableName);
                    batch.clear();

                }
            }

            // Process any remaining records
            if (!batch.isEmpty()) {
               log.info("Processing last Batch Number :"+batchCount);
                processBatch(batch, headers, tableName);
            }
        } catch (FileNotFoundException | SQLException e) {
           log.info(e.getMessage());
        } catch (IOException e) {
           log.info(e.getMessage());
        }
    }

    private void processBatch(List<CSVRecord> lines, List<String> headers, String tableName) throws SQLException {
        // Do something with the batch of records

       log.info("Adding Batch Data to the DB");
        Connection connection = connectionDataSource.getCurrentConnection();

        Statement stmt = connection.createStatement();

        final String template = "INSERT INTO %s (%s) VALUES (%s);";
        List<String> statements = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {


            CSVRecord csvRow = lines.get(i);
            StringBuilder cols = new StringBuilder();
            StringBuilder vals = new StringBuilder();

            for (int j = 0; j < headers.size(); j++) {

                cols.append(headers.get(j));

                vals.append("'").append(RegExUtils.replaceAll(csvRow.get(j),"'","''")).append("'");

                if (j != headers.size() - 1) {
                    cols.append(",");
                    vals.append(",");
                }
            }

            statements.add(String.format(template, tableName, cols.toString(), vals.toString()));
        }
        statements.forEach(statement -> {
            try {
                stmt.addBatch(statement);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
       log.info(String.valueOf(statements));
        stmt.executeBatch();
       log.info("Batch Data Added to the database.");
        connection.close();
    }


    private void createTable(List<String> headers, String fileName) throws SQLException {

       log.info("creating table if does not exists :-"+fileName);
        String createTableQuerySkeleton = "if not exists (select * from sysobjects where name='tablename' and xtype='U')" +
                "       create table tablename";
        createTableQuerySkeleton = createTableQuerySkeleton.replaceAll("tablename", fileName);
        StringBuilder createTableQuery = new StringBuilder(createTableQuerySkeleton)
                .append(" (");

        for (String header : headers) {
            createTableQuery.append(header).append(" VARCHAR(" + properties.tableColumnLength + "), ");
        }

        createTableQuery.delete(createTableQuery.length() - 2, createTableQuery.length());
        createTableQuery.append(")");

        Connection connection = connectionDataSource.getCurrentConnection();

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(createTableQuery.toString());
        } catch (SQLException e) {
           log.info(e.getMessage());
        }
       log.info("Table creation complete.");
        connection.close();

    }


}



