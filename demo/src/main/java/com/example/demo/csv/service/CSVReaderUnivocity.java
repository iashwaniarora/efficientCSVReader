package com.example.demo.csv.service;


import com.example.demo.csv.properties.Property;
import com.example.demo.csv.properties.SpringDataSource;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.RegExUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
@Slf4j
public class CSVReaderUnivocity {

    private static final Logger log = LoggerFactory.getLogger(CSVReaderUnivocity.class);
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
                        } catch (IOException | SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
        }
    }

    private void readCSVFile(String folderLocation, String fileName) throws IOException, SQLException {

       log.info("Reading CSV file {}" , fileName);
        log.info("***************************************************");
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setDelimiter(properties.columnDelimiter);
        settings.setLineSeparatorDetectionEnabled(true);
        settings.setHeaderExtractionEnabled(false);
        CsvParser parser = new CsvParser(settings);
        List<String> headers = null;
        System.out.println(properties.columnDelimiter+" "+properties.lineSeparator);
        int i = 0;
        int batchSize = properties.linesReadBatchLimit;
        List<Record> batch = new ArrayList<>(batchSize);
        String tableName = FilenameUtils.removeExtension(fileName);
        int batchCount=1;
        try(Reader reader = Files.newBufferedReader(Paths.get(folderLocation+FileSystems.getDefault().getSeparator()+fileName))){
            parser.beginParsing(reader);
            Record row;
            while ((row = parser.parseNextRecord()) != null) {
                if(i==0) {
                    headers = Arrays.stream(row.getValues()).toList();
                    log.info("Headers initialized :-" + headers);

                    try {
                        createTable(headers, tableName);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                }

            }
        }
         try (Reader reader = Files.newBufferedReader(Paths.get(folderLocation + FileSystems.getDefault().getSeparator() + fileName))) {

            // Process the file row by row

            parser.beginParsing(reader);
            Record row;
            while ((row = parser.parseNextRecord()) != null) {
                if (i == 0) {
                    i++;
                   continue;//Header record
                }

                //parse row data
                batch.add(row);
                if (batch.size() == batchSize) {
                    log.info("Processing Batch Number :"+batchCount);
                    batchCount++;
                    processBatch(batch, headers, tableName);
                    batch.clear();

                }

            }
            if (!batch.isEmpty()) {
                log.info("Processing last Batch Number :"+batchCount);
                processBatch(batch, headers, tableName);
            }
        }
        //Get Headers

    }

    private void processBatch(List<Record> lines, List<String> headers, String tableName) throws SQLException {
        // Do something with the batch of records

       log.info("Adding Batch Data to the DB");
        Connection connection = connectionDataSource.getCurrentConnection();

        Statement stmt = connection.createStatement();

        final String template = "INSERT INTO %s (%s) VALUES (%s);";
        List<String> statements = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {


            Record csvRow = lines.get(i);
            StringBuilder cols = new StringBuilder();
            StringBuilder vals = new StringBuilder();

            for (int j = 0; j < headers.size(); j++) {

                cols.append(headers.get(j));

                vals.append("'").append(RegExUtils.replaceAll(csvRow.getValue(j,String.class),"'","''")).append("'");

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



