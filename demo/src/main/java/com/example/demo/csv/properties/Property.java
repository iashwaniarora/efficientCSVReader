package com.example.demo.csv.properties;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Configuration
@Getter
@Setter
@NoArgsConstructor
public class Property {

    @Value("${createTable:false}" )
   public  boolean createTable;

     @Value("${folderToReadCSVFilePath}" )
    public String csvFolderLocation;

      @Value("${linesReadBatchLimit}" )
     public int linesReadBatchLimit;

    @Value("${tableColumnLength}" )
      public int tableColumnLength;

    @Value("${delimiter:\"}" )
    public Character columnDelimiter;

    @Value("${lineSeparator:\\r\\n}")
    public String lineSeparator;
}
