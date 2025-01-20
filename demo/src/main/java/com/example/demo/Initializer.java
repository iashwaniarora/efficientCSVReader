package com.example.demo;

import com.example.demo.csv.properties.Property;
import com.example.demo.csv.service.CSVReader;

import com.example.demo.csv.service.CSVReaderUnivocity;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class Initializer {

    @Autowired
    CSVReaderUnivocity csvReader;


    @Autowired
    Property properties;

    @PostConstruct
    public void starProcess(){
        System.out.println("Starting process");
        csvReader.readData();
    }
}
