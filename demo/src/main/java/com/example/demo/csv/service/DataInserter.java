package com.example.demo.csv.service;


import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

@Service
public class DataInserter {

    @PostConstruct
    public void printMe(){
        System.out.print("hello");
    }
}
