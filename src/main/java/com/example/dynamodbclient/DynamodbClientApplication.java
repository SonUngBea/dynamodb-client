package com.example.dynamodbclient;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

//@Import(DynamodbClientApplication.class)
@SpringBootApplication
public class DynamodbClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(DynamodbClientApplication.class, args);
    }

}
