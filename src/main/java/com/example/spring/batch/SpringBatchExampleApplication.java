package com.example.spring.batch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.util.Arrays;
import java.util.Random;
import java.util.Arrays;
@SpringBootApplication
@EnableBatchProcessing
public class SpringBatchExampleApplication {
    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(SpringBatchExampleApplication.class, args)));
    }
}





