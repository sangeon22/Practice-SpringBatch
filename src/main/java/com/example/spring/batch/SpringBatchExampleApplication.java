package com.example.spring.batch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Arrays;
import java.util.Random;
import java.util.Arrays;
@SpringBootApplication
@EnableBatchProcessing
public class SpringBatchExampleApplication {
    public static void main(String[] args) {
        // Async로 실행될때 안전하게 종료되도록
        System.exit(SpringApplication.exit(SpringApplication.run(SpringBatchExampleApplication.class, args)));
    }

    // ㅔ 생성
    @Bean
    @Primary
    TaskExecutor taskExecutor(){
        // ThreadPoolTaskExecutor는 Pool 안에서 Thread를 몇 개 생성해놓고 필요할 때 꺼내쓸 수 있어 다른 구현체보다 효율적이다.
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        // Pool의 기본 Thread 크기 설정
        taskExecutor.setCorePoolSize(10);
        // 최대 Thread 사이즈 설정
        taskExecutor.setMaxPoolSize(20);
        // Pool에서 생성된 Thread를 사용할 때, 로그에 앞에 이름이 찍힌다.
        taskExecutor.setThreadNamePrefix("batch-thread-");
        taskExecutor.initialize();
        return taskExecutor;
    }
}





