package com.example.spring.batch.part1;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class HelloConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public HelloConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    // 하나의 job은 여러개의 step을 가질 수 있고 Bean으로 관리
    // job은 배치 실행단위
    // step은 job의 실행단위
    @Bean
    public Job helloJob(){
        return jobBuilderFactory.get("helloJob")
                // RunIdIncrementer()은 새로운 잡인스턴스를 만들도록 돕는다.
                // RunId라는 파라미터값을 자동으로 시퀀셜하게 생성하기 때문이다.
                // 그래서 파라미터가 없지만 새롭게 잡인스턴스가 만들어지면서 재실행이 되는 것임
                .incrementer(new RunIdIncrementer())
                .start(this.helloStep())
                .build();
    }

    @Bean
    public Step helloStep(){
        return stepBuilderFactory.get("helloStep")
                //태스크 기반, 정크 기반
                .tasklet((contribution, chunkContext) -> {
                    log.info("hello spring batch");
                    return RepeatStatus.FINISHED;
                }).build();
    }


}
