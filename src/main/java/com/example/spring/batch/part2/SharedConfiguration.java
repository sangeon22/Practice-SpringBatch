package com.example.spring.batch.part2;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class SharedConfiguration {

    /*
    JobExecutionContext와 StepExecutionContext의 사용 범위
    JobExecutionContext는 Job이 관리하는 Step내에서 어디서든 데이터를 공유할 수 있고
    StepExecutionContext 해당 Step에서만 데이터를 공유할 수 있다.
     */


    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public SharedConfiguration(JobBuilderFactory jobBuilderFactory,
                               StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Job shareJob() {
        return jobBuilderFactory.get("shareJob")
                .incrementer(new RunIdIncrementer())
                .start(this.shareStep())
                .next(this.shareStep2())
                .build();
    }

    @Bean
    public Step shareStep() {
        return stepBuilderFactory.get("shareStep")
                // contribution 객체를 이용해 StepExecution 객체를 꺼냄
                // StepExecution을 이용해 stepExecutionContext를 꺼냄
                .tasklet((contribution, chunkContext) -> {
                    StepExecution stepExecution = contribution.getStepExecution();
                    ExecutionContext stepExecutionContext = stepExecution.getExecutionContext();
                    // stepExecutionContext에 데이터를 하나 넣음
                    stepExecutionContext.putString("stepKey", "step execution context");

                    // StepExecution을 이용해 JobExecution을 꺼냄
                    JobExecution jobExecution = stepExecution.getJobExecution();
                    // jobExecution에서 jobInstance, jobExecutionContext를 꺼냄
                    JobInstance jobInstance = jobExecution.getJobInstance();
                    ExecutionContext jobExecutionContext = jobExecution.getExecutionContext();
                    // jobExecutionContext에 데이터를 하나 넣음
                    jobExecutionContext.putString("jobKey", "job execution context");
                    JobParameters jobParameters = jobExecution.getJobParameters();

                    log.info("jobName : {}, stepName : {}, parameter: {}",
                            jobInstance.getJobName(),
                            stepExecution.getStepName(),
                            jobParameters.getLong("run.id"));

                    return RepeatStatus.FINISHED;
                })
                .build();

    }


    @Bean
    public Step shareStep2() {
        return stepBuilderFactory.get("shareStep2")
                .tasklet((contribution, chunkContext) -> {
                    StepExecution stepExecution = contribution.getStepExecution();
                    ExecutionContext stepExecutionContext = stepExecution.getExecutionContext();

                    JobExecution jobExecution = stepExecution.getJobExecution();
                    ExecutionContext jobExecutionContext = jobExecution.getExecutionContext();

                    log.info("jobKey : {}, stepKey : {}",
                            jobExecutionContext.getString("jobKey", "emptyJobKey"),
                            stepExecutionContext.getString("stepKey", "emptyStepKey"));

                    return RepeatStatus.FINISHED;
                })
                .build();
    }

}


