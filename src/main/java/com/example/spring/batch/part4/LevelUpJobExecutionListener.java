package com.example.spring.batch.part4;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import java.time.LocalDate;
import java.util.Collection;

@Slf4j
public class LevelUpJobExecutionListener implements JobExecutionListener {

    private final UserRepository userRepository;

    public LevelUpJobExecutionListener(UserRepository userRepository) {
        this.userRepository = userRepository;
    }


    @Override
    public void beforeJob(JobExecution jobExecution) {

    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        Collection<User> users = userRepository.findALlByUpdatedDate(LocalDate.now());

        // Job에 실행된 시간을 측정
        long time = jobExecution.getEndTime().getTime() - jobExecution.getStartTime().getTime();
        log.info("----------------------------");
        log.info("회원등급 업데이트 배치 프로그램");
        log.info("----------------------------");
        log.info("총 데이터 처리 : {}건" ,users.size());
        log.info("총 처리 시간 : {}millis", time);
        log.info("----------------------------");

    }
}
