package com.example.spring.batch.part5;

import io.micrometer.core.instrument.util.StringUtils;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

// JobExecutionDecider를 통한 주문 금액 집계 Step 실행 여부 결정
public class JobParametersDecide implements JobExecutionDecider {

    public static final FlowExecutionStatus CONTINUE = new FlowExecutionStatus("CONTINUE");

    // JobParameters의 key로 key에 해당하는 파라미터값이 있는지 체크하기 위해
    private final String key;

    public JobParametersDecide(String key) {
        this.key = key;
    }

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
        // jobParameter로 주입 받은 키에 밸류가 있는지 체크
        String value = jobExecution.getJobParameters().getString(key);

        if (StringUtils.isEmpty(value)){
            return FlowExecutionStatus.COMPLETED;
        }
        return CONTINUE;
    }
}
