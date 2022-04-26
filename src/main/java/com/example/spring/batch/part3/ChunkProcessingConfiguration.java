package com.example.spring.batch.part3;

import io.micrometer.core.instrument.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StreamUtils;

import java.util.ArrayList;
import java.util.List;

@Configuration
@Slf4j
public class ChunkProcessingConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public ChunkProcessingConfiguration(JobBuilderFactory jobBuilderFactory,
                                        StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    /*

    결과를 보면,
    task 기반은 item size가 100으로 한번에 찍히는 반면에
    chunk 기반은 item size, list size가 10개씩 10번 찍힌다.
    chunkSize를 10으로 설정했기 때문에 배치 크기를 일정크기로 반복해서 처리한다.

    물론 task 기반 작업도 chunk처럼 나눠서 처리할 수 있지만, 양이 방대해지고 chunk 기반이 더 잘 구성되어 있다.

     */

    @Bean
    public Job chunkProcessingJob(){
        return jobBuilderFactory.get("chunkProcessingJob")
                .incrementer(new RunIdIncrementer())
                .start(this.taskBaseStep())
//                .next(this.chunkBaseStep())
                // @JobScope를 달아놓았기 떄문에 null로 가능 Spring이 자동으로 Value 값을 체킹
                .next(this.chunkBaseStep(null))
                .build();
    }

    // task 기반
    @Bean
    public Step taskBaseStep() {
        return stepBuilderFactory.get("taskBaseStep")
                //일반 tasklet
//                .tasklet(this.tasklet()
                // @JobScope를 달아놓았기 떄문에 null로 가능
                .tasklet(this.tasklet(null))
                .build();
    }

    // 정석 task 기반
//    private Tasklet tasklet() {
//        return (contribution, chunkContext) -> {
//            List<String> items = getItems();
//            log.info("task item size : {}", items.size());
//            return RepeatStatus.FINISHED;
//        };
//    }

    @Bean
    @StepScope
    public Tasklet tasklet(@Value("#{jobParameters[chunkSize]}") String value){
        // chunk 기반처럼 일정크기로 반복해서 처리하는 task 기반
        List<String> items = getItems();
        return (contribution, chunkContext) -> {
            StepExecution stepExecution = contribution.getStepExecution();
            // 1. 배치의 JobParmeters 객체를 사용하는 경우(외부에서 주입되는 파라미터 사용)
            // jobParameters를 꺼냄
//            JobParameters jobParameters = stepExecution.getJobParameters();
//
//
////            int chunkSize = 10;
//            // jobParameters에서 정크사이즈를 갖는 키를 꺼낸다. 없다면 기본값으로 10을 반환하도록
//            String value = jobParameters.getString("chunkSize", "10");

            int chunkSize = StringUtils.isNotEmpty(value) ? Integer.parseInt(value) : 10;

            int fromIndex = stepExecution.getReadCount();
            int toIndex = fromIndex + chunkSize;

            if (fromIndex>= items.size()){
                return RepeatStatus.FINISHED;
            }

            List<String> sublist = items.subList(fromIndex, toIndex);

            log.info("task item size : {}", sublist.size());

            stepExecution.setReadCount(toIndex);

            return RepeatStatus.CONTINUABLE;
        };

    }

    // chunk 기반
    // Reader에서 Null을 return할 때 까지 Step이 반복된다.
    // 2. Spring Expression Language, SpringEL을 사용하는 방법(외부에서 주입되는 파라미터 사용)
    // @JobScoper와 @Value는
    @Bean
    @JobScope //잡파라미터스의 chunkSize라는 키를 가지고 chunkSize 변수에 설정할 수 있도록 코드 작성
    public Step chunkBaseStep(@Value("#{jobParameters[chunkSize]}") String chunkSize){
        return stepBuilderFactory.get("chunkBaseStep")
                // 100개 데이터를 10개씩 나눠서 처리
                // <Reader의 input타입, Process의 output타입>으로 반환
//                .<String, String>chunk(10)
                .<String, String>chunk(StringUtils.isNotEmpty(chunkSize) ? Integer.parseInt(chunkSize) : 10)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }


    // 아이템을 만들거나 읽는다.
    private ItemReader<String> itemReader() {
        // ListItemReader는 Java에서 제공 들어가서 명세를 봐보자
        return new ListItemReader<>(getItems());
    }

    // Reader에서 생성한 데이터를 가공하거나 Writer로 넘길지 결정
    // 만약에 null로 리턴이 되면, 해당 아이템은 Writer로 넘어가지 못한다.
    private ItemProcessor<String, String> itemProcessor() {
        return item -> item + ", Spring Batch";
    }

    // itemReader에서 읽은 100개의 문자열 리스트 아이템들을 Processor를 거쳐 가공되고 itemWriter에서 로그로 size를 찍는다.
    private ItemWriter<String> itemWriter() {
        // itemProcessor를 가공한 결과 확인
//       return items -> items.forEach(log::info);

        return items -> log.info("chunk item size : {}", items.size());
    }

    private List<String> getItems() {
        List<String> items = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            items.add(i + " Hello");
        }
        return items;
    }
}
