package com.example.spring.batch.part6;

import com.example.spring.batch.part4.LevelUpJobExecutionListener;
import com.example.spring.batch.part4.SaveUserTasklet;
import com.example.spring.batch.part4.User;
import com.example.spring.batch.part4.UserRepository;
import com.example.spring.batch.part5.JobParametersDecide;
import com.example.spring.batch.part5.OrderStatistics;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;

import javax.persistence.EntityManagerFactory;
import javax.print.attribute.standard.JobName;
import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/*

    템플릿 코드
    save File as Template
    
 */

@Slf4j
@Configuration
public class ParallelUserConfiguration {

    // 성능측정을 위해 JOB_NAME을 설정, 같은 JOB을 복사해서 쓸 것이기 때문에 Bean 등에서 구분할 수 있게
    private final String JOB_NAME = "parallelUserJob";
    private final int CHUNK = 1000;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final UserRepository userRepository;
    private final EntityManagerFactory entityManagerFactory;
    private final DataSource dataSource;
    // SpringBatchExampleApplication에서 만든 taskExecutor Bean
    private final TaskExecutor taskExecutor;


    public ParallelUserConfiguration(JobBuilderFactory jobBuilderFactory,
                                     StepBuilderFactory stepBuilderFactory,
                                     UserRepository userRepository,
                                     EntityManagerFactory entityManagerFactory,
                                     DataSource dataSource,
                                     TaskExecutor taskExecutor) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.userRepository = userRepository;
        this.entityManagerFactory = entityManagerFactory;
        this.dataSource = dataSource;
        this.taskExecutor = taskExecutor;
    }

    @Bean(JOB_NAME)
    public Job userJob() throws Exception {
        return this.jobBuilderFactory.get(JOB_NAME)
                .incrementer(new RunIdIncrementer())
                .listener(new LevelUpJobExecutionListener(userRepository))
                // Flow로 감싼 새로운 saveUserStep을 이식해서 만든 saveUserFlow()
                .start(this.saveUserFlow())
                // userLevelUpFlow와 orderStaticsFlow를 합친 splitFlow가 병렬처리 되도록
                .next(this.splitFlow(null))
                // JobParametersDecide클래스의 오버라이딩한 decide메서드를 통해 해당 특정 조건에 따라 FlowExectionStatus가 리턴이 된다.
                // 특정 조건 = 파라미터의 밸류가 있는지 확인
                // 있으면 CONTINUE, 없으면 COMPLETED
//                .next(new JobParametersDecide("date"))
//                // CONTUNE라면? 아래 to에 있는 orderStatisticsStep을 실행
//                .on(JobParametersDecide.CONTINUE.getName())
//                .to(this.orderStatisticsStep(null))
                .build()
                .build();
    }

    // saveUserFlow를 만들어준다.
    @Bean(JOB_NAME + "_saveUserFlow")
    public Flow saveUserFlow() {
        // 기존에 만들었던 saveUserStep을 Flow로 감싸기위해 TaskletStep 변수에 이식
        TaskletStep saveUserStep = this.stepBuilderFactory.get(JOB_NAME + "_saveUserStep")
                .tasklet(new SaveUserTasklet(userRepository))
                .build();

        // Flow로 감싸서 리턴
        return new FlowBuilder<SimpleFlow>(JOB_NAME + "_saveUserFlow")
                .start(saveUserStep)
                .build();

    }

    // userLevelUpStep과 아래 Bean에서 제거한 orderStatisticsStep을 하나의 Flow로 만든다.
    @SneakyThrows
    @Bean(JOB_NAME + "_splitFlow")
    @JobScope
    public Flow splitFlow(@Value("#{jobParameters[date]}") String date) {
        // userLevelUpFlow를 만든다.
        Flow userLevelUpFLow = new FlowBuilder<SimpleFlow>(JOB_NAME + "_userLevelUpFlow")
                .start(userLevelUpStep())
                .build();

        // userLevelUpStep과 orderStatisticsStep을 하나로 합친다.
        // 하나의 Flow로 만들기위해서 각각도 Flow로 만들어야한다.
        return new FlowBuilder<SimpleFlow>(JOB_NAME + "_splitFlow")
                .split(this.taskExecutor)
                .add(userLevelUpFLow, orderStatisticsFlow(date))
                .build();
    }


    // orderStatisticsFlow를 만든다.
    private Flow orderStatisticsFlow(String date) throws Exception {
        return new FlowBuilder<SimpleFlow>(JOB_NAME + "_orderStatisticsFlow")
                // userJob에 있던 부분을 이식
                .start(new JobParametersDecide("date"))
                // CONTUNE라면? 아래 to에 있는 orderStatisticsStep을 실행
                .on(JobParametersDecide.CONTINUE.getName())
                .to(this.orderStatisticsStep(date))
                .build();
    }

    // bean이 아닌 일반객체로 생성하도록 해서 Flow를 만든다.
    // 위의 splitFlow가 @JobScope를 달고 파라미터를 받은 다음 splitFlow가 userLevelUpFlow와 orderStaticsFlow를 생성하면서 파라미터를 넘겨주기 때문에 빈을 제거한다.
//    @Bean(JOB_NAME + "_orderStatisticsStep")
//    @JobScope
    private Step orderStatisticsStep(@Value("#{jobParameters[date]}") String date) throws Exception {
        return this.stepBuilderFactory.get(JOB_NAME + "_orderStatisticsStep")
                .<OrderStatistics, OrderStatistics>chunk(CHUNK)
                .reader(orderStatisticsItemReader(date))
                .writer(orderStatisticsItemWriter(date))
                .build();
    }

    // ItemReader에서 읽은 OrderStatistics 데이터를 기준으로 csv파일을 생성
    private ItemWriter<? super OrderStatistics> orderStatisticsItemWriter(String date) throws Exception {
        YearMonth yearMonth = YearMonth.parse(date);

        String fileName = yearMonth.getYear() + "년_" + yearMonth.getMonthValue() + "월_일별_주문_금액.csv";

        BeanWrapperFieldExtractor<OrderStatistics> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"amount", "date"});

        DelimitedLineAggregator<OrderStatistics> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor);

        FlatFileItemWriter<OrderStatistics> itemWriter = new FlatFileItemWriterBuilder<OrderStatistics>()
                .resource(new FileSystemResource("output/" + fileName))
                .lineAggregator(lineAggregator)
                .name(JOB_NAME + "_orderStatisticsItemWriter")
                .encoding("UTF-8")
                .headerCallback(writer -> writer.write("total_amount,date"))
                .build();

        itemWriter.afterPropertiesSet();

        return itemWriter;

    }

    // 월별 합산 금액으로 조회된 데이터를 OrderStatistics로 매핑 한 후 ItemWriter로 날림
    // JdbcPagingItemReader 사용
    private ItemReader<? extends OrderStatistics> orderStatisticsItemReader(String date) throws Exception {
        YearMonth yearMonth = YearMonth.parse(date);

        Map<String, Object> parameters = new HashMap<>();
        // 두 개의 조건 추가, 1일과 마지막일을 기준으로, 한달 기준 1~30,31까지 일별 합계 금액
        parameters.put("startDate", yearMonth.atDay(1));
        parameters.put("endDate", yearMonth.atEndOfMonth());

        Map<String, Order> sortKey = new HashMap<>();
        sortKey.put("created_date", Order.ASCENDING);

        JdbcPagingItemReader<OrderStatistics> itemReader = new JdbcPagingItemReaderBuilder<OrderStatistics>()
                .dataSource(this.dataSource)
                .rowMapper((rs, rowNum) -> OrderStatistics.builder()
                        .amount(rs.getString(1))
                        .date(LocalDate.parse(rs.getString(2), DateTimeFormatter.ISO_DATE))
                        .build())
                .pageSize(CHUNK)
                .name(JOB_NAME + "_orderStatisticsItemReader")
                // GruopBy, Where 절을 통해 쿼리문 amount의 합계와 crated_date를 조회한다.
                .selectClause("sum(amount), created_date")
                .fromClause("orders")
                .whereClause("created_date >= :startDate and created_date <= :endDate")
                .groupClause("created_date")
                .parameterValues(parameters)
                .sortKeys(sortKey)
                .build();

        itemReader.afterPropertiesSet();
        return itemReader;
    }

//    @Bean(JOB_NAME + "_saveUserStep")
//    public Step saveUserStep() {
//        return this.stepBuilderFactory.get(JOB_NAME + "_saveUserStep")
//                .tasklet(new SaveUserTasklet(userRepository))
//                .build();
//    }

    @Bean(JOB_NAME + "_userLevelUpStep")
    public Step userLevelUpStep() throws Exception {
        return this.stepBuilderFactory.get(JOB_NAME + "_userLevelUpStep")
                .<User, User>chunk(CHUNK)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    private ItemWriter<? super User> itemWriter() {
        return users -> users.forEach(x -> {
            x.levelUp();
            userRepository.save(x);
        });
    }


    private ItemProcessor<? super User, ? extends User> itemProcessor() {
        return user -> {
            // 등급 상향 대상인지 판별
            if (user.availableLevelUp()) {
                return user;
            }
            return null;
        };
    }

    private ItemReader<? extends User> itemReader() throws Exception {
        JpaPagingItemReader<User> itemReader = new JpaPagingItemReaderBuilder<User>()
                .queryString("select u from User u")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(CHUNK)
                .name(JOB_NAME + "_userItemReader")
                .build();

        itemReader.afterPropertiesSet();

        return itemReader;
    }

}
