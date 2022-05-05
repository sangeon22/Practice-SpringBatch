package com.example.spring.batch.part6;

import com.example.spring.batch.part4.LevelUpJobExecutionListener;
import com.example.spring.batch.part4.SaveUserTasklet;
import com.example.spring.batch.part4.User;
import com.example.spring.batch.part4.UserRepository;
import com.example.spring.batch.part5.JobParametersDecide;
import com.example.spring.batch.part5.OrderStatistics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
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
import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

/*

    템플릿 코드
    save File as Template
    
 */

@Slf4j
@Configuration
public class PartitionUserConfiguration {

    // 성능측정을 위해 JOB_NAME을 설정, 같은 JOB을 복사해서 쓸 것이기 때문에 Bean 등에서 구분할 수 있게
    private final String JOB_NAME = "partitionUserJob";
    private final int CHUNK = 1000;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final UserRepository userRepository;
    private final EntityManagerFactory entityManagerFactory;
    private final DataSource dataSource;
    // SpringBatchExampleApplication에서 만든 taskExecutor Bean
    private final TaskExecutor taskExecutor;

    public PartitionUserConfiguration(JobBuilderFactory jobBuilderFactory,
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
                // saveUserStep - tasklet으로 실행됨, 테스트를 하기 위해 user를 저장하는  스텝
                .start(this.saveUserStep())
//                // user의 등급을 조절하는 userLevelUpStep - chunk로 실행됨, user의 level 상태를 변경하는 스텝
//                .next(this.userLevelUpStep())
                // 기존의 위 스텝을 마스터 스텝으로 변경
                .next(this.userLevelUpManagerStep())
                .listener(new LevelUpJobExecutionListener(userRepository))
                // JobParametersDecide클래스의 오버라이딩한 decide메서드를 통해 해당 특정 조건에 따라 FlowExectionStatus가 리턴이 된다.
                // 특정 조건 = 파라미터의 밸류가 있는지 확인
                // 있으면 CONTINUE, 없으면 COMPLETED
                .next(new JobParametersDecide("date"))
                // CONTUNE라면? 아래 to에 있는 orderStatisticsStep을 실행
                .on(JobParametersDecide.CONTINUE.getName())
                .to(this.orderStatisticsStep(null))
                .build()
                .build();
    }

    @Bean(JOB_NAME + "_orderStatisticsStep")
    @JobScope
    public Step orderStatisticsStep(@Value("#{jobParameters[date]}") String date) throws Exception {
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

    @Bean(JOB_NAME + "_saveUserStep")
    public Step saveUserStep() {
        return this.stepBuilderFactory.get(JOB_NAME + "_saveUserStep")
                .tasklet(new SaveUserTasklet(userRepository))
                .build();
    }

    @Bean(JOB_NAME + "_userLevelUpStep")
    public Step userLevelUpStep() throws Exception {
        return this.stepBuilderFactory.get(JOB_NAME + "_userLevelUpStep")
                //<partitionStep만 적용 시>
//                .<User, User>chunk(CHUNK)

                // <partitionStep에 AsyncStep까지 적용 시> Future 타입으로 output을 감싸줌줌
               .<User, Future<User>>chunk(CHUNK)

                .reader(itemReader(null, null))
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    // Master 슬레이브
    @Bean(JOB_NAME + "_userLevelUpStep.manager")
    public Step userLevelUpManagerStep() throws Exception {
        return this.stepBuilderFactory.get(JOB_NAME + "_userLevelUpStep.manager")
                .partitioner(JOB_NAME + "_userLevelUpStep", new UserLevelUpPartitioner(userRepository))
                // userLevelUpManagerStep이 마스터, userLevelUpStep이 슬레이브가 된다.
                .step(userLevelUpStep())
                .partitionHandler(taskExecutorPartitionHandler())
                .build();
    }

    // 이 PartitionHandler가 파티션을 핸들링할 수 있는 객체가 된다.
    @Bean(JOB_NAME + "_taskExecutorPartitionHandler")
    PartitionHandler taskExecutorPartitionHandler() throws Exception {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        // 핸들러에 슬레이브스텝을 설정하고
        handler.setStep(userLevelUpStep());
        handler.setTaskExecutor(this.taskExecutor);
        // 여기서 사용할 GridSize가 PartitionUserConfiguration에서 사용할 gridSize와 동일
        handler.setGridSize(8);

        return handler;
    }

/* // 기존 itemrWriter, itemProcessor - <partitionStep만 적용 시>
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
    */

    // itemrWriter, itemProcessor 변경 - <partitionStep에 AsyncStep까지 적용 시>
    private AsyncItemWriter<User> itemWriter() {
        ItemWriter<User> itemWriter = users -> users.forEach(x -> {
            x.levelUp();
            userRepository.save(x);
        });
        AsyncItemWriter<User> asyncItemWriter = new AsyncItemWriter<>();
        // AsyncItemWriter는 Delegate 메서드로 itemWriter를 감싸게 된다.
        asyncItemWriter.setDelegate(itemWriter);

        return asyncItemWriter;
    }

    // itemrWriter, itemProcessor 변경 - <partitionStep에 AsyncStep까지 적용 시>
    private AsyncItemProcessor<User, User> itemProcessor() {
        ItemProcessor<User, User> itemProcessor = user -> {
            // 등급 상향 대상인지 판별
            if (user.availableLevelUp()) {
                return user;
            }
            return null;
        };
        AsyncItemProcessor<User, User> asyncItemProcessor = new AsyncItemProcessor<>();
        // asyncItemProcessor는 Delegate 메서드로 itemProcessor를 감싸게 된다.
        asyncItemProcessor.setDelegate(itemProcessor);
        // taskExecutor 주입
        asyncItemProcessor.setTaskExecutor(this.taskExecutor);

        return asyncItemProcessor;

    }


    // PartitionStep을 사용하기 위해
    // UserLevelUpPartitioner에서 생성한 ExecutionContext를 사용하기 위해
    // StepScope가 필요하고 StepSope로 사용하려면 Bean설정을 해야함
    @Bean
    @StepScope
    JpaPagingItemReader<? extends User> itemReader(@Value("#{stepExecutionContext[minId]}") Long minId,
                                                   @Value("#{stepExecutionContext[maxId]}") Long maxId) throws Exception {

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("minId", minId);
        parameters.put("maxId", maxId);

        JpaPagingItemReader<User> itemReader = new JpaPagingItemReaderBuilder<User>()
                .queryString("select u from User u where u.id between :minId and :maxId")
                .parameterValues(parameters)
                .entityManagerFactory(entityManagerFactory)
                .pageSize(CHUNK)
                .name(JOB_NAME + "_userItemReader")
                .build();

        itemReader.afterPropertiesSet();

        return itemReader;
    }
}
