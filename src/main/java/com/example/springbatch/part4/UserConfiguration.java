package com.example.springbatch.part4;

import com.example.springbatch.part5.JobParametersDecide;
import com.example.springbatch.part5.OrderStatistics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
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

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class UserConfiguration {

    private final int CHUNK = 100;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final UserRepository userRepository;
    private final EntityManagerFactory entityManagerFactory;
    private final DataSource dataSource;




    @Bean
    public Job userJob() throws Exception {
        return jobBuilderFactory.get("userJob")
                .incrementer(new RunIdIncrementer())
                .start(saveUserStep())
                .next(userLevelUpStep())
                .listener(new LevelUpJobExecutionListener(userRepository))
                // -date=2020-11 --job.name=userJob 의 date 파라메타 값이 있는지 검사
                .next(new JobParametersDecide("date"))
                //date 를 넣어서 검증후 리턴값이 CONTINUE 인지 확인
                .on(JobParametersDecide.CONTINUE.getName())
                //위에 검증(CONTINUE)면 아래 to 를 실행
                .to(orderStatisticsStep(null))
                .build()
                .build();
    }


    // -date=2020-11 --job.name=userJob
    @Bean
    @JobScope
    public Step orderStatisticsStep(@Value("#{jobParameters[date]}") String date) throws Exception {
        return this.stepBuilderFactory.get("orderStatisticsStep")
                .<OrderStatistics, OrderStatistics>chunk(CHUNK)
                .reader(orderStatisticsItemReader(date))
                .writer(orderStatisticsItemWriter(date))
                .build();
    }

    // CSV 파일 생성
    private ItemWriter<? super OrderStatistics> orderStatisticsItemWriter(String date) throws Exception {
        YearMonth yearMonth = YearMonth.parse(date);
        String fileName = yearMonth.getYear() + "년_" + yearMonth.getMonthValue() + "월_일별_주문_금액.csv";

        //매핑 설정
        BeanWrapperFieldExtractor<OrderStatistics> fieldExtractor = new BeanWrapperFieldExtractor<>();
        //필드 설정
        fieldExtractor.setNames(new String[] {"amount", "date"});

        DelimitedLineAggregator<OrderStatistics> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor);

        FlatFileItemWriter<OrderStatistics> itemWriter = new FlatFileItemWriterBuilder<OrderStatistics>()
                .resource(new FileSystemResource("output/" + fileName))
                .lineAggregator(lineAggregator)
                .name("orderStatisticsItemWriter")
                .encoding("UTF-8")
                .headerCallback(writer -> writer.write("total_amoun,date"))
                .build();
        itemWriter.afterPropertiesSet();

        return itemWriter;
    }

    //jdbc 페이징 itemReader 을 사용해 date 를 기준으로 orders 테이블을 조회
    //SELECT SUM(amount), created_date
    //FROM orders
    //WHERE created_date >= :startDate AND created_date <= :endDate
    //GROUP BY created_date
    //ORDER BY created_date ASC
    private ItemReader<OrderStatistics> orderStatisticsItemReader(String date) throws Exception {
        YearMonth yearMonth = YearMonth.parse(date);
        Map<String, Object> parameters = new HashMap<>();
        //입력값으로 들어온 날짜의 1일
        parameters.put("startDate", yearMonth.atDay(1));
        //입력값으로 들어온 날짜의 마지막날 30일,31일
        parameters.put("endDate", yearMonth.atEndOfMonth());

        Map<String, Order> sortKet = new HashMap<>();
        sortKet.put("created_date", Order.ASCENDING);

        JdbcPagingItemReader<OrderStatistics> itemReader = new JdbcPagingItemReaderBuilder<OrderStatistics>()
                .dataSource(this.dataSource)
                .rowMapper((resultSet, i) -> OrderStatistics.builder()
                        .amount(resultSet.getString(1))
                        .date(LocalDate.parse(resultSet.getString(2), DateTimeFormatter.ISO_DATE))
                        .build())
                .pageSize(CHUNK)
                .name("orderStatisticsItemReader")
                .selectClause("sum(amount), created_date")
                .fromClause("orders")
                .whereClause("created_date >= :startDate and created_date <= :endDate")
                .groupClause("created_date")
                .parameterValues(parameters)
                .sortKeys(sortKet)
                .build();
        itemReader.afterPropertiesSet();
        return itemReader;
    }

    @Bean
    public Step saveUserStep() {
        return stepBuilderFactory.get("saveUserStep")
                .tasklet(new SaveUserTasklet(userRepository))
                .build();
    }

    @Bean
    public Step userLevelUpStep() throws Exception {
        return stepBuilderFactory.get("userLevelUpStep")
                .<User, User>chunk(CHUNK)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    private ItemWriter<User> itemWriter() {
        return users ->
            users.forEach(user -> {
                user.levelUp();
                userRepository.save(user);
            });

    }

    private ItemProcessor<? super User, ? extends User> itemProcessor() {
        return user -> {
            if (user.availableLeveUp()) {
                return user;
            }
            return null;
        };
    }


    private ItemReader<User> itemReader() throws Exception {
      JpaPagingItemReader<User> itemReader = new JpaPagingItemReaderBuilder<User>()
                .queryString("select u from User u")
                .entityManagerFactory(entityManagerFactory)
                //페이지 사이즈는 정크 사이즈랑 보통 동일 하게 함
                .pageSize(CHUNK)
                .name("userItemReader")
                .build();

      itemReader.afterPropertiesSet();
      return itemReader;
    }


}
