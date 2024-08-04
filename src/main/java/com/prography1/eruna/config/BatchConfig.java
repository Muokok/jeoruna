package com.prography1.eruna.config;

import com.prography1.eruna.domain.entity.Alarm;
import com.prography1.eruna.domain.entity.DayOfWeek;
import com.prography1.eruna.domain.enums.Week;
import com.prography1.eruna.domain.repository.AlarmRepository;
import com.prography1.eruna.domain.repository.GroupRepository;
import com.prography1.eruna.service.AlarmService;
import com.prography1.eruna.util.AlarmItemProcessor;
import com.prography1.eruna.util.DayOfWeekRowMapper;
import com.prography1.eruna.util.AlarmsItemWriter;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import org.quartz.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;

@Configuration
@RequiredArgsConstructor
public class BatchConfig{
    private final DataSource dataSource;
    private final EntityManagerFactory entityManagerFactory;
    private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);

    //    @Bean
    public JdbcCursorItemReader<DayOfWeek> reader(AlarmRepository alarmRepository) {
        // 이건 왜 있는건지??

        JdbcCursorItemReader<DayOfWeek> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("select alarm_id, day from day_of_week");
        reader.setRowMapper(new DayOfWeekRowMapper(alarmRepository));
        reader.setMaxRows(10);
        reader.setFetchSize(10);
        reader.setQueryTimeout(10000);
        reader.close();
        return reader;
    }

    @Bean
    public JpaPagingItemReader<DayOfWeek> jpaPagingItemReader(){
        LocalDate localDate = LocalDate.now();
        String today = localDate.getDayOfWeek().getDisplayName(TextStyle.SHORT_STANDALONE, new Locale("eng")).toUpperCase(Locale.ROOT);
        HashMap<String, Object> paramValues = new HashMap<>();
        String query =
            "SELECT dayOfWeek.alarm From DayOfWeek dayOfWeek WHERE dayOfWeek.dayOfWeekId.day = :today ";
        paramValues.put("today", Week.valueOf(today));
        logger.info("Day: " + today);


        return new JpaPagingItemReaderBuilder<DayOfWeek>()
            .name("alarmReader")
            .entityManagerFactory(entityManagerFactory)
            .pageSize(10)
            .queryString(query)
//                .queryString("select d from DayOfWeek d where d.dayOfWeekId.day = :today")
            .parameterValues(paramValues)
            .build();
    }

    @Bean
    public Job readAlarmsJob(JobRepository jobRepository, @Qualifier("readAlarmsStep") Step step) {
        // Job을 구현한다.
        // jobRepository는 Job의 실행 정보를 저장하고 관리하는 저장소임
        // @Qualifier은 같은 타입의 빈이 여러 개 있을 때 특정 빈을 지정하는 데 사용된다.
        // 여기서 @Qualifier가 쓰인 이유는 다음과 같다.
        // 1. 타입 기반 주입의 한계: Spring은 기본적으로 타입을 기반으로 의존성을 주입합니다.
        //    그러나 같은 타입(Step)의 빈이 여러 개 있을 경우,
        //    Spring은 어떤 빈을 주입해야 할지 결정하지 못할 수 있습니다.
        // 2. 명확성: @Qualifier를 사용함으로써 정확히 어떤 Step 빈을 주입받고자 하는지 명시적으로 지정할 수 있습니다.
        //     즉, 같은 타입(Step)의 여러 빈 중에서 "readAlarmsStep"이라는 이름의 특정 Step빈을 주입받아
        //     Job이 어떤 Step에 의존하는지 명확히 알 수 있습니다.
        // 3. 유연성: 여러 개의 Step 빈이 있을 때, 필요에 따라 다른 Step을 주입받을 수 있는 유연성을 제공합니다.

        return new JobBuilder("readAlarmsJob", jobRepository)
            // readAlarmsJob 이라는 이름의 Job을 생성하기 위한 빌더 클래스
            // jobRepository에 Job 실행 정보를 저장한다.

            .incrementer(new RunIdIncrementer())
            // Spring Batch에서 Job의 실행은 JobParameters에 의해 식별되는데,
            // 동일한 JobParameters로 Job을 두 번 실행하는 것을 허용하지 않습니다.
            // 중복 실행을 방지하고 Job 실행의 고유성을 보장하기 위함입니다.
            // 여기서 RunIdIncrementer를 사용하는 주요 이유는 다음과 같습니다.
            // 결론부터 말하자면 같은 파라미터로 Job을 여러 번 실행하고자 사용합니다. 아래는 더 자세한 사용 이유입니다.
            // 고유한 Job 실행 보장: 매 실행마다 고유한 run.id 파라미터를 자동으로 생성합니다.
            // 이를 통해 같은 같은 파라미터로 Job을 여러 번 실행할 수 있게 됩니다.
            // 자동 증가 메커니즘: run.id값은 자동으로 증가하므로, 개발자가 수동으로 관리할 필요가 없습니다.
            // 재시작 가능성: Job이 실패했을 때, 고유한 ID로 인해 해당 Job을 쉽게 재시작할 수 있습니다.
            // 실행 이력 추적: 각 실행마다 고유 ID가 있어 Job 실행 이력을 쉽게 추적하고 관리할 수 있습니다.
            // 스케줄링과의 호환성: 정기적으로 스케줄링된 Job (예: 매일 실행되는 Job)의 경우,
            // 매번 새로운 실행으로 인식되어 문제없이 실행됩니다.
            .start(step)
            // Job에 첫 번째로 실행될 Step을 지정합니다.
            .build();
    }
    @Bean
    public ItemWriter<Alarm> writer(GroupRepository groupRepository, AlarmService alarmService){
        return new AlarmsItemWriter(groupRepository, alarmService);
    }

    @Bean
    AlarmItemProcessor alarmItemProcessor(AlarmRepository alarmRepository) {
        return new AlarmItemProcessor(alarmRepository);
    }

    @Bean
    public Step readAlarmsStep(JobRepository jobRepository, PlatformTransactionManager transactionManager, GroupRepository groupRepository, AlarmService alarmService) {
        return new StepBuilder("step", jobRepository)
            .<DayOfWeek, Alarm> chunk(100, transactionManager)
//                .reader(reader(alarmRepository))
            .reader(jpaPagingItemReader())
            .writer(writer(groupRepository, alarmService))
//                .processor(alarmItemProcessor(alarmRepository))
            .allowStartIfComplete(true)
            .build();
    }

//    private final JobRepository jobRepository;

}
