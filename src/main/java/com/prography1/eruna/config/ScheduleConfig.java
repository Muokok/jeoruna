package com.prography1.eruna.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Scheduler;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Date;


@Slf4j
@Configuration
@RequiredArgsConstructor
public class ScheduleConfig {
    private final ApplicationContext applicationContext; // Spring의 컨테이너로, 빈(Bean)들을 관리
    private final JobLauncher jobLauncher; // Spring Batch 작업을 실행하는 인터페이스
    private final Scheduler scheduler; // Quartz 스케줄러 사용

    @Scheduled(cron = "0 0 0 * * *")
//    @Scheduled(fixedRate = 1000)
    public void launchJob() throws Exception {
        Date date = new Date();
        scheduler.clear(); // 기존 스케줄러의 모든 예약된 작업을 clear

        JobExecution jobExecution = jobLauncher.run( // 매일 정각마다 jobLauncher 실행
            (Job)applicationContext.getBean("readAlarmsJob")
            // BatchConfig에 등록한 readAlarmsJob을 불러온다.
            ,new JobParametersBuilder().addDate("launchDate", date).toJobParameters()
            //빌더 패턴을 사용하여 JobParameters 객체를 쉽게 생성할 수 있게 JobParametersBuilder를 사용한다.
            // JobParametersBuilder에 날짜 타입의 파라미터를 추가한다.
            // 이때 첫 번째 인자 "launchDate"는 파라미터의 키(이름),
            // 두 번째 인자 date는 앞서 생성된 Date 객체로, 현재 날짜와 시간을 나타낸다.
            // JobParametersBuilder는 자기 자신을 반환하므로 메서드 체이닝이 가능하다.
            // .toJobParameters()는 빌더의 작업을 마무리하고 실제 JobParameters 인스턴스를 반환한다.
            // 이렇게 생성된 JobParameters는 Job 실행 시 사용된.
            // "launchDate" 같은 파라미터를 포함함으로써 다음과 같은 이점이 있다.
            // 작업 식별: 각 Job 실행을 고유하게 식별할 수 있다.
            // Spring Batch는 String, Double, Long, Date 이렇게 4가지 타입의 파라미터를 지원한다.
        );
//            batchRunCounter.incrementAndGet();
        log.debug("Batch job ends with status as " + jobExecution.getStatus());
        log.debug("scheduler ends ");
    }
}
