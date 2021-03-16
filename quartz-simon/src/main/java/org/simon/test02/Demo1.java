package org.simon.test02;

import org.junit.Test;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.jdbcjobstore.SimpleSemaphore;
import org.quartz.impl.jdbcjobstore.StdJDBCDelegate;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.spi.OperableTrigger;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;


/**
 * 你搞忘写注释了
 *
 * @author zhang_zhang
 * @date 2021-03-10
 * @since 1.0.0
 */
@Slf4j
public class Demo1 {

    private static SchedulerFactory sf = new StdSchedulerFactory();

    @Test
    public void test01() throws SchedulerException, InterruptedException {
        Scheduler scheduler = sf.getScheduler();

        JobDetail jobDetail = JobBuilder
                .newJob(QuartzCronJob.class)
                .storeDurably()
                .withIdentity("job1", "group1")
                .build();

        Trigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity("trigger1", "group1")
                .startAt(new Date())
                .endAt(new Date(System.currentTimeMillis()+38 * 60 * 1000))
                .withSchedule(CronScheduleBuilder.cronSchedule("*/50 * * * * ?"))
                .build();//时间

        JobDataMap jobDataMap = jobDetail.getJobDataMap();
        jobDataMap.put("param1", "appCode");
        jobDataMap.put("param2", "flowCode");
        jobDataMap.put("param3", "taskOpt");


        try {
            JobKey jobKey = jobDetail.getKey();
            if (scheduler.checkExists(jobKey)) {
                log.warn("调度任务已存在，删除后重新添加:{}", jobKey);
                scheduler.interrupt(jobKey);//停止JOB
                scheduler.deleteJob(jobKey);
            }

            scheduler.scheduleJob(jobDetail, trigger);
            if(!scheduler.isStarted()){
                log.info("begin scheduler:{}", scheduler.isStarted());
                scheduler.start();
                log.info("begin scheduler started:{}", scheduler.isStarted());
            }
        } catch (SchedulerException e) {
            log.error("startNewScheduler失败",e);
        }

        TimeUnit.SECONDS.sleep(10000);

    }


    @Test
    public void test011() throws SchedulerException, InterruptedException {
        Scheduler scheduler = sf.getScheduler();


        JobDetail jobDetail = JobBuilder
                .newJob(QuartzCronJob.class)
                .storeDurably()
                .withIdentity("job1", "group1")
                .build();

        Trigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity("trigger1", "group1")
                .startAt(new Date())
                .endAt(new Date(System.currentTimeMillis()+38 * 60 * 1000))
                .withSchedule(CronScheduleBuilder.cronSchedule("*/50 * * * * ?"))
                .build();//时间

        JobDataMap jobDataMap = jobDetail.getJobDataMap();
        jobDataMap.put("param1", "appCode");
        jobDataMap.put("param2", "flowCode");
        jobDataMap.put("param3", "taskOpt");


        try {
            JobKey jobKey = jobDetail.getKey();
            if (scheduler.checkExists(jobKey)) {
                log.warn("调度任务已存在，删除后重新添加:{}", jobKey);
                scheduler.interrupt(jobKey);//停止JOB
                scheduler.deleteJob(jobKey);
            }

            scheduler.scheduleJob(jobDetail, trigger);
            /*if(!scheduler.isStarted()){
                log.info("begin scheduler:{}", scheduler.isStarted());
                scheduler.start();
                log.info("begin scheduler started:{}", scheduler.isStarted());
            }*/
           log.info("==========>>{}", scheduler.isStarted());
        } catch (SchedulerException e) {
            log.error("startNewScheduler失败",e);
        }

        TimeUnit.SECONDS.sleep(10000);

    }


    @Test
    public void test02() throws SchedulerException, InterruptedException {
        Scheduler scheduler = sf.getScheduler();
        try {
            if(!scheduler.isStarted()){
                log.info("begin scheduler:{}", scheduler.isStarted());
                scheduler.start();
                log.info("begin scheduler started:{}", scheduler.isStarted());
            }
        } catch (SchedulerException e) {
            log.error("startNewScheduler失败",e);
        }

        TimeUnit.SECONDS.sleep(10000);

    }
}