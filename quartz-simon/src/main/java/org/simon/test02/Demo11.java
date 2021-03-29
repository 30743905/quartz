package org.simon.test02;

import org.junit.Test;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.ScheduleBuilder;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.core.QuartzScheduler;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;

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
public class Demo11 {

    private static SchedulerFactory sf = new StdSchedulerFactory();

    /**
     * Job定义表：qrtz_job_details
     * @throws SchedulerException
     * @throws InterruptedException
     */
    @Test
    public void storeJob() throws SchedulerException, InterruptedException {
        Scheduler scheduler = sf.getScheduler();

        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put("name", "zhangsan");
        jobDataMap.put("time", System.currentTimeMillis());
        JobDetail jobDetail = JobBuilder
                .newJob(QuartzCronJob.class)
                .storeDurably()
                .withIdentity("job1", "DEFAULT")
                .usingJobData(jobDataMap)
                .build();

        try {
            JobKey jobKey = jobDetail.getKey();
            if (scheduler.checkExists(jobKey)) {
                log.warn("调度任务已存在，删除后重新添加:{}", jobKey);
                scheduler.interrupt(jobKey);//停止JOB
                /**
                 * deleteJob操作在删除Job之前，会执行unscheduleJob()取消job和trigger关联
                 */
                scheduler.deleteJob(jobKey);
            }

            scheduler.addJob(jobDetail, true);
            System.out.println("=========");


        } catch (SchedulerException e) {
            log.error("startNewScheduler失败",e);
        }
    }

    /**
     * Trigger定义表：qrtz_cron_triggers
     * scheduler.scheduleJob(trigger)：在插入trigger同时，会插入qrtz_triggers
     *
     * 注意：Trigger的JobDataMap没有插入trigger定义表，而是插入到运行表qrtz_triggers中
     * @throws SchedulerException
     * @throws InterruptedException
     */
    @Test
    public void storeTrigger() throws SchedulerException, InterruptedException {
        Scheduler scheduler = sf.getScheduler();

        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put("name", "lisi");
        jobDataMap.put("address", "China");

        Trigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity("trigger1", "DEFAULT")
                .usingJobData(jobDataMap)
                .startAt(new Date())
                .endAt(new Date(System.currentTimeMillis()+38 * 60 * 1000))
                .withSchedule(CronScheduleBuilder.cronSchedule("*/10 * * * * ?"))
                .forJob(new JobKey("job1", "DEFAULT"))
                .build();//时间




        try {
            TriggerKey triggerKey = trigger.getKey();
            if(scheduler.checkExists(triggerKey)){
                scheduler.unscheduleJob(triggerKey);
            }
            scheduler.scheduleJob(trigger);//必须绑定job
            System.out.println("========="+scheduler.isStarted());
        } catch (SchedulerException e) {
            log.error("startNewScheduler失败",e);
        }
    }

    @Test
    public void storeTrigger2() throws SchedulerException, InterruptedException {
        Scheduler scheduler = sf.getScheduler();

        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put("name", "wangwu");
        jobDataMap.put("address", "China");

        Trigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity("trigger2", "group1")
                .usingJobData(jobDataMap)
                .startAt(new Date())
                .endAt(new Date(System.currentTimeMillis()+38 * 60 * 1000))
                .withSchedule(CronScheduleBuilder.cronSchedule("*/5 * * * * ?"))
                .forJob(new JobKey("job1", "group1"))
                .build();//时间

        try {
            TriggerKey triggerKey = trigger.getKey();
            if(scheduler.checkExists(triggerKey)){
                scheduler.unscheduleJob(triggerKey);
            }
            scheduler.scheduleJob(trigger);//必须绑定job
            System.out.println("=========");
        } catch (SchedulerException e) {
            log.error("startNewScheduler失败",e);
        }
    }


    @Test
    public void start() throws SchedulerException, InterruptedException {
        Scheduler scheduler = sf.getScheduler();

        if(!scheduler.isStarted()){
            scheduler.start();
        }
        log.info("scheduler start:{}", scheduler.isStarted());
        TimeUnit.SECONDS.sleep(200);

    }

    @Test
    public void pauseJob() throws SchedulerException, InterruptedException {
        Scheduler scheduler = sf.getScheduler();

        if(!scheduler.isStarted()){
            scheduler.start();
        }
        log.info("scheduler start:{}", scheduler.isStarted());
        TimeUnit.SECONDS.sleep(60);
        /**
         * pauseJob()操作检索job绑定的所有trigger，然后对这些trigger执行pauseTrigger()操作
         * pauseTrigger()操作就是将qrtz_triggers.trigger_state设置成PAUSED
         */
        scheduler.pauseJob(new JobKey("job1", "group1"));
        log.info("pauseJob job1");
        TimeUnit.SECONDS.sleep(100);
    }

    @Test
    public void pauseTrigger() throws SchedulerException, InterruptedException {
        Scheduler scheduler = sf.getScheduler();

        if(!scheduler.isStarted()){
            scheduler.start();
        }
        log.info("scheduler start:{}", scheduler.isStarted());
        TimeUnit.SECONDS.sleep(60);
        /**
         * qrtz_triggers.trigger_state设置成PAUSED
         */
        scheduler.pauseTrigger(new TriggerKey("trigger1", "group1"));
        log.info("pauseTrigger job1");
        TimeUnit.SECONDS.sleep(100);
    }

    @Test
    public void pauseTriggers() throws SchedulerException, InterruptedException {
        Scheduler scheduler = sf.getScheduler();

        /**
         * 根据matcher检索出相关trigger group，然后将qrtz_triggers表中相关trigger group关联记录trigger_state修改成PAUSED或PAUSED_BLOCK
         * 最后在qrtz_paused_trigger_grps插入记录
         */
        scheduler.pauseTriggers(GroupMatcher.groupEquals("group1"));

        log.info("pauseTrigger job1");
        TimeUnit.SECONDS.sleep(100);
    }


    @Test
    public void test0111() throws SchedulerException, InterruptedException {
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