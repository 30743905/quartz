package org.simon.test01;

import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 你搞忘写注释了
 *
 * @author zhang_zhang
 * @date 2020-12-11
 * @since 1.0.0
 */
public class QuartzManager {

    private static Logger log = LoggerFactory.getLogger(QuartzManager.class);

    //1.创建schedulerFactory的工厂
    private static SchedulerFactory sf = new StdSchedulerFactory();
    private static String JOB_GROUP_NAME = "group1";
    private static String TRIGGER_GROUP_NAME = "trigger1";


    /**
     *  添加一个定时任务，使用默认的任务组名，触发器名，触发器组名
     * @param jobName 任务名
     * @param jobClass     任务
     * @throws SchedulerException
     * @throws ParseException
     */
    public static void addJob(String jobName, Class jobClass, String cron, JobDataMap param)
            throws SchedulerException, ParseException, InterruptedException {
        //2.从工厂中获取调度器实例
        Scheduler sched = sf.getScheduler();

        //创建JobDetail
        JobBuilder builder = JobBuilder.newJob(jobClass)
                .withDescription("this is a test job")
                .withIdentity(jobName, Scheduler.DEFAULT_GROUP)
                .storeDurably(true);
        if (param != null) {
            builder.usingJobData(param);
        }

        //任务运行的时间，simpleSchedle类型触发器有效
        long time = System.currentTimeMillis() + 3 * 1000L;
        Date startTime = new Date(time);

        //创建Trigger
        //使用SimpleScheduleBuilder或者CronSchedulerBuilder
        Trigger trigger = TriggerBuilder.newTrigger()
                .withDescription("触发器")
                .withIdentity(jobName+"-trigger", TRIGGER_GROUP_NAME)
                //.startAt(startTime)
                .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                .build();

        //注册任务和定时器
        Date firstFireTime = sched.scheduleJob(builder.build(), trigger);
        log.info("第一次触发时间:{}", firstFireTime);
        if (!sched.isShutdown()){
            //启动调度器
            //TimeUnit.SECONDS.sleep(10);
            log.info("启动时间：{}", new Date());
            sched.start();

        }

    }

    public static void addJobWithMoreTriggers(String jobName, Class jobClass, String cron1, String cron2, JobDataMap param)
            throws SchedulerException, ParseException, InterruptedException {
        //2.从工厂中获取调度器实例
        Scheduler sched = sf.getScheduler();

        //创建JobDetail
        JobBuilder builder = JobBuilder.newJob(jobClass)
                .withDescription("this is a test job")
                .withIdentity(jobName, Scheduler.DEFAULT_GROUP)
                .storeDurably(true);
        if (param != null) {
            builder.usingJobData(param);
        }

        //任务运行的时间，simpleSchedle类型触发器有效
        long time = System.currentTimeMillis() + 3 * 1000L;
        Date startTime = new Date(time);

        //创建Trigger
        //使用SimpleScheduleBuilder或者CronSchedulerBuilder
        Trigger trigger1 = TriggerBuilder.newTrigger()
                .withDescription("触发器")
                .withIdentity(jobName+"-trigger", TRIGGER_GROUP_NAME)
                //.startAt(startTime)
                .withSchedule(CronScheduleBuilder.cronSchedule(cron1))
                .build();

        Trigger trigger2 = TriggerBuilder.newTrigger()
                .withDescription("触发器")
                .withIdentity(jobName+"-trigger", TRIGGER_GROUP_NAME)
                //.startAt(startTime)
                .withSchedule(CronScheduleBuilder.cronSchedule(cron2))
                .build();

        Set<Trigger> triggers = new HashSet();

        //注册任务和定时器
        sched.scheduleJob(builder.build(), triggers, true);
        if (!sched.isShutdown()){
            //启动调度器
            //TimeUnit.SECONDS.sleep(10);
            log.info("启动时间：{}", new Date());
            sched.start();

        }

    }


    /** *//**
     * 修改一个任务的触发时间(使用默认的任务组名，触发器名，触发器组名)
     * @param jobName
     * @throws SchedulerException
     * @throws ParseException
     */
    public static void modifyJobTime(String jobName, String cronExpression)
            throws SchedulerException, ParseException{
        Scheduler sched = sf.getScheduler();
        List<? extends Trigger> triggers = sched.getTriggersOfJob(JobKey.jobKey(jobName, Scheduler.DEFAULT_GROUP));
        System.out.println(triggers);
        for(Trigger trigger:triggers){
            CronTriggerImpl  ct = (CronTriggerImpl)trigger;
            ct.setCronExpression(cronExpression);
            sched.rescheduleJob(ct.getKey(), ct);
        }
    }

    public static void modifyJob(String jobName, Class jobClass)
            throws SchedulerException, ParseException{
        Scheduler sched = sf.getScheduler();

        JobBuilder builder = JobBuilder.newJob(jobClass)
                    .withDescription("this is a test job")
                    .withIdentity(jobName, Scheduler.DEFAULT_GROUP)
                .storeDurably();
        sched.addJob(builder.build(), true);
    }

    /** *//**
     * 移除一个任务(使用默认的任务组名，触发器名，触发器组名)
     * @param jobName
     * @throws SchedulerException
     */
    public static void removeJob(String jobName)
            throws SchedulerException{
        Scheduler sched = sf.getScheduler();

        List<? extends Trigger> triggers = sched.getTriggersOfJob(JobKey.jobKey(jobName, Scheduler.DEFAULT_GROUP));
        for(Trigger trigger:triggers){
            sched.pauseTrigger(trigger.getKey());//停止触发器
            sched.unscheduleJob(trigger.getKey());//移除触发器
        }
        sched.deleteJob(JobKey.jobKey(jobName, Scheduler.DEFAULT_GROUP));//删除任务
    }

    public static void removeJob1(String jobName)
            throws SchedulerException{
        Scheduler sched = sf.getScheduler();


    }

}