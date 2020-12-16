package org.simon.test01;

import org.junit.Test;
import org.quartz.CronScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.calendar.BaseCalendar;
import org.quartz.impl.calendar.CronCalendar;
import org.quartz.impl.triggers.CronTriggerImpl;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 你搞忘写注释了
 *
 * @author zhang_zhang
 * @date 2020-12-16
 * @since 1.0.0
 */
public class CronTriggerImplTest {

    @Test
    public void test01() throws InterruptedException {
        CronTriggerImpl trigger = (CronTriggerImpl) TriggerBuilder.newTrigger()
                .withIdentity("trigger-01", "group-01")
                .withSchedule(CronScheduleBuilder.cronSchedule("0/5 * * * * ?"))
                .build();

        Date date = new Date();
        System.out.println("-->"+date+","+date.getTime());
        trigger.computeFirstFireTime(null);
        System.out.println(trigger.getNextFireTime());

    }

    @Test
    public void test02() throws InterruptedException {
        CronTriggerImpl trigger = (CronTriggerImpl) TriggerBuilder.newTrigger()
                .withIdentity("trigger-01", "group-01")
                .withSchedule(CronScheduleBuilder.cronSchedule("0/5 * * * * ?"))
                .build();

        Date date = new Date();
        System.out.println("-->"+date);
        //trigger.computeFirstFireTime(Calendar.getInstance());


        for(int i=0;i<100;i++){
            Date date1 = new Date();
            System.out.println("date:"+date1);
            //getFireTimeAfter 返回触发器下一次将要触发的时间，如果在给定（参数）的时间之后，触发器不会在被触发，那么返回null
            System.out.println("getFireTimeAfter:"+trigger.getFireTimeAfter(date1));
            System.out.println("getNextFireTime:"+trigger.getNextFireTime());
            TimeUnit.SECONDS.sleep(1);
            System.out.println("\r\n");
        }
    }
}