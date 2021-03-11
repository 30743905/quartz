package org.simon.test01;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 你搞忘写注释了
 *
 * @author zhang_zhang
 * @date 2020-12-11
 * @since 1.0.0
 */
public class Demo01 {

    public static void main(String[] args) {
        SimpleDateFormat DateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date d = new Date();
        String returnstr = DateFormat.format(d);

        try {
            System.out.println(returnstr+ "【系统启动】");
            QuartzManager.addJob("job01", TestJob1.class,"0/55 * * * * ?", null); //每2秒钟执行一次
            //QuartzManager.addJob("job02", TestJob2.class,"0/48 * * * * ?", null); //每2秒钟执行一次
            //QuartzManager.addJobWithMoreTriggers("job01", TestJob1.class,"0/50 * * * * ?", "0/55 * * * * ?", null); //每2秒钟执行一次
            //QuartzManager.addJob("job02", TestJob2.class,"0/5 * * * * ?", null); //每2秒钟执行一次

            TimeUnit.SECONDS.sleep(10000);
            System.out.println("【修改时间】");
            //QuartzManager.modifyJobTime("job01", "0/5 * * * * ?");
            //QuartzManager.modifyJob("job01", TestJob2.class);
            //Thread.sleep(20000);
            //System.out.println("【移除定时】");
//            QuartzManager.removeJob(job_name);
//            Thread.sleep(10000);
//
//            System.out.println("/n【添加定时任务】");
//            QuartzManager.addJob(job_name,job,"0/5 * * * * ?");

        }  catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test01() {
        SimpleDateFormat DateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date d = new Date();
        String returnstr = DateFormat.format(d);

        try {
            System.out.println("启动时间："+new Date());
            QuartzManager.addJob("job01", TestJob1.class,"0/5 * * * * ?", null); //每2秒钟执行一次
            TimeUnit.SECONDS.sleep(1000);
        }  catch (Exception e) {
            e.printStackTrace();
        }
    }

}