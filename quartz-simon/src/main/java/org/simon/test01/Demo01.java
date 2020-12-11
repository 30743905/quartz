package org.simon.test01;

import java.text.SimpleDateFormat;
import java.util.Date;

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

        TestJob job = new TestJob();
        try {
            System.out.println(returnstr+ "【系统启动】");
            QuartzManager.addJob("job01", job,"0/2 * * * * ?", null); //每2秒钟执行一次

//            Thread.sleep(10000);
//            System.out.println("【修改时间】");
//            QuartzManager.modifyJobTime(job_name,"0/10 * * * * ?");
//            Thread.sleep(20000);
//            System.out.println("【移除定时】");
//            QuartzManager.removeJob(job_name);
//            Thread.sleep(10000);
//
//            System.out.println("/n【添加定时任务】");
//            QuartzManager.addJob(job_name,job,"0/5 * * * * ?");

        }  catch (Exception e) {
            e.printStackTrace();
        }
    }

}