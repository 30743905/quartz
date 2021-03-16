
/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package org.quartz.core;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.quartz.JobPersistenceException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The thread responsible for performing the work of firing <code>{@link Trigger}</code>
 * s that are registered with the <code>{@link QuartzScheduler}</code>.
 * </p>
 *
 * @see QuartzScheduler
 * @see org.quartz.Job
 * @see Trigger
 *
 * @author James House
 */
public class QuartzSchedulerThread extends Thread {
    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Data members.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    private QuartzScheduler qs;

    private QuartzSchedulerResources qsRsrcs;

    private final Object sigLock = new Object();

    private boolean signaled;
    private long signaledNextFireTime;

    /**
     * paused:true  暂时还不能提供服务  比如还未启动完成或切换成备服务器等等
     */
    private boolean paused;

    /**
     * halted:true  scheduler被shutdown
     */
    private AtomicBoolean halted;

    private Random random = new Random(System.currentTimeMillis());

    // When the scheduler finds there is no current trigger to fire, how long
    // it should wait until checking again...
    private static long DEFAULT_IDLE_WAIT_TIME = 30L * 1000L;

    private long idleWaitTime = DEFAULT_IDLE_WAIT_TIME;

    private int idleWaitVariablness = 7 * 1000;

    private final Logger log = LoggerFactory.getLogger(getClass());

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Constructors.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    /**
     * <p>
     * Construct a new <code>QuartzSchedulerThread</code> for the given
     * <code>QuartzScheduler</code> as a non-daemon <code>Thread</code>
     * with normal priority.
     * </p>
     */
    QuartzSchedulerThread(QuartzScheduler qs, QuartzSchedulerResources qsRsrcs) {
        this(qs, qsRsrcs, qsRsrcs.getMakeSchedulerThreadDaemon(), Thread.NORM_PRIORITY);
    }

    /**
     * <p>
     * Construct a new <code>QuartzSchedulerThread</code> for the given
     * <code>QuartzScheduler</code> as a <code>Thread</code> with the given
     * attributes.
     * </p>
     */
    QuartzSchedulerThread(QuartzScheduler qs, QuartzSchedulerResources qsRsrcs, boolean setDaemon, int threadPrio) {
        super(qs.getSchedulerThreadGroup(), qsRsrcs.getThreadName());
        this.qs = qs;
        this.qsRsrcs = qsRsrcs;
        this.setDaemon(setDaemon);
        if(qsRsrcs.isThreadsInheritInitializersClassLoadContext()) {
            log.info("QuartzSchedulerThread Inheriting ContextClassLoader of thread: " + Thread.currentThread().getName());
            this.setContextClassLoader(Thread.currentThread().getContextClassLoader());
        }

        this.setPriority(threadPrio);

        // start the underlying thread, but put this object into the 'paused'
        // state
        // so processing doesn't start yet...
        paused = true;
        halted = new AtomicBoolean(false);
    }

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Interface.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    void setIdleWaitTime(long waitTime) {
        idleWaitTime = waitTime;
        idleWaitVariablness = (int) (waitTime * 0.2);
    }

    private long getRandomizedIdleWaitTime() {
        return idleWaitTime - random.nextInt(idleWaitVariablness);
    }

    /**
     * <p>
     * Signals the main processing loop to pause at the next possible point.
     * </p>
     */
    void togglePause(boolean pause) {
        synchronized (sigLock) {
            paused = pause;

            if (paused) {
                signalSchedulingChange(0);
            } else {
                log.info("*********notifyAll");
                sigLock.notifyAll();
            }
        }
    }

    /**
     * <p>
     * Signals the main processing loop to pause at the next possible point.
     * </p>
     */
    void halt(boolean wait) {
        synchronized (sigLock) {
            halted.set(true);

            if (paused) {
                log.info("*********notifyAll");
                sigLock.notifyAll();
            } else {
                signalSchedulingChange(0);
            }
        }
        
        if (wait) {
            boolean interrupted = false;
            try {
                while (true) {
                    try {
                        join();
                        break;
                    } catch (InterruptedException _) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    boolean isPaused() {
        return paused;
    }

    /**
     * <p>
     * Signals the main processing loop that a change in scheduling has been
     * made - in order to interrupt any sleeping that may be occuring while
     * waiting for the fire time to arrive.
     * </p>
     *
     * @param candidateNewNextFireTime the time (in millis) when the newly scheduled trigger
     * will fire.  If this method is being called do to some other even (rather
     * than scheduling a trigger), the caller should pass zero (0).
     */
    public void signalSchedulingChange(long candidateNewNextFireTime) {
        synchronized(sigLock) {
            signaled = true;
            signaledNextFireTime = candidateNewNextFireTime;
            log.info("*********notifyAll");
            sigLock.notifyAll();
        }
    }

    public void clearSignaledSchedulingChange() {
        synchronized(sigLock) {
            signaled = false;
            signaledNextFireTime = 0;
        }
    }

    public boolean isScheduleChanged() {
        synchronized(sigLock) {
            return signaled;
        }
    }

    public long getSignaledNextFireTime() {
        synchronized(sigLock) {
            return signaledNextFireTime;
        }
    }

    /**
     * <p>
     * The main processing loop of the <code>QuartzSchedulerThread</code>.
     * </p>
     */
    @Override
    public void run() {
        //JobStore读取trigger连续失败次数，主要重试时休眠控制
        int acquiresFailed = 0;

        /**
         * 判断scheduler是否被shutdown()操作
         */
        while (!halted.get()) { //todo
            try {
                // check if we're supposed to pause...
                synchronized (sigLock) {
                    /**
                     * 判断是否可以提供服务，paused=false才能提供服务，否则休眠自旋
                     * togglePause()方法中修改paused=false时会调用notifyAll()立即唤醒当前线程
                     */
                    while (paused && !halted.get()) {
                        try {
                            // wait until togglePause(false) is called...
                            sigLock.wait(1000L);
                        } catch (InterruptedException ignore) {
                        }

                        // reset failure counter when paused, so that we don't
                        // wait again after unpausing
                        acquiresFailed = 0;
                    }

                    if (halted.get()) {
                        break;
                    }
                }

                // wait a bit, if reading from job store is consistently
                // failing (e.g. DB is down or restarting)..
                /**
                 * 在前几次进行触发器读取出现问题(acquiresFailed > 1)，则可能是数据库重启一类的原因引发的故障
                 * 避免一直读取一直报错问题，进行一定休眠后再进行读取
                 */
                if (acquiresFailed > 1) {
                    try {
                        long delay = computeDelayForRepeatedErrors(qsRsrcs.getJobStore(), acquiresFailed);
                        Thread.sleep(delay);
                    } catch (Exception ignore) {
                    }
                }

                /**
                 * 获取线程池中可用工作线程数，只有可用工作线程才会从JobStore存储介质中读取trigger，并在触发器触发时使用工作线程去执行
                 * 注意：该方法是阻塞方法，无可用线程时该方法内部自旋休眠一直阻塞，直到有可用线程
                 */
                int availThreadCount = qsRsrcs.getThreadPool().blockForAvailableThreads();
                log.info("线程池中可用工作线程数:{}", availThreadCount);
                if(availThreadCount > 0) { // will always be true, due to semantics of blockForAvailableThreads...
                    List<OperableTrigger> triggers;
                    long now = System.currentTimeMillis();
                    clearSignaledSchedulingChange();
                    try {

                        /**
                         * 从JobStore存储介质中提取将要触发的trigger：
                         *  1、读取的trigger不一定需要立即执行，因为触发时间晚于当前时间idleWaitTime内的也可能被读取出来；
                         *  2、因为读取的trigger按照触发时间排序的，这里只需要判断第一个触发器下次触发时间，没有到达其触发时间则休眠等待；
                         *  3、当第一个trigger到达触发时机后，则对该批trigger都进行执行，所以，可以看出：读取的一批trigger是一起执行的
                         *
                         * acquireNextTriggers方法默认最多每次最多读取1个trigger，可以通过如下参数控制：
                         *  1、long noLaterThan:限制读取触发器下次触发时间必须小于等于noLaterThan，默认当前时间及其将来30s内触发的可以进行提取，
                         *      对应配置项：org.quartz.scheduler.idleWaitTime，默认30*1000
                         *      注意：触发时间小于等于noLaterThan的触发器是可以读取，但不一定都会被都取到，具体还要看后面两个参数限制
                         *
                         *  2、int maxCount:从JobStore一次读取trigger的最大数量，对应配置项org.quartz.scheduler.batchTriggerAcquisitionMaxCount，默认是1
                         *      注意：这里还和availThreadCount比较取最小值，比如配置maxBatchSize=5最大可以读取5个trigger，
                         *           但是可用线程只有2个，另外3个读取出来也没有可用线程去执行它，所以取个最小值
                         *
                         *  3、long timeWindow：容忍时间窗口，默认0，对应配置项：org.quartz.scheduler.batchTriggerAcquisitionFireAheadTimeWindow
                         *
                         *  比如 现在是10:10:05表，有下面几个触发时间任务：
                         *  任务A 10:10:20
                         *  任务B 10:10:25
                         *  任务C 10:10:28
                         *  任务D 10:10:32
                         *  任务E 10:10:37
                         *  任务F 10:10:48
                         *
                         *  noLaterThan = 30、maxCount=5、timeWindow = 10
                         *  a、从qrtz_triggers检索触发时间小于(05+30+10)，最大检索数量小于扽与5，即检索出：任务A、任务B、任务C、任务D、任务E
                         *  b、timeWindow = 6:第一个触发任务A触发时间是10分20秒，容忍时间窗口为6秒，则只能执行10分30秒内任务，则过滤后：任务A、任务B、任务C
                         *  c、任务A、任务B、任务C放入队列，等任务A到达触发时间，会将任务A、任务B、任务C一批都一起执行
                         *
                         *
                         *
                         *
                         *  一般读取trigger逻辑如下：
                         *      1、根据trigger下次触发时间进行排序存储，然后获取一个最早触发的trigger，且其触发时间满足不大于noLaterThan；
                         *      2、然后看看是否到达maxCount，达到则返回，否则继续循环从剩下的trigger中找触发时间不大于第一个trigger的触发时间；
                         *      3、如果对触发时间要求不那么严格，就可以让两个执行时间距离较近的触发器同时被取出执行，这就要利用timeWindow参数，比如timeWindow=5000，
                         *      ，如果工作线程足够的情况下，则触发时间晚于第一个触发器5秒内的也都可以读取出来；
                         *
                         *
                         *  此外在acquireNextTriggers方法内部还有一个参数misfireThreshold，misfireThreshold是一个时间范围，
                         *  用于判定触发器是否延时触发，misfireThreshold默认值是60秒，它相对的实际意义就是:在当前时间的60秒之前本应执行但尚未执行的触发器不被认为是延迟触发,
                         *  这些触发器同样会被acquireNextTriggers发现，有时由于工程线程繁忙、程序重启等原因，原本预定要触发的任务可能延迟，
                         *  我们可以在每个触发器中可以设置MISFIRE_INSTRUCTION,用于指定延迟触发后使用的策略
                         *  举例，对于CronTrigger,延迟处理的策略主要有3种：
                         *      1）一个触发器无论延迟多少次，这些延迟都会被程序尽可能补回来
                         *      2）检测到触发器延迟后，该触发器会在尽可能短的时间内被立即执行一次(只有一次)，然后恢复正常
                         *      3）检测到延迟后不采取任何动作，触发器以现在时间为基准，根据自身的安排等待下一次被执行或停止，
                         *  比如有些触发器只执行一次，一旦延迟后，该触发器也不会被触发
                         *
                         *  关于触发器是否延迟的判定由一个叫MisfireHandler的线程独立负责，它会判定并影响延迟触发器的下一次触发，但不会真正进行触发的动作，
                         *  触发的工作将统一交由QuartzSchedulerThread即本线程处理，如果判定一个触发器延迟，则根据策略修改触发器的下一次执行时间或直接停止触发器
                         *  所以这些延迟触发器被MisfireHandler处理后若仍有下次执行机会，就同样会在其触发时间被发现并触发，要注意的是MisfireHandler只会处理延迟策略不为上述第(1)类的触发器
                         *  第(1)类触发器在延迟后，一旦获取到资源就可触发，这个过程不需被修改下次执行时间就可完成
                         *
                         * (long noLaterThan, int maxCount, long timeWindow)
                         *
                         */
                        triggers = qsRsrcs.getJobStore().acquireNextTriggers(
                                now + idleWaitTime, Math.min(availThreadCount, qsRsrcs.getMaxBatchSize()), qsRsrcs.getBatchTimeWindow());
                        log.info("提取出trigger列表 size:{}, data:{}", triggers.size(), triggers);
                        //重置acquiresFailed=0
                        acquiresFailed = 0;
                        if (log.isDebugEnabled())
                            log.debug("batch acquisition of " + (triggers == null ? 0 : triggers.size()) + " triggers");
                    } catch (JobPersistenceException jpe) {
                        if (acquiresFailed == 0) {
                            qs.notifySchedulerListenersError(
                                "An error occurred while scanning for the next triggers to fire.",
                                jpe);
                        }
                        if (acquiresFailed < Integer.MAX_VALUE)
                            acquiresFailed++;
                        continue;
                    } catch (RuntimeException e) {
                        if (acquiresFailed == 0) {
                            getLog().error("quartzSchedulerThreadLoop: RuntimeException "
                                    +e.getMessage(), e);
                        }
                        if (acquiresFailed < Integer.MAX_VALUE)
                            acquiresFailed++;
                        continue;
                    }

                    if (triggers != null && !triggers.isEmpty()) {
                        now = System.currentTimeMillis();
                        //取出第一个trigger下次触发时间
                        long triggerTime = triggers.get(0).getNextFireTime().getTime();
                        //计算还有多久才能触发，单位毫秒
                        long timeUntilTrigger = triggerTime - now;
                        log.info("---> timeUntilTrigger:{}", timeUntilTrigger);
                        /**
                         * 在该while循环体中，被取出的触发器会阻塞等待到预定时间被触发,这里用了阻塞，因为当外部环境对触发器做了调整或者新增时，会对线程进行唤醒
                         * 在阻塞被唤醒后，会有相关的逻辑判断是否应该重新取出触发器来执行
                         * 比如当前时间是10:00:00，在上述逻辑中已经取出了10:00:05需要执行的触发器
                         * 此时如果新增了一个10:00:03的触发器，则可能需要丢弃10:00:05的，再取出10:00:03的
                         */
                        while(timeUntilTrigger > 2) {
                            synchronized (sigLock) {
                                if (halted.get()) {
                                    break;
                                }
                                /**
                                 * 在通过acquireNextTriggers()方法取得完毕之后，返回的Trigger数组就是接下来准备触发的任务，但此时，并不保证这些已经是最后要触发的任务。
                                 * 此时还有一个问题，如果在这段时间内，又有一个新的任务被加进，同时这个任务的触发时间早于所有刚刚取得的任务的触发时间呢？
                                 *
                                 * 这里每一个新加入的任务触发时间都会更新管理线程的signaledNextFireTime，也就是新任务的触发时间，在取得上述的Trigger数组之后，
                                 * 会调用isCandidateNewTimeEarlierWithinReason()方法，会将最先触发的Trigger时间与signaledNextFireTime相比，如果大于这一时间，
                                 * 就印证了上述的问题的确发生，则会在releaseIfScheduleChangedSignificantly()方法中，将之前取得Trigger就绪态取消，Trigger数组被清空，重新在下一次循环中取所要触发的任务。
                                 *
                                 *
                                 */
                                if (!isCandidateNewTimeEarlierWithinReason(triggerTime, false)) {
                                    try {
                                        //没有出现新加入trigger比当前最早触发trigger还早情况，则进入休眠到trigger触发时再进行执行
                                        // we could have blocked a long while
                                        // on 'synchronize', so we must recompute
                                        now = System.currentTimeMillis();
                                        //计算触发还剩的触发时间
                                        timeUntilTrigger = triggerTime - now;
                                        //如果剩余时间大于1，超时等待
                                        if(timeUntilTrigger >= 1)
                                            sigLock.wait(timeUntilTrigger);//休眠等待到第一个trigger触发时间
                                    } catch (InterruptedException ignore) {
                                    }
                                }
                            }
                            /**
                             * releaseIfScheduleChangedSignificantly方法内部调用isCandidateNewTimeEarlierWithinReason判断是否上述问题发生
                             * 如果发生则：将之前取得Trigger就绪态(STATE_ACQUIRED)取消重置为STATE_WAITING，并重新放回到JobStore.timeTriggers中，并将当前Trigger数组被清空，重新在下一次循环中取所要触发的任务。
                             */
                            if(releaseIfScheduleChangedSignificantly(triggers, triggerTime)) {
                                break;
                            }
                            now = System.currentTimeMillis();
                            timeUntilTrigger = triggerTime - now;
                        }

                        // this happens if releaseIfScheduleChangedSignificantly decided to release triggers
                        /**
                         * 出现新加入任务触发时间比当前最早trigger触发时间还早情况，调用releaseIfScheduleChangedSignificantly清空trigger数组导致
                         */
                        if(triggers.isEmpty())
                            continue;

                        // set triggers to 'executing'
                        List<TriggerFiredResult> bndles = new ArrayList<TriggerFiredResult>();

                        boolean goAhead = true;
                        synchronized(sigLock) {
                            goAhead = !halted.get();
                        }

                        if(goAhead) {
                            try {
                                /**
                                 * triggersFired方法主要有几个作用:
                                 *  1、取出触发器对应应执行的任务
                                 *  2、记录触发器的执行，修改触发器的状态，如果对应的任务是StatefulJob，则阻塞其他触发器
                                 *  3、调整触发器下次执行的时间
                                 */
                                List<TriggerFiredResult> res = qsRsrcs.getJobStore().triggersFired(triggers);
                                if(res != null)
                                    bndles = res;
                            } catch (SchedulerException se) {
                                qs.notifySchedulerListenersError(
                                        "An error occurred while firing triggers '"
                                                + triggers + "'", se);
                                //QTZ-179 : a problem occurred interacting with the triggers from the db
                                //we release them and loop again
                                for (int i = 0; i < triggers.size(); i++) {
                                    qsRsrcs.getJobStore().releaseAcquiredTrigger(triggers.get(i));
                                }
                                continue;
                            }

                        }

                        for (int i = 0; i < bndles.size(); i++) {
                            //这个循环就是将当前取出的触发器挨个执行，并触发相应的监听器
                            TriggerFiredResult result =  bndles.get(i);
                            TriggerFiredBundle bndle =  result.getTriggerFiredBundle();
                            Exception exception = result.getException();

                            if (exception instanceof RuntimeException) {
                                getLog().error("RuntimeException while firing trigger " + triggers.get(i), exception);
                                //异常时，trigger重新放回到JobStore中
                                qsRsrcs.getJobStore().releaseAcquiredTrigger(triggers.get(i));
                                continue;
                            }

                            // it's possible to get 'null' if the triggers was paused,
                            // blocked, or other similar occurrences that prevent it being
                            // fired at this time...  or if the scheduler was shutdown (halted)
                            if (bndle == null) {
                                qsRsrcs.getJobStore().releaseAcquiredTrigger(triggers.get(i));
                                continue;
                            }

                            JobRunShell shell = null;
                            try {
                                //从线程池中取出线程执行任务
                                /**
                                 * 创建JobRunShell，参加：{@link org.quartz.ee.jta.JTAAnnotationAwareJobRunShellFactory#createJobRunShell}
                                 *
                                 */
                                shell = qsRsrcs.getJobRunShellFactory().createJobRunShell(bndle);
                                shell.initialize(qs);
                                log.info("-->创建JobRunShell完成");
                            } catch (SchedulerException se) {
                                qsRsrcs.getJobStore().triggeredJobComplete(triggers.get(i), bndle.getJobDetail(), CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR);
                                continue;
                            }

                            if (qsRsrcs.getThreadPool().runInThread(shell) == false) {
                                // this case should never happen, as it is indicative of the
                                // scheduler being shutdown or a bug in the thread pool or
                                // a thread pool being used concurrently - which the docs
                                // say not to do...
                                getLog().error("ThreadPool.runInThread() return false!");
                                qsRsrcs.getJobStore().triggeredJobComplete(triggers.get(i), bndle.getJobDetail(), CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR);
                            }

                        }
                        /**
                         * 执行完后重新再取下一批触发器，跳转到run方法中最外层while循环
                         */
                        continue; // while (!halted)
                    }
                } else { // if(availThreadCount > 0)
                    // should never happen, if threadPool.blockForAvailableThreads() follows contract
                    // 这里正常应该不会执行到，因为blockForAvailableThreads()是阻塞方法，无可用线程时会阻塞直到存在可用线程为止
                    continue; // while (!halted)
                }

                log.info("*************没有trigger触发");
                //若本次循环未取出触发器，则阻塞一段时间(随机时间)，然后再重试
                /**
                 * 若本次循环未读取出触发器，则阻塞一段时间(随机时间)timeUntilContinue，然后再循环重试
                 * timeUntilContinue = getRandomizedIdleWaitTime()
                 */
                long now = System.currentTimeMillis();
                long waitTime = now + getRandomizedIdleWaitTime();
                long timeUntilContinue = waitTime - now;
                synchronized(sigLock) {
                    try {
                      if(!halted.get()) {
                        // QTZ-336 A job might have been completed in the mean time and we might have
                        // missed the scheduled changed signal by not waiting for the notify() yet
                        // Check that before waiting for too long in case this very job needs to be
                        // scheduled very soon
                        if (!isScheduleChanged()) {
                            //休眠一定时间后再循环执行
                          sigLock.wait(timeUntilContinue);
                        }
                      }
                    } catch (InterruptedException ignore) {
                    }
                }

            } catch(RuntimeException re) {
                getLog().error("Runtime error occurred in main trigger firing loop.", re);
            }
        } // while (!halted)

        // drop references to scheduler stuff to aid garbage collection...
        qs = null;
        qsRsrcs = null;
    }

    private static final long MIN_DELAY = 20;
    private static final long MAX_DELAY = 600000;

    private static long computeDelayForRepeatedErrors(JobStore jobStore, int acquiresFailed) {
        long delay;
        try {
            delay = jobStore.getAcquireRetryDelay(acquiresFailed);
        } catch (Exception ignored) {
            // we're trying to be useful in case of error states, not cause
            // additional errors..
            delay = 100;
        }


        // sanity check per getAcquireRetryDelay specification
        if (delay < MIN_DELAY)
            delay = MIN_DELAY;
        if (delay > MAX_DELAY)
            delay = MAX_DELAY;

        return delay;
    }

    private boolean releaseIfScheduleChangedSignificantly(
            List<OperableTrigger> triggers, long triggerTime) {
        if (isCandidateNewTimeEarlierWithinReason(triggerTime, true)) {
            // above call does a clearSignaledSchedulingChange()
            for (OperableTrigger trigger : triggers) {
                qsRsrcs.getJobStore().releaseAcquiredTrigger(trigger);
            }
            triggers.clear();
            return true;
        }
        return false;
    }

    private boolean isCandidateNewTimeEarlierWithinReason(long oldTime, boolean clearSignal) {

        // So here's the deal: We know due to being signaled that 'the schedule'
        // has changed.  We may know (if getSignaledNextFireTime() != 0) the
        // new earliest fire time.  We may not (in which case we will assume
        // that the new time is earlier than the trigger we have acquired).
        // In either case, we only want to abandon our acquired trigger and
        // go looking for a new one if "it's worth it".  It's only worth it if
        // the time cost incurred to abandon the trigger and acquire a new one
        // is less than the time until the currently acquired trigger will fire,
        // otherwise we're just "thrashing" the job store (e.g. database).
        //
        // So the question becomes when is it "worth it"?  This will depend on
        // the job store implementation (and of course the particular database
        // or whatever behind it).  Ideally we would depend on the job store
        // implementation to tell us the amount of time in which it "thinks"
        // it can abandon the acquired trigger and acquire a new one.  However
        // we have no current facility for having it tell us that, so we make
        // a somewhat educated but arbitrary guess ;-).

        synchronized(sigLock) {

            if (!isScheduleChanged())
                return false;

            boolean earlier = false;

            if(getSignaledNextFireTime() == 0)
                earlier = true;
            else if(getSignaledNextFireTime() < oldTime )
                earlier = true;

            if(earlier) {
                // so the new time is considered earlier, but is it enough earlier?
                long diff = oldTime - System.currentTimeMillis();
                if(diff < (qsRsrcs.getJobStore().supportsPersistence() ? 70L : 7L))
                    earlier = false;
            }

            if(clearSignal) {
                clearSignaledSchedulingChange();
            }

            return earlier;
        }
    }

    public Logger getLog() {
        return log;
    }

} // end of QuartzSchedulerThread
