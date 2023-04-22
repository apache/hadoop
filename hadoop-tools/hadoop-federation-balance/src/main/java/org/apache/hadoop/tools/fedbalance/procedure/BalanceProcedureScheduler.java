/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.tools.fedbalance.procedure;

import org.apache.hadoop.classification.VisibleForTesting;

import java.io.IOException;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.WORK_THREAD_NUM;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.WORK_THREAD_NUM_DEFAULT;
/**
 * <pre>
 * The state machine framework consist of:
 *   Job:                The state machine. It implements the basic logic of the
 *                       state machine.
 *   Procedure:          The components of the job. It implements the custom
 *                       logic.
 *   ProcedureScheduler: The multi-thread model responsible for running,
 *                       recovering, handling errors and job persistence.
 *   Journal:            It handles the job persistence and recover.
 *
 * Example:
 *   Job.Builder builder = new Job.Builder&lt;&gt;();
 *   builder.nextProcedure(new WaitProcedure("wait", 1000, 30 * 1000));
 *   Job job = builder.build();
 *
 *   ProcedureScheduler scheduler = new ProcedureScheduler(CONF);
 *   scheduler.init();
 *   scheduler.submit(job);
 *   scheduler.waitUntilDone(job);
 * </pre>
 */
public class BalanceProcedureScheduler {
  public static final Logger LOG =
      LoggerFactory.getLogger(BalanceProcedureScheduler.class);
  // The set containing all the jobs, including submitted and recovered ones.
  private ConcurrentHashMap<BalanceJob, BalanceJob> jobSet;
  // Containing jobs pending for running.
  private LinkedBlockingQueue<BalanceJob> runningQueue;
  // Containing jobs pending for wake up.
  private DelayQueue<DelayWrapper> delayQueue;
  // Containing jobs pending for recovery.
  private LinkedBlockingQueue<BalanceJob> recoverQueue;
  private Configuration conf;
  private BalanceJournal journal; // handle jobs' journals.

  private Thread readerThread; // consume the runningQueue and send to workers.
  private ThreadPoolExecutor workersPool; // the real threads running the jobs.
  private Thread roosterThread; // wake up the jobs in the delayQueue.
  private Thread recoverThread; // recover the jobs in the recoverQueue.
  // The running state of this scheduler.
  private AtomicBoolean running = new AtomicBoolean(true);

  public BalanceProcedureScheduler(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Init the scheduler.
   *
   * @param recoverJobs whether to recover all the jobs from journal or not.
   */
  public synchronized void init(boolean recoverJobs) throws IOException {
    this.runningQueue = new LinkedBlockingQueue<>();
    this.delayQueue = new DelayQueue<>();
    this.recoverQueue = new LinkedBlockingQueue<>();
    this.jobSet = new ConcurrentHashMap<>();

    // start threads.
    this.roosterThread = new Rooster();
    this.roosterThread.setDaemon(true);
    roosterThread.start();
    this.recoverThread = new Recover();
    this.recoverThread.setDaemon(true);
    recoverThread.start();
    int workerNum = conf.getInt(WORK_THREAD_NUM, WORK_THREAD_NUM_DEFAULT);
    workersPool = new ThreadPoolExecutor(workerNum, workerNum * 2, 1,
        TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
    this.readerThread = new Reader();
    this.readerThread.start();

    // init journal.
    journal = new BalanceJournalInfoHDFS();
    journal.setConf(conf);

    if (recoverJobs) {
      recoverAllJobs();
    }
  }

  /**
   * Submit the job.
   */
  public synchronized void submit(BalanceJob job) throws IOException {
    if (!running.get()) {
      throw new IOException("Scheduler is shutdown.");
    }
    String jobId = allocateJobId();
    job.setId(jobId);
    job.setScheduler(this);
    journal.saveJob(job);
    jobSet.put(job, job);
    runningQueue.add(job);
    LOG.info("Add new job={}", job);
  }

  /**
   * Remove the job from scheduler if it finishes.
   */
  public BalanceJob remove(BalanceJob job) {
    BalanceJob inner = findJob(job);
    if (inner == null) {
      return null;
    } else if (job.isJobDone()) {
      synchronized (this) {
        return jobSet.remove(inner);
      }
    }
    return null;
  }

  /**
   * Find job in scheduler.
   *
   * @return the job in scheduler. Null if the schedule has no job with the
   *         same id.
   */
  public BalanceJob findJob(BalanceJob job) {
    BalanceJob found = null;
    for (BalanceJob j : jobSet.keySet()) {
      if (j.getId().equals(job.getId())) {
        found = j;
        break;
      }
    }
    return found;
  }

  /**
   * Return all jobs in the scheduler.
   */
  public Collection<BalanceJob> getAllJobs() {
    return jobSet.values();
  }

  /**
   * Wait permanently until the job is done.
   */
  public void waitUntilDone(BalanceJob job) {
    BalanceJob found = findJob(job);
    if (found == null || found.isJobDone()) {
      return;
    }
    while (!found.isJobDone()) {
      try {
        found.waitJobDone();
      } catch (InterruptedException e) {
      }
    }
  }

  /**
   * Delay this job.
   */
  void delay(BalanceJob job, long delayInMilliseconds) {
    delayQueue.add(new DelayWrapper(job, delayInMilliseconds));
    LOG.info("Need delay {}ms. Add to delayQueue. job={}", delayInMilliseconds,
        job);
  }

  boolean jobDone(BalanceJob job) {
    try {
      journal.clear(job);
      if (job.shouldRemoveAfterDone()) {
        jobSet.remove(job);
      }
      return true;
    } catch (IOException e) {
      LOG.warn("Clear journal failed, add to recoverQueue. job=" + job, e);
      recoverQueue.add(job);
      return false;
    }
  }

  /**
   * Save current status to journal.
   */
  boolean writeJournal(BalanceJob job) {
    try {
      journal.saveJob(job);
      return true;
    } catch (Exception e) {
      LOG.warn("Save procedure failed, add to recoverQueue. job=" + job, e);
      recoverQueue.add(job);
      return false;
    }
  }

  /**
   * The running state of the scheduler.
   */
  public boolean isRunning() {
    return running.get();
  }

  /**
   * Shutdown the scheduler.
   */
  public synchronized void shutDown() {
    if (!running.get()) {
      return;
    }
    running.set(false);
    readerThread.interrupt();
    roosterThread.interrupt();
    recoverThread.interrupt();
    workersPool.shutdownNow();
  }

  /**
   * Shutdown scheduler and wait at most timeout seconds for procedures to
   * finish.
   * @param timeout Wait at most timeout seconds for procedures to finish.
   */
  public synchronized void shutDownAndWait(int timeout) {
    shutDown();
    while (readerThread.isAlive()) {
      try {
        readerThread.join();
      } catch (InterruptedException e) {
      }
    }
    while (roosterThread.isAlive()) {
      try {
        roosterThread.join();
      } catch (InterruptedException e) {
      }
    }
    while (recoverThread.isAlive()) {
      try {
        recoverThread.join();
      } catch (InterruptedException e) {
      }
    }
    while (!workersPool.isTerminated()) {
      try {
        workersPool.awaitTermination(timeout, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
      }
    }
  }

  /**
   * Search all jobs and add them to recoverQueue. It's called once after the
   * scheduler starts.
   */
  private void recoverAllJobs() throws IOException {
    BalanceJob[] jobs = journal.listAllJobs();
    for (BalanceJob job : jobs) {
      recoverQueue.add(job);
      jobSet.put(job, job);
      LOG.info("Recover federation balance job {}.", job);
    }
  }

  @VisibleForTesting
  static String allocateJobId() {
    return "job-" + UUID.randomUUID();
  }

  @VisibleForTesting
  public void setJournal(BalanceJournal journal) {
    this.journal = journal;
  }

  /**
   * This thread consumes the delayQueue and move the jobs to the runningQueue.
   */
  class Rooster extends Thread {
    @Override
    public void run() {
      while (running.get()) {
        try {
          DelayWrapper dJob = delayQueue.take();
          runningQueue.add(dJob.getJob());
          LOG.info("Wake up job={}", dJob.getJob());
        } catch (InterruptedException e) {
          // ignore interrupt exception.
        }
      }
    }
  }

  /**
   * This thread consumes the runningQueue and give the job to the workers.
   */
  class Reader extends Thread {
    @Override
    public void run() {
      while (running.get()) {
        try {
          final BalanceJob job = runningQueue.poll(500, TimeUnit.MILLISECONDS);
          if (job != null) {
            workersPool.submit(() -> {
              LOG.info("Start job. job_msg={}", job.getDetailMessage());
              job.execute();
              if (!running.get()) {
                return;
              }
              if (job.isJobDone()) {
                if (job.getError() == null) {
                  LOG.info("Job done. job={}", job);
                } else {
                  LOG.warn("Job failed. job=" + job, job.getError());
                }
              }
              return;
            });
          }
        } catch (InterruptedException e) {
          // ignore interrupt exception.
        }
      }
    }
  }

  /**
   * This thread consumes the recoverQueue, recovers the job the adds it to the
   * runningQueue.
   */
  class Recover extends Thread {
    @Override
    public void run() {
      while (running.get()) {
        BalanceJob job = null;
        try {
          job = recoverQueue.poll(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
          // ignore interrupt exception.
        }
        if (job != null) {
          try {
            journal.recoverJob(job);
            job.setScheduler(BalanceProcedureScheduler.this);
            runningQueue.add(job);
            LOG.info("Recover success, add to runningQueue. job={}", job);
          } catch (IOException e) {
            LOG.warn("Recover failed, re-add to recoverQueue. job=" + job, e);
            recoverQueue.add(job);
          }
        }
      }
    }
  }

  /**
   * Wrap the delayed BalanceJob.
   */
  private static class DelayWrapper implements Delayed {
    private BalanceJob job;
    private long time;

    DelayWrapper(BalanceJob job, long delayInMilliseconds) {
      this.job = job;
      this.time = Time.monotonicNow() + delayInMilliseconds;
    }

    BalanceJob getJob() {
      return job;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      long delay = time - Time.monotonicNow();
      if (delay < 0) {
        delay = 0;
      }
      return unit.convert(delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      DelayWrapper dw = (DelayWrapper) o;
      return new CompareToBuilder()
          .append(time, dw.time)
          .append(job, dw.job)
          .toComparison();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder(17, 37)
          .append(time)
          .append(job)
          .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      if (obj.getClass() != getClass()) {
        return false;
      }
      DelayWrapper dw = (DelayWrapper) obj;
      return new EqualsBuilder()
          .appendSuper(super.equals(obj))
          .append(time, dw.time)
          .append(job, dw.job)
          .build();
    }
  }
}
