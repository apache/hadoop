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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.MetaInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.ClusterInfo;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.service.Service;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Loads and manages the Job history cache.
 */
public class JobHistory extends AbstractService implements HistoryContext {
  private static final Log LOG = LogFactory.getLog(JobHistory.class);

  public static final Pattern CONF_FILENAME_REGEX = Pattern.compile("("
      + JobID.JOBID_REGEX + ")_conf.xml(?:\\.[0-9]+\\.old)?");
  public static final String OLD_SUFFIX = ".old";

  // Time interval for the move thread.
  private long moveThreadInterval;

  // Number of move threads.
  private int numMoveThreads;

  private Configuration conf;

  private Thread moveIntermediateToDoneThread = null;
  private MoveIntermediateToDoneRunnable moveIntermediateToDoneRunnable = null;

  private ScheduledThreadPoolExecutor cleanerScheduledExecutor = null;

  private HistoryStorage storage = null;
  private HistoryFileManager hsManager = null;

  @Override
  public void init(Configuration conf) throws YarnException {
    LOG.info("JobHistory Init");
    this.conf = conf;
    this.appID = RecordFactoryProvider.getRecordFactory(conf)
        .newRecordInstance(ApplicationId.class);
    this.appAttemptID = RecordFactoryProvider.getRecordFactory(conf)
        .newRecordInstance(ApplicationAttemptId.class);

    moveThreadInterval = conf.getLong(
        JHAdminConfig.MR_HISTORY_MOVE_INTERVAL_MS,
        JHAdminConfig.DEFAULT_MR_HISTORY_MOVE_INTERVAL_MS);
    numMoveThreads = conf.getInt(JHAdminConfig.MR_HISTORY_MOVE_THREAD_COUNT,
        JHAdminConfig.DEFAULT_MR_HISTORY_MOVE_THREAD_COUNT);

    hsManager = new HistoryFileManager();
    hsManager.init(conf);
    try {
      hsManager.initExisting();
    } catch (IOException e) {
      throw new YarnException("Failed to intialize existing directories", e);
    }

    storage = ReflectionUtils.newInstance(conf.getClass(
        JHAdminConfig.MR_HISTORY_STORAGE, CachedHistoryStorage.class,
        HistoryStorage.class), conf);
    if (storage instanceof Service) {
      ((Service) storage).init(conf);
    }
    storage.setHistoryFileManager(hsManager);

    super.init(conf);
  }

  @Override
  public void start() {
    hsManager.start();
    if (storage instanceof Service) {
      ((Service) storage).start();
    }

    // Start moveIntermediatToDoneThread
    moveIntermediateToDoneRunnable = new MoveIntermediateToDoneRunnable(
        moveThreadInterval, numMoveThreads);
    moveIntermediateToDoneThread = new Thread(moveIntermediateToDoneRunnable);
    moveIntermediateToDoneThread.setName("MoveIntermediateToDoneScanner");
    moveIntermediateToDoneThread.start();

    // Start historyCleaner
    boolean startCleanerService = conf.getBoolean(
        JHAdminConfig.MR_HISTORY_CLEANER_ENABLE, true);
    if (startCleanerService) {
      long maxAgeOfHistoryFiles = conf.getLong(
          JHAdminConfig.MR_HISTORY_MAX_AGE_MS,
          JHAdminConfig.DEFAULT_MR_HISTORY_MAX_AGE);
      cleanerScheduledExecutor = new ScheduledThreadPoolExecutor(1,
          new ThreadFactoryBuilder().setNameFormat("LogCleaner").build());
      long runInterval = conf.getLong(
          JHAdminConfig.MR_HISTORY_CLEANER_INTERVAL_MS,
          JHAdminConfig.DEFAULT_MR_HISTORY_CLEANER_INTERVAL_MS);
      cleanerScheduledExecutor
          .scheduleAtFixedRate(new HistoryCleaner(maxAgeOfHistoryFiles),
              30 * 1000l, runInterval, TimeUnit.MILLISECONDS);
    }
    super.start();
  }

  @Override
  public void stop() {
    LOG.info("Stopping JobHistory");
    if (moveIntermediateToDoneThread != null) {
      LOG.info("Stopping move thread");
      moveIntermediateToDoneRunnable.stop();
      moveIntermediateToDoneThread.interrupt();
      try {
        LOG.info("Joining on move thread");
        moveIntermediateToDoneThread.join();
      } catch (InterruptedException e) {
        LOG.info("Interrupted while stopping move thread");
      }
    }

    if (cleanerScheduledExecutor != null) {
      LOG.info("Stopping History Cleaner");
      cleanerScheduledExecutor.shutdown();
      boolean interrupted = false;
      long currentTime = System.currentTimeMillis();
      while (!cleanerScheduledExecutor.isShutdown()
          && System.currentTimeMillis() > currentTime + 1000l && !interrupted) {
        try {
          Thread.sleep(20);
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
      if (!cleanerScheduledExecutor.isShutdown()) {
        LOG.warn("HistoryCleanerService shutdown may not have succeeded");
      }
    }
    if (storage instanceof Service) {
      ((Service) storage).stop();
    }
    hsManager.stop();
    super.stop();
  }

  public JobHistory() {
    super(JobHistory.class.getName());
  }

  @Override
  public String getApplicationName() {
    return "Job History Server";
  }

  private class MoveIntermediateToDoneRunnable implements Runnable {

    private long sleepTime;
    private ThreadPoolExecutor moveToDoneExecutor = null;
    private boolean running = false;

    public synchronized void stop() {
      running = false;
      notify();
    }

    MoveIntermediateToDoneRunnable(long sleepTime, int numMoveThreads) {
      this.sleepTime = sleepTime;
      ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat(
          "MoveIntermediateToDone Thread #%d").build();
      moveToDoneExecutor = new ThreadPoolExecutor(1, numMoveThreads, 1,
          TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>(), tf);
      running = true;
    }

    @Override
    public void run() {
      Thread.currentThread().setName("IntermediateHistoryScanner");
      try {
        while (true) {
          LOG.info("Starting scan to move intermediate done files");
          for (final MetaInfo metaInfo : hsManager.getIntermediateMetaInfos()) {
            moveToDoneExecutor.execute(new Runnable() {
              @Override
              public void run() {
                try {
                  hsManager.moveToDone(metaInfo);
                } catch (IOException e) {
                  LOG.info(
                      "Failed to process metaInfo for job: "
                          + metaInfo.getJobId(), e);
                }
              }
            });
          }
          synchronized (this) {
            try {
              this.wait(sleepTime);
            } catch (InterruptedException e) {
              LOG.info("IntermediateHistoryScannerThread interrupted");
            }
            if (!running) {
              break;
            }
          }
        }
      } catch (IOException e) {
        LOG.warn("Unable to get a list of intermediate files to be moved");
        // TODO Shut down the entire process!!!!
      }
    }
  }

  /**
   * Helper method for test cases.
   */
  MetaInfo getJobMetaInfo(JobId jobId) throws IOException {
    return hsManager.getMetaInfo(jobId);
  }

  @Override
  public Job getJob(JobId jobId) {
    return storage.getFullJob(jobId);
  }

  @Override
  public Map<JobId, Job> getAllJobs(ApplicationId appID) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Called getAllJobs(AppId): " + appID);
    }
    // currently there is 1 to 1 mapping between app and job id
    org.apache.hadoop.mapreduce.JobID oldJobID = TypeConverter.fromYarn(appID);
    Map<JobId, Job> jobs = new HashMap<JobId, Job>();
    JobId jobID = TypeConverter.toYarn(oldJobID);
    jobs.put(jobID, getJob(jobID));
    return jobs;
  }

  @Override
  public Map<JobId, Job> getAllJobs() {
    return storage.getAllPartialJobs();
  }

  /**
   * Look for a set of partial jobs.
   * 
   * @param offset
   *          the offset into the list of jobs.
   * @param count
   *          the maximum number of jobs to return.
   * @param user
   *          only return jobs for the given user.
   * @param queue
   *          only return jobs for in the given queue.
   * @param sBegin
   *          only return Jobs that started on or after the given time.
   * @param sEnd
   *          only return Jobs that started on or before the given time.
   * @param fBegin
   *          only return Jobs that ended on or after the given time.
   * @param fEnd
   *          only return Jobs that ended on or before the given time.
   * @param jobState
   *          only return jobs that are in the give job state.
   * @return The list of filtered jobs.
   */
  @Override
  public JobsInfo getPartialJobs(Long offset, Long count, String user,
      String queue, Long sBegin, Long sEnd, Long fBegin, Long fEnd,
      JobState jobState) {
    return storage.getPartialJobs(offset, count, user, queue, sBegin, sEnd,
        fBegin, fEnd, jobState);
  }

  public class HistoryCleaner implements Runnable {
    long maxAgeMillis;

    public HistoryCleaner(long maxAge) {
      this.maxAgeMillis = maxAge;
    }

    public void run() {
      LOG.info("History Cleaner started");
      long cutoff = System.currentTimeMillis() - maxAgeMillis;
      try {
        hsManager.clean(cutoff, storage);
      } catch (IOException e) {
        LOG.warn("Error trying to clean up ", e);
      }
      LOG.info("History Cleaner complete");
    }
  }

  // TODO AppContext - Not Required
  private ApplicationAttemptId appAttemptID;

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    // TODO fixme - bogus appAttemptID for now
    return appAttemptID;
  }

  // TODO AppContext - Not Required
  private ApplicationId appID;

  @Override
  public ApplicationId getApplicationID() {
    // TODO fixme - bogus appID for now
    return appID;
  }

  // TODO AppContext - Not Required
  @Override
  public EventHandler getEventHandler() {
    // TODO Auto-generated method stub
    return null;
  }

  // TODO AppContext - Not Required
  private String userName;

  @Override
  public CharSequence getUser() {
    if (userName != null) {
      userName = conf.get(MRJobConfig.USER_NAME, "history-user");
    }
    return userName;
  }

  // TODO AppContext - Not Required
  @Override
  public Clock getClock() {
    return null;
  }

  // TODO AppContext - Not Required
  @Override
  public ClusterInfo getClusterInfo() {
    return null;
  }
}
