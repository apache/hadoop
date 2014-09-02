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
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
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
import org.apache.hadoop.mapreduce.v2.app.ClusterInfo;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;

import com.google.common.annotations.VisibleForTesting;
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

  private Configuration conf;

  private ScheduledThreadPoolExecutor scheduledExecutor = null;

  private HistoryStorage storage = null;
  private HistoryFileManager hsManager = null;
  ScheduledFuture<?> futureHistoryCleaner = null;
  
  //History job cleaner interval
  private long cleanerInterval;
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    LOG.info("JobHistory Init");
    this.conf = conf;
    this.appID = ApplicationId.newInstance(0, 0);
    this.appAttemptID = RecordFactoryProvider.getRecordFactory(conf)
        .newRecordInstance(ApplicationAttemptId.class);

    moveThreadInterval = conf.getLong(
        JHAdminConfig.MR_HISTORY_MOVE_INTERVAL_MS,
        JHAdminConfig.DEFAULT_MR_HISTORY_MOVE_INTERVAL_MS);

    hsManager = createHistoryFileManager();
    hsManager.init(conf);
    try {
      hsManager.initExisting();
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to intialize existing directories", e);
    }

    storage = createHistoryStorage();
    
    if (storage instanceof Service) {
      ((Service) storage).init(conf);
    }
    storage.setHistoryFileManager(hsManager);

    super.serviceInit(conf);
  }

  protected HistoryStorage createHistoryStorage() {
    return ReflectionUtils.newInstance(conf.getClass(
        JHAdminConfig.MR_HISTORY_STORAGE, CachedHistoryStorage.class,
        HistoryStorage.class), conf);
  }
  
  protected HistoryFileManager createHistoryFileManager() {
    return new HistoryFileManager();
  }

  @Override
  protected void serviceStart() throws Exception {
    hsManager.start();
    if (storage instanceof Service) {
      ((Service) storage).start();
    }

    scheduledExecutor = new ScheduledThreadPoolExecutor(2,
        new ThreadFactoryBuilder().setNameFormat("Log Scanner/Cleaner #%d")
            .build());

    scheduledExecutor.scheduleAtFixedRate(new MoveIntermediateToDoneRunnable(),
        moveThreadInterval, moveThreadInterval, TimeUnit.MILLISECONDS);

    // Start historyCleaner
    scheduleHistoryCleaner();
    super.serviceStart();
  }

  protected int getInitDelaySecs() {
    return 30;
  }
  
  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping JobHistory");
    if (scheduledExecutor != null) {
      LOG.info("Stopping History Cleaner/Move To Done");
      scheduledExecutor.shutdown();
      boolean interrupted = false;
      long currentTime = System.currentTimeMillis();
      while (!scheduledExecutor.isShutdown()
          && System.currentTimeMillis() > currentTime + 1000l && !interrupted) {
        try {
          Thread.sleep(20);
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
      if (!scheduledExecutor.isShutdown()) {
        LOG.warn("HistoryCleanerService/move to done shutdown may not have " +
        		"succeeded, Forcing a shutdown");
        scheduledExecutor.shutdownNow();
      }
    }
    if (storage != null && storage instanceof Service) {
      ((Service) storage).stop();
    }
    if (hsManager != null) {
      hsManager.stop();
    }
    super.serviceStop();
  }

  public JobHistory() {
    super(JobHistory.class.getName());
  }

  @Override
  public String getApplicationName() {
    return "Job History Server";
  }

  private class MoveIntermediateToDoneRunnable implements Runnable {
    @Override
    public void run() {
      try {
        LOG.info("Starting scan to move intermediate done files");
        hsManager.scanIntermediateDirectory();
      } catch (IOException e) {
        LOG.error("Error while scanning intermediate done dir ", e);
      }
    }
  }
  
  private class HistoryCleaner implements Runnable {
    public void run() {
      LOG.info("History Cleaner started");
      try {
        hsManager.clean();
      } catch (IOException e) {
        LOG.warn("Error trying to clean up ", e);
      }
      LOG.info("History Cleaner complete");
    }
  }

  /**
   * Helper method for test cases.
   */
  HistoryFileInfo getJobFileInfo(JobId jobId) throws IOException {
    return hsManager.getFileInfo(jobId);
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

  public void refreshLoadedJobCache() {
    if (getServiceState() == STATE.STARTED) {
      if (storage instanceof CachedHistoryStorage) {
        ((CachedHistoryStorage) storage).refreshLoadedJobCache();
      } else {
        throw new UnsupportedOperationException(storage.getClass().getName()
            + " is expected to be an instance of "
            + CachedHistoryStorage.class.getName());
      }
    } else {
      LOG.warn("Failed to execute refreshLoadedJobCache: JobHistory service is not started");
    }
  }

  @VisibleForTesting
  HistoryStorage getHistoryStorage() {
    return storage;
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

  public void refreshJobRetentionSettings() {
    if (getServiceState() == STATE.STARTED) {
      conf = createConf();
      long maxHistoryAge = conf.getLong(JHAdminConfig.MR_HISTORY_MAX_AGE_MS,
          JHAdminConfig.DEFAULT_MR_HISTORY_MAX_AGE);
      hsManager.setMaxHistoryAge(maxHistoryAge);
      if (futureHistoryCleaner != null) {
        futureHistoryCleaner.cancel(false);
      }
      futureHistoryCleaner = null;
      scheduleHistoryCleaner();
    } else {
      LOG.warn("Failed to execute refreshJobRetentionSettings : Job History service is not started");
    }
  }

  private void scheduleHistoryCleaner() {
    boolean startCleanerService = conf.getBoolean(
        JHAdminConfig.MR_HISTORY_CLEANER_ENABLE, true);
    if (startCleanerService) {
      cleanerInterval = conf.getLong(
          JHAdminConfig.MR_HISTORY_CLEANER_INTERVAL_MS,
          JHAdminConfig.DEFAULT_MR_HISTORY_CLEANER_INTERVAL_MS);

      futureHistoryCleaner = scheduledExecutor.scheduleAtFixedRate(
          new HistoryCleaner(), getInitDelaySecs() * 1000l, cleanerInterval,
          TimeUnit.MILLISECONDS);
    }
  }

  protected Configuration createConf() {
    return new Configuration();
  }
  
  public long getCleanerInterval() {
    return cleanerInterval;
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

  // TODO AppContext - Not Required
  @Override
  public Set<String> getBlacklistedNodes() {
    // Not Implemented
    return null;
  }
  @Override
  public ClientToAMTokenSecretManager getClientToAMTokenSecretManager() {
    // Not implemented.
    return null;
  }

  @Override
  public boolean isLastAMRetry() {
    // bogus - Not Required
    return false;
  }

  @Override
  public boolean hasSuccessfullyUnregistered() {
    // bogus - Not Required
    return true;
  }

  @Override
  public String getNMHostname() {
    // bogus - Not Required
    return null;
  }
}
