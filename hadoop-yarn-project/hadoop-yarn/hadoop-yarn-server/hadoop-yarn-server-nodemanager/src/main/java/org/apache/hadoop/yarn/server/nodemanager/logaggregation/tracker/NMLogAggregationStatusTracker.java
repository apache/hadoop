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

package org.apache.hadoop.yarn.server.nodemanager.logaggregation.tracker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link NMLogAggregationStatusTracker} is used to cache log aggregation
 * status for finished applications. It will also delete the old cached
 * log aggregation status periodically.
 *
 */
public class NMLogAggregationStatusTracker extends CompositeService {

  private static final Logger LOG =
       LoggerFactory.getLogger(NMLogAggregationStatusTracker.class);

  private final ReadLock readLocker;
  private final WriteLock writeLocker;
  private final Context nmContext;
  private final long rollingInterval;
  private final Timer timer;
  private final Map<ApplicationId, AppLogAggregationStatusForRMRecovery>
      recoveryStatuses;
  private boolean disabled = false;

  public NMLogAggregationStatusTracker(Context context) {
    super(NMLogAggregationStatusTracker.class.getName());
    this.nmContext = context;
    Configuration conf = context.getConf();
    if (!conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
      disabled = true;
    }
    this.recoveryStatuses = new ConcurrentHashMap<>();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLocker = lock.readLock();
    this.writeLocker = lock.writeLock();
    this.timer = new Timer();
    long configuredRollingInterval = conf.getLong(
        YarnConfiguration.LOG_AGGREGATION_STATUS_TIME_OUT_MS,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS);
    if (configuredRollingInterval <= 0) {
      this.rollingInterval = YarnConfiguration
          .DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS;
      LOG.warn("The configured log-aggregation-status.time-out.ms is "
          + configuredRollingInterval + " which should be larger than 0. "
          + "Using the default value:" + this.rollingInterval + " instead.");
    } else {
      this.rollingInterval = configuredRollingInterval;
    }
    LOG.info("the rolling interval seconds for the NodeManager Cached Log "
        + "aggregation status is " + (rollingInterval/1000));
  }

  @Override
  protected void serviceStart() throws Exception {
    if (disabled) {
      LOG.warn("Log Aggregation is disabled."
          + "So is the LogAggregationStatusTracker.");
    } else {
      this.timer.scheduleAtFixedRate(new LogAggregationStatusRoller(),
          rollingInterval, rollingInterval);
    }
  }

  @Override
  public void serviceStop() throws Exception {
    this.timer.cancel();
  }

  public void updateLogAggregationStatus(ApplicationId appId,
      LogAggregationStatus logAggregationStatus, long updateTime,
      String diagnosis, boolean finalized) {
    if (disabled) {
      LOG.warn("The log aggregation is disabled. No need to update "
          + "the log aggregation status");
    }
    // In NM, each application has exactly one appLogAggregator thread
    // to handle the log aggregation. So, it is fine which multiple
    // appLogAggregator thread to update log aggregation status for its
    // own application. This is why we are using readLocker here.
    this.readLocker.lock();
    try {
      AppLogAggregationStatusForRMRecovery tracker = recoveryStatuses
          .get(appId);
      if (tracker == null) {
        Application application = this.nmContext.getApplications().get(appId);
        if (application == null) {
          LOG.warn("The application:" + appId + " has already finished,"
              + " and has been removed from NodeManager, we should not "
              + "receive the log aggregation status update for "
              + "this application.");
          return;
        }
        AppLogAggregationStatusForRMRecovery newTracker =
            new AppLogAggregationStatusForRMRecovery(logAggregationStatus,
                diagnosis);
        newTracker.setLastModifiedTime(updateTime);
        newTracker.setFinalized(finalized);
        recoveryStatuses.put(appId, newTracker);
      } else {
        if (tracker.isFinalized()) {
          LOG.warn("Ignore the log aggregation status update request "
              + "for the application:" + appId + ". The cached log aggregation "
              + "status is " + tracker.getLogAggregationStatus() + ".");
        } else {
          if (tracker.getLastModifiedTime() > updateTime) {
            LOG.warn("Ignore the log aggregation status update request "
                + "for the application:" + appId + ". The request log "
                + "aggregation status update is older than the cached "
                + "log aggregation status.");
          } else {
            tracker.setLogAggregationStatus(logAggregationStatus);
            tracker.setDiagnosis(diagnosis);
            tracker.setLastModifiedTime(updateTime);
            tracker.setFinalized(finalized);
            recoveryStatuses.put(appId, tracker);
          }
        }
      }
    } finally {
      this.readLocker.unlock();
    }
  }

  public List<LogAggregationReport> pullCachedLogAggregationReports() {
    List<LogAggregationReport> reports = new ArrayList<>();
    if (disabled) {
      LOG.warn("The log aggregation is disabled."
          + "There is no cached log aggregation status.");
      return reports;
    }
    // When we pull cached Log aggregation reports for all application in
    // this NM, we should make sure that we need to block all of the
    // updateLogAggregationStatus calls. So, the writeLocker is used here.
    this.writeLocker.lock();
    try {
      for(Entry<ApplicationId, AppLogAggregationStatusForRMRecovery> tracker :
          recoveryStatuses.entrySet()) {
        AppLogAggregationStatusForRMRecovery current = tracker.getValue();
        LogAggregationReport report = LogAggregationReport.newInstance(
            tracker.getKey(), current.getLogAggregationStatus(),
            current.getDiagnosis());
        reports.add(report);
      }
      return reports;
    } finally {
      this.writeLocker.unlock();
    }
  }

  private class LogAggregationStatusRoller extends TimerTask {
    @Override
    public void run() {
      rollLogAggregationStatus();
    }
  }

  private void rollLogAggregationStatus() {
    // When we call rollLogAggregationStatus, basically fetch all
    // cached log aggregation status and delete the out-of-timeout period
    // log aggregation status, we should block the rollLogAggregationStatus
    // calls as well as pullCachedLogAggregationReports call. So, the
    // writeLocker is used here.
    this.writeLocker.lock();
    try {
      long currentTimeStamp = System.currentTimeMillis();
      LOG.info("Rolling over the cached log aggregation status.");
      Iterator<Entry<ApplicationId, AppLogAggregationStatusForRMRecovery>> it
          = recoveryStatuses.entrySet().iterator();
      while (it.hasNext()) {
        Entry<ApplicationId, AppLogAggregationStatusForRMRecovery> tracker =
            it.next();
        // the application has finished.
        if (nmContext.getApplications().get(tracker.getKey()) == null) {
          if (currentTimeStamp - tracker.getValue().getLastModifiedTime()
              > rollingInterval) {
            it.remove();
          }
        }
      }
    } finally {
      this.writeLocker.unlock();
    }
  }

  private static class AppLogAggregationStatusForRMRecovery {
    private LogAggregationStatus logAggregationStatus;
    private long lastModifiedTime;
    private boolean finalized;
    private String diagnosis;

    AppLogAggregationStatusForRMRecovery(
        LogAggregationStatus logAggregationStatus, String diagnosis) {
      this.setLogAggregationStatus(logAggregationStatus);
      this.setDiagnosis(diagnosis);
    }

    public LogAggregationStatus getLogAggregationStatus() {
      return logAggregationStatus;
    }

    public void setLogAggregationStatus(
        LogAggregationStatus logAggregationStatus) {
      this.logAggregationStatus = logAggregationStatus;
    }

    public long getLastModifiedTime() {
      return lastModifiedTime;
    }

    public void setLastModifiedTime(long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
    }

    public boolean isFinalized() {
      return finalized;
    }

    public void setFinalized(boolean finalized) {
      this.finalized = finalized;
    }

    public String getDiagnosis() {
      return diagnosis;
    }

    public void setDiagnosis(String diagnosis) {
      this.diagnosis = diagnosis;
    }
  }
}
