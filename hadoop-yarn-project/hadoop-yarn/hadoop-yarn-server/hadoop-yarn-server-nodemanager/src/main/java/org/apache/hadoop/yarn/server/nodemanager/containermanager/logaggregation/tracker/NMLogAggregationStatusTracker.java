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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.tracker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NMLogAggregationStatusTracker {

  private static final Logger LOG =
       LoggerFactory.getLogger(NMLogAggregationStatusTracker.class);

  private final ReadLock updateLocker;
  private final WriteLock pullLocker;
  private final Context nmContext;
  private final long rollingInterval;
  private final Timer timer;
  private final Map<ApplicationId, LogAggregationTrakcer> trackers;
  private boolean disabled = false;

  public NMLogAggregationStatusTracker(Context context) {
    this.nmContext = context;
    Configuration conf = context.getConf();
    if (!conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
      disabled = true;
    }
    this.trackers = new HashMap<>();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.updateLocker = lock.readLock();
    this.pullLocker = lock.writeLock();
    this.timer = new Timer();
    this.rollingInterval = conf.getLong(
        YarnConfiguration.LOG_AGGREGATION_STATUS_TIME_OUT_MS,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS);
    LOG.info("the rolling interval seconds for the NodeManager Cached Log "
        + "aggregation status is " + (rollingInterval/1000));
  }

  public void start() {
    if (disabled) {
      LOG.warn("Log Aggregation is disabled."
          + "So is the LogAggregationStatusTracker.");
    } else {
      this.timer.scheduleAtFixedRate(new LogAggregationStatusRoller(),
          rollingInterval, rollingInterval);
    }
  }

  public void stop() {
    this.timer.cancel();
  }

  public void updateLogAggregationStatus(ApplicationId appId,
      LogAggregationStatus logAggregationStatus, long updateTime,
      String diagnosis, boolean finalized) {
    if (disabled) {
      LOG.warn("The log aggregation is diabled. No need to update "
          + "the log aggregation status");
    }
    this.updateLocker.lock();
    try {
      LogAggregationTrakcer tracker = trackers.get(appId);
      if (tracker == null) {
        Application application = this.nmContext.getApplications().get(appId);
        if (application == null) {
          // the application has already finished or
          // this application is unknown application.
          // Check the log aggregation status update time, if the update time is
          // still in the period of timeout, we add it to the trackers map.
          // Otherwise, we ignore it.
          long currentTime = System.currentTimeMillis();
          if (currentTime - updateTime > rollingInterval) {
            LOG.warn("Ignore the log aggregation status update request "
                + "for the application:" + appId + ". The log aggregation status"
                + " update time is " + updateTime + " while the request process "
                + "time is " + currentTime + ".");
            return;
          }
        }
        LogAggregationTrakcer newTracker = new LogAggregationTrakcer(
            logAggregationStatus, diagnosis);
        newTracker.setLastModifiedTime(updateTime);
        newTracker.setFinalized(finalized);
        trackers.put(appId, newTracker);
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
            trackers.put(appId, tracker);
          }
        }
      }
    } finally {
      this.updateLocker.unlock();
    }
  }

  public List<LogAggregationReport> pullCachedLogAggregationReports() {
    List<LogAggregationReport> reports = new ArrayList<>();
    if (disabled) {
      LOG.warn("The log aggregation is diabled."
          + "There is no cached log aggregation status.");
      return reports;
    }
    this.pullLocker.lock();
    try {
      for(Entry<ApplicationId, LogAggregationTrakcer> tracker :
          trackers.entrySet()) {
        LogAggregationTrakcer current = tracker.getValue();
        LogAggregationReport report = LogAggregationReport.newInstance(
            tracker.getKey(), current.getLogAggregationStatus(),
            current.getDiagnosis());
        reports.add(report);
      }
      return reports;
    } finally {
      this.pullLocker.unlock();
    }
  }

  private class LogAggregationStatusRoller extends TimerTask {
    @Override
    public void run() {
      rollLogAggregationStatus();
    }
  }

  @Private
  void rollLogAggregationStatus() {
    this.pullLocker.lock();
    try {
      long currentTimeStamp = System.currentTimeMillis();
      LOG.info("Rolling over the cached log aggregation status.");
      Iterator<Entry<ApplicationId, LogAggregationTrakcer>> it = trackers
          .entrySet().iterator();
      while (it.hasNext()) {
        Entry<ApplicationId, LogAggregationTrakcer> tracker = it.next(); 
        // the application has finished.
        if (nmContext.getApplications().get(tracker.getKey()) == null) {
          if (currentTimeStamp - tracker.getValue().getLastModifiedTime()
              > rollingInterval) {
            it.remove();
          }
        }
      }
    } finally {
      this.pullLocker.unlock();
    }
  }

  private static class LogAggregationTrakcer {
    private LogAggregationStatus logAggregationStatus;
    private long lastModifiedTime;
    private boolean finalized;
    private String diagnosis;

    public LogAggregationTrakcer(
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
