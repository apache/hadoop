/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * Log aggregation logic used by RMApp.
 *
 */
public class RMAppLogAggregation {
  private final boolean logAggregationEnabled;
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private long logAggregationStartTime = 0;
  private final long logAggregationStatusTimeout;
  private final Map<NodeId, LogAggregationReport> logAggregationStatus =
      new ConcurrentHashMap<>();
  private volatile LogAggregationStatus logAggregationStatusForAppReport;
  private int logAggregationSucceed = 0;
  private int logAggregationFailed = 0;
  private Map<NodeId, List<String>> logAggregationDiagnosticsForNMs =
      new HashMap<>();
  private Map<NodeId, List<String>> logAggregationFailureMessagesForNMs =
      new HashMap<>();
  private final int maxLogAggregationDiagnosticsInMemory;

  RMAppLogAggregation(Configuration conf, ReadLock readLock,
      WriteLock writeLock) {
    this.readLock = readLock;
    this.writeLock = writeLock;
    this.logAggregationStatusTimeout = getLogAggregationStatusTimeout(conf);
    this.logAggregationEnabled = getEnabledFlagFromConf(conf);
    this.logAggregationStatusForAppReport =
        this.logAggregationEnabled ? LogAggregationStatus.NOT_START :
            LogAggregationStatus.DISABLED;
    this.maxLogAggregationDiagnosticsInMemory =
        getMaxLogAggregationDiagnostics(conf);
  }

  private long getLogAggregationStatusTimeout(Configuration conf) {
    long statusTimeout =
        conf.getLong(YarnConfiguration.LOG_AGGREGATION_STATUS_TIME_OUT_MS,
            YarnConfiguration.DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS);
    if (statusTimeout <= 0) {
      return YarnConfiguration.DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS;
    } else {
      return statusTimeout;
    }
  }

  private boolean getEnabledFlagFromConf(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED);
  }

  private int getMaxLogAggregationDiagnostics(Configuration conf) {
    return conf.getInt(
        YarnConfiguration.RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY,
        YarnConfiguration.DEFAULT_RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY);
  }

  Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp(
      RMAppImpl rmApp) {
    this.readLock.lock();
    try {
      if (!isLogAggregationFinished() && RMAppImpl.isAppInFinalState(rmApp) &&
          rmApp.getSystemClock().getTime() > this.logAggregationStartTime
              + this.logAggregationStatusTimeout) {
        for (Map.Entry<NodeId, LogAggregationReport> output :
            logAggregationStatus.entrySet()) {
          if (!output.getValue().getLogAggregationStatus()
              .equals(LogAggregationStatus.TIME_OUT)
              && !output.getValue().getLogAggregationStatus()
              .equals(LogAggregationStatus.SUCCEEDED)
              && !output.getValue().getLogAggregationStatus()
              .equals(LogAggregationStatus.FAILED)) {
            output.getValue().setLogAggregationStatus(
                LogAggregationStatus.TIME_OUT);
          }
        }
      }
      return Collections.unmodifiableMap(logAggregationStatus);
    } finally {
      this.readLock.unlock();
    }
  }

  void aggregateLogReport(NodeId nodeId, LogAggregationReport report,
      RMAppImpl rmApp) {
    this.writeLock.lock();
    try {
      if (this.logAggregationEnabled && !isLogAggregationFinished()) {
        LogAggregationReport curReport = this.logAggregationStatus.get(nodeId);
        boolean stateChangedToFinal = false;
        if (curReport == null) {
          this.logAggregationStatus.put(nodeId, report);
          if (isLogAggregationFinishedForNM(report)) {
            stateChangedToFinal = true;
          }
        } else {
          if (isLogAggregationFinishedForNM(report)) {
            if (!isLogAggregationFinishedForNM(curReport)) {
              stateChangedToFinal = true;
            }
          }
          if (report.getLogAggregationStatus() != LogAggregationStatus.RUNNING
              || curReport.getLogAggregationStatus() !=
              LogAggregationStatus.RUNNING_WITH_FAILURE) {
            if (curReport.getLogAggregationStatus()
                == LogAggregationStatus.TIME_OUT
                && report.getLogAggregationStatus()
                == LogAggregationStatus.RUNNING) {
              // If the log aggregation status got from latest NM heartbeat
              // is RUNNING, and current log aggregation status is TIME_OUT,
              // based on whether there are any failure messages for this NM,
              // we will reset the log aggregation status as RUNNING or
              // RUNNING_WITH_FAILURE
              if (isThereFailureMessageForNM(nodeId)) {
                report.setLogAggregationStatus(
                    LogAggregationStatus.RUNNING_WITH_FAILURE);
              }
            }
            curReport.setLogAggregationStatus(report
                .getLogAggregationStatus());
          }
        }
        updateLogAggregationDiagnosticMessages(nodeId, report);
        if (RMAppImpl.isAppInFinalState(rmApp) && stateChangedToFinal) {
          updateLogAggregationStatus(nodeId);
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  public LogAggregationStatus getLogAggregationStatusForAppReport(
      RMAppImpl rmApp) {
    boolean appInFinalState = RMAppImpl.isAppInFinalState(rmApp);
    this.readLock.lock();
    try {
      if (!logAggregationEnabled) {
        return LogAggregationStatus.DISABLED;
      }
      if (isLogAggregationFinished()) {
        return this.logAggregationStatusForAppReport;
      }
      Map<NodeId, LogAggregationReport> reports =
          getLogAggregationReportsForApp(rmApp);
      if (reports.size() == 0) {
        return this.logAggregationStatusForAppReport;
      }
      int logNotStartCount = 0;
      int logCompletedCount = 0;
      int logTimeOutCount = 0;
      int logFailedCount = 0;
      int logRunningWithFailure = 0;
      for (Map.Entry<NodeId, LogAggregationReport> report :
          reports.entrySet()) {
        switch (report.getValue().getLogAggregationStatus()) {
          case NOT_START:
            logNotStartCount++;
            break;
          case RUNNING_WITH_FAILURE:
            logRunningWithFailure ++;
            break;
          case SUCCEEDED:
            logCompletedCount++;
            break;
          case FAILED:
            logFailedCount++;
            logCompletedCount++;
            break;
          case TIME_OUT:
            logTimeOutCount++;
            logCompletedCount++;
            break;
          default:
            break;
        }
      }
      if (logNotStartCount == reports.size()) {
        return LogAggregationStatus.NOT_START;
      } else if (logCompletedCount == reports.size()) {
        // We should satisfy two condition in order to return
        // SUCCEEDED or FAILED.
        // 1) make sure the application is in final state
        // 2) logs status from all NMs are SUCCEEDED/FAILED/TIMEOUT
        // The SUCCEEDED/FAILED status is the final status which means
        // the log aggregation is finished. And the log aggregation status will
        // not be updated anymore.
        if (logFailedCount > 0 && appInFinalState) {
          this.logAggregationStatusForAppReport =
              LogAggregationStatus.FAILED;
          return LogAggregationStatus.FAILED;
        } else if (logTimeOutCount > 0) {
          this.logAggregationStatusForAppReport =
              LogAggregationStatus.TIME_OUT;
          return LogAggregationStatus.TIME_OUT;
        }
        if (appInFinalState) {
          this.logAggregationStatusForAppReport =
              LogAggregationStatus.SUCCEEDED;
          return LogAggregationStatus.SUCCEEDED;
        }
      } else if (logRunningWithFailure > 0) {
        return LogAggregationStatus.RUNNING_WITH_FAILURE;
      }
      return LogAggregationStatus.RUNNING;
    } finally {
      this.readLock.unlock();
    }
  }

  private boolean isLogAggregationFinished() {
    return this.logAggregationStatusForAppReport
        .equals(LogAggregationStatus.SUCCEEDED)
        || this.logAggregationStatusForAppReport
        .equals(LogAggregationStatus.FAILED)
        || this.logAggregationStatusForAppReport
        .equals(LogAggregationStatus.TIME_OUT);

  }

  private boolean isLogAggregationFinishedForNM(LogAggregationReport report) {
    return report.getLogAggregationStatus() == LogAggregationStatus.SUCCEEDED
        || report.getLogAggregationStatus() == LogAggregationStatus.FAILED;
  }

  private void updateLogAggregationDiagnosticMessages(NodeId nodeId,
      LogAggregationReport report) {
    if (report.getDiagnosticMessage() != null
        && !report.getDiagnosticMessage().isEmpty()) {
      if (report.getLogAggregationStatus()
          == LogAggregationStatus.RUNNING ) {
        List<String> diagnostics = logAggregationDiagnosticsForNMs.get(nodeId);
        if (diagnostics == null) {
          diagnostics = new ArrayList<>();
          logAggregationDiagnosticsForNMs.put(nodeId, diagnostics);
        } else {
          if (diagnostics.size()
              == maxLogAggregationDiagnosticsInMemory) {
            diagnostics.remove(0);
          }
        }
        diagnostics.add(report.getDiagnosticMessage());
        this.logAggregationStatus.get(nodeId).setDiagnosticMessage(
            StringUtils.join(diagnostics, "\n"));
      } else if (report.getLogAggregationStatus()
          == LogAggregationStatus.RUNNING_WITH_FAILURE) {
        List<String> failureMessages =
            logAggregationFailureMessagesForNMs.get(nodeId);
        if (failureMessages == null) {
          failureMessages = new ArrayList<>();
          logAggregationFailureMessagesForNMs.put(nodeId, failureMessages);
        } else {
          if (failureMessages.size()
              == maxLogAggregationDiagnosticsInMemory) {
            failureMessages.remove(0);
          }
        }
        failureMessages.add(report.getDiagnosticMessage());
      }
    }
  }

  private void updateLogAggregationStatus(NodeId nodeId) {
    LogAggregationStatus status =
        this.logAggregationStatus.get(nodeId).getLogAggregationStatus();
    if (status.equals(LogAggregationStatus.SUCCEEDED)) {
      this.logAggregationSucceed++;
    } else if (status.equals(LogAggregationStatus.FAILED)) {
      this.logAggregationFailed++;
    }
    if (this.logAggregationSucceed == this.logAggregationStatus.size()) {
      this.logAggregationStatusForAppReport =
          LogAggregationStatus.SUCCEEDED;
      // Since the log aggregation status for this application for all NMs
      // is SUCCEEDED, it means all logs are aggregated successfully.
      // We could remove all the cached log aggregation reports
      this.logAggregationStatus.clear();
      this.logAggregationDiagnosticsForNMs.clear();
      this.logAggregationFailureMessagesForNMs.clear();
    } else if (this.logAggregationSucceed + this.logAggregationFailed
        == this.logAggregationStatus.size()) {
      this.logAggregationStatusForAppReport = LogAggregationStatus.FAILED;
      // We have collected the log aggregation status for all NMs.
      // The log aggregation status is FAILED which means the log
      // aggregation fails in some NMs. We are only interested in the
      // nodes where the log aggregation is failed. So we could remove
      // the log aggregation details for those succeeded NMs
      this.logAggregationStatus.entrySet().removeIf(entry ->
          entry.getValue().getLogAggregationStatus()
          .equals(LogAggregationStatus.SUCCEEDED));
      // the log aggregation has finished/failed.
      // and the status will not be updated anymore.
      this.logAggregationDiagnosticsForNMs.clear();
    }
  }

  String getLogAggregationFailureMessagesForNM(NodeId nodeId) {
    this.readLock.lock();
    try {
      List<String> failureMessages =
          this.logAggregationFailureMessagesForNMs.get(nodeId);
      if (failureMessages == null || failureMessages.isEmpty()) {
        return StringUtils.EMPTY;
      }
      return StringUtils.join(failureMessages, "\n");
    } finally {
      this.readLock.unlock();
    }
  }

  void recordLogAggregationStartTime(long time) {
    logAggregationStartTime = time;
  }

  public boolean isEnabled() {
    return logAggregationEnabled;
  }

  private boolean hasReportForNodeManager(NodeId nodeId) {
    return logAggregationStatus.containsKey(nodeId);
  }

  private void addReportForNodeManager(NodeId nodeId,
      LogAggregationReport report) {
    logAggregationStatus.put(nodeId, report);
  }

  public boolean isFinished() {
    return isLogAggregationFinished();
  }

  private boolean isThereFailureMessageForNM(NodeId nodeId) {
    return logAggregationFailureMessagesForNMs.get(nodeId) != null
        && !logAggregationFailureMessagesForNMs.get(nodeId).isEmpty();
  }

  long getLogAggregationStartTime() {
    return logAggregationStartTime;
  }

  void addReportIfNecessary(NodeId nodeId, ApplicationId applicationId) {
    if (!hasReportForNodeManager(nodeId)) {
      LogAggregationStatus status = isEnabled() ? LogAggregationStatus.NOT_START
          : LogAggregationStatus.DISABLED;
      addReportForNodeManager(nodeId,
          LogAggregationReport.newInstance(applicationId, status, ""));
    }
  }
}
