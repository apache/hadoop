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

package org.apache.hadoop.yarn.logaggregation;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * A service that periodically deletes aggregated logs.
 */
@InterfaceAudience.LimitedPrivate({"yarn", "mapreduce"})
public class AggregatedLogDeletionService extends AbstractService {
  private static final Logger LOG =
      LoggerFactory.getLogger(AggregatedLogDeletionService.class);
  
  private Timer timer = null;
  private long checkIntervalMsecs;
  private LogDeletionTask task;
  
  static class LogDeletionTask extends TimerTask {
    private Configuration conf;
    private long retentionMillis;
    private String suffix = null;
    private Path remoteRootLogDir = null;
    private ApplicationClientProtocol rmClient = null;
    
    public LogDeletionTask(Configuration conf, long retentionSecs, ApplicationClientProtocol rmClient) {
      this.conf = conf;
      this.retentionMillis = retentionSecs * 1000;
      this.suffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(conf);
      this.remoteRootLogDir =
        new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
      this.rmClient = rmClient;
    }
    
    @Override
    public void run() {
      long cutoffMillis = System.currentTimeMillis() - retentionMillis;
      LOG.info("aggregated log deletion started.");
      try {
        FileSystem fs = remoteRootLogDir.getFileSystem(conf);
        for(FileStatus userDir : fs.listStatus(remoteRootLogDir)) {
          if(userDir.isDirectory()) {
            Path userDirPath = new Path(userDir.getPath(), suffix);
            deleteOldLogDirsFrom(userDirPath, cutoffMillis, fs, rmClient);
          }
        }
      } catch (Throwable t) {
        logException("Error reading root log dir this deletion " +
            "attempt is being aborted", t);
      }
      LOG.info("aggregated log deletion finished.");
    }
    
    private static void deleteOldLogDirsFrom(Path dir, long cutoffMillis, 
        FileSystem fs, ApplicationClientProtocol rmClient) {
      FileStatus[] appDirs;
      try {
        appDirs = fs.listStatus(dir);
      } catch (IOException e) {
        logException("Could not read the contents of " + dir, e);
        return;
      }
      for (FileStatus appDir : appDirs) {
        deleteAppDirLogs(cutoffMillis, fs, rmClient, appDir);
      }
    }

    private static void deleteAppDirLogs(long cutoffMillis, FileSystem fs,
                                         ApplicationClientProtocol rmClient,
                                         FileStatus appDir) {
      try {
        if (appDir.isDirectory() &&
            appDir.getModificationTime() < cutoffMillis) {
          ApplicationId appId = ApplicationId.fromString(
              appDir.getPath().getName());
          boolean appTerminated = isApplicationTerminated(appId, rmClient);
          if (!appTerminated) {
            // Application is still running
            FileStatus[] logFiles;
            try {
              logFiles = fs.listStatus(appDir.getPath());
            } catch (IOException e) {
              logException("Error reading the contents of "
                  + appDir.getPath(), e);
              return;
            }
            for (FileStatus node : logFiles) {
              if (node.getModificationTime() < cutoffMillis) {
                try {
                  fs.delete(node.getPath(), true);
                } catch (IOException ex) {
                  logException("Could not delete " + appDir.getPath(), ex);
                }
              }
            }
          } else if (shouldDeleteLogDir(appDir, cutoffMillis, fs)) {
            // Application is no longer running
            try {
              LOG.info("Deleting aggregated logs in " + appDir.getPath());
              fs.delete(appDir.getPath(), true);
            } catch (IOException e) {
              logException("Could not delete " + appDir.getPath(), e);
            }
          }
        }
      } catch (Exception e) {
        logException("Could not delete " + appDir.getPath(), e);
      }
    }

    private static boolean shouldDeleteLogDir(FileStatus dir, long cutoffMillis, 
        FileSystem fs) {
      boolean shouldDelete = true;
      try {
        for(FileStatus node: fs.listStatus(dir.getPath())) {
          if(node.getModificationTime() >= cutoffMillis) {
            shouldDelete = false;
            break;
          }
        }
      } catch(IOException e) {
        logException("Error reading the contents of " + dir.getPath(), e);
        shouldDelete = false;
      }
      return shouldDelete;
    }

    private static boolean isApplicationTerminated(ApplicationId appId,
        ApplicationClientProtocol rmClient) throws IOException {
      ApplicationReport appReport = null;
      try {
        appReport =
            rmClient.getApplicationReport(
              GetApplicationReportRequest.newInstance(appId))
              .getApplicationReport();
      } catch (ApplicationNotFoundException e) {
        return true;
      } catch (YarnException e) {
        throw new IOException(e);
      }
      YarnApplicationState currentState = appReport.getYarnApplicationState();
      return currentState == YarnApplicationState.FAILED
          || currentState == YarnApplicationState.KILLED
          || currentState == YarnApplicationState.FINISHED;
    }

    public ApplicationClientProtocol getRMClient() {
      return this.rmClient;
    }
  }
  
  private static void logException(String comment, Throwable t) {
    if(t instanceof AccessControlException) {
      String message = t.getMessage();
      //TODO fix this after HADOOP-8661
      message = message.split("\n")[0];
      LOG.warn(comment + " " + message);
    } else {
      LOG.error(comment, t);
    }
  }
  
  public AggregatedLogDeletionService() {
    super(AggregatedLogDeletionService.class.getName());
  }

  @Override
  protected void serviceStart() throws Exception {
    scheduleLogDeletionTask();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopRMClient();
    stopTimer();
    super.serviceStop();
  }
  
  private void setLogAggCheckIntervalMsecs(long retentionSecs) {
    Configuration conf = getConfig();
    checkIntervalMsecs = 1000 * conf
        .getLong(
            YarnConfiguration.LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS,
            YarnConfiguration.DEFAULT_LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS);
    if (checkIntervalMsecs <= 0) {
      // when unspecified compute check interval as 1/10th of retention
      checkIntervalMsecs = (retentionSecs * 1000) / 10;
    }
  }
  
  public void refreshLogRetentionSettings() throws IOException {
    if (getServiceState() == STATE.STARTED) {
      Configuration conf = createConf();
      setConfig(conf);
      stopRMClient();
      stopTimer();
      scheduleLogDeletionTask();
    } else {
      LOG.warn("Failed to execute refreshLogRetentionSettings : Aggregated Log Deletion Service is not started");
    }
  }
  
  private void scheduleLogDeletionTask() throws IOException {
    Configuration conf = getConfig();
    if (!conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
      // Log aggregation is not enabled so don't bother
      return;
    }
    long retentionSecs = conf.getLong(
        YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_RETAIN_SECONDS);
    if (retentionSecs < 0) {
      LOG.info("Log Aggregation deletion is disabled because retention is"
          + " too small (" + retentionSecs + ")");
      return;
    }
    setLogAggCheckIntervalMsecs(retentionSecs);
    task = new LogDeletionTask(conf, retentionSecs, createRMClient());
    timer = new Timer();
    timer.scheduleAtFixedRate(task, 0, checkIntervalMsecs);
  }

  private void stopTimer() {
    if (timer != null) {
      timer.cancel();
    }
  }
  
  public long getCheckIntervalMsecs() {
    return checkIntervalMsecs;
  }

  protected Configuration createConf() {
    return new Configuration();
  }

  // Directly create and use ApplicationClientProtocol.
  // We have already marked ApplicationClientProtocol.getApplicationReport
  // as @Idempotent, it will automatically take care of RM restart/failover.
  @VisibleForTesting
  protected ApplicationClientProtocol createRMClient() throws IOException {
    return ClientRMProxy.createRMProxy(getConfig(),
      ApplicationClientProtocol.class);
  }

  @VisibleForTesting
  protected void stopRMClient() {
    if (task != null && task.getRMClient() != null) {
      RPC.stopProxy(task.getRMClient());
    }
  }
}
