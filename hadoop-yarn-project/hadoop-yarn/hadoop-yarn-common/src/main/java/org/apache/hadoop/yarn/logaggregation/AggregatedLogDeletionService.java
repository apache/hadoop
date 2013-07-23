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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * A service that periodically deletes aggregated logs.
 */
@Private
public class AggregatedLogDeletionService extends AbstractService {
  private static final Log LOG = LogFactory.getLog(AggregatedLogDeletionService.class);
  
  private Timer timer = null;
  private long checkIntervalMsecs;
  
  static class LogDeletionTask extends TimerTask {
    private Configuration conf;
    private long retentionMillis;
    private String suffix = null;
    private Path remoteRootLogDir = null;
    
    public LogDeletionTask(Configuration conf, long retentionSecs) {
      this.conf = conf;
      this.retentionMillis = retentionSecs * 1000;
      this.suffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(conf);
      this.remoteRootLogDir =
        new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
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
            deleteOldLogDirsFrom(userDirPath, cutoffMillis, fs);
          }
        }
      } catch (IOException e) {
        logIOException("Error reading root log dir this deletion " +
        		"attempt is being aborted", e);
      }
      LOG.info("aggregated log deletion finished.");
    }
    
    private static void deleteOldLogDirsFrom(Path dir, long cutoffMillis, 
        FileSystem fs) {
      try {
        for(FileStatus appDir : fs.listStatus(dir)) {
          if(appDir.isDirectory() && 
              appDir.getModificationTime() < cutoffMillis) {
            if(shouldDeleteLogDir(appDir, cutoffMillis, fs)) {
              try {
                LOG.info("Deleting aggregated logs in "+appDir.getPath());
                fs.delete(appDir.getPath(), true);
              } catch (IOException e) {
                logIOException("Could not delete "+appDir.getPath(), e);
              }
            }
          }
        }
      } catch (IOException e) {
        logIOException("Could not read the contents of " + dir, e);
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
        logIOException("Error reading the contents of " + dir.getPath(), e);
        shouldDelete = false;
      }
      return shouldDelete;
    }
  }
  
  private static void logIOException(String comment, IOException e) {
    if(e instanceof AccessControlException) {
      String message = e.getMessage();
      //TODO fix this after HADOOP-8661
      message = message.split("\n")[0];
      LOG.warn(comment + " " + message);
    } else {
      LOG.error(comment, e);
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
  
  public void refreshLogRetentionSettings() {
    if (getServiceState() == STATE.STARTED) {
      Configuration conf = createConf();
      setConfig(conf);
      stopTimer();
      scheduleLogDeletionTask();
    } else {
      LOG.warn("Failed to execute refreshLogRetentionSettings : Aggregated Log Deletion Service is not started");
    }
  }
  
  private void scheduleLogDeletionTask() {
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
    TimerTask task = new LogDeletionTask(conf, retentionSecs);
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
}
