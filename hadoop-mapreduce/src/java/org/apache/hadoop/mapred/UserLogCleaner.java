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

package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.mapreduce.util.MRAsyncDiskService;

/**
 * This is a thread in TaskTracker to cleanup user logs.
 * 
 * Responsibilities of this thread include:
 * <ol>
 * <li>Removing old user logs</li>
 * </ol>
 */
@InterfaceAudience.Private
class UserLogCleaner extends Thread {
  private static final Log LOG = LogFactory.getLog(UserLogCleaner.class);
  static final int DEFAULT_USER_LOG_RETAIN_HOURS = 24; // 1 day
  static final long DEFAULT_THREAD_SLEEP_TIME = 1000 * 60 * 60; // 1 hour

  private Map<JobID, Long> completedJobs = Collections
      .synchronizedMap(new HashMap<JobID, Long>());
  private final long threadSleepTime;
  private MRAsyncDiskService logAsyncDisk;
  private Clock clock;

  UserLogCleaner(Configuration conf) throws IOException {
    threadSleepTime = conf.getLong(TTConfig.TT_USERLOGCLEANUP_SLEEPTIME,
        DEFAULT_THREAD_SLEEP_TIME);
    logAsyncDisk = new MRAsyncDiskService(FileSystem.getLocal(conf), TaskLog
        .getUserLogDir().toString());
    setClock(new Clock());
  }

  void setClock(Clock clock) {
    this.clock = clock;
  }

  Clock getClock() {
    return this.clock;
  }

  @Override
  public void run() {
    // This thread wakes up after every threadSleepTime interval
    // and deletes if there are any old logs.
    while (true) {
      try {
        // sleep
        Thread.sleep(threadSleepTime);
        processCompletedJobs();
      } catch (Throwable e) {
        LOG.warn(getClass().getSimpleName()
            + " encountered an exception while monitoring :", e);
        LOG.info("Ingoring the exception and continuing monitoring.");
      }
    }
  }

  void processCompletedJobs() throws IOException {
    long now = clock.getTime();
    // iterate through completedJobs and remove old logs.
    synchronized (completedJobs) {
      Iterator<Entry<JobID, Long>> completedJobIter = completedJobs.entrySet()
          .iterator();
      while (completedJobIter.hasNext()) {
        Entry<JobID, Long> entry = completedJobIter.next();
        // see if the job is old enough
        if (entry.getValue().longValue() <= now) {
          // add the job logs directory to for delete
          deleteLogPath(TaskLog.getJobDir(entry.getKey()).getAbsolutePath());
          completedJobIter.remove();
        }
      }
    }
  }

  /**
   * Clears all the logs in userlog directory.
   * 
   * Adds the job directories for deletion with default retain hours. Deletes
   * all other directories, if any. This is usually called on reinit/restart of
   * the TaskTracker
   * 
   * @param conf
   * @throws IOException
   */
  void clearOldUserLogs(Configuration conf) throws IOException {
    File userLogDir = TaskLog.getUserLogDir();
    if (userLogDir.exists()) {
      String[] logDirs = userLogDir.list();
      if (logDirs.length > 0) {
        // add all the log dirs to taskLogsMnonitor.
        long now = clock.getTime();
        for (String logDir : logDirs) {
          if (logDir.equals(logAsyncDisk.TOBEDELETED)) {
            // skip this
            continue;
          }
          JobID jobid = null;
          try {
            jobid = JobID.forName(logDir);
          } catch (IllegalArgumentException ie) {
            // if the directory is not a jobid, delete it immediately
            deleteLogPath(new File(userLogDir, logDir).getAbsolutePath());
            continue;
          }
          // add the job log directory with default retain hours, if it is not
          // already added
          if (!completedJobs.containsKey(jobid)) {
            markJobLogsForDeletion(now, conf, jobid);
          }
        }
      }
    }
  }

  private int getUserlogRetainMillis(Configuration conf) {
    return (conf == null ? UserLogCleaner.DEFAULT_USER_LOG_RETAIN_HOURS
        : conf.getInt(MRJobConfig.USER_LOG_RETAIN_HOURS,
            UserLogCleaner.DEFAULT_USER_LOG_RETAIN_HOURS)) * 1000 * 60 * 60;
  }

  /**
   * Adds job user-log directory to cleanup thread to delete logs after user-log
   * retain hours.
   * 
   * If the configuration is null or user-log retain hours is not configured, it
   * is deleted after
   * {@value UserLogCleaner#DEFAULT_USER_LOG_RETAIN_HOURS}
   * 
   * @param jobCompletionTime
   *          job completion time in millis
   * @param conf
   *          The configuration from which user-log retain hours should be read
   * @param jobid
   *          JobID for which user logs should be deleted
   */
  public void markJobLogsForDeletion(long jobCompletionTime, Configuration conf,
      JobID jobid) {
    long retainTimeStamp = jobCompletionTime + (getUserlogRetainMillis(conf));
    LOG.info("Adding " + jobid + " for user-log deletion with retainTimeStamp:"
        + retainTimeStamp);
    completedJobs.put(jobid, Long.valueOf(retainTimeStamp));
  }

  /**
   * Remove job from user log deletion.
   * 
   * @param jobid
   */
  public void unmarkJobFromLogDeletion(JobID jobid) {
    if (completedJobs.remove(jobid) != null) {
      LOG.info("Removing " + jobid + " from user-log deletion");
    }
  }

  /**
   * Deletes the log path.
   * 
   * This path will be removed immediately through {@link MRAsyncDiskService}
   * 
   * @param logPath
   * @throws IOException
   */
  private void deleteLogPath(String logPath) throws IOException {
    LOG.info("Deleting user log path " + logPath);
    logAsyncDisk.moveAndDeleteAbsolutePath(logPath);
  }
}
