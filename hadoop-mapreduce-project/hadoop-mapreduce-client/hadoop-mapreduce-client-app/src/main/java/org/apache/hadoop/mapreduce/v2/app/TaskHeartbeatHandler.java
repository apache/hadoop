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

package org.apache.hadoop.mapreduce.v2.app;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.util.MRJobConfUtil;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class keeps track of tasks that have already been launched. It
 * determines if a task is alive and running or marks a task as dead if it does
 * not hear from it for a long time.
 * 
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TaskHeartbeatHandler extends AbstractService {

  static class ReportTime {
    private long lastProgress;
    private final AtomicBoolean reported;

    public ReportTime(long time) {
      setLastProgress(time);
      reported = new AtomicBoolean(false);
    }
    
    public synchronized void setLastProgress(long time) {
      lastProgress = time;
    }

    public synchronized long getLastProgress() {
      return lastProgress;
    }

    public boolean isReported(){
      return reported.get();
    }
  }
  
  private static final Logger LOG =
      LoggerFactory.getLogger(TaskHeartbeatHandler.class);
  
  //thread which runs periodically to see the last time since a heartbeat is
  //received from a task.
  private Thread lostTaskCheckerThread;
  private volatile boolean stopped;
  private long taskTimeOut;
  private long unregisterTimeOut;
  private long taskStuckTimeOut;
  private int taskTimeOutCheckInterval = 30 * 1000; // 30 seconds.

  private final EventHandler eventHandler;
  private final Clock clock;
  
  private ConcurrentMap<TaskAttemptId, ReportTime> runningAttempts;
  private ConcurrentMap<TaskAttemptId, ReportTime> recentlyUnregisteredAttempts;

  public TaskHeartbeatHandler(EventHandler eventHandler, Clock clock,
      int numThreads) {
    super("TaskHeartbeatHandler");
    this.eventHandler = eventHandler;
    this.clock = clock;
    runningAttempts =
      new ConcurrentHashMap<TaskAttemptId, ReportTime>(16, 0.75f, numThreads);
    recentlyUnregisteredAttempts =
        new ConcurrentHashMap<TaskAttemptId, ReportTime>(16, 0.75f, numThreads);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    taskTimeOut = conf.getLong(
        MRJobConfig.TASK_TIMEOUT, MRJobConfig.DEFAULT_TASK_TIMEOUT_MILLIS);
    unregisterTimeOut = conf.getLong(MRJobConfig.TASK_EXIT_TIMEOUT,
        MRJobConfig.TASK_EXIT_TIMEOUT_DEFAULT);
    taskStuckTimeOut = conf.getLong(MRJobConfig.TASK_STUCK_TIMEOUT_MS,
        MRJobConfig.DEFAULT_TASK_STUCK_TIMEOUT_MS);

    // enforce task timeout is at least twice as long as task report interval
    long taskProgressReportIntervalMillis = MRJobConfUtil.
        getTaskProgressReportInterval(conf);
    long minimumTaskTimeoutAllowed = taskProgressReportIntervalMillis * 2;
    if(taskTimeOut < minimumTaskTimeoutAllowed) {
      taskTimeOut = minimumTaskTimeoutAllowed;
      LOG.info("Task timeout must be as least twice as long as the task " +
          "status report interval. Setting task timeout to " + taskTimeOut);
    }

    taskTimeOutCheckInterval =
        conf.getInt(MRJobConfig.TASK_TIMEOUT_CHECK_INTERVAL_MS, 30 * 1000);
  }

  @Override
  protected void serviceStart() throws Exception {
    lostTaskCheckerThread = new Thread(new PingChecker());
    lostTaskCheckerThread.setName("TaskHeartbeatHandler PingChecker");
    lostTaskCheckerThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (lostTaskCheckerThread != null) {
      lostTaskCheckerThread.interrupt();
    }
    super.serviceStop();
  }

  public void progressing(TaskAttemptId attemptID) {
  //only put for the registered attempts
    //TODO throw an exception if the task isn't registered.
    ReportTime time = runningAttempts.get(attemptID);
    if(time != null) {
      time.reported.compareAndSet(false, true);
      time.setLastProgress(clock.getTime());
    }
  }

  
  public void register(TaskAttemptId attemptID) {
    runningAttempts.put(attemptID, new ReportTime(clock.getTime()));
  }

  public void unregister(TaskAttemptId attemptID) {
    runningAttempts.remove(attemptID);
    recentlyUnregisteredAttempts.put(attemptID,
        new ReportTime(clock.getTime()));
  }

  public boolean hasRecentlyUnregistered(TaskAttemptId attemptID) {
    return recentlyUnregisteredAttempts.containsKey(attemptID);
  }

  private class PingChecker implements Runnable {

    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        long currentTime = clock.getTime();
        checkRunning(currentTime);
        checkRecentlyUnregistered(currentTime);
        try {
          Thread.sleep(taskTimeOutCheckInterval);
        } catch (InterruptedException e) {
          LOG.info("TaskHeartbeatHandler thread interrupted");
          break;
        }
      }
    }

    private void checkRunning(long currentTime) {
      Iterator<Map.Entry<TaskAttemptId, ReportTime>> iterator =
          runningAttempts.entrySet().iterator();

      while (iterator.hasNext()) {
        Map.Entry<TaskAttemptId, ReportTime> entry = iterator.next();
        boolean taskTimedOut = (taskTimeOut > 0) &&
            (currentTime > (entry.getValue().getLastProgress() + taskTimeOut));
        // when container in NM not started in a long time,
        // we think the taskAttempt is stuck
        boolean taskStuck = (taskStuckTimeOut > 0) &&
            (!entry.getValue().isReported()) &&
            (currentTime >
                (entry.getValue().getLastProgress() + taskStuckTimeOut));

        if(taskTimedOut || taskStuck) {
          // task is lost, remove from the list and raise lost event
          iterator.remove();
          eventHandler.handle(new TaskAttemptDiagnosticsUpdateEvent(entry
              .getKey(), "AttemptID:" + entry.getKey().toString()
              + " task timeout set: " + taskTimeOut / 1000 + "s,"
              + " taskTimedOut: " + taskTimedOut + ";"
              + " task stuck timeout set: " + taskStuckTimeOut / 1000 + "s,"
              + " taskStuck: " + taskStuck));
          eventHandler.handle(new TaskAttemptEvent(entry.getKey(),
              TaskAttemptEventType.TA_TIMED_OUT));
        }
      }
    }

    private void checkRecentlyUnregistered(long currentTime) {
      Iterator<ReportTime> iterator =
          recentlyUnregisteredAttempts.values().iterator();
      while (iterator.hasNext()) {
        ReportTime unregisteredTime = iterator.next();
        if (currentTime >
            unregisteredTime.getLastProgress() + unregisterTimeOut) {
          iterator.remove();
        }
      }
    }
  }

  @VisibleForTesting
  ConcurrentMap<TaskAttemptId, ReportTime> getRunningAttempts(){
    return runningAttempts;
  }

  @VisibleForTesting
  public long getTaskTimeOut() {
    return taskTimeOut;
  }
}
