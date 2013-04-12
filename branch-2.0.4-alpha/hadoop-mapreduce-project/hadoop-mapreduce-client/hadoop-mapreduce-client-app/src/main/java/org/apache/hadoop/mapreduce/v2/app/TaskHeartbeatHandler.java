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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;


/**
 * This class keeps track of tasks that have already been launched. It
 * determines if a task is alive and running or marks a task as dead if it does
 * not hear from it for a long time.
 * 
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TaskHeartbeatHandler extends AbstractService {
  
  private static class ReportTime {
    private long lastProgress;
    
    public ReportTime(long time) {
      setLastProgress(time);
    }
    
    public synchronized void setLastProgress(long time) {
      lastProgress = time;
    }

    public synchronized long getLastProgress() {
      return lastProgress;
    }
  }
  
  private static final Log LOG = LogFactory.getLog(TaskHeartbeatHandler.class);
  
  //thread which runs periodically to see the last time since a heartbeat is
  //received from a task.
  private Thread lostTaskCheckerThread;
  private volatile boolean stopped;
  private int taskTimeOut = 5 * 60 * 1000;// 5 mins
  private int taskTimeOutCheckInterval = 30 * 1000; // 30 seconds.

  private final EventHandler eventHandler;
  private final Clock clock;
  
  private ConcurrentMap<TaskAttemptId, ReportTime> runningAttempts;

  public TaskHeartbeatHandler(EventHandler eventHandler, Clock clock,
      int numThreads) {
    super("TaskHeartbeatHandler");
    this.eventHandler = eventHandler;
    this.clock = clock;
    runningAttempts =
      new ConcurrentHashMap<TaskAttemptId, ReportTime>(16, 0.75f, numThreads);
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    taskTimeOut = conf.getInt(MRJobConfig.TASK_TIMEOUT, 5 * 60 * 1000);
    taskTimeOutCheckInterval =
        conf.getInt(MRJobConfig.TASK_TIMEOUT_CHECK_INTERVAL_MS, 30 * 1000);
  }

  @Override
  public void start() {
    lostTaskCheckerThread = new Thread(new PingChecker());
    lostTaskCheckerThread.setName("TaskHeartbeatHandler PingChecker");
    lostTaskCheckerThread.start();
    super.start();
  }

  @Override
  public void stop() {
    stopped = true;
    lostTaskCheckerThread.interrupt();
    super.stop();
  }

  public void progressing(TaskAttemptId attemptID) {
  //only put for the registered attempts
    //TODO throw an exception if the task isn't registered.
    ReportTime time = runningAttempts.get(attemptID);
    if(time != null) {
      time.setLastProgress(clock.getTime());
    }
  }

  
  public void register(TaskAttemptId attemptID) {
    runningAttempts.put(attemptID, new ReportTime(clock.getTime()));
  }

  public void unregister(TaskAttemptId attemptID) {
    runningAttempts.remove(attemptID);
  }

  private class PingChecker implements Runnable {

    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        Iterator<Map.Entry<TaskAttemptId, ReportTime>> iterator =
            runningAttempts.entrySet().iterator();

        // avoid calculating current time everytime in loop
        long currentTime = clock.getTime();

        while (iterator.hasNext()) {
          Map.Entry<TaskAttemptId, ReportTime> entry = iterator.next();
          boolean taskTimedOut = (taskTimeOut > 0) && 
              (currentTime > (entry.getValue().getLastProgress() + taskTimeOut));
           
          if(taskTimedOut) {
            // task is lost, remove from the list and raise lost event
            iterator.remove();
            eventHandler.handle(new TaskAttemptDiagnosticsUpdateEvent(entry
                .getKey(), "AttemptID:" + entry.getKey().toString()
                + " Timed out after " + taskTimeOut / 1000 + " secs"));
            eventHandler.handle(new TaskAttemptEvent(entry.getKey(),
                TaskAttemptEventType.TA_TIMED_OUT));
          }
        }
        try {
          Thread.sleep(taskTimeOutCheckInterval);
        } catch (InterruptedException e) {
          LOG.info("TaskHeartbeatHandler thread interrupted");
          break;
        }
      }
    }
  }

}
