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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
public class TaskHeartbeatHandler extends AbstractService {

  private static final Log LOG = LogFactory.getLog(TaskHeartbeatHandler.class);

  //thread which runs periodically to see the last time since a heartbeat is
  //received from a task.
  private Thread lostTaskCheckerThread;
  private volatile boolean stopped;
  private int taskTimeOut = 5*60*1000;//5 mins

  private final EventHandler eventHandler;
  private final Clock clock;

  private Map<TaskAttemptId, Long> runningAttempts 
    = new HashMap<TaskAttemptId, Long>();

  public TaskHeartbeatHandler(EventHandler eventHandler, Clock clock) {
    super("TaskHeartbeatHandler");
    this.eventHandler = eventHandler;
    this.clock = clock;
  }

  @Override
  public void init(Configuration conf) {
   super.init(conf);
   taskTimeOut = conf.getInt("mapreduce.task.timeout", 5*60*1000);
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

  public synchronized void receivedPing(TaskAttemptId attemptID) {
    //only put for the registered attempts
    if (runningAttempts.containsKey(attemptID)) {
      runningAttempts.put(attemptID, clock.getTime());
    }
  }

  public synchronized void register(TaskAttemptId attemptID) {
    runningAttempts.put(attemptID, clock.getTime());
  }

  public synchronized void unregister(TaskAttemptId attemptID) {
    runningAttempts.remove(attemptID);
  }

  private class PingChecker implements Runnable {

    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        synchronized (TaskHeartbeatHandler.this) {
          Iterator<Map.Entry<TaskAttemptId, Long>> iterator = 
            runningAttempts.entrySet().iterator();

          //avoid calculating current time everytime in loop
          long currentTime = clock.getTime();

          while (iterator.hasNext()) {
            Map.Entry<TaskAttemptId, Long> entry = iterator.next();
            if (currentTime > entry.getValue() + taskTimeOut) {
              //task is lost, remove from the list and raise lost event
              iterator.remove();
              eventHandler.handle(
                  new TaskAttemptDiagnosticsUpdateEvent(entry.getKey(),
                      "AttemptID:" + entry.getKey().toString() + 
                      " Timed out after " + taskTimeOut/1000 + " secs"));
              eventHandler.handle(new TaskAttemptEvent(entry
                  .getKey(), TaskAttemptEventType.TA_TIMED_OUT));
            }
          }
        }
        try {
          Thread.sleep(taskTimeOut);
        } catch (InterruptedException e) {
          LOG.info("TaskHeartbeatHandler thread interrupted");
          break;
        }
      }
    }
    
  }

}
