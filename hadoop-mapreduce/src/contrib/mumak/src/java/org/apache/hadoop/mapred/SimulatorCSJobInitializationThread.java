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

import java.io.IOException;
import java.util.Collections;
import java.util.List;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobInitializationPoller.JobInitializationThread;

public class SimulatorCSJobInitializationThread implements SimulatorEventListener {

  long lastCalled;
  CapacityTaskScheduler taskScheduler;
  JobInitializationPoller jobPoller;
  private final String queue;
  final long sleepInterval;
  /** The log object to send our messages to; only used for debugging. */
  private static final Log LOG = LogFactory.getLog(SimulatorCSJobInitializationThread.class);

  public SimulatorCSJobInitializationThread(TaskScheduler taskScheduler, 
      String queue) {
    this.taskScheduler = (CapacityTaskScheduler) taskScheduler;
    jobPoller = this.taskScheduler.getInitializationPoller(); 
    sleepInterval = jobPoller.getSleepInterval();
    this.queue = queue;
  }

  @Override
  public List<SimulatorEvent> accept(SimulatorEvent event) throws IOException {

    SimulatorThreadWakeUpEvent e;
    if(event instanceof SimulatorThreadWakeUpEvent) {
      e = (SimulatorThreadWakeUpEvent) event;
    }
    else {
      throw new IOException("Received an unexpected type of event in " + 
      "SimThrdCapSchedJobInit");
    }
    jobPoller.cleanUpInitializedJobsList();
    jobPoller.selectJobsToInitialize();
    JobInitializationThread thread = 
      jobPoller.getThreadsToQueueMap().get(this.queue);
    thread.initializeJobs();   	
    lastCalled = e.getTimeStamp();
    List<SimulatorEvent> returnEvents = 
      Collections.<SimulatorEvent>singletonList(
          new SimulatorThreadWakeUpEvent(this,
              lastCalled + sleepInterval));
    return returnEvents;
  }

  @Override
  public List<SimulatorEvent> init(long when) throws IOException {

    return Collections.<SimulatorEvent>singletonList(
        new SimulatorThreadWakeUpEvent(this,
            when + sleepInterval));
  }

}
