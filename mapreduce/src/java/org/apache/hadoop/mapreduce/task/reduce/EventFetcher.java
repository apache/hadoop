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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.MapTaskCompletionEventsUpdate;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.TaskAttemptID;

class EventFetcher<K,V> extends Thread {
  private static final long SLEEP_TIME = 1000;
  private static final int MAX_EVENTS_TO_FETCH = 10000;
  private static final int MAX_RETRIES = 10;
  private static final int RETRY_PERIOD = 5000;
  private static final Log LOG = LogFactory.getLog(EventFetcher.class);

  private final TaskAttemptID reduce;
  private final TaskUmbilicalProtocol umbilical;
  private final ShuffleScheduler<K,V> scheduler;
  private int fromEventId = 0;
  private ExceptionReporter exceptionReporter = null;
  
  private int maxMapRuntime = 0;
  
  public EventFetcher(TaskAttemptID reduce,
                      TaskUmbilicalProtocol umbilical,
                      ShuffleScheduler<K,V> scheduler,
                      ExceptionReporter reporter) {
    setName("EventFetcher for fetching Map Completion Events");
    setDaemon(true);    
    this.reduce = reduce;
    this.umbilical = umbilical;
    this.scheduler = scheduler;
    exceptionReporter = reporter;
  }

  @Override
  public void run() {
    int failures = 0;
    LOG.info(reduce + " Thread started: " + getName());
    
    try {
      while (true) {
        try {
          int numNewMaps = getMapCompletionEvents();
          failures = 0;
          if (numNewMaps > 0) {
            LOG.info(reduce + ": " + "Got " + numNewMaps + " new map-outputs");
          }
          LOG.debug("GetMapEventsThread about to sleep for " + SLEEP_TIME);
          Thread.sleep(SLEEP_TIME);
        } catch (IOException ie) {
          LOG.info("Exception in getting events", ie);
          // check to see whether to abort
          if (++failures >= MAX_RETRIES) {
            throw new IOException("too many failures downloading events", ie);
          }
          // sleep for a bit
          Thread.sleep(RETRY_PERIOD);
        }
      }
    } catch (InterruptedException e) {
      return;
    } catch (Throwable t) {
      exceptionReporter.reportException(t);
      return;
    }
  }
  
  /** 
   * Queries the {@link TaskTracker} for a set of map-completion events 
   * from a given event ID.
   * @throws IOException
   */  
  private int getMapCompletionEvents() throws IOException {
    
    int numNewMaps = 0;
    
    MapTaskCompletionEventsUpdate update = 
      umbilical.getMapCompletionEvents((org.apache.hadoop.mapred.JobID)
                                       reduce.getJobID(), 
                                       fromEventId, 
                                       MAX_EVENTS_TO_FETCH,
                                       (org.apache.hadoop.mapred.TaskAttemptID)
                                         reduce);
    TaskCompletionEvent events[] = update.getMapTaskCompletionEvents();
    LOG.debug("Got " + events.length + " map completion events from " + 
             fromEventId);
      
    // Check if the reset is required.
    // Since there is no ordering of the task completion events at the 
    // reducer, the only option to sync with the new jobtracker is to reset 
    // the events index
    if (update.shouldReset()) {
      fromEventId = 0;
      scheduler.resetKnownMaps();
    }
    
    // Update the last seen event ID
    fromEventId += events.length;
    
    // Process the TaskCompletionEvents:
    // 1. Save the SUCCEEDED maps in knownOutputs to fetch the outputs.
    // 2. Save the OBSOLETE/FAILED/KILLED maps in obsoleteOutputs to stop 
    //    fetching from those maps.
    // 3. Remove TIPFAILED maps from neededOutputs since we don't need their
    //    outputs at all.
    for (TaskCompletionEvent event : events) {
      switch (event.getTaskStatus()) {
        case SUCCEEDED:
          URI u = getBaseURI(event.getTaskTrackerHttp());
          scheduler.addKnownMapOutput(u.getHost() + ":" + u.getPort(),
                                      u.toString(),
                                      event.getTaskAttemptId());
          numNewMaps ++;
          int duration = event.getTaskRunTime();
          if (duration > maxMapRuntime) {
            maxMapRuntime = duration;
            scheduler.informMaxMapRunTime(maxMapRuntime);
          }
          break;
        case FAILED:
        case KILLED:
        case OBSOLETE:
          scheduler.obsoleteMapOutput(event.getTaskAttemptId());
          LOG.info("Ignoring obsolete output of " + event.getTaskStatus() + 
                   " map-task: '" + event.getTaskAttemptId() + "'");
          break;
        case TIPFAILED:
          scheduler.tipFailed(event.getTaskAttemptId().getTaskID());
          LOG.info("Ignoring output of failed map TIP: '" +  
               event.getTaskAttemptId() + "'");
          break;
      }
    }
    return numNewMaps;
  }
  
  private URI getBaseURI(String url) {
    StringBuffer baseUrl = new StringBuffer(url);
    if (!url.endsWith("/")) {
      baseUrl.append("/");
    }
    baseUrl.append("mapOutput?job=");
    baseUrl.append(reduce.getJobID());
    baseUrl.append("&reduce=");
    baseUrl.append(reduce.getTaskID().getId());
    baseUrl.append("&map=");
    URI u = URI.create(baseUrl.toString());
    return u;
  }
}