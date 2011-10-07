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

package org.apache.hadoop.util;

import org.apache.commons.logging.Log;

/**
 * Hadoop has several work queues, such as 
 * {@link org.apache.hadoop.hdfs.server.namenode.FSNamesystem#neededReplications}
 * With a properly throttled queue, a worker thread cycles repeatedly,
 * doing a chunk of work each cycle then resting a bit, until the queue is
 * empty.  This class is intended to collect statistics about the behavior of
 * such queues and consumers.  It reports the amount of work done and 
 * how long it took, for the first cycle after collection starts, and for 
 * the total number of cycles needed to flush the queue.  We use a state 
 * machine to detect when the queue has been flushed and then we log the 
 * stats; see {@link State} for enumeration of the states and their meanings.
 */
public abstract class QueueProcessingStatistics {
  //All member variables and methods that would normally be access "private"
  //are instead package-private so we can subclass for unit testing.
  State state = State.BEGIN_COLLECTING;
  long startTimeCurrentCycle;
  long startTime;
  long processDuration;
  long clockDuration;
  long workItemCount;
  int  cycleCount;
  
  String queueName;
  String workItemsName;
  Log LOG;

  /**
   * This enum provides the "states" of a state machine for 
   * {@link QueueProcessingStatistics}.
   * The meanings of the states are: <ul>
   * <li> BEGIN_COLLECTING - Ready to begin.
   * <li> IN_FIRST_CYCLE - Started the first cycle.
   * <li> IN_SOLE_CYCLE - Still in first cycle, but already know there will be 
   *                      no further cycles because this one will complete all
   *                      needed work. When done, will go straight to
   *                      "DONE_COLLECTING".
   * <li> DONE_FIRST_CYCLE - Done first cycle, doing subsequent cycles.
   * <li> IN_LAST_CYCLE - Started the last cycle.
   * <li> DONE_COLLECTING - Done with last cycle, finishing up.
   * </ul>
   */
  public enum State {
    BEGIN_COLLECTING,
    IN_FIRST_CYCLE,
    IN_SOLE_CYCLE,
    DONE_FIRST_CYCLE,
    IN_LAST_CYCLE,
    DONE_COLLECTING,
  }

  /**
   * @param queueName - Human-readable name of the queue being monitored, 
   *                    used as first word in the log messages.
   * @param workItemsName - what kind of work items are being managed
   *                    on the queue?  A plural word is best here, for logging.
   * @param logObject - What log do you want the log messages to be sent to?
   */
  public QueueProcessingStatistics(String queueName, String workItemsName,
      Log logObject) {
    this.queueName = queueName;
    this.workItemsName = workItemsName;
    this.LOG = logObject;
  }
  
  public void startCycle(int maxWorkToProcess) {
    //only collect stats for one complete flush of the queue
    if (state == State.DONE_COLLECTING) return;
    
    //regardless of state, record the start of this cycle
    startTimeCurrentCycle = now();
    boolean preDetectLastCycle = preCheckIsLastCycle(maxWorkToProcess);
    
    switch (state) {
    case BEGIN_COLLECTING:
      startTime = startTimeCurrentCycle;
      state = preDetectLastCycle ? State.IN_SOLE_CYCLE : State.IN_FIRST_CYCLE;
      break;
    default:
      if (preDetectLastCycle)
        state = State.IN_LAST_CYCLE;
      break;
    }
  }
  
  public void endCycle(int workFound) {
    //only collect stats for first pass through the queue
    if (state == State.DONE_COLLECTING) return;
    
    //regardless of state, record the end of this cycle
    //and accumulate the cycle's stats
    long endTimeCurrentCycle = now();
    processDuration += endTimeCurrentCycle - startTimeCurrentCycle;
    clockDuration = endTimeCurrentCycle - startTime;
    workItemCount += workFound;
    cycleCount++;
    boolean postDetectLastCycle = postCheckIsLastCycle(workFound);
    
    switch (state) {
    case BEGIN_COLLECTING:
      logError("endCycle() called before startCycle(), "
          + "exiting stats collection");
      state = State.DONE_COLLECTING;
      break;
    case IN_FIRST_CYCLE:
      if (postDetectLastCycle) {
        state = State.IN_SOLE_CYCLE;
        //and fall through
      } else {
        logEndFirstCycle();
        state = State.DONE_FIRST_CYCLE;
        break;
      }
    case IN_SOLE_CYCLE:
      logEndFirstCycle();
      logEndLastCycle();
      state = State.DONE_COLLECTING;
      break;
    case DONE_FIRST_CYCLE:
      if (postDetectLastCycle) {
        state = State.IN_LAST_CYCLE;
        //and fall through
      } else {
        break;
      }
    case IN_LAST_CYCLE:
      logEndLastCycle();
      state = State.DONE_COLLECTING;
      break;
    default:
      logError("unallowed state reached, exiting stats collection");
      state = State.DONE_COLLECTING;
      break;
    }
  }
  
  public void checkRestart() {
    switch (state) {
    case BEGIN_COLLECTING:
      //situation normal
      return;
    case DONE_COLLECTING:
      logError("Restarted stats collection after completion of first "
          + "queue flush.");
      initialize();
      break;
    default:
      //for all other cases, we are in the middle of stats collection,
      //so output the stats collected so far before re-initializing
      logErrorWithStats("Restarted stats collection before completion of "
          + "first queue flush.");
      initialize();
      break;        
    }
  }

  void initialize() {
    state = State.BEGIN_COLLECTING;
    startTimeCurrentCycle = 0;
    startTime = 0;
    processDuration = 0;
    clockDuration = 0;
    workItemCount = 0;
    cycleCount = 0;
  }

  /**
   * The termination condition is to identify the last cycle that will 
   * empty the queue.  Two abstract APIs are called: {@code preCheckIsLastCycle}
   * is called at the beginning of each cycle, and 
   * {@link #postCheckIsLastCycle} is called at the end of each cycle. 
   * At least one of them must correctly provide the termination 
   * condition. The other may always return 'false'.  If either of them 
   * returns 'true' in a given cycle, then at the end of that cycle the 
   * stats will be output to log, and stats collection will end.
   * 
   * @param maxWorkToProcess - if this number is greater than the amount
   *            of work remaining at the start of a cycle, then it will
   *            be the last cycle.
   * @return - true if last cycle detected, else false
   */
  public abstract boolean preCheckIsLastCycle(int maxWorkToProcess);

  /**
   * See {@link #preCheckIsLastCycle}.
   * @param workFound - may not be useful
   * @return - true if remaining work is zero at end of cycle,
   *           else false
   */
  public abstract boolean postCheckIsLastCycle(int workFound);
  
  String msgEndFirstCycle() {
    return queueName + " QueueProcessingStatistics: First cycle completed " 
    + workItemCount + " " + workItemsName + " in " + processDuration 
    + " msec";
  }
  
  void logEndFirstCycle() {
    LOG.info(msgEndFirstCycle());
  }
  
  String msgEndLastCycle() {
    return queueName
    + " QueueProcessingStatistics: Queue flush completed " 
    + workItemCount + " " + workItemsName + " in "
    + processDuration + " msec processing time, "
    + clockDuration + " msec clock time, " 
    + cycleCount + " cycles";
  }
  
  void logEndLastCycle() {
    LOG.info(msgEndLastCycle());
  }
  
  String msgError(String msg) {
    return queueName
    + " QueueProcessingStatistics - Error: " + msg;
  }
  
  void logError(String msg) {
    LOG.error(msgError(msg));
  }
  
  String msgErrorWithStats(String msg) {
    return queueName
    + " QueueProcessingStatistics - Error: " + msg 
    + " Completed " + workItemCount + " " + workItemsName + " in "
    + processDuration + " msec processing time, "
    + clockDuration + " msec clock time, " 
    + cycleCount + " cycles";
  }

  void logErrorWithStats(String msg) {
    LOG.error(msgErrorWithStats(msg));
  }
  
  /**
   * Current system time.
   * @return current time in msec.
   */
  static long now() {
    return System.currentTimeMillis();
  }
    
}

