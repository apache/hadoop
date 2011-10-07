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
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.QueueProcessingStatistics.State;

import org.junit.Before;
import org.junit.Test;


/**
 * Standalone unit tests for QueueProcessingStatistics
 */
public class TestQueueProcessingStatistics extends junit.framework.TestCase {
  
  public static final Log testLog = LogFactory.getLog(
      TestQueueProcessingStatistics.class.getName() + ".testLog");
  
  private UnitQueueProcessingStats qStats = new UnitQueueProcessingStats();

  public enum ExpectedLogResult {NONE, EXPECT_END_FIRST_CYCLE, 
    EXPECT_END_SOLE_CYCLE, EXPECT_END_LAST_CYCLE, 
    EXPECT_ERROR, EXPECT_ERROR_WITH_STATUS,}
  
  private static final long DEFAULT_CYCLE_DURATION = 25;
  private static final long DEFAULT_CYCLE_DELAY = 10;
  
  //minimum expected duration of each cycle must be >= 0 msec, 
  //ignored if zero.
  private long cycleDuration = DEFAULT_CYCLE_DURATION; 
  //minimum expected rest time after each cycle must be >= 0 msec, 
  //ignored if zero.
  private long cycleDelay = DEFAULT_CYCLE_DELAY;

  @Before
  public void initialize() {
    qStats.initialize();
    cycleDuration = DEFAULT_CYCLE_DURATION;
    cycleDelay = DEFAULT_CYCLE_DELAY;
    assertExpectedValues(false, State.BEGIN_COLLECTING, 0, 0);
  }

  /**
   * Does a set of asserts, appropriate to the current state of qStats.
   *                        
   * Note that any or all of cycleDuration, cycleDelay, and workItemCount 
   * may be zero, which restricts the assertions we can make about the growth
   * of the qStats values as cycleCount increases.
   * 
   * @param inCycle - true if believed to be currently in the middle of a cycle,
   *                  false if believed to have ended the most recent cycle.
   * @param state - expected state (ignored if null)
   * @param workItemCount - expected count (ignored if null)
   * @param cycleCount - expected count (ignored if null)
   */
  public void assertExpectedValues(
      boolean inCycle, QueueProcessingStatistics.State state, 
      Integer workItemCount, Integer cycleCount) {
    //Check implicit arguments
    assertTrue(cycleDuration >= 0L);
    assertTrue(cycleDelay >= 0L);
    
    //Asserts based on expected values
    if (state != null) 
      assertEquals(failMsg(), state, qStats.state);
    if (workItemCount != null) 
      assertEquals(failMsg(), (long) workItemCount, qStats.workItemCount);
    if (cycleCount != null)
      assertEquals(failMsg(), (int) cycleCount, qStats.cycleCount);
    
    //Asserts based on general principles
    assertTrue(failMsg(), qStats.startTime >= 0);
    if (qStats.state != State.BEGIN_COLLECTING) {
      /* The following timing calculations don't work in many 
       * automated test environments (e.g., on heavily loaded servers with
       * very unreliable "sleep()" durations), but are useful during 
       * development work.  Commenting them out but leaving them here 
       * for dev use.
       */
      /*
      assertAlmostEquals(failMsg() + " inCycle=" + inCycle, 
          qStats.startTimeCurrentCycle, 
          qStats.startTime 
          + (cycleDuration + cycleDelay) 
            * (qStats.cycleCount - (inCycle ? 0 : 1)));
      assertAlmostEquals(failMsg(), 
          qStats.processDuration, cycleDuration * qStats.cycleCount);
      assertAlmostEquals(failMsg(), 
          qStats.clockDuration, 
          qStats.processDuration 
          + cycleDelay * (qStats.cycleCount - (qStats.cycleCount > 0 ? 1 : 0)));
       */
    }
    assertTrue(failMsg(), qStats.workItemCount >= 0);
    assertTrue(failMsg(), qStats.cycleCount >= 0);
   
    //Asserts based on state machine State.
    switch (qStats.state) {
    case BEGIN_COLLECTING:
      assertFalse(failMsg(), inCycle);
      assertEquals(failMsg(), 0, qStats.startTime);
      assertEquals(failMsg(), 0, qStats.startTimeCurrentCycle);
      assertEquals(failMsg(), 0, qStats.processDuration);
      assertEquals(failMsg(), 0, qStats.clockDuration);
      assertEquals(failMsg(), 0, qStats.workItemCount);
      assertEquals(failMsg(), 0, qStats.cycleCount);
      break;
    case IN_FIRST_CYCLE:
    case IN_SOLE_CYCLE:
      assertTrue(failMsg(), inCycle);
      assertTrue(failMsg(), qStats.startTime > 0);
      assertEquals(failMsg(), qStats.startTime, qStats.startTimeCurrentCycle);
      assertEquals(failMsg(), 0, qStats.processDuration);
      assertEquals(failMsg(), 0, qStats.clockDuration);
      assertEquals(failMsg(), 0, qStats.workItemCount);
      assertEquals(failMsg(), 0, qStats.cycleCount);
      break;
    case DONE_FIRST_CYCLE:
      //Can't make any assertions about "inCycle".
      //For other qStats values, the general principles are the strongest
      //assertions that can be made.
      assertTrue(failMsg(), qStats.startTime > 0);
      assertTrue(failMsg(), qStats.cycleCount > 0);
      break;
    case IN_LAST_CYCLE:
      assertTrue(failMsg(), inCycle);
      assertTrue(failMsg(), qStats.startTime > 0);
      assertTrue(failMsg(), qStats.cycleCount > 0);
     break;
    case DONE_COLLECTING:
      assertFalse(failMsg(), inCycle);
      assertTrue(failMsg(), qStats.startTime > 0);
      assertTrue(failMsg(), qStats.cycleCount > 0);
      break;
    default:
      fail(failMsg() + " Reached unallowed state");
      break;
    }
  }
  
  private String failMsg() {
    return "State=" + qStats.state + " cycleCount=" + qStats.cycleCount;
  }
  
  /**
   * The cycleDuration and cycleDelay are simulated by sleeping for those
   * numbers of milliseconds.  Since sleep() is inexact, we'll assume it may
   * vary by +/- 1 msec per call.  Since there are two calls per
   * virtual cycle, the potential error is twice that.  And we add 1 to
   * account for sheer paranoia and the possibility of two consecutive 
   * calls to "now()" occurring across a tick boundary.
   * We'll use values > 10 for cycleDuration and cycleDelay,
   * so the error isn't huge, percentage-wise.
   * 
   * @return - whether the difference between inputs a and b is within
   * that expected error
   */
  private boolean almostEquals(long a, long b) {
    long diff = a - b;
    if (diff < 0) diff = -diff;
    return diff < 2 * (qStats.cycleCount + 1);
  }
  
  private void assertAlmostEquals(long a, long b) {
    if (!almostEquals(a, b)) 
      fail("Failed almostEquals test: " + a + ", " + b);
  }

  private void assertAlmostEquals(String msg, long a, long b) {
    if (!almostEquals(a, b)) 
      fail(msg + "; Failed almostEquals test: " + a + ", " + b);
  }

  /*
   * Concrete subclasses of {@link QueueProcessingStatistics} for unit test
   */
 private class UnitQueueProcessingStats 
 extends QueueProcessingStatistics {
   public boolean triggerPreDetectLastCycle = false;
   public boolean triggerPostDetectLastCycle = false;
   public ExpectedLogResult expectedLogResult = ExpectedLogResult.NONE;
   
   UnitQueueProcessingStats() {
     super("UnitTestQueue", "blocks", testLog);
   }
   
   @Override
   void initialize() {
     super.initialize();
     triggerPreDetectLastCycle = false;
     triggerPostDetectLastCycle = false;
     expectedLogResult = ExpectedLogResult.NONE;
   }
   
   // @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem.QueueProcessingStatistics#preCheckIsLastCycle(int)
   @Override
   public boolean preCheckIsLastCycle(int maxWorkToProcess) {
     return triggerPreDetectLastCycle;
   }

   // @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem.QueueProcessingStatistics#postCheckIsLastCycle(int)
   @Override
   public boolean postCheckIsLastCycle(int workFound) {
     return triggerPostDetectLastCycle;
   }

   // @see org.apache.hadoop.util.QueueProcessingStatistics#logEndFirstCycle()
   @Override
   void logEndFirstCycle() {
     assertTrue(expectedLogResult == ExpectedLogResult.EXPECT_END_FIRST_CYCLE
         || expectedLogResult == ExpectedLogResult.EXPECT_END_SOLE_CYCLE);
     super.logEndFirstCycle();
   }

   // @see org.apache.hadoop.util.QueueProcessingStatistics#logEndLastCycle()
   @Override
   void logEndLastCycle() {
     assertTrue(expectedLogResult == ExpectedLogResult.EXPECT_END_LAST_CYCLE
         || expectedLogResult == ExpectedLogResult.EXPECT_END_SOLE_CYCLE);
     super.logEndLastCycle();
   }

   // @see org.apache.hadoop.util.QueueProcessingStatistics#logError(String)
   @Override
   void logError(String msg) {
     assertEquals(ExpectedLogResult.EXPECT_ERROR, expectedLogResult);
     super.logError(msg);
   }

   // @see org.apache.hadoop.util.QueueProcessingStatistics#logErrorWithStats(String)
   @Override
   void logErrorWithStats(String msg) {
     assertEquals(ExpectedLogResult.EXPECT_ERROR_WITH_STATUS, expectedLogResult);
     super.logErrorWithStats(msg);
   }
 }
 
 /*
  * Simulate doing a cycle of work
  * @return - whatever was passed in
  */
 private int simulateWork(int work) throws InterruptedException {
   Thread.sleep(DEFAULT_CYCLE_DURATION);
   return work;
 }
 
 /*
  * Simulate the inter-cycle delay by ... delaying!
  */
 private void simulateIntercycleDelay() throws InterruptedException {
   Thread.sleep(DEFAULT_CYCLE_DELAY);
   return;
 }

 @Test
 public void testSingleCyclePreDetect() throws InterruptedException {
   int workToDo = 8;
   int maxWorkPerCycle = 10;
   int workDone = 0;
   
   qStats.checkRestart();
   assertExpectedValues(false, State.BEGIN_COLLECTING, 0, 0);
   
   qStats.triggerPreDetectLastCycle = true;
   qStats.startCycle(workToDo);
   assertExpectedValues(true, State.IN_SOLE_CYCLE, 0, 0);

   workDone = simulateWork(workToDo);
   qStats.expectedLogResult = ExpectedLogResult.EXPECT_END_SOLE_CYCLE;
   qStats.endCycle(workDone);   
   assertExpectedValues(false, State.DONE_COLLECTING, 8, 1);
 }
 
 @Test
 public void testSingleCyclePostDetect() throws InterruptedException {
   int workToDo = 8;
   int maxWorkPerCycle = 10;
   int workDone = 0;
   
   qStats.checkRestart();
   assertExpectedValues(false, State.BEGIN_COLLECTING, 0, 0);
   
   qStats.startCycle(maxWorkPerCycle);
   assertExpectedValues(true, State.IN_FIRST_CYCLE, 0, 0);

   workDone = simulateWork(workToDo);
   qStats.triggerPostDetectLastCycle = true;
   qStats.expectedLogResult = ExpectedLogResult.EXPECT_END_SOLE_CYCLE;
   qStats.endCycle(workDone);   
   assertExpectedValues(false, State.DONE_COLLECTING, 8, 1);
 }
 
 @Test
 public void testMultiCyclePreDetect() throws InterruptedException {
   int workToDo = 28;
   int maxWorkPerCycle = 10;
   int workFound = 0;
   int workDone = 0;
   
   qStats.checkRestart();
   assertExpectedValues(false, State.BEGIN_COLLECTING, 0, 0);
   
   qStats.startCycle(maxWorkPerCycle);
   assertExpectedValues(true, State.IN_FIRST_CYCLE, workDone, 0);
   workFound = simulateWork(maxWorkPerCycle);
   workDone += workFound;
   workToDo -= workFound;
   qStats.expectedLogResult = ExpectedLogResult.EXPECT_END_FIRST_CYCLE;
   qStats.endCycle(workFound);   
   assertExpectedValues(false, State.DONE_FIRST_CYCLE, 10, 1);
   qStats.expectedLogResult = ExpectedLogResult.NONE;
   simulateIntercycleDelay();
   
   qStats.startCycle(maxWorkPerCycle);
   assertExpectedValues(true, State.DONE_FIRST_CYCLE, workDone, 1);
   workFound = simulateWork(maxWorkPerCycle);
   workDone += workFound;
   workToDo -= workFound;
   qStats.endCycle(workFound);   
   assertExpectedValues(false, State.DONE_FIRST_CYCLE, 20, 2);
   simulateIntercycleDelay();

   qStats.triggerPreDetectLastCycle = true;
   qStats.startCycle(maxWorkPerCycle);
   assertExpectedValues(true, State.IN_LAST_CYCLE, workDone, 2);
   workFound = simulateWork(workToDo);
   workDone += workFound;
   workToDo -= workFound;
   qStats.expectedLogResult = ExpectedLogResult.EXPECT_END_LAST_CYCLE;
   qStats.endCycle(workFound);   
   assertExpectedValues(false, State.DONE_COLLECTING, 28, 3);
}
 
 @Test
 public void testMultiCyclePostDetect() throws InterruptedException {
   int workToDo = 28;
   int maxWorkPerCycle = 10;
   int workFound = 0;
   int workDone = 0;
   
   qStats.checkRestart();
   assertExpectedValues(false, State.BEGIN_COLLECTING, 0, 0);
   
   qStats.startCycle(maxWorkPerCycle);
   assertExpectedValues(true, State.IN_FIRST_CYCLE, workDone, 0);
   workFound = simulateWork(maxWorkPerCycle);
   workDone += workFound;
   workToDo -= workFound;
   qStats.expectedLogResult = ExpectedLogResult.EXPECT_END_FIRST_CYCLE;
   qStats.endCycle(workFound);   
   assertExpectedValues(false, State.DONE_FIRST_CYCLE, 10, 1);
   qStats.expectedLogResult = ExpectedLogResult.NONE;
   simulateIntercycleDelay();

   qStats.startCycle(maxWorkPerCycle);
   assertExpectedValues(true, State.DONE_FIRST_CYCLE, workDone, 1);
   workFound = simulateWork(maxWorkPerCycle);
   workDone += workFound;
   workToDo -= workFound;
   qStats.endCycle(workFound);   
   assertExpectedValues(false, State.DONE_FIRST_CYCLE, 20, 2);
   simulateIntercycleDelay();

   qStats.startCycle(maxWorkPerCycle);
   assertExpectedValues(true, State.DONE_FIRST_CYCLE, workDone, 2);
   workFound = simulateWork(workToDo);
   workDone += workFound;
   workToDo -= workFound;
   qStats.triggerPostDetectLastCycle = true;
   qStats.expectedLogResult = ExpectedLogResult.EXPECT_END_LAST_CYCLE;
   qStats.endCycle(workFound);   
   assertExpectedValues(false, State.DONE_COLLECTING, 28, 3);
}
 
 @Test
 public void testRestartIncycle() throws InterruptedException {
   int workToDo = 28;
   int maxWorkPerCycle = 10;
   int workFound = 0;
   int workDone = 0;
   
   qStats.checkRestart();
   assertExpectedValues(false, State.BEGIN_COLLECTING, 0, 0);
   
   qStats.startCycle(maxWorkPerCycle);
   assertExpectedValues(true, State.IN_FIRST_CYCLE, workDone, 0);
   workFound = simulateWork(maxWorkPerCycle);
   workDone += workFound;
   workToDo -= workFound;
   qStats.expectedLogResult = ExpectedLogResult.EXPECT_END_FIRST_CYCLE;
   qStats.endCycle(workFound);   
   assertExpectedValues(false, State.DONE_FIRST_CYCLE, 10, 1);
   
   qStats.expectedLogResult = ExpectedLogResult.EXPECT_ERROR_WITH_STATUS;
   qStats.checkRestart();
   assertExpectedValues(false, State.BEGIN_COLLECTING, 0, 0);
 }
 
 @Test
 public void testRestartAfter() throws InterruptedException {
   testSingleCyclePostDetect();
   qStats.expectedLogResult = ExpectedLogResult.EXPECT_ERROR;
   qStats.checkRestart();
   assertExpectedValues(false, State.BEGIN_COLLECTING, 0, 0);   
 } 
}
