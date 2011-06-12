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
package org.apache.hadoop.mapreduce.jobhistory;

import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;

import junit.framework.TestCase;

/**
 * Test various jobhistory events
 */
public class TestJobHistoryEvents extends TestCase {
  /**
   * Test {@link TaskAttemptStartedEvent} for various task types.
   */
  private static void testAttemptStartedEventForTypes(EventType expected, 
                                                      TaskAttemptID id,
                                                      TaskType[] types) {
    for (TaskType t : types) {
      TaskAttemptStartedEvent tase = 
        new TaskAttemptStartedEvent(id, t, 0L, "", 0);
      assertEquals(expected, tase.getEventType());
    }
  }
  
  /**
   * Test {@link TaskAttemptStartedEvent}.
   */
  public void testTaskAttemptStartedEvent() {
    EventType expected = EventType.MAP_ATTEMPT_STARTED;
    TaskAttemptID fakeId = new TaskAttemptID("1234", 1, TaskType.MAP, 1, 1);
    
    // check the events for job-setup, job-cleanup and map task-types
    testAttemptStartedEventForTypes(expected, fakeId,
                                    new TaskType[] {TaskType.JOB_SETUP, 
                                                    TaskType.JOB_CLEANUP, 
                                                    TaskType.MAP});
    
    expected = EventType.REDUCE_ATTEMPT_STARTED;
    fakeId = new TaskAttemptID("1234", 1, TaskType.REDUCE, 1, 1);
    
    // check the events for job-setup, job-cleanup and reduce task-types
    testAttemptStartedEventForTypes(expected, fakeId,
                                    new TaskType[] {TaskType.JOB_SETUP, 
                                                    TaskType.JOB_CLEANUP, 
                                                    TaskType.REDUCE});
  }
  
  /**
   * Test {@link TaskAttemptUnsuccessfulCompletionEvent} for various task types.
   */
  private static void testFailedKilledEventsForTypes(EventType expected, 
                                                     TaskAttemptID id,
                                                     TaskType[] types,
                                                     String state) {
    for (TaskType t : types) {
      TaskAttemptUnsuccessfulCompletionEvent tauce = 
        new TaskAttemptUnsuccessfulCompletionEvent(id, t, state, 0L, "", "");
      assertEquals(expected, tauce.getEventType());
    }
  }
  
  /**
   * Test {@link TaskAttemptUnsuccessfulCompletionEvent} for killed/failed task.
   */
  public void testTaskAttemptUnsuccessfulCompletionEvent() {
    TaskAttemptID fakeId = new TaskAttemptID("1234", 1, TaskType.MAP, 1, 1);
    
    // check killed events for job-setup, job-cleanup and map task-types
    testFailedKilledEventsForTypes(EventType.MAP_ATTEMPT_KILLED, fakeId,
                                   new TaskType[] {TaskType.JOB_SETUP, 
                                                   TaskType.JOB_CLEANUP, 
                                                   TaskType.MAP},
                                   TaskStatus.State.KILLED.toString());
    // check failed events for job-setup, job-cleanup and map task-types
    testFailedKilledEventsForTypes(EventType.MAP_ATTEMPT_FAILED, fakeId,
                                   new TaskType[] {TaskType.JOB_SETUP, 
                                                   TaskType.JOB_CLEANUP, 
                                                   TaskType.MAP},
                                   TaskStatus.State.FAILED.toString());
    
    fakeId = new TaskAttemptID("1234", 1, TaskType.REDUCE, 1, 1);
    
    // check killed events for job-setup, job-cleanup and reduce task-types
    testFailedKilledEventsForTypes(EventType.REDUCE_ATTEMPT_KILLED, fakeId,
                                   new TaskType[] {TaskType.JOB_SETUP, 
                                                   TaskType.JOB_CLEANUP, 
                                                   TaskType.REDUCE},
                                   TaskStatus.State.KILLED.toString());
    // check failed events for job-setup, job-cleanup and reduce task-types
    testFailedKilledEventsForTypes(EventType.REDUCE_ATTEMPT_FAILED, fakeId,
                                   new TaskType[] {TaskType.JOB_SETUP, 
                                                   TaskType.JOB_CLEANUP, 
                                                   TaskType.REDUCE},
                                   TaskStatus.State.FAILED.toString());
  }
  
  /**
   * Test {@link TaskAttemptFinishedEvent} for various task types.
   */
  private static void testFinishedEventsForTypes(EventType expected, 
                                                 TaskAttemptID id,
                                                 TaskType[] types) {
    for (TaskType t : types) {
      TaskAttemptFinishedEvent tafe = 
        new TaskAttemptFinishedEvent(id, t, 
            TaskStatus.State.SUCCEEDED.toString(), 0L, "", "", new Counters());
      assertEquals(expected, tafe.getEventType());
    }
  }
  
  /**
   * Test {@link TaskAttemptFinishedEvent} for finished task.
   */
  public void testTaskAttemptFinishedEvent() {
    EventType expected = EventType.MAP_ATTEMPT_FINISHED;
    TaskAttemptID fakeId = new TaskAttemptID("1234", 1, TaskType.MAP, 1, 1);
    
    // check the events for job-setup, job-cleanup and map task-types
    testFinishedEventsForTypes(expected, fakeId,
                               new TaskType[] {TaskType.JOB_SETUP, 
                                               TaskType.JOB_CLEANUP, 
                                               TaskType.MAP});
    
    expected = EventType.REDUCE_ATTEMPT_FINISHED;
    fakeId = new TaskAttemptID("1234", 1, TaskType.REDUCE, 1, 1);
    
    // check the events for job-setup, job-cleanup and reduce task-types
    testFinishedEventsForTypes(expected, fakeId,
                               new TaskType[] {TaskType.JOB_SETUP, 
                                               TaskType.JOB_CLEANUP, 
                                               TaskType.REDUCE});
  }
}
