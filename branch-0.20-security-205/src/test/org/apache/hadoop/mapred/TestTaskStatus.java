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

import junit.framework.TestCase;

public class TestTaskStatus extends TestCase {

  public void testMapTaskStatusStartAndFinishTimes() {
    checkTaskStatues(true);
  }

  public void testReduceTaskStatusStartAndFinishTimes() {
    checkTaskStatues(false);
  }

  /**
   * Private utility method which ensures uniform testing of newly created
   * TaskStatus object.
   * 
   * @param isMap
   *          true to test map task status, false for reduce.
   */
  private void checkTaskStatues(boolean isMap) {

    TaskStatus status = null;
    if (isMap) {
      status = new MapTaskStatus();
    } else {
      status = new ReduceTaskStatus();
    }
    long currentTime = System.currentTimeMillis();
    // first try to set the finish time before
    // start time is set.
    status.setFinishTime(currentTime);
    assertEquals("Finish time of the task status set without start time", 0,
        status.getFinishTime());
    // Now set the start time to right time.
    status.setStartTime(currentTime);
    assertEquals("Start time of the task status not set correctly.",
        currentTime, status.getStartTime());
    // try setting wrong start time to task status.
    long wrongTime = -1;
    status.setStartTime(wrongTime);
    assertEquals(
        "Start time of the task status is set to wrong negative value",
        currentTime, status.getStartTime());
    // finally try setting wrong finish time i.e. negative value.
    status.setFinishTime(wrongTime);
    assertEquals("Finish time of task status is set to wrong negative value",
        0, status.getFinishTime());
    status.setFinishTime(currentTime);
    assertEquals("Finish time of the task status not set correctly.",
        currentTime, status.getFinishTime());
  }
}
