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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.Progress;
import org.junit.Assert;
import org.junit.Test;

public class TestShuffleScheduler {

  @SuppressWarnings("rawtypes")
  @Test
  public void testTipFailed() throws Exception {
    JobConf job = new JobConf();
    job.setNumMapTasks(2);

    TaskStatus status = new TaskStatus() {
      @Override
      public boolean getIsMap() {
        return false;
      }

      @Override
      public void addFetchFailedMap(TaskAttemptID mapTaskId) {
      }
    };
    Progress progress = new Progress();

    TaskAttemptID reduceId = new TaskAttemptID("314159", 0, TaskType.REDUCE,
        0, 0);
    ShuffleSchedulerImpl scheduler = new ShuffleSchedulerImpl(job, status,
        reduceId, null, progress, null, null, null);

    JobID jobId = new JobID();
    TaskID taskId1 = new TaskID(jobId, TaskType.REDUCE, 1);
    scheduler.tipFailed(taskId1);

    Assert.assertEquals("Progress should be 0.5", 0.5f, progress.getProgress(),
        0.0f);
    Assert.assertFalse(scheduler.waitUntilDone(1));

    TaskID taskId0 = new TaskID(jobId, TaskType.REDUCE, 0);
    scheduler.tipFailed(taskId0);
    Assert.assertEquals("Progress should be 1.0", 1.0f, progress.getProgress(),
        0.0f);
    Assert.assertTrue(scheduler.waitUntilDone(1));
  }
}
