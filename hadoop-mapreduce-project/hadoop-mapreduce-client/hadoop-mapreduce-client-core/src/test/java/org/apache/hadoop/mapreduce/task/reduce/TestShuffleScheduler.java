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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.ShuffleConsumerPlugin;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Task.CombineOutputCollector;
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

  @SuppressWarnings("rawtypes")
  @Test
  public <K, V> void TestAggregatedTransferRate() throws Exception {
    JobConf job = new JobConf();
    job.setNumMapTasks(10);
    //mock creation
    TaskUmbilicalProtocol mockUmbilical = mock(TaskUmbilicalProtocol.class);
    Reporter mockReporter = mock(Reporter.class);
    FileSystem mockFileSystem = mock(FileSystem.class);
    Class<? extends org.apache.hadoop.mapred.Reducer>  combinerClass = job.getCombinerClass();
    @SuppressWarnings("unchecked")  // needed for mock with generic
    CombineOutputCollector<K, V>  mockCombineOutputCollector =
        (CombineOutputCollector<K, V>) mock(CombineOutputCollector.class);
    org.apache.hadoop.mapreduce.TaskAttemptID mockTaskAttemptID =
        mock(org.apache.hadoop.mapreduce.TaskAttemptID.class);
    LocalDirAllocator mockLocalDirAllocator = mock(LocalDirAllocator.class);
    CompressionCodec mockCompressionCodec = mock(CompressionCodec.class);
    Counter mockCounter = mock(Counter.class);
    TaskStatus mockTaskStatus = mock(TaskStatus.class);
    Progress mockProgress = mock(Progress.class);
    MapOutputFile mockMapOutputFile = mock(MapOutputFile.class);
    Task mockTask = mock(Task.class);
    @SuppressWarnings("unchecked")
    MapOutput<K, V> output = mock(MapOutput.class);

    ShuffleConsumerPlugin.Context<K, V> context =
        new ShuffleConsumerPlugin.Context<K, V>(mockTaskAttemptID, job, mockFileSystem,
                                                     mockUmbilical, mockLocalDirAllocator,
                                                     mockReporter, mockCompressionCodec,
                                                     combinerClass, mockCombineOutputCollector,
                                                     mockCounter, mockCounter, mockCounter,
                                                     mockCounter, mockCounter, mockCounter,
                                                     mockTaskStatus, mockProgress, mockProgress,
                                                     mockTask, mockMapOutputFile, null);
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
    ShuffleSchedulerImpl<K, V> scheduler = new ShuffleSchedulerImpl<K, V>(job, status, null,
        null, progress, context.getShuffledMapsCounter(),
        context.getReduceShuffleBytes(), context.getFailedShuffleCounter());
    TaskAttemptID attemptID0 = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
        new JobID("test",0), TaskType.MAP, 0), 0);

    //adding the 1st interval, 40MB from 60s to 100s
    long bytes = (long)40 * 1024 * 1024;
    scheduler.copySucceeded(attemptID0, new MapHost(null, null), bytes, 60000, 100000, output);
    Assert.assertEquals(copyMessage(1, 1, 1), progress.toString());

    TaskAttemptID attemptID1 = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
        new JobID("test",0), TaskType.MAP, 1), 1);

    //adding the 2nd interval before the 1st interval, 50MB from 0s to 50s
    bytes = (long)50 * 1024 * 1024;
    scheduler.copySucceeded(attemptID1, new MapHost(null, null), bytes, 0, 50000, output);
    Assert.assertEquals(copyMessage(2, 1, 1), progress.toString());

    TaskAttemptID attemptID2 = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
        new JobID("test",0), TaskType.MAP, 2), 2);

    //adding the 3rd interval overlapping with the 1st and the 2nd interval
    //110MB from 25s to 80s
    bytes = (long)110 * 1024 * 1024;
    scheduler.copySucceeded(attemptID2, new MapHost(null, null), bytes, 25000, 80000, output);
    Assert.assertEquals(copyMessage(3, 2, 2), progress.toString());

    TaskAttemptID attemptID3 = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
        new JobID("test",0), TaskType.MAP, 3), 3);

    //adding the 4th interval just after the 2nd interval, 100MB from 100s to 300s
    bytes = (long)100 * 1024 * 1024;
    scheduler.copySucceeded(attemptID3, new MapHost(null, null), bytes, 100000, 300000, output);
    Assert.assertEquals(copyMessage(4, 0.5, 1), progress.toString());

    TaskAttemptID attemptID4 = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
        new JobID("test",0), TaskType.MAP, 4), 4);

    //adding the 5th interval between after 4th, 50MB from 350s to 400s
    bytes = (long)50 * 1024 * 1024;
    scheduler.copySucceeded(attemptID4, new MapHost(null, null), bytes, 350000, 400000, output);
    Assert.assertEquals(copyMessage(5, 1, 1), progress.toString());


    TaskAttemptID attemptID5 = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
        new JobID("test",0), TaskType.MAP, 5), 5);
    //adding the 6th interval between after 5th, 50MB from 450s to 500s
    bytes = (long)50 * 1024 * 1024;
    scheduler.copySucceeded(attemptID5, new MapHost(null, null), bytes, 450000, 500000, output);
    Assert.assertEquals(copyMessage(6, 1, 1), progress.toString());

    TaskAttemptID attemptID6 = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
        new JobID("test",0), TaskType.MAP, 6), 6);
    //adding the 7th interval between after 5th and 6th interval, 20MB from 320s to 340s
    bytes = (long)20 * 1024 * 1024;
    scheduler.copySucceeded(attemptID6, new MapHost(null, null), bytes, 320000, 340000, output);
    Assert.assertEquals(copyMessage(7, 1, 1), progress.toString());

    TaskAttemptID attemptID7 = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
        new JobID("test",0), TaskType.MAP, 7), 7);
    //adding the 8th interval overlapping with 4th, 5th, and 7th 30MB from 290s to 350s
    bytes = (long)30 * 1024 * 1024;
    scheduler.copySucceeded(attemptID7, new MapHost(null, null), bytes, 290000, 350000, output);
    Assert.assertEquals(copyMessage(8, 0.5, 1), progress.toString());

    TaskAttemptID attemptID8 = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
        new JobID("test",0), TaskType.MAP, 8), 8);
    //adding the 9th interval overlapping with 5th and 6th, 50MB from 400s to 450s
    bytes = (long)50 * 1024 * 1024;
    scheduler.copySucceeded(attemptID8, new MapHost(null, null), bytes, 400000, 450000, output);
    Assert.assertEquals(copyMessage(9, 1, 1), progress.toString());

    TaskAttemptID attemptID9 = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
        new JobID("test",0), TaskType.MAP, 9), 9);
    //adding the 10th interval overlapping with all intervals, 500MB from 0s to 500s
    bytes = (long)500 * 1024 * 1024;
    scheduler.copySucceeded(attemptID9, new MapHost(null, null), bytes, 0, 500000, output);
    Assert.assertEquals(copyMessage(10, 1, 2), progress.toString());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public <K, V> void TestSucceedAndFailedCopyMap() throws Exception {
    JobConf job = new JobConf();
    job.setNumMapTasks(2);
    //mock creation
    TaskUmbilicalProtocol mockUmbilical = mock(TaskUmbilicalProtocol.class);
    Reporter mockReporter = mock(Reporter.class);
    FileSystem mockFileSystem = mock(FileSystem.class);
    Class<? extends org.apache.hadoop.mapred.Reducer>  combinerClass = job.getCombinerClass();
    @SuppressWarnings("unchecked")  // needed for mock with generic
    CombineOutputCollector<K, V>  mockCombineOutputCollector =
        (CombineOutputCollector<K, V>) mock(CombineOutputCollector.class);
    org.apache.hadoop.mapreduce.TaskAttemptID mockTaskAttemptID =
        mock(org.apache.hadoop.mapreduce.TaskAttemptID.class);
    LocalDirAllocator mockLocalDirAllocator = mock(LocalDirAllocator.class);
    CompressionCodec mockCompressionCodec = mock(CompressionCodec.class);
    Counter mockCounter = mock(Counter.class);
    TaskStatus mockTaskStatus = mock(TaskStatus.class);
    Progress mockProgress = mock(Progress.class);
    MapOutputFile mockMapOutputFile = mock(MapOutputFile.class);
    Task mockTask = mock(Task.class);
    @SuppressWarnings("unchecked")
    MapOutput<K, V> output = mock(MapOutput.class);

    ShuffleConsumerPlugin.Context<K, V> context =
        new ShuffleConsumerPlugin.Context<K, V>(
            mockTaskAttemptID, job, mockFileSystem,
            mockUmbilical, mockLocalDirAllocator,
            mockReporter, mockCompressionCodec,
            combinerClass, mockCombineOutputCollector,
            mockCounter, mockCounter, mockCounter,
            mockCounter, mockCounter, mockCounter,
            mockTaskStatus, mockProgress, mockProgress,
            mockTask, mockMapOutputFile, null);
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
    ShuffleSchedulerImpl<K, V> scheduler = new ShuffleSchedulerImpl<K, V>(job,
        status, null, null, progress, context.getShuffledMapsCounter(),
        context.getReduceShuffleBytes(), context.getFailedShuffleCounter());

    MapHost host1 = new MapHost("host1", null);
    TaskAttemptID failedAttemptID = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
        new JobID("test",0), TaskType.MAP, 0), 0);

    TaskAttemptID succeedAttemptID = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
        new JobID("test",0), TaskType.MAP, 1), 1);

    // handle output fetch failure for failedAttemptID, part I
    scheduler.hostFailed(host1.getHostName());

    // handle output fetch succeed for succeedAttemptID
    long bytes = (long)500 * 1024 * 1024;
    scheduler.copySucceeded(succeedAttemptID, host1, bytes, 0, 500000, output);

    // handle output fetch failure for failedAttemptID, part II
    // for MAPREDUCE-6361: verify no NPE exception get thrown out
    scheduler.copyFailed(failedAttemptID, host1, true, false);
  }

  @Test
  public <K, V> void testDuplicateCopySucceeded() throws Exception {
    JobConf job = new JobConf();
    job.setNumMapTasks(2);
    //mock creation
    TaskUmbilicalProtocol mockUmbilical = mock(TaskUmbilicalProtocol.class);
    Reporter mockReporter = mock(Reporter.class);
    FileSystem mockFileSystem = mock(FileSystem.class);
    Class<? extends org.apache.hadoop.mapred.Reducer>  combinerClass =
        job.getCombinerClass();
    @SuppressWarnings("unchecked")  // needed for mock with generic
        CombineOutputCollector<K, V>  mockCombineOutputCollector =
        (CombineOutputCollector<K, V>) mock(CombineOutputCollector.class);
    org.apache.hadoop.mapreduce.TaskAttemptID mockTaskAttemptID =
        mock(org.apache.hadoop.mapreduce.TaskAttemptID.class);
    LocalDirAllocator mockLocalDirAllocator = mock(LocalDirAllocator.class);
    CompressionCodec mockCompressionCodec = mock(CompressionCodec.class);
    Counter mockCounter = mock(Counter.class);
    TaskStatus mockTaskStatus = mock(TaskStatus.class);
    Progress mockProgress = mock(Progress.class);
    MapOutputFile mockMapOutputFile = mock(MapOutputFile.class);
    Task mockTask = mock(Task.class);
    @SuppressWarnings("unchecked")
    MapOutput<K, V> output1 = mock(MapOutput.class);
    @SuppressWarnings("unchecked")
    MapOutput<K, V> output2 = mock(MapOutput.class);
    @SuppressWarnings("unchecked")
    MapOutput<K, V> output3 = mock(MapOutput.class);

    ShuffleConsumerPlugin.Context<K, V> context =
        new ShuffleConsumerPlugin.Context<K, V>(
            mockTaskAttemptID, job, mockFileSystem,
            mockUmbilical, mockLocalDirAllocator,
            mockReporter, mockCompressionCodec,
            combinerClass, mockCombineOutputCollector,
            mockCounter, mockCounter, mockCounter,
            mockCounter, mockCounter, mockCounter,
            mockTaskStatus, mockProgress, mockProgress,
            mockTask, mockMapOutputFile, null);
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
    ShuffleSchedulerImpl<K, V> scheduler = new ShuffleSchedulerImpl<K, V>(job,
        status, null, null, progress, context.getShuffledMapsCounter(),
        context.getReduceShuffleBytes(), context.getFailedShuffleCounter());

    MapHost host1 = new MapHost("host1", null);
    TaskAttemptID succeedAttempt1ID = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
            new JobID("test", 0), TaskType.MAP, 0), 0);
    TaskAttemptID succeedAttempt2ID = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
            new JobID("test", 0), TaskType.MAP, 0), 1);
    TaskAttemptID succeedAttempt3ID = new TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(
            new JobID("test", 0), TaskType.MAP, 1), 0);

    long bytes = (long)500 * 1024 * 1024;
    //First successful copy for map 0 should commit output
    scheduler.copySucceeded(succeedAttempt1ID, host1, bytes, 0, 1, output1);
    verify(output1).commit();

    //Second successful copy for map 0 should abort output
    scheduler.copySucceeded(succeedAttempt2ID, host1, bytes, 0, 1, output2);
    verify(output2).abort();

    //First successful copy for map 1 should commit output
    scheduler.copySucceeded(succeedAttempt3ID, host1, bytes, 0, 1, output3);
    verify(output3).commit();
  }

  private static String copyMessage(int attemptNo, double rate1, double rate2) {
    int attemptZero = attemptNo - 1;
    return String.format("copy task(attempt_test_0000_m_%06d_%d succeeded at %1.2f MB/s)"
            + " Aggregated copy rate(%d of 10 at %1.2f MB/s)", attemptZero
            , attemptZero, rate1, attemptNo, rate2);
  }
}
