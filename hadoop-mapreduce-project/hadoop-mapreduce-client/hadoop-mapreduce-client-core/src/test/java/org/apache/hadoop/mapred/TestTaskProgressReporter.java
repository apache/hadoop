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
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapred.SortedRanges.Range;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.apache.hadoop.util.ExitUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestTaskProgressReporter {
  private static int statusUpdateTimes = 0;

  // set to true if the thread is existed with ExitUtil.terminate
  volatile boolean threadExited = false;

  final static int LOCAL_BYTES_WRITTEN = 1024;

  private FakeUmbilical fakeUmbilical = new FakeUmbilical();

  private static class DummyTask extends Task {
    @Override
    public void run(JobConf job, TaskUmbilicalProtocol umbilical)
        throws IOException, ClassNotFoundException, InterruptedException {
    }

    @Override
    public boolean isMapTask() {
      return true;
    }
  }

  private static class FakeUmbilical implements TaskUmbilicalProtocol {
    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      return 0;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      return null;
    }

    @Override
    public JvmTask getTask(JvmContext context) throws IOException {
      return null;
    }

    @Override
    public AMFeedback statusUpdate(TaskAttemptID taskId,
        TaskStatus taskStatus) throws IOException, InterruptedException {
      statusUpdateTimes++;
      AMFeedback feedback = new AMFeedback();
      feedback.setTaskFound(true);
      feedback.setPreemption(true);
      return feedback;
    }

    @Override
    public void reportDiagnosticInfo(TaskAttemptID taskid, String trace)
        throws IOException {
    }

    @Override
    public void reportNextRecordRange(TaskAttemptID taskid, Range range)
        throws IOException {
    }

    @Override
    public void done(TaskAttemptID taskid) throws IOException {
    }

    @Override
    public void commitPending(TaskAttemptID taskId, TaskStatus taskStatus)
        throws IOException, InterruptedException {
    }

    @Override
    public boolean canCommit(TaskAttemptID taskid) throws IOException {
      return false;
    }

    @Override
    public void shuffleError(TaskAttemptID taskId, String message)
        throws IOException {
    }

    @Override
    public void fsError(TaskAttemptID taskId, String message)
        throws IOException {
    }

    @Override
    public void fatalError(TaskAttemptID taskId, String message)
        throws IOException {
    }

    @Override
    public MapTaskCompletionEventsUpdate getMapCompletionEvents(
        JobID jobId, int fromIndex, int maxLocs, TaskAttemptID id)
        throws IOException {
      return null;
    }

    @Override
    public void preempted(TaskAttemptID taskId, TaskStatus taskStatus)
        throws IOException, InterruptedException {
    }

    @Override
    public TaskCheckpointID getCheckpointID(TaskID taskID) {
      return null;
    }

    @Override
    public void setCheckpointID(TaskID tid, TaskCheckpointID cid) {
    }
  }

  private class DummyTaskReporter extends Task.TaskReporter {
    volatile boolean taskLimitIsChecked = false;

    public DummyTaskReporter(Task task) {
      task.super(task.getProgress(), fakeUmbilical);
    }

    @Override
    public void setProgress(float progress) {
      super.setProgress(progress);
    }

    @Override
    protected void checkTaskLimits() throws TaskLimitException {
      taskLimitIsChecked = true;
      super.checkTaskLimits();
    }
  }

  @Test (timeout=10000)
  public void testTaskProgress() throws Exception {
    JobConf job = new JobConf();
    job.setLong(MRJobConfig.TASK_PROGRESS_REPORT_INTERVAL, 1000);
    Task task = new DummyTask();
    task.setConf(job);
    DummyTaskReporter reporter = new DummyTaskReporter(task);
    Thread t = new Thread(reporter);
    t.start();
    Thread.sleep(2100);
    task.setTaskDone();
    reporter.resetDoneFlag();
    t.join();
    Assert.assertEquals(statusUpdateTimes, 2);
  }

  @Test(timeout=10000)
  public void testBytesWrittenRespectingLimit() throws Exception {
    // add 1024 to the limit to account for writes not controlled by the test
    testBytesWrittenLimit(LOCAL_BYTES_WRITTEN + 1024, false);
  }

  @Test(timeout=10000)
  public void testBytesWrittenExceedingLimit() throws Exception {
    testBytesWrittenLimit(LOCAL_BYTES_WRITTEN - 1, true);
  }

  /**
   * This is to test the limit on BYTES_WRITTEN. The test is limited in that
   * the check is done only once at the first loop of TaskReport#run.
   * @param limit the limit on BYTES_WRITTEN in local file system
   * @param failFast should the task fail fast with such limit?
   * @throws Exception
   */
  public void testBytesWrittenLimit(long limit, boolean failFast)
          throws Exception {
    ExitUtil.disableSystemExit();
    threadExited = false;
    Thread.UncaughtExceptionHandler h = new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread th, Throwable ex) {
        System.out.println("Uncaught exception: " + ex);
        if (ex instanceof ExitUtil.ExitException) {
          threadExited = true;
        }
      }
    };
    JobConf conf = new JobConf();
    // To disable task reporter sleeping
    conf.getLong(MRJobConfig.TASK_PROGRESS_REPORT_INTERVAL, 0);
    conf.setLong(MRJobConfig.TASK_LOCAL_WRITE_LIMIT_BYTES, limit);
    LocalFileSystem localFS = FileSystem.getLocal(conf);
    Path tmpPath = new Path("/tmp/testBytesWrittenLimit-tmpFile-"
            + new Random(System.currentTimeMillis()).nextInt());
    FSDataOutputStream out = localFS.create(tmpPath, true);
    out.write(new byte[LOCAL_BYTES_WRITTEN]);
    out.close();

    Task task = new DummyTask();
    task.setConf(conf);
    DummyTaskReporter reporter = new DummyTaskReporter(task);
    Thread t = new Thread(reporter);
    t.setUncaughtExceptionHandler(h);
    reporter.setProgressFlag();

    t.start();
    while (!reporter.taskLimitIsChecked) {
      Thread.yield();
    }

    task.setTaskDone();
    reporter.resetDoneFlag();
    t.join();
    Assert.assertEquals(failFast, threadExited);
  }
}