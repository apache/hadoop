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

import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapred.SortedRanges.Range;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.Assert;
import org.junit.Test;

public class TestTaskProgressReporter {
  private static int statusUpdateTimes = 0;
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
    public boolean statusUpdate(TaskAttemptID taskId,
        TaskStatus taskStatus) throws IOException, InterruptedException {
      return true;
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
	public boolean ping(TaskAttemptID taskid) throws IOException {
      statusUpdateTimes++;
      return true;
	}
  }

  private class DummyTaskReporter extends Task.TaskReporter {
    public DummyTaskReporter(Task task) {
      task.super(task.getProgress(), fakeUmbilical);
    }
    @Override
    public void setProgress(float progress) {
      super.setProgress(progress);
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
}