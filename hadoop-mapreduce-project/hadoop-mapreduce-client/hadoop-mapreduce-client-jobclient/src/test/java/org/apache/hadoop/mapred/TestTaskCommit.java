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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapred.SortedRanges.Range;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;



public class TestTaskCommit extends HadoopTestCase {
  Path rootDir = 
    new Path(System.getProperty("test.build.data",  "/tmp"), "test");

  static class CommitterWithCommitFail extends FileOutputCommitter {
    public void commitTask(TaskAttemptContext context) throws IOException {
      Path taskOutputPath = getTaskAttemptPath(context);
      TaskAttemptID attemptId = context.getTaskAttemptID();
      JobConf job = context.getJobConf();
      if (taskOutputPath != null) {
        FileSystem fs = taskOutputPath.getFileSystem(job);
        if (fs.exists(taskOutputPath)) {
          throw new IOException();
        }
      }
    }
  }

  /**
   * Special Committer that does not cleanup temporary files in
   * abortTask
   * 
   * The framework's FileOutputCommitter cleans up any temporary
   * files left behind in abortTask. We want the test case to
   * find these files and hence short-circuit abortTask.
   */
  static class CommitterWithoutCleanup extends FileOutputCommitter {
    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
      // does nothing
    }
  }
  
  /**
   * Special committer that always requires commit.
   */
  static class CommitterThatAlwaysRequiresCommit extends FileOutputCommitter {
    @Override
    public boolean needsTaskCommit(TaskAttemptContext context) 
      throws IOException {
      return true;
    }
  }

  public TestTaskCommit() throws IOException {
    super(LOCAL_MR, LOCAL_FS, 1, 1);
  }
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    FileUtil.fullyDelete(new File(rootDir.toString()));
  }

  @Test
  public void testCommitFail() throws IOException {
    final Path inDir = new Path(rootDir, "./input");
    final Path outDir = new Path(rootDir, "./output");
    JobConf jobConf = createJobConf();
    jobConf.setMaxMapAttempts(1);
    jobConf.setOutputCommitter(CommitterWithCommitFail.class);
    RunningJob rJob = UtilsForTests.runJob(jobConf, inDir, outDir, 1, 0);
    rJob.waitForCompletion();
    assertEquals(JobStatus.FAILED, rJob.getJobState());
  }
  
  private class MyUmbilical implements TaskUmbilicalProtocol {
    boolean taskDone = false;

    @Override
    public boolean canCommit(TaskAttemptID taskid) throws IOException {
      return false;
    }

    @Override
    public void commitPending(TaskAttemptID taskId, TaskStatus taskStatus)
        throws IOException, InterruptedException {
      fail("Task should not go to commit-pending");
    }

    @Override
    public void done(TaskAttemptID taskid) throws IOException {
      taskDone = true;
    }

    @Override
    public void fatalError(TaskAttemptID taskId, String message, boolean fastFail)
        throws IOException { }

    @Override
    public void fsError(TaskAttemptID taskId, String message)
        throws IOException { }

    @Override
    public MapTaskCompletionEventsUpdate getMapCompletionEvents(JobID jobId,
        int fromIndex, int maxLocs, TaskAttemptID id) throws IOException {
      return null;
    }

    @Override
    public JvmTask getTask(JvmContext context) throws IOException {
      return null;
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
    public void shuffleError(TaskAttemptID taskId, String message)
        throws IOException {
    }

    @Override
    public AMFeedback statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus)
        throws IOException, InterruptedException {
      AMFeedback a = new AMFeedback();
      a.setTaskFound(true);
      return a;
    }

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
    public void preempted(TaskAttemptID taskId, TaskStatus taskStatus)
        throws IOException, InterruptedException {
      fail("Task should not go to commit-pending");
    }

    @Override
    public TaskCheckpointID getCheckpointID(TaskID taskId) {
      return null;
    }

    @Override
    public void setCheckpointID(TaskID downgrade, TaskCheckpointID cid) {
      // ignore
    }
  }
  
  /**
   * A test that mimics a failed task to ensure that it does
   * not get into the COMMIT_PENDING state, by using a fake
   * UmbilicalProtocol's implementation that fails if the commit.
   * protocol is played.
   * 
   * The test mocks the various steps in a failed task's 
   * life-cycle using a special OutputCommitter and UmbilicalProtocol
   * implementation.
   * 
   * @throws Exception
   */
  @Test
  public void testTaskCleanupDoesNotCommit() throws Exception {
    // Mimic a job with a special committer that does not cleanup
    // files when a task fails.
    JobConf job = new JobConf();
    job.setOutputCommitter(CommitterWithoutCleanup.class);
    Path outDir = new Path(rootDir, "output"); 
    FileOutputFormat.setOutputPath(job, outDir);

    // Mimic job setup
    String dummyAttemptID = "attempt_200707121733_0001_m_000000_0";
    TaskAttemptID attemptID = TaskAttemptID.forName(dummyAttemptID);
    OutputCommitter committer = new CommitterWithoutCleanup();
    JobContext jContext = new JobContextImpl(job, attemptID.getJobID());
    committer.setupJob(jContext);
    

    // Mimic a map task
    dummyAttemptID = "attempt_200707121733_0001_m_000001_0";
    attemptID = TaskAttemptID.forName(dummyAttemptID);
    Task task = new MapTask(null, attemptID, 0, null, 1);
    task.setConf(job);
    task.localizeConfiguration(job);
    task.initialize(job, attemptID.getJobID(), Reporter.NULL, false);
    
    // Mimic the map task writing some output.
    String file = "test.txt";
    FileSystem localFs = FileSystem.getLocal(job);
    TextOutputFormat<Text, Text> theOutputFormat 
      = new TextOutputFormat<Text, Text>();
    RecordWriter<Text, Text> theRecordWriter = 
      theOutputFormat.getRecordWriter(localFs,
        job, file, Reporter.NULL);
    theRecordWriter.write(new Text("key"), new Text("value"));
    theRecordWriter.close(Reporter.NULL);

    // Mimic a task failure; setting up the task for cleanup simulates
    // the abort protocol to be played.
    // Without checks in the framework, this will fail
    // as the committer will cause a COMMIT to happen for
    // the cleanup task.
    task.setTaskCleanupTask();
    MyUmbilical umbilical = new MyUmbilical();
    task.run(job, umbilical);
    assertTrue("Task did not succeed", umbilical.taskDone);
  }

  @Test
  public void testCommitRequiredForMapTask() throws Exception {
    Task testTask = createDummyTask(TaskType.MAP);
    assertTrue("MapTask should need commit", testTask.isCommitRequired());
  }

  @Test
  public void testCommitRequiredForReduceTask() throws Exception {
    Task testTask = createDummyTask(TaskType.REDUCE);
    assertTrue("ReduceTask should need commit", testTask.isCommitRequired());
  }

  @Test
  public void testCommitNotRequiredForJobSetup() throws Exception {
    Task testTask = createDummyTask(TaskType.MAP);
    testTask.setJobSetupTask();
    assertFalse("Job setup task should not need commit", 
        testTask.isCommitRequired());
  }

  @Test
  public void testCommitNotRequiredForJobCleanup() throws Exception {
    Task testTask = createDummyTask(TaskType.MAP);
    testTask.setJobCleanupTask();
    assertFalse("Job cleanup task should not need commit", 
        testTask.isCommitRequired());
  }

  @Test
  public void testCommitNotRequiredForTaskCleanup() throws Exception {
    Task testTask = createDummyTask(TaskType.REDUCE);
    testTask.setTaskCleanupTask();
    assertFalse("Task cleanup task should not need commit", 
        testTask.isCommitRequired());
  }

  private Task createDummyTask(TaskType type) throws IOException, ClassNotFoundException,
  InterruptedException {
    JobConf conf = new JobConf();
    conf.setOutputCommitter(CommitterThatAlwaysRequiresCommit.class);
    Path outDir = new Path(rootDir, "output"); 
    FileOutputFormat.setOutputPath(conf, outDir);
    JobID jobId = JobID.forName("job_201002121132_0001");
    Task testTask;
    if (type == TaskType.MAP) {
      testTask = new MapTask();
    } else {
      testTask = new ReduceTask();
    }
    testTask.setConf(conf);
    testTask.initialize(conf, jobId, Reporter.NULL, false);
    return testTask;
  }


  
  public static void main(String[] argv) throws Exception {
    TestTaskCommit td = new TestTaskCommit();
    td.testCommitFail();
  }
}
