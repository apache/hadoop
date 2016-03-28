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

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.jobhistory.JobSubmittedEvent;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.split.JobSplit.SplitMetaInfo;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.util.ReflectionUtils;

/**
 *  Validates map phase progress.
 *  Testcase uses newApi.
 *  We extend Task.TaskReporter class and override setProgress()
 *  to validate the map phase progress being set.
 *  We extend MapTask and override startReporter() method that creates
 *  TestTaskReporter instead of TaskReporter and call mapTask.run().
 *  Similar to LocalJobRunner, we set up splits and call mapTask.run()
 *  directly. No job is run, only map task is run.
 *  As the reporter's setProgress() validates progress after
 *  every record is read, we are done with the validation of map phase progress
 *  once mapTask.run() is finished. Sort phase progress in map task is not
 *  validated here.
 */
public class TestMapProgress extends TestCase {
  public static final Log LOG = LogFactory.getLog(TestMapProgress.class);
  private static String TEST_ROOT_DIR;
  static {
    String root = new File(System.getProperty("test.build.data", "/tmp"))
      .getAbsolutePath();
    TEST_ROOT_DIR = new Path(root, "mapPhaseprogress").toString();
  }

  static class FakeUmbilical implements TaskUmbilicalProtocol {

    public long getProtocolVersion(String protocol, long clientVersion) {
      return TaskUmbilicalProtocol.versionID;
    }
    
    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      return ProtocolSignature.getProtocolSignature(
          this, protocol, clientVersion, clientMethodsHash);
    }

    public void done(TaskAttemptID taskid) throws IOException {
      LOG.info("Task " + taskid + " reporting done.");
    }

    public void fsError(TaskAttemptID taskId, String message) throws IOException {
      LOG.info("Task " + taskId + " reporting file system error: " + message);
    }

    public void shuffleError(TaskAttemptID taskId, String message) throws IOException {
      LOG.info("Task " + taskId + " reporting shuffle error: " + message);
    }

    public void fatalError(TaskAttemptID taskId, String msg) throws IOException {
      LOG.info("Task " + taskId + " reporting fatal error: " + msg);
    }

    public JvmTask getTask(JvmContext context) throws IOException {
      return null;
    }

    public boolean ping(TaskAttemptID taskid) throws IOException {
      return true;
    }

    public void commitPending(TaskAttemptID taskId, TaskStatus taskStatus) 
    throws IOException, InterruptedException {
      statusUpdate(taskId, taskStatus);
    }
    
    public boolean canCommit(TaskAttemptID taskid) throws IOException {
      return true;
    }
    
    public boolean statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus) 
    throws IOException, InterruptedException {
      StringBuffer buf = new StringBuffer("Task ");
      buf.append(taskId);
      if (taskStatus != null) {
        buf.append(" making progress to ");
        buf.append(taskStatus.getProgress());
        String state = taskStatus.getStateString();
        if (state != null) {
          buf.append(" and state of ");
          buf.append(state);
        }
      }
      LOG.info(buf.toString());
      // ignore phase
      // ignore counters
      return true;
    }

    public void reportDiagnosticInfo(TaskAttemptID taskid, String trace) throws IOException {
      LOG.info("Task " + taskid + " has problem " + trace);
    }
    
    public MapTaskCompletionEventsUpdate getMapCompletionEvents(JobID jobId, 
        int fromEventId, int maxLocs, TaskAttemptID id) throws IOException {
      return new MapTaskCompletionEventsUpdate(TaskCompletionEvent.EMPTY_ARRAY, 
                                               false);
    }

    public void reportNextRecordRange(TaskAttemptID taskid, 
        SortedRanges.Range range) throws IOException {
      LOG.info("Task " + taskid + " reportedNextRecordRange " + range);
    }
  }
  
  private FileSystem fs = null;
  private TestMapTask map = null;
  private JobID jobId = null;
  private FakeUmbilical fakeUmbilical = new FakeUmbilical();

  /**
   *  Task Reporter that validates map phase progress after each record is
   *  processed by map task
   */ 
  public class TestTaskReporter extends Task.TaskReporter {
    private int recordNum = 0; // number of records processed
    TestTaskReporter(Task task) {
      task.super(task.getProgress(), fakeUmbilical);
    }

    @Override
    public void setProgress(float progress) {
      super.setProgress(progress);
      float mapTaskProgress = map.getProgress().getProgress();
      LOG.info("Map task progress is " + mapTaskProgress);
      if (recordNum < 3) {
        // only 3 records are there; Ignore validating progress after 3 times
        recordNum++;
      }
      else {
        return;
      }
      // validate map task progress when the map task is in map phase
      assertTrue("Map progress is not the expected value.",
                 Math.abs(mapTaskProgress - ((float)recordNum/3)) < 0.001);
    }
  }

  /**
   * Map Task that overrides run method and uses TestTaskReporter instead of
   * TaskReporter and uses FakeUmbilical.
   */
  class TestMapTask extends MapTask {
    public TestMapTask(String jobFile, TaskAttemptID taskId, 
        int partition, TaskSplitIndex splitIndex,
        int numSlotsRequired) {
      super(jobFile, taskId, partition, splitIndex, numSlotsRequired);
    }
    
    /**
     * Create a TestTaskReporter and use it for validating map phase progress
     */
    @Override
    TaskReporter startReporter(final TaskUmbilicalProtocol umbilical) {  
      // start thread that will handle communication with parent
      TaskReporter reporter = new TestTaskReporter(map);
      return reporter;
    }
  }
  
  // In the given dir, creates part-0 file with 3 records of same size
  private void createInputFile(Path rootDir) throws IOException {
    if(fs.exists(rootDir)){
      fs.delete(rootDir, true);
    }
    
    String str = "The quick brown fox\n" + "The brown quick fox\n"
    + "The fox brown quick\n";
    DataOutputStream inpFile = fs.create(new Path(rootDir, "part-0"));
    inpFile.writeBytes(str);
    inpFile.close();
  }

  /**
   *  Validates map phase progress after each record is processed by map task
   *  using custom task reporter.
   */ 
  public void testMapProgress() throws Exception {
    JobConf job = new JobConf();
    fs = FileSystem.getLocal(job);
    Path rootDir = new Path(TEST_ROOT_DIR);
    createInputFile(rootDir);

    job.setNumReduceTasks(0);
    TaskAttemptID taskId = TaskAttemptID.forName(
                                  "attempt_200907082313_0424_m_000000_0");
    job.setClass("mapreduce.job.outputformat.class",
                 NullOutputFormat.class, OutputFormat.class);
    job.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR,
            TEST_ROOT_DIR);
    jobId = taskId.getJobID();
    
    JobContext jContext = new JobContextImpl(job, jobId);
    InputFormat<?, ?> input =
      ReflectionUtils.newInstance(jContext.getInputFormatClass(), job);

    List<InputSplit> splits = input.getSplits(jContext);
    JobSplitWriter.createSplitFiles(new Path(TEST_ROOT_DIR), job, 
                   new Path(TEST_ROOT_DIR).getFileSystem(job),
                   splits);
    TaskSplitMetaInfo[] splitMetaInfo = 
      SplitMetaInfoReader.readSplitMetaInfo(jobId, fs, job, new Path(TEST_ROOT_DIR));
    job.setUseNewMapper(true); // use new api    
    for (int i = 0; i < splitMetaInfo.length; i++) {// rawSplits.length is 1
      map = new TestMapTask(
          job.get(JTConfig.JT_SYSTEM_DIR, "/tmp/hadoop/mapred/system") +
          jobId + "job.xml",  
          taskId, i,
          splitMetaInfo[i].getSplitIndex(), 1);

      JobConf localConf = new JobConf(job);
      map.localizeConfiguration(localConf);
      map.setConf(localConf);
      map.run(localConf, fakeUmbilical);
    }
    // clean up
    fs.delete(rootDir, true);
  }
}
