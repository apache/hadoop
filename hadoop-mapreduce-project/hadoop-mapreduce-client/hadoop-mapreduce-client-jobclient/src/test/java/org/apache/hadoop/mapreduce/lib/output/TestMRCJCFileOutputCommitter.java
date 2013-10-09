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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.*;
import java.net.URI;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.UtilsForTests;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;


public class TestMRCJCFileOutputCommitter extends TestCase {
  private static Path outDir = new Path(System.getProperty("test.build.data",
      "/tmp"), "output");

  // A random task attempt id for testing.
  private static String attempt = "attempt_200707121733_0001_m_000000_0";
  private static String partFile = "part-m-00000";
  private static TaskAttemptID taskID = TaskAttemptID.forName(attempt);
  private Text key1 = new Text("key1");
  private Text key2 = new Text("key2");
  private Text val1 = new Text("val1");
  private Text val2 = new Text("val2");

  @SuppressWarnings("unchecked")
  private void writeOutput(RecordWriter theRecordWriter,
      TaskAttemptContext context) throws IOException, InterruptedException {
    NullWritable nullWritable = NullWritable.get();

    try {
      theRecordWriter.write(key1, val1);
      theRecordWriter.write(null, nullWritable);
      theRecordWriter.write(null, val1);
      theRecordWriter.write(nullWritable, val2);
      theRecordWriter.write(key2, nullWritable);
      theRecordWriter.write(key1, null);
      theRecordWriter.write(null, null);
      theRecordWriter.write(key2, val2);
    } finally {
      theRecordWriter.close(context);
    }
  }

  private static void cleanup() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = outDir.getFileSystem(conf);
    fs.delete(outDir, true);
  }
  
  @Override
  public void setUp() throws IOException {
    cleanup();
  }
  
  @Override
  public void tearDown() throws IOException {
    cleanup();
  }
  
  @SuppressWarnings("unchecked")
  public void testCommitter() throws Exception {
    Job job = Job.getInstance();
    FileOutputFormat.setOutputPath(job, outDir);
    Configuration conf = job.getConfiguration();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    FileOutputCommitter committer = new FileOutputCommitter(outDir, tContext);

    // setup
    committer.setupJob(jContext);
    committer.setupTask(tContext);

    // write output
    TextOutputFormat theOutputFormat = new TextOutputFormat();
    RecordWriter theRecordWriter = theOutputFormat.getRecordWriter(tContext);
    writeOutput(theRecordWriter, tContext);

    // do commit
    committer.commitTask(tContext);
    committer.commitJob(jContext);

    // validate output
    File expectedFile = new File(new Path(outDir, partFile).toString());
    StringBuffer expectedOutput = new StringBuffer();
    expectedOutput.append(key1).append('\t').append(val1).append("\n");
    expectedOutput.append(val1).append("\n");
    expectedOutput.append(val2).append("\n");
    expectedOutput.append(key2).append("\n");
    expectedOutput.append(key1).append("\n");
    expectedOutput.append(key2).append('\t').append(val2).append("\n");
    String output = UtilsForTests.slurp(expectedFile);
    assertEquals(output, expectedOutput.toString());
    FileUtil.fullyDelete(new File(outDir.toString()));
  }
  
  public void testEmptyOutput() throws Exception {
    Job job = Job.getInstance();
    FileOutputFormat.setOutputPath(job, outDir);
    Configuration conf = job.getConfiguration();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    FileOutputCommitter committer = new FileOutputCommitter(outDir, tContext);

    // setup
    committer.setupJob(jContext);
    committer.setupTask(tContext);

    // Do not write any output

    // do commit
    committer.commitTask(tContext);
    committer.commitJob(jContext);
    
    FileUtil.fullyDelete(new File(outDir.toString()));
  }

  @SuppressWarnings("unchecked")
  public void testAbort() throws IOException, InterruptedException {
    Job job = Job.getInstance();
    FileOutputFormat.setOutputPath(job, outDir);
    Configuration conf = job.getConfiguration();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    FileOutputCommitter committer = new FileOutputCommitter(outDir, tContext);

    // do setup
    committer.setupJob(jContext);
    committer.setupTask(tContext);

    // write output
    TextOutputFormat theOutputFormat = new TextOutputFormat();
    RecordWriter theRecordWriter = theOutputFormat.getRecordWriter(tContext);
    writeOutput(theRecordWriter, tContext);

    // do abort
    committer.abortTask(tContext);
    File expectedFile = new File(new Path(committer.getWorkPath(), partFile)
        .toString());
    assertFalse("task temp dir still exists", expectedFile.exists());

    committer.abortJob(jContext, JobStatus.State.FAILED);
    expectedFile = new File(new Path(outDir, FileOutputCommitter.PENDING_DIR_NAME)
        .toString());
    assertFalse("job temp dir still exists", expectedFile.exists());
    assertEquals("Output directory not empty", 0, new File(outDir.toString())
        .listFiles().length);
    FileUtil.fullyDelete(new File(outDir.toString()));
  }

  public static class FakeFileSystem extends RawLocalFileSystem {
    public FakeFileSystem() {
      super();
    }

    public URI getUri() {
      return URI.create("faildel:///");
    }

    @Override
    public boolean delete(Path p, boolean recursive) throws IOException {
      throw new IOException("fake delete failed");
    }
  }

  @SuppressWarnings("unchecked")
  public void testFailAbort() throws IOException, InterruptedException {
    Job job = Job.getInstance();
    Configuration conf = job.getConfiguration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "faildel:///");
    conf.setClass("fs.faildel.impl", FakeFileSystem.class, FileSystem.class);
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
    FileOutputFormat.setOutputPath(job, outDir);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    FileOutputCommitter committer = new FileOutputCommitter(outDir, tContext);

    // do setup
    committer.setupJob(jContext);
    committer.setupTask(tContext);

    // write output
    TextOutputFormat<?, ?> theOutputFormat = new TextOutputFormat();
    RecordWriter<?, ?> theRecordWriter = theOutputFormat
        .getRecordWriter(tContext);
    writeOutput(theRecordWriter, tContext);

    // do abort
    Throwable th = null;
    try {
      committer.abortTask(tContext);
    } catch (IOException ie) {
      th = ie;
    }
    assertNotNull(th);
    assertTrue(th instanceof IOException);
    assertTrue(th.getMessage().contains("fake delete failed"));
    //Path taskBaseDirName = committer.getTaskAttemptBaseDirName(tContext);
    File jobTmpDir = new File(committer.getJobAttemptPath(jContext).toUri().getPath());
    File taskTmpDir = new File(committer.getTaskAttemptPath(tContext).toUri().getPath());
    File expectedFile = new File(taskTmpDir, partFile);
    assertTrue(expectedFile + " does not exists", expectedFile.exists());

    th = null;
    try {
      committer.abortJob(jContext, JobStatus.State.FAILED);
    } catch (IOException ie) {
      th = ie;
    }
    assertNotNull(th);
    assertTrue(th instanceof IOException);
    assertTrue(th.getMessage().contains("fake delete failed"));
    assertTrue("job temp dir does not exists", jobTmpDir.exists());
    FileUtil.fullyDelete(new File(outDir.toString()));
  }
}
