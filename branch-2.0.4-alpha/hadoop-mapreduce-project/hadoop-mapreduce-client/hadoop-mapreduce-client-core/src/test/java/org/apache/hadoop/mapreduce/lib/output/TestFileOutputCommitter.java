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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

@SuppressWarnings("unchecked")
public class TestFileOutputCommitter extends TestCase {
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

  private void writeMapFileOutput(RecordWriter theRecordWriter,
      TaskAttemptContext context) throws IOException, InterruptedException {
    try {
      int key = 0;
      for (int i = 0 ; i < 10; ++i) {
        key = i;
        Text val = (i%2 == 1) ? val1 : val2;
        theRecordWriter.write(new LongWritable(key),
            val);        
      }
    } finally {
      theRecordWriter.close(context);
    }
  }
  
  public void testRecovery() throws Exception {
    Job job = Job.getInstance();
    FileOutputFormat.setOutputPath(job, outDir);
    Configuration conf = job.getConfiguration();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
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
    Path jobTempDir1 = committer.getCommittedTaskPath(tContext);
    File jtd = new File(jobTempDir1.toUri().getPath());
    assertTrue(jtd.exists());
    validateContent(jtd);    
    
    //now while running the second app attempt, 
    //recover the task output from first attempt
    Configuration conf2 = job.getConfiguration();
    conf2.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
    conf2.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 2);
    JobContext jContext2 = new JobContextImpl(conf2, taskID.getJobID());
    TaskAttemptContext tContext2 = new TaskAttemptContextImpl(conf2, taskID);
    FileOutputCommitter committer2 = new FileOutputCommitter(outDir, tContext2);
    committer2.setupJob(tContext2);
    Path jobTempDir2 = committer2.getCommittedTaskPath(tContext2);
    File jtd2 = new File(jobTempDir2.toUri().getPath());
    
    committer2.recoverTask(tContext2);
    assertTrue(jtd2.exists());
    validateContent(jtd2);
    
    committer2.commitJob(jContext2);
    validateContent(outDir);
    FileUtil.fullyDelete(new File(outDir.toString()));
  }

  private void validateContent(Path dir) throws IOException {
    validateContent(new File(dir.toUri().getPath()));
  }
  
  private void validateContent(File dir) throws IOException {
    File expectedFile = new File(dir, partFile);
    assertTrue("Could not find "+expectedFile, expectedFile.exists());
    StringBuffer expectedOutput = new StringBuffer();
    expectedOutput.append(key1).append('\t').append(val1).append("\n");
    expectedOutput.append(val1).append("\n");
    expectedOutput.append(val2).append("\n");
    expectedOutput.append(key2).append("\n");
    expectedOutput.append(key1).append("\n");
    expectedOutput.append(key2).append('\t').append(val2).append("\n");
    String output = slurp(expectedFile);
    assertEquals(output, expectedOutput.toString());
  }

  private void validateMapFileOutputContent(
      FileSystem fs, Path dir) throws IOException {
    // map output is a directory with index and data files
    Path expectedMapDir = new Path(dir, partFile);
    assert(fs.getFileStatus(expectedMapDir).isDirectory());    
    FileStatus[] files = fs.listStatus(expectedMapDir);
    int fileCount = 0;
    boolean dataFileFound = false; 
    boolean indexFileFound = false; 
    for (FileStatus f : files) {
      if (f.isFile()) {
        ++fileCount;
        if (f.getPath().getName().equals(MapFile.INDEX_FILE_NAME)) {
          indexFileFound = true;
        }
        else if (f.getPath().getName().equals(MapFile.DATA_FILE_NAME)) {
          dataFileFound = true;
        }
      }
    }
    assert(fileCount > 0);
    assert(dataFileFound && indexFileFound);
  }
  
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
    validateContent(outDir);
    FileUtil.fullyDelete(new File(outDir.toString()));
  }

  public void testMapFileOutputCommitter() throws Exception {
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
    MapFileOutputFormat theOutputFormat = new MapFileOutputFormat();
    RecordWriter theRecordWriter = theOutputFormat.getRecordWriter(tContext);
    writeMapFileOutput(theRecordWriter, tContext);

    // do commit
    committer.commitTask(tContext);
    committer.commitJob(jContext);

    // validate output
    validateMapFileOutputContent(FileSystem.get(job.getConfiguration()), outDir);
    FileUtil.fullyDelete(new File(outDir.toString()));
  }
  
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

  
  public void testFailAbort() throws IOException, InterruptedException {
    Job job = Job.getInstance();
    Configuration conf = job.getConfiguration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "faildel:///");
    conf.setClass("fs.faildel.impl", FakeFileSystem.class, FileSystem.class);
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
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
    Path jtd = committer.getJobAttemptPath(jContext);
    File jobTmpDir = new File(jtd.toUri().getPath());
    Path ttd = committer.getTaskAttemptPath(tContext);
    File taskTmpDir = new File(ttd.toUri().getPath());
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

  public static String slurp(File f) throws IOException {
    int len = (int) f.length();
    byte[] buf = new byte[len];
    FileInputStream in = new FileInputStream(f);
    String contents = null;
    try {
      in.read(buf, 0, len);
      contents = new String(buf, "UTF-8");
    } finally {
      in.close();
    }
    return contents;
  }

}
