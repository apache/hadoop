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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


@SuppressWarnings("unchecked")
public class TestFileOutputCommitter extends TestCase {
  private static Path outDir = new Path(System.getProperty("test.build.data",
      "/tmp"), "output");

  // A random task attempt id for testing.
  private static String attempt = "attempt_200707121733_0001_m_000000_0";
  private static String partFile = "part-00000";
  private static TaskAttemptID taskID = TaskAttemptID.forName(attempt);
  private Text key1 = new Text("key1");
  private Text key2 = new Text("key2");
  private Text val1 = new Text("val1");
  private Text val2 = new Text("val2");

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
      theRecordWriter.close(null);
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
      theRecordWriter.close(null);
    }
  }

  private void testRecoveryInternal(int commitVersion, int recoveryVersion)
      throws Exception {
  JobConf conf = new JobConf();
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
    conf.setInt(MRConstants.APPLICATION_ATTEMPT_ID, 1);
    conf.setInt(org.apache.hadoop.mapreduce.lib.output.
        FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION,
        commitVersion);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    FileOutputCommitter committer = new FileOutputCommitter();

    // setup
    committer.setupJob(jContext);
    committer.setupTask(tContext);

    // write output
    TextOutputFormat theOutputFormat = new TextOutputFormat();
    RecordWriter theRecordWriter =
        theOutputFormat.getRecordWriter(null, conf, partFile, null);
    writeOutput(theRecordWriter, tContext);

    // do commit
    if(committer.needsTaskCommit(tContext)) {
      committer.commitTask(tContext);
    }

    Path jobTempDir1 = committer.getCommittedTaskPath(tContext);
    File jtd1 = new File(jobTempDir1.toUri().getPath());
    if (commitVersion == 1) {
      assertTrue("Version 1 commits to temporary dir " + jtd1, jtd1.exists());
      validateContent(jobTempDir1);
    } else {
      assertFalse("Version 2 commits to output dir " + jtd1, jtd1.exists());
    }

    //now while running the second app attempt,
    //recover the task output from first attempt
    JobConf conf2 = new JobConf(conf);
    conf2.set(JobContext.TASK_ATTEMPT_ID, attempt);
    conf2.setInt(MRConstants.APPLICATION_ATTEMPT_ID, 2);
    conf2.setInt(org.apache.hadoop.mapreduce.lib.output.
        FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION,
        recoveryVersion);
    JobContext jContext2 = new JobContextImpl(conf2, taskID.getJobID());
    TaskAttemptContext tContext2 = new TaskAttemptContextImpl(conf2, taskID);
    FileOutputCommitter committer2 = new FileOutputCommitter();
    committer2.setupJob(jContext2);

    committer2.recoverTask(tContext2);

    Path jobTempDir2 = committer2.getCommittedTaskPath(tContext2);
    File jtd2 = new File(jobTempDir2.toUri().getPath());
    if (recoveryVersion == 1) {
      assertTrue("Version 1 recovers to " + jtd2, jtd2.exists());
      validateContent(jobTempDir2);
    } else {
        assertFalse("Version 2 commits to output dir " + jtd2, jtd2.exists());
        if (commitVersion == 1) {
          assertTrue("Version 2  recovery moves to output dir from "
              + jtd1 , jtd1.list().length == 0);
        }
      }

    committer2.commitJob(jContext2);
    validateContent(outDir);
    FileUtil.fullyDelete(new File(outDir.toString()));
  }
  public void testRecoveryV1() throws Exception {
    testRecoveryInternal(1, 1);
  }

  public void testRecoveryV2() throws Exception {
    testRecoveryInternal(2, 2);
  }

  public void testRecoveryUpgradeV1V2() throws Exception {
    testRecoveryInternal(1, 2);
  }

  private void validateContent(Path dir) throws IOException {
    File fdir = new File(dir.toUri().getPath());
    File expectedFile = new File(fdir, partFile);
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

  private void testCommitterInternal(int version) throws Exception {
    JobConf conf = new JobConf();
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
    conf.setInt(org.apache.hadoop.mapreduce.lib.output.
        FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, version);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    FileOutputCommitter committer = new FileOutputCommitter();

    // setup
    committer.setupJob(jContext);
    committer.setupTask(tContext);

    // write output
    TextOutputFormat theOutputFormat = new TextOutputFormat();
    RecordWriter theRecordWriter = 
        theOutputFormat.getRecordWriter(null, conf, partFile, null);
    writeOutput(theRecordWriter, tContext);

    // do commit
    if(committer.needsTaskCommit(tContext)) {
      committer.commitTask(tContext);
    }
    committer.commitJob(jContext);

    // validate output
    validateContent(outDir);
    FileUtil.fullyDelete(new File(outDir.toString()));
  }

  public void testCommitterV1() throws Exception {
    testCommitterInternal(1);
  }

  public void testCommitterV2() throws Exception {
    testCommitterInternal(2);
  }

  private void testMapFileOutputCommitterInternal(int version)
      throws Exception {
    JobConf conf = new JobConf();
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
    conf.setInt(org.apache.hadoop.mapreduce.lib.output.
        FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, version);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    FileOutputCommitter committer = new FileOutputCommitter();

    // setup
    committer.setupJob(jContext);
    committer.setupTask(tContext);

    // write output
    MapFileOutputFormat theOutputFormat = new MapFileOutputFormat();
    RecordWriter theRecordWriter =
        theOutputFormat.getRecordWriter(null, conf, partFile, null);
    writeMapFileOutput(theRecordWriter, tContext);

    // do commit
    if(committer.needsTaskCommit(tContext)) {
      committer.commitTask(tContext);
    }
    committer.commitJob(jContext);

    // validate output
    validateMapFileOutputContent(FileSystem.get(conf), outDir);
    FileUtil.fullyDelete(new File(outDir.toString()));
  }

  public void testMapFileOutputCommitterV1() throws Exception {
    testMapFileOutputCommitterInternal(1);
  }

  public void testMapFileOutputCommitterV2() throws Exception {
    testMapFileOutputCommitterInternal(2);
  }

  public void testMapOnlyNoOutputV1() throws Exception {
    testMapOnlyNoOutputInternal(1);
  }

  public void testMapOnlyNoOutputV2() throws Exception {
    testMapOnlyNoOutputInternal(2);
  }

  private void testMapOnlyNoOutputInternal(int version) throws Exception {
    JobConf conf = new JobConf();
    //This is not set on purpose. FileOutputFormat.setOutputPath(conf, outDir);
    conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
    conf.setInt(org.apache.hadoop.mapreduce.lib.output.
        FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, version);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    FileOutputCommitter committer = new FileOutputCommitter();    
    
    // setup
    committer.setupJob(jContext);
    committer.setupTask(tContext);
    
    if(committer.needsTaskCommit(tContext)) {
      // do commit
      committer.commitTask(tContext);
    }
    committer.commitJob(jContext);

    // validate output
    FileUtil.fullyDelete(new File(outDir.toString()));
  }

  private void testAbortInternal(int version)
      throws IOException, InterruptedException {
    JobConf conf = new JobConf();
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
    conf.setInt(org.apache.hadoop.mapreduce.lib.output.
        FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, version);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    FileOutputCommitter committer = new FileOutputCommitter();

    // do setup
    committer.setupJob(jContext);
    committer.setupTask(tContext);

    // write output
    TextOutputFormat theOutputFormat = new TextOutputFormat();
    RecordWriter theRecordWriter = 
        theOutputFormat.getRecordWriter(null, conf, partFile, null);
    writeOutput(theRecordWriter, tContext);

    // do abort
    committer.abortTask(tContext);
    File out = new File(outDir.toUri().getPath());
    Path workPath = committer.getWorkPath(tContext, outDir);
    File wp = new File(workPath.toUri().getPath());
    File expectedFile = new File(wp, partFile);
    assertFalse("task temp dir still exists", expectedFile.exists());

    committer.abortJob(jContext, JobStatus.State.FAILED);
    expectedFile = new File(out, FileOutputCommitter.TEMP_DIR_NAME);
    assertFalse("job temp dir still exists", expectedFile.exists());
    assertEquals("Output directory not empty", 0, out.listFiles().length);
    FileUtil.fullyDelete(out);
  }

  public void testAbortV1() throws Exception {
    testAbortInternal(1);
  }

  public void testAbortV2() throws Exception {
    testAbortInternal(2);
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

  
  private void testFailAbortInternal(int version)
      throws IOException, InterruptedException {
    JobConf conf = new JobConf();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "faildel:///");
    conf.setClass("fs.faildel.impl", FakeFileSystem.class, FileSystem.class);
    conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
    conf.setInt(org.apache.hadoop.mapreduce.lib.output.
        FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, version);
    conf.setInt(MRConstants.APPLICATION_ATTEMPT_ID, 1);
    FileOutputFormat.setOutputPath(conf, outDir);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    FileOutputCommitter committer = new FileOutputCommitter();

    // do setup
    committer.setupJob(jContext);
    committer.setupTask(tContext);

    // write output
    File jobTmpDir = new File(new Path(outDir,
        FileOutputCommitter.TEMP_DIR_NAME + Path.SEPARATOR +
        conf.getInt(MRConstants.APPLICATION_ATTEMPT_ID, 0) +
        Path.SEPARATOR +
        FileOutputCommitter.TEMP_DIR_NAME).toString());
    File taskTmpDir = new File(jobTmpDir, "_" + taskID);
    File expectedFile = new File(taskTmpDir, partFile);
    TextOutputFormat<?, ?> theOutputFormat = new TextOutputFormat();
    RecordWriter<?, ?> theRecordWriter = 
        theOutputFormat.getRecordWriter(null, conf, 
            expectedFile.getAbsolutePath(), null);
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

  public void testFailAbortV1() throws Exception {
    testFailAbortInternal(1);
  }

  public void testFailAbortV2() throws Exception {
    testFailAbortInternal(2);
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
