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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.TestJobInProgressListener.MyListener;
import org.apache.hadoop.mapred.UtilsForTests.FailMapper;
import org.apache.hadoop.mapred.UtilsForTests.KillMapper;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.security.UserGroupInformation;

/** 
 * This is a test case that tests several miscellaneous functionality. 
 * This is intended for a fast test and encompasses the following:
 * TestJobName
 * TestJobClient
 * TestJobDirCleanup
 * TestJobKillAndFail
 * TestUserDefinedCounters
 * TestJobInProgressListener
 * TestJobHistory
 * TestMiniMRClassPath
 * TestMiniMRWithDFSWithDistinctUsers
 */

@SuppressWarnings("deprecation")
public class TestSeveral extends TestCase {

  static final UserGroupInformation DFS_UGI = 
    TestMiniMRWithDFSWithDistinctUsers.createUGI("dfs", true); 
  static final UserGroupInformation TEST1_UGI = 
    TestMiniMRWithDFSWithDistinctUsers.createUGI("pi", false); 
  static final UserGroupInformation TEST2_UGI = 
    TestMiniMRWithDFSWithDistinctUsers.createUGI("wc", false);

  private static MiniMRCluster mrCluster = null;
  private static MiniDFSCluster dfs = null;
  private static FileSystem fs = null;
  private static MyListener myListener = null;

  private int numReduces = 5;
  private static final int numTT = 5;

  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestSeveral.class)) {
      protected void setUp() throws Exception {

        Configuration conf = new Configuration();
        conf.setInt("dfs.replication", 1);
        dfs = new MiniDFSCluster(conf, numTT, true, null);
        fs = DFS_UGI.doAs(new PrivilegedExceptionAction<FileSystem>() {
          public FileSystem run() throws IOException {
            return dfs.getFileSystem();
          }
        });

        TestMiniMRWithDFSWithDistinctUsers.mkdir(fs, "/user");
        TestMiniMRWithDFSWithDistinctUsers.mkdir(fs, "/mapred");
        TestMiniMRWithDFSWithDistinctUsers.mkdir(fs, 
            conf.get(JTConfig.JT_STAGING_AREA_ROOT));

        UserGroupInformation MR_UGI = UserGroupInformation.getLoginUser(); 

        // Create a TestJobInProgressListener.MyListener and associate
        // it with the MiniMRCluster

        myListener = new MyListener();
        conf.set(JTConfig.JT_IPC_HANDLER_COUNT, "1");
        mrCluster =   new MiniMRCluster(0, 0,
            numTT, fs.getUri().toString(), 
            1, null, null, MR_UGI, new JobConf());
        // make cleanup inline sothat validation of existence of these directories
        // can be done
        mrCluster.setInlineCleanupThreads();

        mrCluster.getJobTrackerRunner().getJobTracker()
        .addJobInProgressListener(myListener);
      }
      
      protected void tearDown() throws Exception {
        if (fs != null) { fs.close(); }
        if (dfs != null) { dfs.shutdown(); }
        if (mrCluster != null) { mrCluster.shutdown(); }
      }
    };
    return setup;
  }

  /** 
   * Utility class to create input for the jobs
   * @param inDir
   * @param conf
   * @throws IOException
   */
  private void makeInput(Path inDir, JobConf conf) throws IOException {
    FileSystem inFs = inDir.getFileSystem(conf);

    if (inFs.exists(inDir)) {
      inFs.delete(inDir, true);
    }
    inFs.mkdirs(inDir);
    Path inFile = new Path(inDir, "part-0");
    DataOutputStream file = inFs.create(inFile);
    for (int i = 0; i < numReduces; i++) {
      file.writeBytes("b a\n");
    }
    file.close();
  }

  /**
   * Clean the Output directories before running a Job
   * @param fs
   * @param outDir
   */
  private void clean(FileSystem fs, Path outDir) {
    try {
      fs.delete(outDir, true);
    } catch (Exception e) {}
  }

  private void verifyOutput(FileSystem fs, Path outDir) throws IOException {
    Path[] outputFiles = FileUtil.stat2Paths(
        fs.listStatus(outDir, new Utils.OutputFileUtils.OutputFilesFilter()));
    assertEquals(numReduces, outputFiles.length);
    InputStream is = fs.open(outputFiles[0]);
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    String s = reader.readLine().split("\t")[1];
    assertEquals("b a",s);
    assertNull(reader.readLine());
    reader.close();
  }


  @SuppressWarnings("unchecked")
  static class DoNothingReducer extends MapReduceBase implements 
  Reducer<WritableComparable, Writable, WritableComparable, Writable> {
    public void reduce(WritableComparable key, Iterator<Writable> val, 
        OutputCollector<WritableComparable, Writable> output,
        Reporter reporter)
    throws IOException { // Do nothing
    }
  }

  /**
   * Submit a job with a complex name (TestJobName.testComplexName)
   * Check the status of the job as successful (TestJobKillAndFail)
   * Check that the task tracker directory is cleaned up (TestJobDirCleanup)
   * Create some user defined counters and check them (TestUserDefinedCounters)
   * Job uses a reducer from an External Jar (TestMiniMRClassPath)
   * Check task directories (TestMiniMRWithDFS)
   * Check if the listener notifications are received(TestJobInProgressListener)
   * Verify if priority changes to the job are reflected (TestJobClient)
   * Validate JobHistory file format, content, userlog location (TestJobHistory)
   * 
   * @throws Exception
   */
  public void testSuccessfulJob() throws Exception {
    final JobConf conf = mrCluster.createJobConf();

    // Set a complex Job name (TestJobName)
    conf.setJobName("[name][some other value that gets" +
    " truncated internally that this test attempts to aggravate]");
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setCompressMapOutput(true);

    // Set the Mapper class to a Counting Mapper that defines user
    // defined counters
    conf.setMapperClass(TestUserDefinedCounters.CountingMapper.class);

    conf.set("mapred.reducer.class", "testjar.ExternalIdentityReducer");

    conf.setLong(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.SPLIT_MINSIZE, 1024*1024);

    conf.setNumReduceTasks(numReduces);
    conf.setJobPriority(JobPriority.HIGH);
    conf.setJar("build/test/mapred/testjar/testjob.jar");

    String pattern = 
      TaskAttemptID.getTaskAttemptIDsPattern(null, null, TaskType.MAP, 1, null);
    conf.setKeepTaskFilesPattern(pattern);

    final Path inDir = new Path("./test/input");
    final Path outDir = new Path("./test/output");

    TEST1_UGI.doAs(new PrivilegedExceptionAction<Void>() {
      public Void run() {
        FileInputFormat.setInputPaths(conf, inDir);
        FileOutputFormat.setOutputPath(conf, outDir);   
        return null;
      }
    });

    clean(fs, outDir);
    final RunningJob job = TEST1_UGI.doAs(new PrivilegedExceptionAction<RunningJob>() {
      public RunningJob run() throws IOException {
        makeInput(inDir, conf);
        JobClient jobClient = new JobClient(conf);
        return jobClient.submitJob(conf);
      }
    });
    
    final JobID jobId = job.getID();

    while (job.getJobState() != JobStatus.RUNNING) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        break;
      }
    }

    // Check for JobInProgress Listener notification
    assertFalse("Missing event notification for a running job", 
        myListener.contains(jobId, true));

    job.waitForCompletion();

    assertTrue(job.isComplete());
    assertEquals(JobStatus.SUCCEEDED,job.getJobState());

    // check if the job success was notified
    assertFalse("Missing event notification for a successful job", 
        myListener.contains(jobId, false));

    // Check Task directories
    TaskAttemptID taskid = new TaskAttemptID(
        new TaskID(jobId, TaskType.MAP, 1),0);
    TestMiniMRWithDFS.checkTaskDirectories(mrCluster, TEST1_UGI.getUserName(),
        new String[] { jobId.toString() }, new String[] { taskid.toString() });

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int exitCode = TestJobClient.runTool(conf, new JobClient(),
        new String[] { "-counter", jobId.toString(),
      "org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS" },
      out);
    assertEquals(0, exitCode);
    assertEquals(numReduces, Integer.parseInt(out.toString().trim()));

    // Verify if user defined counters have been updated properly
    TestUserDefinedCounters.verifyCounters(job, numTT);

    // Verify job priority change (TestJobClient)
    TestJobClient.verifyJobPriority(jobId.toString(), "HIGH", conf);
    
    // Basic check if the job did run fine
    TEST1_UGI.doAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws IOException {
        verifyOutput(outDir.getFileSystem(conf), outDir);


        //TestJobHistory
        TestJobHistory.validateJobHistoryFileFormat(
            mrCluster.getJobTrackerRunner().getJobTracker().getJobHistory(),
            jobId, conf, "SUCCEEDED", false);

        TestJobHistory.validateJobHistoryFileContent(mrCluster, job, conf);

        // Since we keep setKeepTaskFilesPattern, these files should still be
        // present and will not be cleaned up.
        for(int i=0; i < numTT; ++i) {
          Path jobDirPath =
            new Path(mrCluster.getTaskTrackerLocalDir(i), TaskTracker
                .getJobCacheSubdir(TEST1_UGI.getUserName()));
          boolean b = FileSystem.getLocal(conf).delete(jobDirPath, true);
          assertTrue(b);
        }
        return null;
      }
    });
    
  }

  /**
   * Submit a job with BackSlashed name (TestJobName) that will fail
   * Test JobHistory User Location to none (TetsJobHistory)
   * Verify directory up for the Failed Job (TestJobDirCleanup)
   * Verify Event is generated for the failed job (TestJobInProgressListener)
   * 
   * @throws Exception
   */
  public void testFailedJob() throws Exception {
    JobConf conf = mrCluster.createJobConf();

    // Name with regex
    conf.setJobName("name \\Evalue]");

    conf.setInputFormat(TextInputFormat.class);

    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(FailMapper.class);
    conf.setOutputFormat(NullOutputFormat.class);
    conf.setJobPriority(JobPriority.HIGH);

    conf.setLong(JobContext.MAP_MAX_ATTEMPTS, 1);

    conf.set(JobContext.HISTORY_LOCATION, "none");

    conf.setNumReduceTasks(0);

    final Path inDir = new Path("./wc/input");
    final Path outDir = new Path("./wc/output");

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);

    clean(fs, outDir);
    makeInput(inDir, conf);

    JobClient jobClient = new JobClient(conf);
    RunningJob job = jobClient.submitJob(conf);
    JobID jobId = job.getID();
    job.waitForCompletion();

    assertTrue(job.isComplete());
    assertEquals(JobStatus.FAILED, job.getJobState());

    // check if the job failure was notified
    assertFalse("Missing event notification on failing a running job", 
        myListener.contains(jobId));

    TestJobDirCleanup.verifyJobDirCleanup(mrCluster, numTT, job.getID());
  }

  /**
   * Submit a job that will get Killed with a Regex Name (TestJobName)
   * Verify Job Directory Cleanup (TestJobDirCleanup)
   * Verify Even is generated for Killed Job (TestJobInProgressListener)
   * 
   * @throws Exception
   */
  public void testKilledJob() throws Exception {
    JobConf conf = mrCluster.createJobConf();

    // Name with regex
    conf.setJobName("name * abc + Evalue]");

    conf.setInputFormat(TextInputFormat.class);

    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(KillMapper.class);
    conf.setOutputFormat(NullOutputFormat.class);
    conf.setNumReduceTasks(0);

    conf.setLong(JobContext.MAP_MAX_ATTEMPTS, 2);

    final Path inDir = new Path("./wc/input");
    final Path outDir = new Path("./wc/output");
    final Path histDir = new Path("./wc/history");

    conf.set(JobContext.HISTORY_LOCATION, histDir.toString());

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);

    clean(fs, outDir);
    makeInput(inDir, conf);

    JobClient jobClient = new JobClient(conf);
    RunningJob job = jobClient.submitJob(conf);

    while (job.getJobState() != JobStatus.RUNNING) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        break;
      }
    }
    job.killJob();

    job.waitForCompletion();

    assertTrue(job.isComplete());
    assertEquals(JobStatus.KILLED, job.getJobState());

    // check if the job failure was notified
    assertFalse("Missing event notification on killing a running job", 
        myListener.contains(job.getID()));

    TestJobDirCleanup.verifyJobDirCleanup(mrCluster, numTT, job.getID());
  }

}


