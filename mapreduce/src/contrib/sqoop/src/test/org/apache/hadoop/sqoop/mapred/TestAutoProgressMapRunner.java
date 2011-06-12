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

package org.apache.hadoop.sqoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.sqoop.manager.ConnManager;
import org.apache.hadoop.sqoop.manager.ManagerFactory;

import junit.framework.TestCase;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Test the AutoProgressMapRunner implementation and prove that it makes
 * progress updates when the mapper itself isn't.
 */
public class TestAutoProgressMapRunner extends TestCase {

  /** Parameter: how long should each map() call sleep for? */
  public static final String MAPPER_SLEEP_INTERVAL_KEY = "sqoop.test.mapper.sleep.ival";


  /** Mapper that just sleeps for a configurable amount of time. */
  public static class SleepingMapper<K1, V1, K2, V2> extends MapReduceBase
      implements Mapper<K1, V1, K2, V2> {

    private int sleepInterval;

    public void configure(JobConf job) {
      this.sleepInterval = job.getInt(MAPPER_SLEEP_INTERVAL_KEY, 100);
    }

    public void map(K1 k, V1 v, OutputCollector<K2, V2> out, Reporter r) throws IOException {
      while (true) {
        try {
          Thread.sleep(this.sleepInterval);
          break;
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  /** Mapper that sleeps for 1 second then fails. */
  public static class FailingMapper<K1, V1, K2, V2> extends MapReduceBase
      implements Mapper<K1, V1, K2, V2> {

    public void map(K1 k, V1 v, OutputCollector<K2, V2> out, Reporter r) throws IOException {
      throw new IOException("Causing job failure.");
    }
  }

  private final Path inPath  = new Path("./input");
  private final Path outPath = new Path("./output");

  private MiniMRCluster mr = null;
  private FileSystem fs = null;

  private final static int NUM_NODES = 1;

  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    fs = FileSystem.get(conf);
    mr = new MiniMRCluster(NUM_NODES, fs.getUri().toString(), 1, null, null, new JobConf(conf));

    // Create a file to use as a dummy input
    DataOutputStream os = fs.create(new Path(inPath, "part-0"));
    os.writeBytes("This is a line of text.");
    os.close();
  }

  public void tearDown() throws IOException {
    if (null != fs) {
      fs.delete(inPath, true);
      fs.delete(outPath, true);
    }

    if (null != mr) {
      mr.shutdown();
      this.mr = null;
    }
  }


  /**
   * Test that even if the mapper just sleeps, the auto-progress thread keeps it all alive
   */
  public void testBackgroundProgress() throws IOException {
    // Report progress every 2.5 seconds.
    final int REPORT_INTERVAL = 2500;

    // Tasks need to report progress once every ten seconds.
    final int TASK_KILL_TIMEOUT = 4 * REPORT_INTERVAL;

    // Create and run the job.
    JobConf job = mr.createJobConf();

    // Set the task timeout to be pretty strict.
    job.setInt("mapred.task.timeout", TASK_KILL_TIMEOUT);

    // Set the mapper itself to block for long enough that it should be killed on its own.
    job.setInt(MAPPER_SLEEP_INTERVAL_KEY, 2 * TASK_KILL_TIMEOUT);

    // Report progress frequently..
    job.setInt(AutoProgressMapRunner.SLEEP_INTERVAL_KEY, REPORT_INTERVAL);
    job.setInt(AutoProgressMapRunner.REPORT_INTERVAL_KEY, REPORT_INTERVAL);

    job.setMapRunnerClass(AutoProgressMapRunner.class);
    job.setMapperClass(SleepingMapper.class);

    job.setNumReduceTasks(0);
    job.setNumMapTasks(1);

    FileInputFormat.addInputPath(job, inPath);
    FileOutputFormat.setOutputPath(job, outPath);

    RunningJob runningJob = JobClient.runJob(job);
    runningJob.waitForCompletion();

    assertEquals("Sleep job failed!", JobStatus.SUCCEEDED, runningJob.getJobState());
  }

  /** Test that if the mapper bails early, we shut down the progress thread
      in a timely fashion.
    */
  public void testEarlyExit() throws IOException {
    JobConf job = mr.createJobConf();

    final int REPORT_INTERVAL = 30000;

    job.setInt(AutoProgressMapRunner.SLEEP_INTERVAL_KEY, REPORT_INTERVAL);
    job.setInt(AutoProgressMapRunner.REPORT_INTERVAL_KEY, REPORT_INTERVAL);

    job.setNumReduceTasks(0);
    job.setNumMapTasks(1);

    job.setInt("mapred.map.max.attempts", 1);

    job.setMapRunnerClass(AutoProgressMapRunner.class);
    job.setMapperClass(FailingMapper.class);

    FileInputFormat.addInputPath(job, inPath);
    FileOutputFormat.setOutputPath(job, outPath);

    RunningJob runningJob = null;
    long startTime = System.currentTimeMillis();
    try {
      runningJob = JobClient.runJob(job);
      runningJob.waitForCompletion();
      assertEquals("Failing job succeded!", JobStatus.FAILED, runningJob.getJobState());
    } catch(IOException ioe) {
      // Expected
    }

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    assertTrue("Job took too long to clean up (" + duration + ")",
        duration < (REPORT_INTERVAL * 2));
  }
}
