/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;

public class TestSleepJob {

  public static final Log LOG = LogFactory.getLog(Gridmix.class);

  {
    ((Log4JLogger) LogFactory.getLog("org.apache.hadoop.mapred.gridmix"))
      .getLogger().setLevel(Level.DEBUG);
  }

  static GridmixJobSubmissionPolicy policy = GridmixJobSubmissionPolicy.REPLAY;
  private static final int NJOBS = 2;
  private static final long GENDATA = 50; // in megabytes


  @BeforeClass
  public static void init() throws IOException {
    GridmixTestUtils.initCluster();
  }

  @AfterClass
  public static void shutDown() throws IOException {
    GridmixTestUtils.shutdownCluster();
  }

  static class TestMonitor extends JobMonitor {
    private final BlockingQueue<Job> retiredJobs;
    private final int expected;

    public TestMonitor(int expected, Statistics stats) {
      super(stats);
      this.expected = expected;
      retiredJobs = new LinkedBlockingQueue<Job>();
    }

    @Override
    protected void onSuccess(Job job) {
      System.out.println(" Job Sucess " + job);
      retiredJobs.add(job);
    }

    @Override
    protected void onFailure(Job job) {
      fail("Job failure: " + job);
    }

    public void verify(ArrayList<JobStory> submitted) throws Exception  {
      assertEquals("Bad job count", expected, retiredJobs.size());
    }
  }


  static class DebugGridmix extends Gridmix {

    private JobFactory factory;
    private TestMonitor monitor;

    @Override
    protected JobMonitor createJobMonitor(Statistics stats) {
      monitor = new TestMonitor(NJOBS + 1, stats);
      return monitor;
    }

    @Override
    protected JobFactory createJobFactory(
      JobSubmitter submitter, String traceIn, Path scratchDir,
      Configuration conf, CountDownLatch startFlag, UserResolver userResolver)
      throws IOException {
      factory = DebugJobFactory.getFactory(
        submitter, scratchDir, NJOBS, conf, startFlag, userResolver);
      return factory;
    }

    public void checkMonitor() throws Exception {
      monitor.verify(((DebugJobFactory.Debuggable) factory).getSubmitted());
    }
  }


  @Test
  public void testReplaySubmit() throws Exception {
    policy = GridmixJobSubmissionPolicy.REPLAY;
    System.out.println(" Replay started at " + System.currentTimeMillis());
    doSubmission();
    System.out.println(" Replay ended at " + System.currentTimeMillis());
  }

  @Test
  public void testStressSubmit() throws Exception {
    policy = GridmixJobSubmissionPolicy.STRESS;
    System.out.println(" Stress started at " + System.currentTimeMillis());
    doSubmission();
    System.out.println(" Stress ended at " + System.currentTimeMillis());
  }

  @Test
  public void testSerialSubmit() throws Exception {
    policy = GridmixJobSubmissionPolicy.SERIAL;
    System.out.println("Serial started at " + System.currentTimeMillis());
    doSubmission();
    System.out.println("Serial ended at " + System.currentTimeMillis());
  }


  private void doSubmission() throws Exception {
    final Path in = new Path("foo").makeQualified(GridmixTestUtils.dfs);
    final Path out = GridmixTestUtils.DEST.makeQualified(GridmixTestUtils.dfs);
    final Path root = new Path("/user");
    Configuration conf = null;
    try {
      final String[] argv = {"-D" + FilePool.GRIDMIX_MIN_FILE + "=0",
        "-D" + Gridmix.GRIDMIX_OUT_DIR + "=" + out,
        "-D" + Gridmix.GRIDMIX_USR_RSV + "=" + EchoUserResolver.class.getName(),
        "-D" + JobCreator.GRIDMIX_JOB_TYPE + "=" + JobCreator.SLEEPJOB.name(),
        "-D" + SleepJob.GRIDMIX_SLEEP_INTERVAL +"=" +"10",
        "-generate",String.valueOf(GENDATA) + "m", in.toString(), "-"
        // ignored by DebugGridmix
      };
      DebugGridmix client = new DebugGridmix();
      conf = new Configuration();
      conf.setEnum(GridmixJobSubmissionPolicy.JOB_SUBMISSION_POLICY, policy);
      conf = GridmixTestUtils.mrCluster.createJobConf(new JobConf(conf));
//    GridmixTestUtils.createHomeAndStagingDirectory((JobConf)conf);
      // allow synthetic users to create home directories
      GridmixTestUtils.dfs.mkdirs(root, new FsPermission((short) 0777));
      GridmixTestUtils.dfs.setPermission(root, new FsPermission((short) 0777));
      int res = ToolRunner.run(conf, client, argv);
      assertEquals("Client exited with nonzero status", 0, res);
      client.checkMonitor();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      in.getFileSystem(conf).delete(in, true);
      out.getFileSystem(conf).delete(out, true);
      root.getFileSystem(conf).delete(root, true);
    }
  }

}
