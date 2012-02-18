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
import java.io.FileInputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.SleepJob;

/**
 * A JUnit test to test Kill Job & Fail Job functionality with local file
 * system.
 */
public class TestJobKillAndFail extends TestCase {

  static final Log LOG = LogFactory.getLog(TestJobKillAndFail.class);

  private static String TEST_ROOT_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")).toURI().toString().replace(' ', '+');

  /**
   * TaskController instance that just sets a flag when a stack dump
   * is performed in a child thread.
   */
  static class MockStackDumpTaskController extends DefaultTaskController {

    static volatile int numStackDumps = 0;

    static final Log LOG = LogFactory.getLog(TestJobKillAndFail.class);

    public MockStackDumpTaskController() {
      LOG.info("Instantiated MockStackDumpTC");
    }

    @Override
    void dumpTaskStack(TaskControllerContext context) {
      LOG.info("Got stack-dump request in TaskController");
      MockStackDumpTaskController.numStackDumps++;
      super.dumpTaskStack(context);
    }

  }

  /** If a task was killed, then dumpTaskStack() should have been
    * called. Test whether or not the counter was incremented
    * and succeed/fail based on this. */
  private void checkForStackDump(boolean expectDump, int lastNumDumps) {
    int curNumDumps = MockStackDumpTaskController.numStackDumps;

    LOG.info("curNumDumps=" + curNumDumps + "; lastNumDumps=" + lastNumDumps
        + "; expect=" + expectDump);

    if (expectDump) {
      assertTrue("No stack dump recorded!", lastNumDumps < curNumDumps);
    } else {
      assertTrue("Stack dump happened anyway!", lastNumDumps == curNumDumps);
    }
  }

  public void testJobFailAndKill() throws Exception {
    MiniMRCluster mr = null;
    try {
      JobConf jtConf = new JobConf();
      jtConf.set("mapred.jobtracker.instrumentation", 
          JTInstrumentation.class.getName());
      jtConf.set("mapreduce.tasktracker.taskcontroller",
          MockStackDumpTaskController.class.getName());
      mr = new MiniMRCluster(2, "file:///", 3, null, null, jtConf);
      JTInstrumentation instr = (JTInstrumentation) 
        mr.getJobTrackerRunner().getJobTracker().getInstrumentation();

      // run the TCs
      JobConf conf = mr.createJobConf();
      conf.setInt(Job.COMPLETION_POLL_INTERVAL_KEY, 50);
      
      Path inDir = new Path(TEST_ROOT_DIR + "/failkilljob/input");
      Path outDir = new Path(TEST_ROOT_DIR + "/failkilljob/output");
      RunningJob runningJob = UtilsForTests.runJobFail(conf, inDir, outDir);
      // Checking that the Job got failed
      assertEquals(runningJob.getJobState(), JobStatus.FAILED);
      assertTrue(instr.verifyJob());
      assertEquals(1, instr.failed);
      instr.reset();

      int prevNumDumps = MockStackDumpTaskController.numStackDumps;
      runningJob = UtilsForTests.runJobKill(conf, inDir, outDir);
      // Checking that the Job got killed
      assertTrue(runningJob.isComplete());
      assertEquals(runningJob.getJobState(), JobStatus.KILLED);
      assertTrue(instr.verifyJob());
      assertEquals(1, instr.killed);
      // check that job kill does not put a stacktrace in task logs.
      checkForStackDump(false, prevNumDumps);

      // Test that a task that times out does have a stack trace
      conf = mr.createJobConf();
      conf.setInt(JobContext.TASK_TIMEOUT, 10000);
      conf.setInt(Job.COMPLETION_POLL_INTERVAL_KEY, 50);
      SleepJob sleepJob = new SleepJob();
      sleepJob.setConf(conf);
      Job job = sleepJob.createJob(1, 0, 30000, 1,0, 0);
      job.setMaxMapAttempts(1);
      prevNumDumps = MockStackDumpTaskController.numStackDumps;
      job.waitForCompletion(true);
      checkForStackDump(true, prevNumDumps);
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }
  
  static class JTInstrumentation extends JobTrackerInstrumentation {
    volatile int failed;
    volatile int killed;
    volatile int addPrep;
    volatile int decPrep;
    volatile int addRunning;
    volatile int decRunning;

    void reset() {
      failed = 0;
      killed = 0;
      addPrep = 0;
      decPrep = 0;
      addRunning = 0;
      decRunning = 0;
    }

    boolean verifyJob() {
      return addPrep==1 && decPrep==1 && addRunning==1 && decRunning==1;
    }

    public JTInstrumentation(JobTracker jt, JobConf conf) {
      super(jt, conf);
    }

    public synchronized void addPrepJob(JobConf conf, JobID id) 
    {
      addPrep++;
    }
    
    public synchronized void decPrepJob(JobConf conf, JobID id) 
    {
      decPrep++;
    }

    public synchronized void addRunningJob(JobConf conf, JobID id) 
    {
      addRunning++;
    }

    public synchronized void decRunningJob(JobConf conf, JobID id) 
    {
      decRunning++;
    }
    
    public synchronized void failedJob(JobConf conf, JobID id) 
    {
      failed++;
    }

    public synchronized void killedJob(JobConf conf, JobID id) 
    {
      killed++;
    }
  }
  
}
