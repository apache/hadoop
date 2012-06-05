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
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.util.ToolRunner;

/**
 * Test a java-based mapred job with LinuxTaskController running the jobs as a
 * user different from the user running the cluster. See
 * {@link ClusterWithLinuxTaskController}
 */
public class TestJobExecutionAsDifferentUser extends
    ClusterWithLinuxTaskController {

  public void testJobExecution()
      throws Exception {
    if (!shouldRun()) {
      return;
    }
    startCluster();

    
    jobOwner.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        Path inDir = new Path("input");
        Path outDir = new Path("output");

        RunningJob job;
        // Run a job with zero maps/reduces
        job = UtilsForTests.runJob(getClusterConf(), inDir, outDir, 0, 0);
        job.waitForCompletion();
        assertTrue("Job failed", job.isSuccessful());
        assertOwnerShip(outDir);

        // Run a job with 1 map and zero reduces
        job = UtilsForTests.runJob(getClusterConf(), inDir, outDir, 1, 0);
        job.waitForCompletion();
        assertTrue("Job failed", job.isSuccessful());
        assertOwnerShip(outDir);

        // Run a normal job with maps/reduces
        job = UtilsForTests.runJob(getClusterConf(), inDir, outDir, 1, 1);
        job.waitForCompletion();
        assertTrue("Job failed", job.isSuccessful());
        assertOwnerShip(outDir);

        // Run a job with jvm reuse
        JobConf myConf = getClusterConf();
        myConf.set(JobContext.JVM_NUMTASKS_TORUN, "-1");
        String[] args = { "-m", "6", "-r", "3", "-mt", "1000", "-rt", "1000" };
        assertEquals(0, ToolRunner.run(myConf, new SleepJob(), args)); 
        return null;
      }
    });

  }
  
  public void testEnvironment() throws Exception {
    if (!shouldRun()) {
      return;
    }
    startCluster();
    jobOwner.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {

        TestMiniMRChildTask childTask = new TestMiniMRChildTask();
        Path inDir = new Path("input1");
        Path outDir = new Path("output1");
        try {
          childTask.runTestTaskEnv(getClusterConf(), inDir, outDir, false);
        } catch (IOException e) {
          fail("IOException thrown while running enviroment test."
              + e.getMessage());
        } finally {
          FileSystem outFs = outDir.getFileSystem(getClusterConf());
          if (outFs.exists(outDir)) {
            assertOwnerShip(outDir);
            outFs.delete(outDir, true);
          } else {
            fail("Output directory does not exist" + outDir.toString());
          }
          return null;
        }
      }
    });
  }

  /** Ensure that SIGQUIT can be properly sent by the LinuxTaskController
   * if a task times out.
   */
  public void testTimeoutStackTrace() throws Exception {
    if (!shouldRun()) {
      return;
    }

    // Run a job that should timeout and trigger a SIGQUIT.
    startCluster();
    jobOwner.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        JobConf conf = getClusterConf();
        conf.setInt(JobContext.TASK_TIMEOUT, 10000);
        conf.setInt(Job.COMPLETION_POLL_INTERVAL_KEY, 50);
        SleepJob sleepJob = new SleepJob();
        sleepJob.setConf(conf);
        Job job = sleepJob.createJob(1, 0, 30000, 1, 0, 0);
        job.setMaxMapAttempts(1);
        int prevNumSigQuits = MyLinuxTaskController.attemptedSigQuits;
        job.waitForCompletion(true);
        assertTrue("Did not detect a new SIGQUIT!",
            prevNumSigQuits < MyLinuxTaskController.attemptedSigQuits);
        assertEquals("A SIGQUIT attempt failed!", 0,
            MyLinuxTaskController.failedSigQuits);
        return null;
      }
    });
  }
}
