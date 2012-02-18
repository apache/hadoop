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
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.security.UserGroupInformation;

public class TestJobDirCleanup extends TestCase {
  //The testcase brings up a cluster with many trackers, and
  //runs a job with a single map and many reduces. The check is 
  //to see whether the job directories are cleaned up at the
  //end of the job (indirectly testing whether all tasktrackers
  //got a KillJobAction).
  private JobID runSleepJob(JobConf conf) throws Exception {
    SleepJob sleep = new SleepJob();
    sleep.setConf(conf);
    Job job = sleep.createJob(1, 10, 1000, 1, 10000, 1);
    job.waitForCompletion(true);
    return job.getJobID();
  }

  public void testJobDirCleanup() throws Exception {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    try {
      final int taskTrackers = 10;
      Configuration conf = new Configuration();
      JobConf mrConf = new JobConf();
      mrConf.set(TTConfig.TT_REDUCE_SLOTS, "1");
      dfs = new MiniDFSCluster(conf, 1, true, null);
      fileSys = dfs.getFileSystem();
      namenode = fileSys.getUri().toString();
      mr = new MiniMRCluster(10, namenode, 3, 
          null, null, mrConf);
      // make cleanup inline sothat validation of existence of these directories
      // can be done
      mr.setInlineCleanupThreads();

      // run the sleep job
      JobConf jobConf = mr.createJobConf();
      JobID jobid = runSleepJob(jobConf);
      
      // verify the job directories are cleaned up.
      verifyJobDirCleanup(mr, taskTrackers, jobid);
    } finally {
      if (fileSys != null) { fileSys.close(); }
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
  }

  static void verifyJobDirCleanup(MiniMRCluster mr, int numTT, JobID jobid)
      throws IOException {
    // wait till killJobAction is sent to all trackers.
    // this loops waits atmost for 10 seconds
    boolean sent = true;
    for (int i = 0; i < 100; i++) {
      sent = true;
      for (int j = 0; j < numTT; j++) {
        if (mr.getTaskTrackerRunner(j).getTaskTracker().getRunningJob(
            org.apache.hadoop.mapred.JobID.downgrade(jobid)) != null) {
          sent = false;
          break;
        }
      }
      if (!sent) {
        UtilsForTests.waitFor(100);
      } else {
        break;
      }
    }
    
    assertTrue("KillJobAction not sent for all trackers", sent);
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    String jobDirStr = TaskTracker.getLocalJobDir(user, jobid.toString());
    for(int i=0; i < numTT; ++i) {
      for (String localDir : mr.getTaskTrackerLocalDirs(i)) {
        File jobDir = new File(localDir, jobDirStr);
        assertFalse(jobDir + " is not cleaned up.", jobDir.exists());
      }
    }
  }
}


