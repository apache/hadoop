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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.util.ToolRunner;

public class TestJobDirCleanup extends TestCase {
  //The testcase brings up a cluster with many trackers, and
  //runs a job with a single map and many reduces. The check is 
  //to see whether the job directories are cleaned up at the
  //end of the job (indirectly testing whether all tasktrackers
  //got a KillJobAction).
  private static final Log LOG =
    LogFactory.getLog(TestEmptyJob.class.getName());
  private void runSleepJob(JobConf conf) throws Exception {
    String[] args = { "-m", "1", "-r", "10", "-mt", "1000", "-rt", "10000" };
    ToolRunner.run(conf, new SleepJob(), args);
  }
  public void testJobDirCleanup() throws IOException {
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
      final String jobTrackerName = "localhost:" + mr.getJobTrackerPort();
      JobConf jobConf = mr.createJobConf();
      runSleepJob(jobConf);
      verifyJobDirCleanup(mr, taskTrackers);
    } catch (Exception ee){
      assertTrue(false);
    } finally {
      if (fileSys != null) { fileSys.close(); }
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
  }

  static void verifyJobDirCleanup(MiniMRCluster mr, int numTT)
  throws IOException {
    for(int i=0; i < numTT; ++i) {
      String jobDirStr = mr.getTaskTrackerLocalDir(i)+
      "/taskTracker/jobcache";
      File jobDir = new File(jobDirStr);
      String[] contents = jobDir.list();
      if (contents == null || contents.length == 0) {
        return;
      }
      while (contents.length > 0) {
        try {
          Thread.sleep(1000);
          LOG.warn(jobDir +" not empty yet, contents are");
          for (String s: contents) {
            LOG.info(s);
          }
          contents = jobDir.list();
        } catch (InterruptedException ie){}
      }
    }
  }
}


