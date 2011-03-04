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

import java.util.Collection;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.test.system.FinishTaskControlAction;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.TTInfo;
import org.apache.hadoop.mapreduce.test.system.TTTaskInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCluster {

  private static final Log LOG = LogFactory.getLog(TestCluster.class);

  private static MRCluster cluster;

  public TestCluster() throws Exception {
    
  }

  @BeforeClass
  public static void before() throws Exception {
    cluster = MRCluster.createCluster(new Configuration());
    cluster.setUp();
  }

  @AfterClass
  public static void after() throws Exception {
    cluster.tearDown();
  }

  @Test
  public void testProcessInfo() throws Exception {
    LOG.info("Process info of JobTracker is : "
        + cluster.getJTClient().getProcessInfo());
    Assert.assertNotNull(cluster.getJTClient().getProcessInfo());
    Collection<TTClient> tts = cluster.getTTClients();
    for (TTClient tt : tts) {
      LOG.info("Process info of TaskTracker is : " + tt.getProcessInfo());
      Assert.assertNotNull(tt.getProcessInfo());
    }
  }
  
  @Test
  public void testJobSubmission() throws Exception {
    Configuration conf = new Configuration(cluster.getConf());
    SleepJob job = new SleepJob();
    job.setConf(conf);
    conf = job.setupJobConf(1, 1, 100, 100, 100, 100);
    RunningJob rJob = cluster.getJTClient().submitAndVerifyJob(conf);
    cluster.getJTClient().verifyJobHistory(rJob.getID());
  }

  @Test
  public void testFileStatus() throws Exception {
    JTClient jt = cluster.getJTClient();
    String dir = ".";
    checkFileStatus(jt.getFileStatus(dir, true));
    checkFileStatus(jt.listStatus(dir, false, true), dir);
    for (TTClient tt : cluster.getTTClients()) {
      String[] localDirs = tt.getMapredLocalDirs();
      for (String localDir : localDirs) {
        checkFileStatus(tt.listStatus(localDir, true, false), localDir);
        checkFileStatus(tt.listStatus(localDir, true, true), localDir);
      }
    }
    String systemDir = jt.getClient().getSystemDir().toString();
    checkFileStatus(jt.listStatus(systemDir, false, true), systemDir);
    checkFileStatus(jt.listStatus(jt.getLogDir(), true, true), jt.getLogDir());
  }

  private void checkFileStatus(FileStatus[] fs, String path) {
    Assert.assertNotNull(fs);
    LOG.info("-----Listing for " + path + "  " + fs.length);
    for (FileStatus fz : fs) {
      checkFileStatus(fz);
    }
  }

  private void checkFileStatus(FileStatus fz) {
    Assert.assertNotNull(fz);
    LOG.info("FileStatus is " + fz.getPath() 
        + "  " + fz.getPermission()
        +"  " + fz.getOwner()
        +"  " + fz.getGroup()
        +"  " + fz.getClass());
  }

  /**
   * Test to showcase how to get a task status from a TaskTracker.
   * Does the following;
   * 1. Contacts the job tracker to get TaskInfo
   * 2. Uses taskinfo to get list of tts
   * 3. Contacts TT and gets task info.
   * 
   * Care should be taken that the task which you are searching
   * can need not be around.
   * @throws Exception
   */
  @Test
  public void testTaskStatus() throws Exception {
    Configuration conf = new Configuration(cluster.getConf());
    JTProtocol wovenClient = cluster.getJTClient().getProxy();
    FinishTaskControlAction.configureControlActionForJob(conf);
    SleepJob job = new SleepJob();
    job.setConf(conf);

    conf = job.setupJobConf(1, 0, 100, 100, 100, 100);
    JobClient client = cluster.getJTClient().getClient();

    RunningJob rJob = client.submitJob(new JobConf(conf));
    JobID id = rJob.getID();

    JobInfo jInfo = wovenClient.getJobInfo(id);

    while (jInfo.getStatus().getRunState() != JobStatus.RUNNING) {
      Thread.sleep(1000);
      jInfo = wovenClient.getJobInfo(id);
    }

    LOG.info("Waiting till job starts running one map");

    TaskInfo[] myTaskInfos = wovenClient.getTaskInfo(id);
    for(TaskInfo info : myTaskInfos) {
      if(!info.isSetupOrCleanup()) {
        String[] taskTrackers = info.getTaskTrackers();
        for(String taskTracker : taskTrackers) {
          TTInfo ttInfo = wovenClient.getTTInfo(taskTracker);
          TTClient ttCli =  cluster.getTTClient(ttInfo.getStatus().getHost());
          TTTaskInfo ttTaskInfo = ttCli.getProxy().getTask(info.getTaskID());
          Assert.assertNotNull(ttTaskInfo);
          FinishTaskControlAction action = new FinishTaskControlAction(
              TaskID.downgrade(info.getTaskID()));
          ttCli.getProxy().sendAction(action);
        }
      }
    }
    rJob.killJob();
  }
}
