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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobTracker.SafeModeAction;
import org.apache.hadoop.mapred.tools.MRAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** 
 * A test for JobTracker safemode. In safemode, no tasks are scheduled, and
 * no tasks are marked as failed (they are killed instead).
 */
public class TestJobTrackerQuiescence {
  final Path testDir = 
    new Path(System.getProperty("test.build.data", "/tmp"), "jt-safemode");
  final Path inDir = new Path(testDir, "input");
  final Path shareDir = new Path(testDir, "share");
  final Path outputDir = new Path(testDir, "output");
  
  final int maxMapTasks = 1;
  
  private MiniDFSCluster dfs;
  private MiniMRCluster mr;
  private FileSystem fileSys;
  private JobTracker jt;
  
  private static final Log LOG = 
    LogFactory.getLog(TestJobTrackerQuiescence.class);
  
  @Before
  public void setUp() throws IOException {
    
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.replication.considerLoad", false);
    dfs = new MiniDFSCluster(conf, 1, true, null, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    
    // clean up
    fileSys.delete(testDir, true);
    
    if (!fileSys.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }

    // Write the input file
    UtilsForTests.writeFile(dfs.getNameNode(), conf, 
                            new Path(inDir + "/file"), (short)1);

    dfs.startDataNodes(conf, 1, true, null, null, null, null);
    dfs.waitActive();
    String namenode = (dfs.getFileSystem()).getUri().getHost() + ":" 
    + (dfs.getFileSystem()).getUri().getPort();
    
    JobConf jtConf = new JobConf();
    jtConf.setInt("mapred.tasktracker.map.tasks.maximum", maxMapTasks);
    jtConf.setInt("mapred.tasktracker.reduce.tasks.maximum", 1);
    jtConf.setBoolean(JobTracker.JT_HDFS_MONITOR_ENABLE, true);
    jtConf.setInt(JobTracker.JT_HDFS_MONITOR_THREAD_INTERVAL, 1000);
    mr = new MiniMRCluster(1, namenode, 1, null, null, jtConf);
    mr.waitUntilIdle();
    mr.setInlineCleanupThreads();
    jt = mr.getJobTrackerRunner().getJobTracker();
  }
  
  @After
  public void tearDown() {
    if (mr != null) {
      try {
        mr.shutdown();
      } catch (Exception e) {}
    }
    if (dfs != null) {
      try {
        dfs.shutdown();
      } catch (Exception e) {}
    }
  }
  
  @Test
  public void testHDFSMonitor() throws Exception {
    /*
     * Try 'automatic' safe-mode 
     */
    // Put HDFS in safe-mode
    dfs.getNameNode().setSafeMode(
        org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction.SAFEMODE_ENTER);
    int numTries = 20;
    while (!jt.isInSafeMode() && numTries > 0) {
      Thread.sleep(1000);
      --numTries;
    }
    
    // By now JT should be in safe-mode
    assertEquals(true, jt.isInSafeMode());
      
    // Remove HDFS from safe-mode
    dfs.getNameNode().setSafeMode(
        org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction.SAFEMODE_LEAVE);
    
    numTries = 20;
    while (jt.isInSafeMode() && numTries > 0) {
      Thread.sleep(1000);
      --numTries;
    }
    
    // By now JT should not be in safe-mode
    assertEquals(false, jt.isInSafeMode());
      
    /*
     * Now ensure 'automatic' mode doesn't interfere with 'admin set' safe-mode
     */
    dfs.getNameNode().setSafeMode(
        org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction.SAFEMODE_ENTER);
    numTries = 20;
    while (!jt.isInSafeMode() && numTries > 0) {
      Thread.sleep(1000);
      --numTries;
    }
    
    // By now JT should be in safe-mode
    assertEquals(true, jt.isInSafeMode());

    // Now, put JT in admin set safe-mode
    enterSafeMode();
    
    // Bring HDFS back from safe-mode
    dfs.getNameNode().setSafeMode(
        org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction.SAFEMODE_LEAVE);
    
    numTries = 20;
    while (jt.isInSafeMode() && numTries > 0) {
      Thread.sleep(1000);
      --numTries;
    }
    
    // But now JT should *still* be in safe-mode
    assertEquals(true, jt.isInSafeMode());
    assertEquals(true, jt.isInAdminSafeMode());
    
    // Leave JT safe-mode
    leaveSafeMode();
    assertEquals(false, jt.isInAdminSafeMode());
    
    // Bounce HDFS back in-out
    dfs.getNameNode().setSafeMode(
        org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction.SAFEMODE_ENTER);
    Thread.sleep(5000);
    dfs.getNameNode().setSafeMode(
        org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction.SAFEMODE_LEAVE);
    
    numTries = 20;
    while (jt.isInSafeMode() && numTries > 0) {
      Thread.sleep(1000);
      --numTries;
    }
    
    // By now JT should not be in safe-mode
    assertEquals(false, jt.isInSafeMode());
      
  }
  
  @Test
  public void testMRAdminSafeModeWait() throws Exception {
    
    enterSafeMode();

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Void> future = executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        MRAdmin mrAdmin = new MRAdmin(mr.createJobConf());
        mrAdmin.run(new String[] { "-safemode", "wait" });
        return null;
      }
    });
    try {
      future.get(1, TimeUnit.SECONDS);
      fail("JT should still be in safemode");
    } catch (TimeoutException e) {
      // expected
    }

    leaveSafeMode();

    try {
      future.get(10, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      fail("JT should no longer be in safemode");
    }
  }
  
  @Test
  public void testJobsPauseInSafeMode() throws Exception {
    FileSystem fileSys = dfs.getFileSystem();
    JobConf jobConf = mr.createJobConf();
    int numMaps = 10;
    int numReds = 1;
    String mapSignalFile = UtilsForTests.getMapSignalFile(shareDir);
    String redSignalFile = UtilsForTests.getReduceSignalFile(shareDir);
    jobConf.set("user.name", UserGroupInformation.getCurrentUser().getUserName());
    // Configure the job
    JobConf job = configureJob(jobConf, numMaps, numReds, 
                               mapSignalFile, redSignalFile);
      
    fileSys.delete(shareDir, true);
    
    // Submit the job   
    JobClient jobClient = new JobClient(job);
    RunningJob rJob = jobClient.submitJob(job);
    JobID id = rJob.getID();
    
    // wait for the job to be inited
    mr.initializeJob(id);
    
    // Make sure that the master job is 50% completed
    while (UtilsForTests.getJobStatus(jobClient, id).mapProgress() < 0.5f) {
      UtilsForTests.waitFor(10);
    }
    assertEquals(numMaps / 2, getCompletedMapCount(rJob));

    enterSafeMode();

    // Signal all the maps to complete
    UtilsForTests.signalTasks(dfs, fileSys, true, mapSignalFile, redSignalFile);
    
    // Signal the reducers to complete
    UtilsForTests.signalTasks(dfs, fileSys, false, mapSignalFile, 
                              redSignalFile);

    // only assigned maps complete in safemode since no more maps may be
    // assigned
    Thread.sleep(10000);
    assertEquals(numMaps / 2 + maxMapTasks, getCompletedMapCount(rJob));

    leaveSafeMode();
    
    // job completes after leaving safemode
    UtilsForTests.waitTillDone(jobClient);

    assertTrue(rJob.isSuccessful());
  }
  
  private int getCompletedMapCount(RunningJob rJob) throws IOException {
    TaskCompletionEvent[] taskCompletionEvents = rJob.getTaskCompletionEvents(0);
    int mapCount = 0;
    for (TaskCompletionEvent tce : taskCompletionEvents) {
      if (tce.isMap) {
        mapCount++;
      }
    }
    return mapCount;
  }

  private JobConf configureJob(JobConf conf, int maps, int reduces,
      String mapSignal, String redSignal) throws IOException {
    UtilsForTests.configureWaitingJobConf(conf, inDir, outputDir, maps,
        reduces, "test-jt-safemode", mapSignal, redSignal);
    return conf;
  }
  
  private void enterSafeMode() throws IOException {
    jt.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
  }

  private void leaveSafeMode() throws IOException {
    jt.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
  }
}
