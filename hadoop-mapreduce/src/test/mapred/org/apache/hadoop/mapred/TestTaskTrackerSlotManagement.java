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
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression test for MAPREDUCE-913
 */
public class TestTaskTrackerSlotManagement {

  private static final Path TEST_DIR = new Path(System.getProperty(
      "test.build.data", "/tmp"), "tt_slots");
  private static final String CACHE_FILE_PATH = new Path(TEST_DIR, "test.txt")
      .toString();

  /**
   * Test-setup. Create the cache-file.
   * 
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    new File(TEST_DIR.toString()).mkdirs();
    File myFile = new File(CACHE_FILE_PATH);
    myFile.createNewFile();
  }

  /**
   * Test-cleanup. Remove the cache-file.
   * 
   * @throws Exception
   */
  @After
  public void tearDown() throws Exception {
    File myFile = new File(CACHE_FILE_PATH);
    myFile.delete();
    new File(TEST_DIR.toString()).delete();
  }

  /**
   * Test case to test addition of free slot when the job fails localization due
   * to cache file being modified after the job has started running.
   * 
   * @throws Exception
   */
  @Test
  public void testFreeingOfTaskSlots() throws Exception {
    // Start a cluster with no task tracker.
    MiniMRCluster mrCluster = new MiniMRCluster(0, "file:///", 1);
    Configuration conf = mrCluster.createJobConf();
    Cluster cluster = new Cluster(conf);
    // set the debug script so that TT tries to launch the debug
    // script for failed tasks.
    conf.set(JobContext.MAP_DEBUG_SCRIPT, "/bin/echo");
    conf.set(JobContext.REDUCE_DEBUG_SCRIPT, "/bin/echo");
    Job j = MapReduceTestUtil.createJob(conf, new Path(TEST_DIR, "in"),
        new Path(TEST_DIR, "out"), 0, 0);
    // Add the local filed created to the cache files of the job
    j.addCacheFile(new URI(CACHE_FILE_PATH));
    j.setMaxMapAttempts(1);
    j.setMaxReduceAttempts(1);
    // Submit the job and return immediately.
    // Job submit now takes care setting the last
    // modified time of the cache file.
    j.submit();
    // Look up the file and modify the modification time.
    File myFile = new File(CACHE_FILE_PATH);
    myFile.setLastModified(0L);
    // Start up the task tracker after the time has been changed.
    mrCluster.startTaskTracker(null, null, 0, 1);
    // Now wait for the job to fail.
    j.waitForCompletion(false);
    Assert.assertFalse("Job successfully completed.", j.isSuccessful());

    ClusterMetrics metrics = cluster.getClusterStatus();
    // validate number of slots in JobTracker
    Assert.assertEquals(0, metrics.getOccupiedMapSlots());
    Assert.assertEquals(0, metrics.getOccupiedReduceSlots());

    // validate number of slots in TaskTracker
    TaskTracker tt = mrCluster.getTaskTrackerRunner(0).getTaskTracker();
    Assert.assertEquals(metrics.getMapSlotCapacity(), tt.getFreeSlots(true));
    Assert.assertEquals(metrics.getReduceSlotCapacity(), tt.getFreeSlots(false));

  }
}
