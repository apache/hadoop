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

package org.apache.hadoop.mapreduce.v2;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.SleepJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMRAMWithNonNormalizedCapabilities {
  private static final Log LOG = LogFactory.getLog(TestMRAMWithNonNormalizedCapabilities.class);
  private static FileSystem localFs;
  protected static MiniMRYarnCluster mrCluster = null;

  private static Configuration conf = new Configuration();

  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  private static Path TEST_ROOT_DIR = new Path("target",
          TestMRAMWithNonNormalizedCapabilities.class.getName() + "-tmpDir")
          .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
  static Path APP_JAR = new Path(TEST_ROOT_DIR, "MRAppJar.jar");

  @Before
  public void setup() throws Exception {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
        + " not found. Not running test.");
      return;
    }

    if (mrCluster == null) {
      mrCluster = new MiniMRYarnCluster(getClass().getSimpleName());
      mrCluster.init(new Configuration());
      mrCluster.start();
    }
    // Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
    // workaround the absent public discache.
    localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), APP_JAR);
    localFs.setPermission(APP_JAR, new FsPermission("700"));
  }

  /**
   * To ensure nothing broken after we removed normalization 
   * from the MRAM side
   * @throws Exception
   */
  @Test
  public void testJobWithNonNormalizedCapabilities() throws Exception {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
                + " not found. Not running test.");
      return;
    }

    JobConf jobConf = new JobConf(mrCluster.getConfig());
    jobConf.setInt("mapreduce.map.memory.mb", 700);
    jobConf.setInt("mapred.reduce.memory.mb", 1500);

    SleepJob sleepJob = new SleepJob();
    sleepJob.setConf(jobConf);
    Job job = sleepJob.createJob(3, 2, 1000, 1, 500, 1);
    job.setJarByClass(SleepJob.class);
    job.addFileToClassPath(APP_JAR); // The AppMaster jar itself.
    job.submit();
    boolean completed = job.waitForCompletion(true);
    Assert.assertTrue("Job should be completed", completed);
    Assert.assertEquals("Job should be finished successfully", 
                    JobStatus.State.SUCCEEDED, job.getJobState());
  }

  @After
  public void tearDown() {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
          + " not found. Not running test.");
      return;
    }

    if (mrCluster != null) {
      mrCluster.stop();
    }
  }
}
