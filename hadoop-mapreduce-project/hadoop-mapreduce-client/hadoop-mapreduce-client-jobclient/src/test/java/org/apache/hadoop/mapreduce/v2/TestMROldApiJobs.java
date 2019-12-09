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

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.CustomOutputCommitter;
import org.apache.hadoop.FailMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMROldApiJobs {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestMROldApiJobs.class);

  protected static MiniMRYarnCluster mrCluster;
  private static Configuration conf = new Configuration();
  private static FileSystem localFs;
  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  @BeforeClass
  public static void setup()  throws IOException {

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    if (mrCluster == null) {
      mrCluster = new MiniMRYarnCluster(TestMROldApiJobs.class.getName());
      mrCluster.init(new Configuration());
      mrCluster.start();
    }

    // TestMRJobs is for testing non-uberized operation only; see TestUberAM
    // for corresponding uberized tests.
    mrCluster.getConfig().setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    
    // Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
    // workaround the absent public discache.
    localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), TestMRJobs.APP_JAR);
    localFs.setPermission(TestMRJobs.APP_JAR, new FsPermission("700"));
  }

  @AfterClass
  public static void tearDown() {
    if (mrCluster != null) {
      mrCluster.stop();
      mrCluster = null;
    }
  }

  @Test
  public void testJobSucceed() throws IOException, InterruptedException,
      ClassNotFoundException { 

    LOG.info("\n\n\nStarting testJobSucceed().");

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    JobConf conf = new JobConf(mrCluster.getConfig());
    
    Path in = new Path(mrCluster.getTestWorkDir().getAbsolutePath(),
      "in");
    Path out = new Path(mrCluster.getTestWorkDir().getAbsolutePath(),
      "out"); 
    runJobSucceed(conf, in, out);
    
    FileSystem fs = FileSystem.get(conf);
    Assert.assertTrue(fs.exists(new Path(out, CustomOutputCommitter.JOB_SETUP_FILE_NAME)));
    Assert.assertFalse(fs.exists(new Path(out, CustomOutputCommitter.JOB_ABORT_FILE_NAME)));
    Assert.assertTrue(fs.exists(new Path(out, CustomOutputCommitter.JOB_COMMIT_FILE_NAME)));
    Assert.assertTrue(fs.exists(new Path(out, CustomOutputCommitter.TASK_SETUP_FILE_NAME)));
    Assert.assertFalse(fs.exists(new Path(out, CustomOutputCommitter.TASK_ABORT_FILE_NAME)));
    Assert.assertTrue(fs.exists(new Path(out, CustomOutputCommitter.TASK_COMMIT_FILE_NAME)));
  }

  @Test
  public void testJobFail() throws IOException, InterruptedException,
      ClassNotFoundException { 

    LOG.info("\n\n\nStarting testJobFail().");

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    JobConf conf = new JobConf(mrCluster.getConfig());
    
    Path in = new Path(mrCluster.getTestWorkDir().getAbsolutePath(),
      "fail-in");
    Path out = new Path(mrCluster.getTestWorkDir().getAbsolutePath(),
      "fail-out"); 
    runJobFail(conf, in, out);
    
    FileSystem fs = FileSystem.get(conf);
    Assert.assertTrue(fs.exists(new Path(out, CustomOutputCommitter.JOB_SETUP_FILE_NAME)));
    Assert.assertTrue(fs.exists(new Path(out, CustomOutputCommitter.JOB_ABORT_FILE_NAME)));
    Assert.assertFalse(fs.exists(new Path(out, CustomOutputCommitter.JOB_COMMIT_FILE_NAME)));
    Assert.assertTrue(fs.exists(new Path(out, CustomOutputCommitter.TASK_SETUP_FILE_NAME)));
    Assert.assertTrue(fs.exists(new Path(out, CustomOutputCommitter.TASK_ABORT_FILE_NAME)));
    Assert.assertFalse(fs.exists(new Path(out, CustomOutputCommitter.TASK_COMMIT_FILE_NAME)));
  }

  //Run a job that will be failed and wait until it completes
  public static void runJobFail(JobConf conf, Path inDir, Path outDir)
         throws IOException, InterruptedException {
    conf.setJobName("test-job-fail");
    conf.setMapperClass(FailMapper.class);
    conf.setJarByClass(FailMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    conf.setMaxMapAttempts(1);
    
    boolean success = runJob(conf, inDir, outDir, 1, 0);
    Assert.assertFalse("Job expected to fail succeeded", success);
  }

  //Run a job that will be succeeded and wait until it completes
  public static void runJobSucceed(JobConf conf, Path inDir, Path outDir)
         throws IOException, InterruptedException {
    conf.setJobName("test-job-succeed");
    conf.setMapperClass(IdentityMapper.class);
    //conf.setJar(new File(MiniMRYarnCluster.APPJAR).getAbsolutePath());
    conf.setReducerClass(IdentityReducer.class);
    
    boolean success = runJob(conf, inDir, outDir, 1 , 1);
    Assert.assertTrue("Job expected to succeed failed", success);
  }

  static boolean runJob(JobConf conf, Path inDir, Path outDir, int numMaps, 
                           int numReds) throws IOException, InterruptedException {

    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    if (!fs.exists(inDir)) {
      fs.mkdirs(inDir);
    }
    String input = "The quick brown fox\n" + "has many silly\n"
        + "red fox sox\n";
    for (int i = 0; i < numMaps; ++i) {
      DataOutputStream file = fs.create(new Path(inDir, "part-" + i));
      file.writeBytes(input);
      file.close();
    }

    DistributedCache.addFileToClassPath(TestMRJobs.APP_JAR, conf, fs);
    conf.setOutputCommitter(CustomOutputCommitter.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReds);

    JobClient jobClient = new JobClient(conf);
    
    RunningJob job = jobClient.submitJob(conf);
    return jobClient.monitorAndPrintJob(conf, job);
  }
}
