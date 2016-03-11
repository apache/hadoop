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

package org.apache.hadoop.mapreduce.lib.jobcontrol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.junit.Test;

/**
 * This class performs unit test for Job/JobControl classes.
 *  
 */
public class TestMapReduceJobControl extends HadoopTestCase {

  public static final Log LOG = 
      LogFactory.getLog(TestMapReduceJobControl.class.getName());

  static Path rootDataDir = new Path(
    System.getProperty("test.build.data", "."), "TestData");
  static Path indir = new Path(rootDataDir, "indir");
  static Path outdir_1 = new Path(rootDataDir, "outdir_1");
  static Path outdir_2 = new Path(rootDataDir, "outdir_2");
  static Path outdir_3 = new Path(rootDataDir, "outdir_3");
  static Path outdir_4 = new Path(rootDataDir, "outdir_4");
  static ControlledJob cjob1 = null;
  static ControlledJob cjob2 = null;
  static ControlledJob cjob3 = null;
  static ControlledJob cjob4 = null;

  public TestMapReduceJobControl() throws IOException {
    super(HadoopTestCase.LOCAL_MR , HadoopTestCase.LOCAL_FS, 2, 2);
  }
  
  private void cleanupData(Configuration conf) throws Exception {
    FileSystem fs = FileSystem.get(conf);
    MapReduceTestUtil.cleanData(fs, indir);
    MapReduceTestUtil.generateData(fs, indir);

    MapReduceTestUtil.cleanData(fs, outdir_1);
    MapReduceTestUtil.cleanData(fs, outdir_2);
    MapReduceTestUtil.cleanData(fs, outdir_3);
    MapReduceTestUtil.cleanData(fs, outdir_4);
  }
  
  /**
   * This is a main function for testing JobControl class.
   * It requires 4 jobs: 
   *      Job 1: passed as parameter. input:indir  output:outdir_1
   *      Job 2: copy data from indir to outdir_2
   *      Job 3: copy data from outdir_1 and outdir_2 to outdir_3
   *      Job 4: copy data from outdir to outdir_4
   * The jobs 1 and 2 have no dependency. The job 3 depends on jobs 1 and 2.
   * The job 4 depends on job 3.
   * 
   * Then it creates a JobControl object and add the 4 jobs to 
   * the JobControl object.
   * Finally, it creates a thread to run the JobControl object
   */
  private JobControl createDependencies(Configuration conf, Job job1) 
      throws Exception {
    List<ControlledJob> dependingJobs = null;
    cjob1 = new ControlledJob(job1, dependingJobs);
    Job job2 = MapReduceTestUtil.createCopyJob(conf, outdir_2, indir);
    cjob2 = new ControlledJob(job2, dependingJobs);

    Job job3 = MapReduceTestUtil.createCopyJob(conf, outdir_3, 
	                                   outdir_1, outdir_2);
    dependingJobs = new ArrayList<ControlledJob>();
    dependingJobs.add(cjob1);
    dependingJobs.add(cjob2);
    cjob3 = new ControlledJob(job3, dependingJobs);

    Job job4 = MapReduceTestUtil.createCopyJob(conf, outdir_4, outdir_3);
    dependingJobs = new ArrayList<ControlledJob>();
    dependingJobs.add(cjob3);
    cjob4 = new ControlledJob(job4, dependingJobs);

    JobControl theControl = new JobControl("Test");
    theControl.addJob(cjob1);
    theControl.addJob(cjob2);
    theControl.addJob(cjob3);
    theControl.addJob(cjob4);
    Thread theController = new Thread(theControl);
    theController.start();
    return theControl;
  }
  
  private void waitTillAllFinished(JobControl theControl) {
    while (!theControl.allFinished()) {
      try {
        Thread.sleep(100);
      } catch (Exception e) {}
    }
  }
  
  public void testJobControlWithFailJob() throws Exception {
    LOG.info("Starting testJobControlWithFailJob");
    Configuration conf = createJobConf();

    cleanupData(conf);
    
    // create a Fail job
    Job job1 = MapReduceTestUtil.createFailJob(conf, outdir_1, indir);
    
    // create job dependencies
    JobControl theControl = createDependencies(conf, job1);
    
    // wait till all the jobs complete
    waitTillAllFinished(theControl);
    
    assertTrue(cjob1.getJobState() == ControlledJob.State.FAILED);
    assertTrue(cjob2.getJobState() == ControlledJob.State.SUCCESS);
    assertTrue(cjob3.getJobState() == ControlledJob.State.DEPENDENT_FAILED);
    assertTrue(cjob4.getJobState() == ControlledJob.State.DEPENDENT_FAILED);

    theControl.stop();
  }

  public void testJobControlWithKillJob() throws Exception {
    LOG.info("Starting testJobControlWithKillJob");

    Configuration conf = createJobConf();
    cleanupData(conf);
    Job job1 = MapReduceTestUtil.createKillJob(conf, outdir_1, indir);
    JobControl theControl = createDependencies(conf, job1);

    while (cjob1.getJobState() != ControlledJob.State.RUNNING) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        break;
      }
    }
    // verify adding dependingJo to RUNNING job fails.
    assertFalse(cjob1.addDependingJob(cjob2));

    // suspend jobcontrol and resume it again
    theControl.suspend();
    assertTrue(
      theControl.getThreadState() == JobControl.ThreadState.SUSPENDED);
    theControl.resume();
    
    // kill the first job.
    cjob1.killJob();

    // wait till all the jobs complete
    waitTillAllFinished(theControl);
    
    assertTrue(cjob1.getJobState() == ControlledJob.State.FAILED);
    assertTrue(cjob2.getJobState() == ControlledJob.State.SUCCESS);
    assertTrue(cjob3.getJobState() == ControlledJob.State.DEPENDENT_FAILED);
    assertTrue(cjob4.getJobState() == ControlledJob.State.DEPENDENT_FAILED);

    theControl.stop();
  }

  public void testJobControl() throws Exception {
    LOG.info("Starting testJobControl");

    Configuration conf = createJobConf();

    cleanupData(conf);
    
    Job job1 = MapReduceTestUtil.createCopyJob(conf, outdir_1, indir);
    
    JobControl theControl = createDependencies(conf, job1);
    
    // wait till all the jobs complete
    waitTillAllFinished(theControl);
    
    assertEquals("Some jobs failed", 0, theControl.getFailedJobList().size());
    
    theControl.stop();
  }
  
  @Test(timeout = 30000)
  public void testControlledJob() throws Exception {
    LOG.info("Starting testControlledJob");

    Configuration conf = createJobConf();
    cleanupData(conf);
    Job job1 = MapReduceTestUtil.createCopyJob(conf, outdir_1, indir);
    JobControl theControl = createDependencies(conf, job1);
    while (cjob1.getJobState() != ControlledJob.State.RUNNING) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        break;
      }
    }
    Assert.assertNotNull(cjob1.getMapredJobId());

    // wait till all the jobs complete
    waitTillAllFinished(theControl);
    assertEquals("Some jobs failed", 0, theControl.getFailedJobList().size());
    theControl.stop();
  }
}
