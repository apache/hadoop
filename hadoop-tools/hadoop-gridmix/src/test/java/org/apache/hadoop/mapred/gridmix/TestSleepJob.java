/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class TestSleepJob extends CommonJobTest {

  public static final Log LOG = LogFactory.getLog(Gridmix.class);

  static {
    ((Log4JLogger) LogFactory.getLog("org.apache.hadoop.mapred.gridmix"))
            .getLogger().setLevel(Level.DEBUG);
  }

  static GridmixJobSubmissionPolicy policy = GridmixJobSubmissionPolicy.REPLAY;

  @BeforeClass
  public static void init() throws IOException {
    GridmixTestUtils.initCluster(TestSleepJob.class);
  }

  @AfterClass
  public static void shutDown() throws IOException {
    GridmixTestUtils.shutdownCluster();
  }


  @Test  (timeout=600000)
  public void testMapTasksOnlySleepJobs() throws Exception {
    Configuration configuration = GridmixTestUtils.mrvl.getConfig();

    DebugJobProducer jobProducer = new DebugJobProducer(5, configuration);
    configuration.setBoolean(SleepJob.SLEEPJOB_MAPTASK_ONLY, true);

    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    JobStory story;
    int seq = 1;
    while ((story = jobProducer.getNextJob()) != null) {
      GridmixJob gridmixJob = JobCreator.SLEEPJOB.createGridmixJob(configuration, 0,
              story, new Path("ignored"), ugi, seq++);
      gridmixJob.buildSplits(null);
      Job job = gridmixJob.call();
      assertEquals(0, job.getNumReduceTasks());
    }
    jobProducer.close();
    assertEquals(6, seq);
  }

  /*
  * test RandomLocation
  */
  @Test (timeout=600000)
  public void testRandomLocation() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();

    testRandomLocation(1, 10, ugi);
    testRandomLocation(2, 10, ugi);
  }

  // test Serial submit
  @Test  (timeout=600000)
  public void testSerialSubmit() throws Exception {
    // set policy
    policy = GridmixJobSubmissionPolicy.SERIAL;
    LOG.info("Serial started at " + System.currentTimeMillis());
    doSubmission(JobCreator.SLEEPJOB.name(), false);
    LOG.info("Serial ended at " + System.currentTimeMillis());
  }

  @Test  (timeout=600000)
  public void testReplaySubmit() throws Exception {
    policy = GridmixJobSubmissionPolicy.REPLAY;
    LOG.info(" Replay started at " + System.currentTimeMillis());
    doSubmission(JobCreator.SLEEPJOB.name(), false);
    LOG.info(" Replay ended at " + System.currentTimeMillis());
  }

  @Test   (timeout=600000)
  public void testStressSubmit() throws Exception {
    policy = GridmixJobSubmissionPolicy.STRESS;
    LOG.info(" Replay started at " + System.currentTimeMillis());
    doSubmission(JobCreator.SLEEPJOB.name(), false);
    LOG.info(" Replay ended at " + System.currentTimeMillis());
  }

  private void testRandomLocation(int locations, int njobs,
                                  UserGroupInformation ugi) throws Exception {
    Configuration configuration = new Configuration();

    DebugJobProducer jobProducer = new DebugJobProducer(njobs, configuration);
    Configuration jconf = GridmixTestUtils.mrvl.getConfig();
    jconf.setInt(JobCreator.SLEEPJOB_RANDOM_LOCATIONS, locations);

    JobStory story;
    int seq = 1;
    while ((story = jobProducer.getNextJob()) != null) {
      GridmixJob gridmixJob = JobCreator.SLEEPJOB.createGridmixJob(jconf, 0,
              story, new Path("ignored"), ugi, seq++);
      gridmixJob.buildSplits(null);
      List<InputSplit> splits = new SleepJob.SleepInputFormat()
              .getSplits(gridmixJob.getJob());
      for (InputSplit split : splits) {
        assertEquals(locations, split.getLocations().length);
      }
    }
    jobProducer.close();
  }

}
