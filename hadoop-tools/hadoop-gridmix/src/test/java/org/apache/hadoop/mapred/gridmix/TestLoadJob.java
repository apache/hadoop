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
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/*
 Test LoadJob Gridmix sends data to job and after that
 */
public class TestLoadJob extends CommonJobTest {

  public static final Log LOG = LogFactory.getLog(Gridmix.class);

  static {
    ((Log4JLogger) LogFactory.getLog("org.apache.hadoop.mapred.gridmix"))
            .getLogger().setLevel(Level.DEBUG);
    ((Log4JLogger) LogFactory.getLog(StressJobFactory.class)).getLogger()
            .setLevel(Level.DEBUG);
  }


  @BeforeClass
  public static void init() throws IOException {
    GridmixTestUtils.initCluster(TestLoadJob.class);
  }

  @AfterClass
  public static void shutDown() throws IOException {
    GridmixTestUtils.shutdownCluster();
  }


  /*
  * test serial policy  with LoadJob. Task should execute without exceptions
  */
  @Test (timeout=500000)
  public void testSerialSubmit() throws Exception {
    policy = GridmixJobSubmissionPolicy.SERIAL;
    LOG.info("Serial started at " + System.currentTimeMillis());
    doSubmission(JobCreator.LOADJOB.name(), false);

    LOG.info("Serial ended at " + System.currentTimeMillis());
  }

  /*
   * test reply policy with LoadJob
   */
  @Test  (timeout=500000)
  public void testReplaySubmit() throws Exception {
    policy = GridmixJobSubmissionPolicy.REPLAY;
    LOG.info(" Replay started at " + System.currentTimeMillis());
    doSubmission(JobCreator.LOADJOB.name(), false);

    LOG.info(" Replay ended at " + System.currentTimeMillis());
  }


}
