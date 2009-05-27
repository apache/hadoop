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

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Service;

import java.io.File;
import java.io.IOException;

/**
 * Test that the task tracker follows the service lifecycle
 */

public class TestTaskTrackerLifecycle extends TestCase {
  private TaskTracker tracker;
  private static final Log LOG = LogFactory.getLog(TestTaskTrackerLifecycle.class);

  /**
   * Tears down the fixture, for example, close a network connection. This method
   * is called after a test is executed.
   */
  protected void tearDown() throws Exception {
    super.tearDown();
    Service.close(tracker);
  }

  /**
   * Create a job conf suitable for testing
   * @return a new job conf instance
   */
  private JobConf createJobConf() {
    JobConf config = new JobConf();
    String dataDir = System.getProperty("test.build.data");
    File hdfsDir = new File(dataDir, "dfs");
    config.set("dfs.name.dir", new File(hdfsDir, "name1").getPath());
    FileSystem.setDefaultUri(config, "hdfs://localhost:0");
    config.set("dfs.http.address", "hdfs://localhost:0");
    config.set("mapred.job.tracker", "localhost:8012");
    config.set("ipc.client.connect.max.retries", "1");
    return config;
  }

  private void assertConnectionRefused(IOException e) throws Throwable {
    if(!e.getMessage().contains("Connection refused")) {
      LOG.error("Wrong exception",e);
      throw e;
    }
  }

  /**
   * Test that if a tracker isn't started, we can still terminate it cleanly
   * @throws Throwable on a failure
   */
  public void testTerminateUnstartedTracker() throws Throwable {
    tracker = new TaskTracker(createJobConf(), false);
    tracker.ping();
    tracker.close();
  }

  public void testOrphanTrackerFailure() throws Throwable {
    try {
      tracker = new TaskTracker(createJobConf());
      fail("Expected a failure");
    } catch (IOException e) {
      assertConnectionRefused(e);
    }
  }

  public void testFailingTracker() throws Throwable {
    tracker = new TaskTracker(createJobConf(), false);
    try {
      tracker.start();
      fail("Expected a failure");
    } catch (IOException e) {
      assertConnectionRefused(e);
      assertEquals(Service.ServiceState.FAILED, tracker.getServiceState());
    }
  }

  public void testStartedTracker() throws Throwable {
    tracker = new TaskTracker(createJobConf(), false);
    try {
      Service.startService(tracker);
      fail("Expected a failure");
    } catch (IOException e) {
      assertConnectionRefused(e);
      assertEquals(Service.ServiceState.CLOSED, tracker.getServiceState());
    }
    tracker.ping();
    tracker.close();
    tracker.ping();
  }

}
