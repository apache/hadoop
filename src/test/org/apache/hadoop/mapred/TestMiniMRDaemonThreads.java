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

import java.io.IOException;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 * Validate the list of non-daemon threads running in the JobTracker
 * and TaskTracker. Each non-daemon thread is a possible candidate for
 * the service hang, so please make sure to validate all failure
 * codepaths of a new non-daemon thread before making a change
 * to this test.
 */
public class TestMiniMRDaemonThreads {

  static String[] whitelistedDaemonKeys = {
    "jobTrackerMain",
    "httpServerThreadPool",
    "taskTrackerMain",
    "expireTrackers",
    "jobEndNotifier",
    "ReaderThread",
    "Socket Reader",
    "expireLaunchingTasks",
    "retireJobs"
    };

  @Test
  public void testValidateDaemonThreads() throws IOException, InterruptedException {
    MiniMRCluster mr = null;
    try {
      JobConf jtConf = new JobConf();
      mr = new MiniMRCluster(1, "file:///", 1, null, null, jtConf);
      // Give it one second for all threads to start up
      Thread.sleep(1000);

      Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
      for (Thread t : threadSet) {
        if (!t.isDaemon() && !Thread.currentThread().equals(t)) {
          boolean found = false;
          for(String key : whitelistedDaemonKeys) {
            if (t.getName().contains(key)) {
              found = true;
              break;
            }
          }
          if (!found) {
            Assert.fail(
                String.format("Thread '%s' is not a whitelisted daemon thread",
                    t.getName()));
          }
        }
      }

    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }
}
