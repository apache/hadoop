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

package org.apache.hadoop.yarn.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestMiniYarnCluster {
  @Test
  public void testTimelineServiceStartInMiniCluster() throws Exception {
    Configuration conf = new YarnConfiguration();
    int numNodeManagers = 1;
    int numLocalDirs = 1;
    int numLogDirs = 1;
    boolean enableAHS;

    /*
     * Timeline service should not start if TIMELINE_SERVICE_ENABLED == false
     * and enableAHS flag == false
     */
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false);
    enableAHS = false;
    MiniYARNCluster cluster = null;
    try {
      cluster = new MiniYARNCluster(TestMiniYarnCluster.class.getSimpleName(),
          numNodeManagers, numLocalDirs, numLogDirs, numLogDirs, enableAHS);
      cluster.init(conf);
      cluster.start();

      //verify that the timeline service is not started.
      Assert.assertNull("Timeline Service should not have been started",
          cluster.getApplicationHistoryServer());
    }
    finally {
      if(cluster != null) {
        cluster.stop();
      }
    }

    /*
     * Timeline service should start if TIMELINE_SERVICE_ENABLED == true
     * and enableAHS == false
     */
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    enableAHS = false;
    cluster = null;
    try {
      cluster = new MiniYARNCluster(TestMiniYarnCluster.class.getSimpleName(),
          numNodeManagers, numLocalDirs, numLogDirs, numLogDirs, enableAHS);
      cluster.init(conf);
      cluster.start();

      //Timeline service may sometime take a while to get started
      int wait = 0;
      while(cluster.getApplicationHistoryServer() == null && wait < 20) {
        Thread.sleep(500);
        wait++;
      }
      //verify that the timeline service is started.
      Assert.assertNotNull("Timeline Service should have been started",
          cluster.getApplicationHistoryServer());
    }
    finally {
      if(cluster != null) {
        cluster.stop();
      }
    }
    /*
     * Timeline service should start if TIMELINE_SERVICE_ENABLED == false
     * and enableAHS == true
     */
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false);
    enableAHS = true;
    cluster = null;
    try {
      cluster = new MiniYARNCluster(TestMiniYarnCluster.class.getSimpleName(),
          numNodeManagers, numLocalDirs, numLogDirs, numLogDirs, enableAHS);
      cluster.init(conf);
      cluster.start();

      //Timeline service may sometime take a while to get started
      int wait = 0;
      while(cluster.getApplicationHistoryServer() == null && wait < 20) {
        Thread.sleep(500);
        wait++;
      }
      //verify that the timeline service is started.
      Assert.assertNotNull("Timeline Service should have been started",
          cluster.getApplicationHistoryServer());
    }
    finally {
      if(cluster != null) {
        cluster.stop();
      }
    }
  }
}
