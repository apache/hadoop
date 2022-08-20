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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestMiniYARNClusterForHA {
  MiniYARNCluster cluster;

  @BeforeEach
  public void setup() throws IOException, InterruptedException {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:0");

    cluster = new MiniYARNCluster(TestMiniYARNClusterForHA.class.getName(),
        2, 1, 1, 1);
    cluster.init(conf);
    cluster.start();

    assertFalse(-1 == cluster.getActiveRMIndex(), "RM never turned active");
  }

  @Test
  void testClusterWorks() throws YarnException, InterruptedException {
    assertTrue(cluster.waitForNodeManagersToConnect(5000),
        "NMs fail to connect to the RM");
  }
}
