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
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The Balancer ensures that it disperses RPCs to the NameNode
 * in order to avoid NN's RPC queue saturation.
 */
public class TestBalancerRPCDelay {

  private TestBalancer testBalancer;

  @Before
  public void setup() {
    testBalancer = new TestBalancer();
    testBalancer.setup();
  }

  @After
  public void teardown() throws Exception {
    if (testBalancer != null) {
      testBalancer.shutdown();
    }
  }

  @Test(timeout=100000)
  public void testBalancerRPCDelayQps3() throws Exception {
    testBalancer.testBalancerRPCDelay(3);
  }

  @Test(timeout=100000)
  public void testBalancerRPCDelayQpsDefault() throws Exception {
    testBalancer.testBalancerRPCDelay(
        DFSConfigKeys.DFS_NAMENODE_GETBLOCKS_MAX_QPS_DEFAULT);
  }
}
