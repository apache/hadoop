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

package org.apache.hadoop.yarn.server.api;

import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test ServerRMProxy.
 */
public class TestServerRMProxy {

  @Test
  public void testDistributedProtocol() {

    YarnConfiguration conf = new YarnConfiguration();
    try {
      ServerRMProxy.createRMProxy(conf, DistributedSchedulingAMProtocol.class);
    } catch (Exception e) {
      Assert.fail("DistributedSchedulingAMProtocol fail in non HA");
    }

    // HA is enabled
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2");
    conf.set(HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME, "rm1"), "0.0.0.0");
    conf.set(HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME, "rm2"), "0.0.0.0");
    try {
      ServerRMProxy.createRMProxy(conf, DistributedSchedulingAMProtocol.class);
    } catch (Exception e) {
      Assert.fail("DistributedSchedulingAMProtocol fail in HA");
    }
  }
}
