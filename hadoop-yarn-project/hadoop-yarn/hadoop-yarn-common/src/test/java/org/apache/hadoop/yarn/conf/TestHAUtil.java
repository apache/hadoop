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

package org.apache.hadoop.yarn.conf;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestHAUtil {
  private Configuration conf;

  private static final String RM1_ADDRESS = "1.2.3.4:8021";
  private static final String RM2_ADDRESS = "localhost:8022";
  private static final String RM1_NODE_ID = "rm1";
  private static final String RM2_NODE_ID = "rm2";

  @Before
  public void setUp() {
    conf = new Configuration();
    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    conf.set(YarnConfiguration.RM_HA_ID, RM1_NODE_ID);

    for (String confKey : HAUtil.RPC_ADDRESS_CONF_KEYS) {
      conf.set(HAUtil.addSuffix(confKey, RM1_NODE_ID), RM1_ADDRESS);
      conf.set(HAUtil.addSuffix(confKey, RM2_NODE_ID), RM2_ADDRESS);
    }
  }

  @Test
  public void testGetRMServiceId() throws Exception {
    Collection<String> rmhaIds = HAUtil.getRMHAIds(conf);
    assertEquals(2, rmhaIds.size());
  }

  @Test
  public void testGetRMId() throws Exception {
    assertEquals("Does not honor " + YarnConfiguration.RM_HA_ID,
        RM1_NODE_ID, HAUtil.getRMHAId(conf));
    conf = new YarnConfiguration();
    try {
      HAUtil.getRMHAId(conf);
      fail("getRMHAId() fails to throw an exception when RM_HA_ID is not set");
    } catch (YarnRuntimeException yre) {
      // do nothing
    }
  }

  @Test
  public void testSetGetRpcAddresses() throws Exception {
    HAUtil.setAllRpcAddresses(conf);
    for (String confKey : HAUtil.RPC_ADDRESS_CONF_KEYS) {
      assertEquals("RPC address not set for " + confKey,
          RM1_ADDRESS, conf.get(confKey));
    }
  }
}
