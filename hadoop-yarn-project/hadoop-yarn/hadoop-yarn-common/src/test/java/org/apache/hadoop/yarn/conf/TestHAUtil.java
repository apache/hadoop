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

import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestHAUtil {
  private Configuration conf;

  private static final String RM1_ADDRESS_UNTRIMMED = "  \t\t\n 1.2.3.4:8021  \n\t ";
  private static final String RM1_ADDRESS = RM1_ADDRESS_UNTRIMMED.trim();
  private static final String RM2_ADDRESS = "localhost:8022";
  private static final String RM3_ADDRESS = "localhost:8033";
  private static final String RM1_NODE_ID_UNTRIMMED = "rm1 ";
  private static final String RM1_NODE_ID = RM1_NODE_ID_UNTRIMMED.trim();
  private static final String RM2_NODE_ID = "rm2";
  private static final String RM3_NODE_ID = "rm3";
  private static final String RM_INVALID_NODE_ID = ".rm";
  private static final String RM_NODE_IDS_UNTRIMMED = RM1_NODE_ID_UNTRIMMED + "," + RM2_NODE_ID;
  private static final String RM_NODE_IDS = RM1_NODE_ID + "," + RM2_NODE_ID;

  @BeforeEach
  public void setUp() {
    conf = new Configuration();
    conf.set(YarnConfiguration.RM_HA_IDS, RM_NODE_IDS_UNTRIMMED);
    conf.set(YarnConfiguration.RM_HA_ID, RM1_NODE_ID_UNTRIMMED);

    for (String confKey : YarnConfiguration.getServiceAddressConfKeys(conf)) {
      // configuration key itself cannot contains space/tab/return chars.
      conf.set(HAUtil.addSuffix(confKey, RM1_NODE_ID), RM1_ADDRESS_UNTRIMMED);
      conf.set(HAUtil.addSuffix(confKey, RM2_NODE_ID), RM2_ADDRESS);
    }
  }

  @Test
  void testGetRMServiceId() throws Exception {
    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    Collection<String> rmhaIds = HAUtil.getRMHAIds(conf);
    assertEquals(2, rmhaIds.size());

    String[] ids = rmhaIds.toArray(new String[0]);
    assertEquals(RM1_NODE_ID, ids[0]);
    assertEquals(RM2_NODE_ID, ids[1]);
  }

  @Test
  void testGetRMId() throws Exception {
    conf.set(YarnConfiguration.RM_HA_ID, RM1_NODE_ID);
    assertEquals(RM1_NODE_ID, HAUtil.getRMHAId(conf),
        "Does not honor " + YarnConfiguration.RM_HA_ID);

    conf.clear();
    assertNull(HAUtil.getRMHAId(conf), "Return null when " + YarnConfiguration.RM_HA_ID
        + " is not set");
  }

  @Test
  void testVerifyAndSetConfiguration() throws Exception {
    Configuration myConf = new Configuration(conf);

    try {
      HAUtil.verifyAndSetConfiguration(myConf);
    } catch (YarnRuntimeException e) {
      fail("Should not throw any exceptions.");
    }

    assertEquals(StringUtils.getStringCollection(RM_NODE_IDS),
        HAUtil.getRMHAIds(myConf),
        "Should be saved as Trimmed collection");
    assertEquals(RM1_NODE_ID, HAUtil.getRMHAId(myConf), "Should be saved as Trimmed string");
    for (String confKey : YarnConfiguration.getServiceAddressConfKeys(myConf)) {
      assertEquals(RM1_ADDRESS, myConf.get(confKey), "RPC address not set for " + confKey);
    }

    myConf = new Configuration(conf);
    myConf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID);
    try {
      HAUtil.verifyAndSetConfiguration(myConf);
    } catch (YarnRuntimeException e) {
      assertEquals(HAUtil.BAD_CONFIG_MESSAGE_PREFIX +
          HAUtil.getInvalidValueMessage(YarnConfiguration.RM_HA_IDS,
              myConf.get(YarnConfiguration.RM_HA_IDS) +
                  "\nHA mode requires atleast two RMs"),
          e.getMessage(),
          "YarnRuntimeException by verifyAndSetRMHAIds()");
    }

    myConf = new Configuration(conf);
    // simulate the case YarnConfiguration.RM_HA_ID is not set
    myConf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + ","
        + RM2_NODE_ID);
    for (String confKey : YarnConfiguration.getServiceAddressConfKeys(myConf)) {
      myConf.set(HAUtil.addSuffix(confKey, RM1_NODE_ID), RM1_ADDRESS);
      myConf.set(HAUtil.addSuffix(confKey, RM2_NODE_ID), RM2_ADDRESS);
    }
    try {
      HAUtil.verifyAndSetConfiguration(myConf);
    } catch (YarnRuntimeException e) {
      assertEquals(HAUtil.BAD_CONFIG_MESSAGE_PREFIX +
          HAUtil.getNeedToSetValueMessage(YarnConfiguration.RM_HA_ID),
          e.getMessage(),
          "YarnRuntimeException by getRMId()");
    }

    myConf = new Configuration(conf);
    myConf.set(YarnConfiguration.RM_HA_ID, RM_INVALID_NODE_ID);
    myConf.set(YarnConfiguration.RM_HA_IDS, RM_INVALID_NODE_ID + ","
        + RM1_NODE_ID);
    for (String confKey : YarnConfiguration.getServiceAddressConfKeys(myConf)) {
      // simulate xml with invalid node id
      myConf.set(confKey + RM_INVALID_NODE_ID, RM_INVALID_NODE_ID);
    }
    try {
      HAUtil.verifyAndSetConfiguration(myConf);
    } catch (YarnRuntimeException e) {
      assertEquals(HAUtil.BAD_CONFIG_MESSAGE_PREFIX +
          HAUtil.getInvalidValueMessage(YarnConfiguration.RM_HA_ID,
              RM_INVALID_NODE_ID),
          e.getMessage(),
          "YarnRuntimeException by addSuffix()");
    }

    myConf = new Configuration();
    // simulate the case HAUtil.RM_RPC_ADDRESS_CONF_KEYS are not set
    myConf.set(YarnConfiguration.RM_HA_ID, RM1_NODE_ID);
    myConf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    try {
      HAUtil.verifyAndSetConfiguration(myConf);
      fail("Should throw YarnRuntimeException. by Configuration#set()");
    } catch (YarnRuntimeException e) {
      String confKey =
          HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS, RM1_NODE_ID);
      assertEquals(HAUtil.BAD_CONFIG_MESSAGE_PREFIX + HAUtil.getNeedToSetValueMessage(
          HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME, RM1_NODE_ID)
              + " or " + confKey), e.getMessage(), "YarnRuntimeException by Configuration#set()");
    }

    // simulate the case YarnConfiguration.RM_HA_IDS doesn't contain
    // the value of YarnConfiguration.RM_HA_ID
    myConf = new Configuration(conf);
    myConf.set(YarnConfiguration.RM_HA_IDS, RM2_NODE_ID + "," + RM3_NODE_ID);
    myConf.set(YarnConfiguration.RM_HA_ID, RM1_NODE_ID_UNTRIMMED);
    for (String confKey : YarnConfiguration.getServiceAddressConfKeys(myConf)) {
      myConf.set(HAUtil.addSuffix(confKey, RM1_NODE_ID), RM1_ADDRESS_UNTRIMMED);
      myConf.set(HAUtil.addSuffix(confKey, RM2_NODE_ID), RM2_ADDRESS);
      myConf.set(HAUtil.addSuffix(confKey, RM3_NODE_ID), RM3_ADDRESS);
    }
    try {
      HAUtil.verifyAndSetConfiguration(myConf);
    } catch (YarnRuntimeException e) {
      assertEquals(HAUtil.BAD_CONFIG_MESSAGE_PREFIX +
          HAUtil.getRMHAIdNeedToBeIncludedMessage("[rm2, rm3]", RM1_NODE_ID),
          e.getMessage(),
          "YarnRuntimeException by getRMId()'s validation");
    }

    // simulate the case that no leader election is enabled
    myConf = new Configuration(conf);
    myConf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    myConf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, true);
    myConf.setBoolean(YarnConfiguration.AUTO_FAILOVER_EMBEDDED, false);
    myConf.setBoolean(YarnConfiguration.CURATOR_LEADER_ELECTOR, false);

    try {
      HAUtil.verifyAndSetConfiguration(myConf);
    } catch (YarnRuntimeException e) {
      assertEquals(HAUtil.BAD_CONFIG_MESSAGE_PREFIX + HAUtil.NO_LEADER_ELECTION_MESSAGE,
          e.getMessage(),
          "YarnRuntimeException by getRMId()'s validation");
    }
  }

  @Test
  void testGetConfKeyForRMInstance() {
    assertTrue(HAUtil.getConfKeyForRMInstance(YarnConfiguration.RM_ADDRESS, conf)
            .contains(HAUtil.getRMHAId(conf)),
        "RM instance id is not suffixed");
    assertFalse(HAUtil.getConfKeyForRMInstance(YarnConfiguration.NM_ADDRESS, conf)
            .contains(HAUtil.getRMHAId(conf)),
        "RM instance id is suffixed");
  }
}
