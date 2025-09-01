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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyState;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test that ErasureCodingPolicyManager correctly parses the set of enabled
 * erasure coding policies from configuration and exposes this information.
 */
@Timeout(600)
public class TestEnabledECPolicies {

  private void expectInvalidPolicy(String value) throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY,
        value);
    try {
      ErasureCodingPolicyManager.getInstance().init(conf);
      fail("Expected exception when instantiating ECPolicyManager");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("is not a valid policy", e);
    }
  }

  private void expectValidPolicy(String value, final int numEnabled) throws
      Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    ErasureCodingPolicyManager manager =
        ErasureCodingPolicyManager.getInstance();
    manager.init(conf);
    manager.enablePolicy(value);
    assertEquals(numEnabled, manager.getEnabledPolicies().length,
        "Incorrect number of enabled policies");
  }

  @Test
  public void testDefaultPolicy() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    String defaultECPolicies = conf.get(
        DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY,
        DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY_DEFAULT);
    expectValidPolicy(defaultECPolicies, 1);
  }

  @Test
  public void testInvalid() throws Exception {
    // Test first with an invalid policy
    expectInvalidPolicy("not-a-policy");
    // Test with an invalid policy and a valid policy
    expectInvalidPolicy("not-a-policy," +
        StripedFileTestUtil.getDefaultECPolicy().getName());
    // Test with a valid and an invalid policy
    expectInvalidPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName() + ", not-a-policy");
    // Some more invalid values
    expectInvalidPolicy("not-a-policy, ");
    expectInvalidPolicy("     ,not-a-policy, ");
  }

  @Test
  public void testValid() throws Exception {
    String ecPolicyName = StripedFileTestUtil.getDefaultECPolicy().getName();
    expectValidPolicy(ecPolicyName, 1);
  }

  @Test
  public void testGetPolicies() throws Exception {
    ErasureCodingPolicy[] enabledPolicies;
    // Enable no policies
    enabledPolicies = new ErasureCodingPolicy[] {};
    testGetPolicies(enabledPolicies);

    // Enable one policy
    enabledPolicies = new ErasureCodingPolicy[]{
        SystemErasureCodingPolicies.getPolicies().get(1)
    };
    testGetPolicies(enabledPolicies);

    // Enable two policies
    enabledPolicies = new ErasureCodingPolicy[]{
        SystemErasureCodingPolicies.getPolicies().get(1),
        SystemErasureCodingPolicies.getPolicies().get(2)
    };
    testGetPolicies(enabledPolicies);
  }

  @Test
  public void testChangeDefaultPolicy() throws Exception {
    final HdfsConfiguration conf = new HdfsConfiguration();
    final String testPolicy = "RS-3-2-1024k";
    final String defaultPolicy = conf.getTrimmed(
        DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY,
        DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY_DEFAULT);
    assertNotEquals(testPolicy, defaultPolicy,
        "The default policy and the next default policy " + "should not be the same!");

    ErasureCodingPolicyManager manager =
        ErasureCodingPolicyManager.getInstance();
    // Change the default policy to a new one
    conf.set(
        DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY,
        testPolicy);
    manager.init(conf);
    // Load policies similar to when fsimage is loaded at namenode startup
    manager.loadPolicies(constructAllDisabledInitialPolicies(), conf);

    ErasureCodingPolicyInfo[] getPoliciesResult = manager.getPolicies();
    boolean isEnabled = isPolicyEnabled(testPolicy, getPoliciesResult);
    assertTrue(isEnabled, "The new default policy should be " + "in enabled state!");
    ErasureCodingPolicyInfo[] getPersistedPoliciesResult
        = manager.getPersistedPolicies();
    isEnabled = isPolicyEnabled(testPolicy, getPersistedPoliciesResult);
    assertFalse(isEnabled,
        "The new default policy should be " + "in disabled state in the persisted list!");

    manager.disablePolicy(testPolicy);
    getPoliciesResult = manager.getPolicies();
    isEnabled = isPolicyEnabled(testPolicy, getPoliciesResult);
    assertFalse(isEnabled, "The new default policy should be " + "in disabled state!");
    getPersistedPoliciesResult
        = manager.getPersistedPolicies();
    isEnabled = isPolicyEnabled(testPolicy, getPersistedPoliciesResult);
    assertFalse(isEnabled,
        "The new default policy should be " + "in disabled state in the persisted list!");

    manager.enablePolicy(testPolicy);
    getPoliciesResult = manager.getPolicies();
    isEnabled = isPolicyEnabled(testPolicy, getPoliciesResult);
    assertTrue(isEnabled, "The new default policy should be " + "in enabled state!");
    getPersistedPoliciesResult
        = manager.getPersistedPolicies();
    isEnabled = isPolicyEnabled(testPolicy, getPersistedPoliciesResult);
    assertTrue(isEnabled,
        "The new default policy should be " + "in enabled state in the persisted list!");

    final String emptyPolicy = "";
    // Change the default policy to a empty
    conf.set(
        DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY, emptyPolicy);
    manager.init(conf);
    // Load policies similar to when fsimage is loaded at namenode startup
    manager.loadPolicies(constructAllDisabledInitialPolicies(), conf);
    // All the policies are disabled if the default policy is empty
    getPoliciesResult = manager.getPolicies();
    assertAllPoliciesAreDisabled(getPoliciesResult);
  }

  private void testGetPolicies(ErasureCodingPolicy[] enabledPolicies)
      throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    ErasureCodingPolicyManager manager =
        ErasureCodingPolicyManager.getInstance();
    manager.init(conf);
    for (ErasureCodingPolicy p : enabledPolicies) {
      manager.enablePolicy(p.getName());
    }
    // Check that returned values are unique
    Set<String> found = new HashSet<>();
    for (ErasureCodingPolicy p : manager.getEnabledPolicies()) {
      assertFalse(found.contains(p.getName()),
          "Duplicate policy name found: " + p.getName());
      found.add(p.getName());
    }
    // Check that the policies specified in conf are found
    for (ErasureCodingPolicy p: enabledPolicies) {
      assertTrue(found.contains(p.getName()),
          "Did not find specified EC policy " + p.getName());
    }
    assertEquals(enabledPolicies.length, found.size() - 1);
    // Check that getEnabledPolicyByName only returns enabled policies
    for (ErasureCodingPolicy p: SystemErasureCodingPolicies.getPolicies()) {
      if (found.contains(p.getName())) {
        // Enabled policy should be present
        assertNotNull(
            manager.getEnabledPolicyByName(p.getName()),
            "getEnabledPolicyByName did not find enabled policy" + p.getName());
      } else {
        // Disabled policy should not be present
        assertNull(
            manager.getEnabledPolicyByName(p.getName()),
            "getEnabledPolicyByName found disabled policy " + p.getName());
      }
    }
  }

  private List<ErasureCodingPolicyInfo> constructAllDisabledInitialPolicies() {
    List<ErasureCodingPolicyInfo> policies = new ArrayList<>();
    for (ErasureCodingPolicy p: SystemErasureCodingPolicies.getPolicies()) {
      policies.add(new ErasureCodingPolicyInfo(p,
          ErasureCodingPolicyState.DISABLED));
    }
    return policies;
  }

  private boolean isPolicyEnabled(String testPolicy,
                               ErasureCodingPolicyInfo[] policies) {
    for (ErasureCodingPolicyInfo p : policies) {
      if (testPolicy.equals(p.getPolicy().getName())) {
        return p.isEnabled();
      }
    }
    fail("The result should contain the test policy!");
    return false;
  }

  private void assertAllPoliciesAreDisabled(
      ErasureCodingPolicyInfo[] policies) {
    for (ErasureCodingPolicyInfo p : policies) {
      assertTrue(p.isDisabled(), "Policy should be disabled");
    }
  }
}
