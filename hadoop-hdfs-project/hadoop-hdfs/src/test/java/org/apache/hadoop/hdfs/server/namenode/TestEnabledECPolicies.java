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
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test that ErasureCodingPolicyManager correctly parses the set of enabled
 * erasure coding policies from configuration and exposes this information.
 */
public class TestEnabledECPolicies {

  @Rule
  public Timeout testTimeout = new Timeout(60000);

  private void expectInvalidPolicy(String value) {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY,
        value);
    try {
      ErasureCodingPolicyManager.getInstance().init(conf);
      fail("Expected exception when instantiating ECPolicyManager");
    } catch (IllegalArgumentException e) {
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
    assertEquals("Incorrect number of enabled policies",
        numEnabled, manager.getEnabledPolicies().length);
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
      Assert.assertFalse("Duplicate policy name found: " + p.getName(),
          found.contains(p.getName()));
      found.add(p.getName());
    }
    // Check that the policies specified in conf are found
    for (ErasureCodingPolicy p: enabledPolicies) {
      Assert.assertTrue("Did not find specified EC policy " + p.getName(),
          found.contains(p.getName()));
    }
    Assert.assertEquals(enabledPolicies.length, found.size()-1);
    // Check that getEnabledPolicyByName only returns enabled policies
    for (ErasureCodingPolicy p: SystemErasureCodingPolicies.getPolicies()) {
      if (found.contains(p.getName())) {
        // Enabled policy should be present
        Assert.assertNotNull(
            "getEnabledPolicyByName did not find enabled policy" + p.getName(),
            manager.getEnabledPolicyByName(p.getName()));
      } else {
        // Disabled policy should not be present
        Assert.assertNull(
            "getEnabledPolicyByName found disabled policy " + p.getName(),
            manager.getEnabledPolicyByName(p.getName()));
      }
    }
  }
}
