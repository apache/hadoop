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
package org.apache.hadoop.hdfs.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests some ECAdmin scenarios that are hard to test from
 * {@link org.apache.hadoop.cli.TestErasureCodingCLI}.
 */
public class TestECAdmin {
  public static final Logger LOG = LoggerFactory.getLogger(TestECAdmin.class);
  private Configuration conf = new Configuration();
  private MiniDFSCluster cluster;
  private ECAdmin admin = new ECAdmin(conf);

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();

  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Before
  public void setup() throws Exception {
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
  }

  @After
  public void tearDown() throws Exception {
    try {
      System.out.flush();
      System.err.flush();
      resetOutputs();
    } finally {
      System.setOut(OLD_OUT);
      System.setErr(OLD_ERR);
    }

    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testRS63MinDN() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 6, 3, 0);
    String[] args = {"-verifyClusterSetup"};
    final int ret = admin.run(args);
    LOG.info("Command stdout: {}", out.toString());
    LOG.info("Command stderr: {}", err.toString());
    assertEquals("Return value of the command is not successful", 2, ret);
    assertTrue("Result of cluster topology verify " +
        "should be logged correctly", out.toString()
        .contains("less than the minimum required number of DataNodes"));
    assertTrue("Error output should be empty", err.toString().isEmpty());
  }

  @Test
  public void testRS104MinRacks() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 15, 3, 0);
    cluster.getFileSystem().enableErasureCodingPolicy(
        SystemErasureCodingPolicies
            .getByID(SystemErasureCodingPolicies.RS_10_4_POLICY_ID).getName());
    String[] args = {"-verifyClusterSetup"};
    final int ret = admin.run(args);
    LOG.info("Command stdout: {}", out.toString());
    LOG.info("Command stderr: {}", err.toString());
    assertEquals("Return value of the command is not successful", 2, ret);
    assertTrue("Result of cluster topology verify " +
        "should be logged correctly", out.toString()
        .contains("less than the minimum required number of racks"));
    assertTrue("Error output should be empty", err.toString().isEmpty());
  }

  @Test
  public void testXOR21MinRacks() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 5, 2, 0);
    cluster.getFileSystem().disableErasureCodingPolicy(
        SystemErasureCodingPolicies
            .getByID(SystemErasureCodingPolicies.RS_6_3_POLICY_ID).getName());
    cluster.getFileSystem().enableErasureCodingPolicy(
        SystemErasureCodingPolicies
            .getByID(SystemErasureCodingPolicies.XOR_2_1_POLICY_ID).getName());
    String[] args = {"-verifyClusterSetup"};
    final int ret = admin.run(args);
    LOG.info("Command stdout: {}", out.toString());
    LOG.info("Command stderr: {}", err.toString());
    assertEquals("Return value of the command is not successful", 2, ret);
    assertTrue("Result of cluster topology verify " +
        "should be logged correctly", out.toString()
        .contains("less than the minimum required number of racks"));
    assertTrue("Error output should be empty", err.toString().isEmpty());
  }

  @Test
  public void testRS32MinRacks() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 5, 2, 0);
    cluster.getFileSystem().disableErasureCodingPolicy(
        SystemErasureCodingPolicies
            .getByID(SystemErasureCodingPolicies.RS_6_3_POLICY_ID).getName());
    cluster.getFileSystem().enableErasureCodingPolicy(
        SystemErasureCodingPolicies
            .getByID(SystemErasureCodingPolicies.RS_3_2_POLICY_ID).getName());
    String[] args = {"-verifyClusterSetup"};
    final int ret = admin.run(args);
    LOG.info("Command stdout: {}", out.toString());
    LOG.info("Command stderr: {}", err.toString());
    assertEquals("Return value of the command is not successful", 2, ret);
    assertTrue("Result of cluster topology verify " +
        "should be logged correctly", out.toString()
        .contains("less than the minimum required number of racks"));
    assertTrue("Error output should be empty", err.toString().isEmpty());
  }

  @Test
  public void testRS63Good() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 9, 3, 0);
    String[] args = {"-verifyClusterSetup"};
    final int ret = admin.run(args);
    LOG.info("Command stdout: {}", out.toString());
    LOG.info("Command stderr: {}", err.toString());
    assertEquals("Return value of the command is successful", 0, ret);
    assertTrue("Result of cluster topology verify " +
        "should be logged correctly", out.toString().contains(
        "The cluster setup can support EC policies: RS-6-3-1024k"));
    assertTrue("Error output should be empty", err.toString().isEmpty());
  }

  @Test
  public void testNoECEnabled() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 9, 3, 0);
    cluster.getFileSystem().disableErasureCodingPolicy(
        SystemErasureCodingPolicies
            .getByID(SystemErasureCodingPolicies.RS_6_3_POLICY_ID).getName());
    String[] args = {"-verifyClusterSetup"};
    final int ret = admin.run(args);
    LOG.info("Command stdout: {}", out.toString());
    LOG.info("Command stderr: {}", err.toString());
    assertEquals("Return value of the command is successful", 0, ret);
    assertTrue("Result of cluster topology verify " +
            "should be logged correctly",
        out.toString().contains("No erasure coding policy is given"));
    assertTrue("Error output should be empty", err.toString().isEmpty());
  }

  @Test
  public void testUnsuccessfulEnablePolicyMessage() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 5, 2, 0);
    cluster.getFileSystem().disableErasureCodingPolicy(
        SystemErasureCodingPolicies
            .getByID(SystemErasureCodingPolicies.RS_6_3_POLICY_ID).getName());
    String[] args = {"-enablePolicy", "-policy", "RS-3-2-1024k"};

    final int ret = admin.run(args);
    LOG.info("Command stdout: {}", out.toString());
    LOG.info("Command stderr: {}", err.toString());
    assertEquals("Return value of the command is successful", 0, ret);
    assertTrue("Enabling policy should be logged", out.toString()
        .contains("Erasure coding policy RS-3-2-1024k is enabled"));
    assertTrue("Warning about cluster topology should be printed",
        err.toString().contains("Warning: The cluster setup does not support " +
        "EC policy RS-3-2-1024k. Reason:"));
    assertTrue("Warning about cluster topology should be printed",
        err.toString()
            .contains("less than the minimum required number of racks"));
  }

  @Test
  public void testSuccessfulEnablePolicyMessage() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 5, 3, 0);
    cluster.getFileSystem().disableErasureCodingPolicy(
        SystemErasureCodingPolicies
            .getByID(SystemErasureCodingPolicies.RS_6_3_POLICY_ID).getName());
    String[] args = {"-enablePolicy", "-policy", "RS-3-2-1024k"};

    final int ret = admin.run(args);
    LOG.info("Command stdout: {}", out.toString());
    LOG.info("Command stderr: {}", err.toString());
    assertEquals("Return value of the command is successful", 0, ret);
    assertTrue("Enabling policy should be logged", out.toString()
        .contains("Erasure coding policy RS-3-2-1024k is enabled"));
    assertFalse("Warning about cluster topology should not be printed",
        out.toString().contains("Warning: The cluster setup does not support"));
    assertTrue("Error output should be empty", err.toString().isEmpty());
  }

  @Test
  public void testEnableNonExistentPolicyMessage() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 5, 3, 0);
    cluster.getFileSystem().disableErasureCodingPolicy(
        SystemErasureCodingPolicies
            .getByID(SystemErasureCodingPolicies.RS_6_3_POLICY_ID).getName());
    String[] args = {"-enablePolicy", "-policy", "NonExistentPolicy"};

    final int ret = admin.run(args);
    LOG.info("Command stdout: {}", out.toString());
    LOG.info("Command stderr: {}", err.toString());
    assertEquals("Return value of the command is unsuccessful", 2, ret);
    assertFalse("Enabling policy should not be logged when " +
        "it was unsuccessful", out.toString().contains("is enabled"));
    assertTrue("Error message should be printed",
        err.toString().contains("RemoteException: The policy name " +
            "NonExistentPolicy does not exist"));
  }

  @Test
  public void testVerifyClusterSetupWithGivenPolicies() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 5, 2, 0);

    String[] args = new String[]{"-verifyClusterSetup", "-policy",
        "RS-3-2-1024k"};
    int ret = admin.run(args);
    LOG.info("Command stdout: {}", out.toString());
    LOG.info("Command stderr: {}", err.toString());
    assertEquals("Return value of the command is not successful", 2, ret);
    assertTrue("Result of cluster topology verify " +
        "should be logged correctly", out.toString()
        .contains("less than the minimum required number of racks (3) " +
            "for the erasure coding policies: RS-3-2-1024k"));
    assertTrue("Error output should be empty", err.toString().isEmpty());

    resetOutputs();
    args = new String[]{"-verifyClusterSetup", "-policy",
        "RS-10-4-1024k", "RS-3-2-1024k"};
    ret = admin.run(args);
    LOG.info("Command stdout: {}", out.toString());
    LOG.info("Command stderr: {}", err.toString());
    assertEquals("Return value of the command is not successful", 2, ret);
    assertTrue("Result of cluster topology verify " +
        "should be logged correctly", out.toString()
        .contains(
            "for the erasure coding policies: RS-10-4-1024k, RS-3-2-1024k"));
    assertTrue("Error output should be empty", err.toString().isEmpty());

    resetOutputs();
    args = new String[]{"-verifyClusterSetup", "-policy", "invalidPolicy"};
    ret = admin.run(args);
    LOG.info("Command stdout: {}", out.toString());
    LOG.info("Command stderr: {}", err.toString());
    assertEquals("Return value of the command is not successful", -1, ret);
    assertTrue("Error message should be logged", err.toString()
        .contains("The given erasure coding policy invalidPolicy " +
            "does not exist."));

    resetOutputs();
    args = new String[]{"-verifyClusterSetup", "-policy"};
    ret = admin.run(args);
    LOG.info("Command stdout: {}", out.toString());
    LOG.info("Command stderr: {}", err.toString());
    assertEquals("Return value of the command is not successful", -1, ret);
    assertTrue("Error message should be logged", err.toString()
        .contains("NotEnoughArgumentsException: Not enough arguments: " +
            "expected 1 but got 0"));
  }

  private void resetOutputs() {
    out.reset();
    err.reset();
  }
}
