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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests some ECAdmin scenarios that are hard to test from
 * {@link org.apache.hadoop.cli.TestErasureCodingCLI}.
 */
@Timeout(300)
public class TestECAdmin {
  public static final Logger LOG = LoggerFactory.getLogger(TestECAdmin.class);
  private Configuration conf = new Configuration();
  private MiniDFSCluster cluster;
  private ECAdmin admin = new ECAdmin(conf);

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();

  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

  private final static String RS_3_2 =
      SystemErasureCodingPolicies.getByID(
          SystemErasureCodingPolicies.RS_3_2_POLICY_ID).getName();
  private final static String RS_6_3 =
      SystemErasureCodingPolicies.getByID(
          SystemErasureCodingPolicies.RS_6_3_POLICY_ID).getName();
  private final static String RS_10_4 =
      SystemErasureCodingPolicies.getByID(
          SystemErasureCodingPolicies.RS_10_4_POLICY_ID).getName();
  private final static String XOR_2_1 =
      SystemErasureCodingPolicies.getByID(
          SystemErasureCodingPolicies.XOR_2_1_POLICY_ID).getName();

  @BeforeEach
  public void setup() throws Exception {
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
  }

  @AfterEach
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
    final int numDataNodes = 6;
    final int numRacks = 3;
    final int expectedNumDataNodes = 9;

    cluster = DFSTestUtil.setupCluster(conf, numDataNodes, numRacks, 0);
    int ret = runCommandWithParams("-verifyClusterSetup");
    assertEquals(2, ret, "Return value of the command is not successful");
    assertNotEnoughDataNodesMessage(RS_6_3, numDataNodes, expectedNumDataNodes);
  }

  @Test
  public void testRS104MinRacks() throws Exception {
    final String testPolicy = RS_10_4;
    final int numDataNodes = 15;
    final int numRacks = 3;
    final int expectedNumRacks = 4;

    cluster = DFSTestUtil.setupCluster(conf, numDataNodes, numRacks, 0);
    cluster.getFileSystem().disableErasureCodingPolicy(RS_6_3);
    cluster.getFileSystem().enableErasureCodingPolicy(testPolicy);
    int ret = runCommandWithParams("-verifyClusterSetup");
    assertEquals(2, ret, "Return value of the command is not successful");
    assertNotEnoughRacksMessage(testPolicy, numRacks, expectedNumRacks);
  }

  @Test
  public void testXOR21MinRacks() throws Exception {
    final String testPolicy = XOR_2_1;
    final int numDataNodes = 5;
    final int numRacks = 2;
    final int expectedNumRacks = 3;

    cluster = DFSTestUtil.setupCluster(conf, numDataNodes, numRacks, 0);
    cluster.getFileSystem().disableErasureCodingPolicy(RS_6_3);
    cluster.getFileSystem().enableErasureCodingPolicy(testPolicy);
    int ret = runCommandWithParams("-verifyClusterSetup");
    assertEquals(2, ret, "Return value of the command is not successful");
    assertNotEnoughRacksMessage(testPolicy, numRacks, expectedNumRacks);
  }

  @Test
  public void testRS32MinRacks() throws Exception {
    final String testPolicy = RS_3_2;
    final int numDataNodes = 5;
    final int numRacks = 2;
    final int expectedNumRacks = 3;

    cluster = DFSTestUtil.setupCluster(conf, numDataNodes, numRacks, 0);
    cluster.getFileSystem().disableErasureCodingPolicy(RS_6_3);
    cluster.getFileSystem().enableErasureCodingPolicy(testPolicy);
    int ret = runCommandWithParams("-verifyClusterSetup");
    assertEquals(2, ret, "Return value of the command is not successful");
    assertNotEnoughRacksMessage(testPolicy, numRacks, expectedNumRacks);
  }

  @Test
  public void testRS63Good() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 9, 3, 0);
    int ret = runCommandWithParams("-verifyClusterSetup");
    assertEquals(0, ret, "Return value of the command is successful");
    assertTrue(out.toString().contains(
            "The cluster setup can support EC policies: " + RS_6_3),
        "Result of cluster topology verify " +
            "should be logged correctly");
    assertTrue(err.toString().isEmpty(), "Error output should be empty");
  }

  @Test
  public void testNoECEnabled() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 9, 3, 0);
    cluster.getFileSystem().disableErasureCodingPolicy(RS_6_3);
    int ret = runCommandWithParams("-verifyClusterSetup");
    assertEquals(0, ret, "Return value of the command is successful");
    assertTrue(out.toString().contains("No erasure coding policy is given"),
        "Result of cluster topology verify " +
            "should be logged correctly");
    assertTrue(err.toString().isEmpty(), "Error output should be empty");
  }

  @Test
  public void testUnsuccessfulEnablePolicyMessage() throws Exception {
    final String testPolicy = RS_3_2;
    final int numDataNodes = 5;
    final int numRacks = 2;

    cluster = DFSTestUtil.setupCluster(conf, numDataNodes, numRacks, 0);
    cluster.getFileSystem().disableErasureCodingPolicy(RS_6_3);
    final int ret = runCommandWithParams("-enablePolicy", "-policy",
        testPolicy);

    assertEquals(0, ret, "Return value of the command is successful");
    assertTrue(out.toString().contains("Erasure coding policy " + testPolicy + " is enabled"),
        "Enabling policy should be logged");
    assertTrue(err.toString().contains("Warning: The cluster setup does not support " +
            "EC policy " + testPolicy + ". Reason:"),
        "Warning about cluster topology should be printed");
    assertTrue(err.toString()
        .contains(" racks are required for the erasure coding policies: " +
            testPolicy), "Warning about cluster topology should be printed");
  }

  @Test
  public void testSuccessfulEnablePolicyMessage() throws Exception {
    final String testPolicy = RS_3_2;
    cluster = DFSTestUtil.setupCluster(conf, 5, 3, 0);
    cluster.getFileSystem().disableErasureCodingPolicy(RS_6_3);
    final int ret = runCommandWithParams("-enablePolicy", "-policy",
        testPolicy);

    assertEquals(0, ret, "Return value of the command is successful");
    assertTrue(out.toString()
            .contains("Erasure coding policy " + testPolicy + " is enabled"),
        "Enabling policy should be logged");
    assertFalse(out.toString().contains("Warning: The cluster setup does not support"),
        "Warning about cluster topology should not be printed");
    assertTrue(err.toString().isEmpty(), "Error output should be empty");
  }

  @Test
  public void testEnableNonExistentPolicyMessage() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 5, 3, 0);
    cluster.getFileSystem().disableErasureCodingPolicy(RS_6_3);
    final int ret = runCommandWithParams("-enablePolicy", "-policy",
        "NonExistentPolicy");

    assertEquals(2, ret, "Return value of the command is unsuccessful");
    assertFalse(out.toString().contains("is enabled"),
        "Enabling policy should not be logged when " + "it was unsuccessful");
    assertTrue(err.toString().contains("RemoteException: The policy name " +
        "NonExistentPolicy does not exist"), "Error message should be printed");
  }

  @Test
  public void testVerifyClusterSetupWithGivenPolicies() throws Exception {
    final int numDataNodes = 5;
    final int numRacks = 2;
    cluster = DFSTestUtil.setupCluster(conf, numDataNodes, numRacks, 0);

    int ret = runCommandWithParams("-verifyClusterSetup", "-policy", RS_3_2);
    assertEquals(2, ret, "Return value of the command is not successful");
    assertNotEnoughRacksMessage(RS_3_2, numRacks, 3);

    resetOutputs();
    ret = runCommandWithParams("-verifyClusterSetup", "-policy",
        RS_10_4, RS_3_2);
    assertEquals(2, ret, "Return value of the command is not successful");
    assertNotEnoughDataNodesMessage(RS_10_4 + ", " + RS_3_2,
        numDataNodes, 14);

    resetOutputs();
    ret = runCommandWithParams("-verifyClusterSetup", "-policy",
        "invalidPolicy");
    assertEquals(-1, ret, "Return value of the command is not successful");
    assertTrue(err.toString()
        .contains("The given erasure coding policy invalidPolicy " +
            "does not exist."), "Error message should be logged");

    resetOutputs();
    ret = runCommandWithParams("-verifyClusterSetup", "-policy");
    assertEquals(-1, ret, "Return value of the command is not successful");
    assertTrue(err.toString()
        .contains("NotEnoughArgumentsException: Not enough arguments: " +
            "expected 1 but got 0"), "Error message should be logged");
  }

  @Test
  public void testVerifyClusterSetupSpecifiedPolicies() throws Exception {
    final int numDataNodes = 5;
    final int numRacks = 3;

    cluster = DFSTestUtil.setupCluster(conf, numDataNodes, numRacks, 0);
    cluster.getFileSystem().enableErasureCodingPolicy(XOR_2_1);

    int ret = runCommandWithParams("-verifyClusterSetup", XOR_2_1);
    assertEquals(1, ret, "Return value of the command is not successful");
    assertTrue(err.toString().contains("Too many arguments"), "Error message should be logged");

    resetOutputs();
    ret = runCommandWithParams("-verifyClusterSetup", "-policy");
    assertEquals(-1, ret, "Return value of the command is not successful");
    assertTrue(err.toString().contains("NotEnoughArgumentsException: Not enough arguments: " +
            "expected 1 but got 0"),
        "Error message should be logged");

    resetOutputs();
    ret = runCommandWithParams("-verifyClusterSetup", "-policy", XOR_2_1);
    assertEquals(0, ret, "Return value of the command is successful");
    assertTrue(
        out.toString().contains("The cluster setup can support EC policies: " + XOR_2_1),
        "Result of cluster topology verify " + "should be logged correctly");
    assertTrue(err.toString().isEmpty(), "Error output should be empty");

    resetOutputs();
    ret = runCommandWithParams("-verifyClusterSetup", "-policy", RS_6_3);
    assertEquals(2, ret, "Return value of the command is not successful");
    assertNotEnoughDataNodesMessage(RS_6_3, numDataNodes, 9);
  }

  private void resetOutputs() {
    out.reset();
    err.reset();
  }

  private void assertNotEnoughDataNodesMessage(String policy,
                                               int numDataNodes,
                                               int expectedNumDataNodes) {
    assertTrue(out.toString()
            .contains(expectedNumDataNodes + " DataNodes are required " +
                "for the erasure coding policies: " +
                policy + ". The number of DataNodes is only " + numDataNodes),
        "Result of cluster topology verify " +
            "should be logged correctly");
    assertTrue(
        err.toString().isEmpty(), "Error output should be empty");
  }

  private void assertNotEnoughRacksMessage(String policy,
                                           int numRacks,
                                           int expectedNumRacks) {
    assertTrue(out.toString()
            .contains(expectedNumRacks + " racks are required for " +
                "the erasure coding policies: " +
                policy + ". The number of racks is only " + numRacks),
        "Result of cluster topology verify " +
            "should be logged correctly");
    assertTrue(err.toString().isEmpty(), "Error output should be empty");
  }

  private int runCommandWithParams(String... args) throws Exception{
    final int ret = admin.run(args);
    LOG.info("Command stdout: {}", out.toString());
    LOG.info("Command stderr: {}", err.toString());
    return ret;
  }
}
