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
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
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
  private static final PrintStream OLD_OUT = System.out;

  @Rule
  public Timeout globalTimeout =
      new Timeout(300000, TimeUnit.MILLISECONDS);

  @Before
  public void setup() throws Exception {
    System.setOut(new PrintStream(out));
  }

  @After
  public void tearDown() throws Exception {
    try {
      System.out.flush();
      System.err.flush();
      out.reset();
    } finally {
      System.setOut(OLD_OUT);
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
    LOG.info("Commend stdout: {}", out.toString());
    assertEquals(2, ret);
    assertTrue(out.toString()
        .contains("less than the minimum required number of DataNodes"));
  }

  @Test
  public void testRS104MinRacks() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 15, 3, 0);
    cluster.getFileSystem().enableErasureCodingPolicy(
        SystemErasureCodingPolicies
            .getByID(SystemErasureCodingPolicies.RS_10_4_POLICY_ID).getName());
    String[] args = {"-verifyClusterSetup"};
    final int ret = admin.run(args);
    LOG.info("Commend stdout: {}", out.toString());
    assertEquals(2, ret);
    assertTrue(out.toString()
        .contains("less than the minimum required number of racks"));
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
    LOG.info("Commend stdout: {}", out.toString());
    assertEquals(2, ret);
    assertTrue(out.toString()
        .contains("less than the minimum required number of racks"));
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
    LOG.info("Commend stdout: {}", out.toString());
    assertEquals(2, ret);
    assertTrue(out.toString()
        .contains("less than the minimum required number of racks"));
  }

  @Test
  public void testRS63Good() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 9, 3, 0);
    String[] args = {"-verifyClusterSetup"};
    final int ret = admin.run(args);
    LOG.info("Commend stdout: {}", out.toString());
    assertEquals(0, ret);
  }

  @Test
  public void testNoECEnabled() throws Exception {
    cluster = DFSTestUtil.setupCluster(conf, 9, 3, 0);
    cluster.getFileSystem().disableErasureCodingPolicy(
        SystemErasureCodingPolicies
            .getByID(SystemErasureCodingPolicies.RS_6_3_POLICY_ID).getName());
    String[] args = {"-verifyClusterSetup"};
    final int ret = admin.run(args);
    LOG.info("Commend stdout: {}", out.toString());
    assertEquals(0, ret);
    assertTrue(out.toString().contains("No erasure coding policy is enabled"));
  }
}
