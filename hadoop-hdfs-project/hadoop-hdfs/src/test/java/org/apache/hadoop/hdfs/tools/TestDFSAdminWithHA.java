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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.ha.BootstrapStandby;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestDFSAdminWithHA {

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private MiniQJMHACluster cluster;
  private Configuration conf;
  private DFSAdmin admin;
  private static final PrintStream oldOut = System.out;
  private static final PrintStream oldErr = System.err;

  private static final String NSID = "ns1";
  private static String newLine = System.getProperty("line.separator");

  private void assertOutputMatches(String string) {
    String errOutput = new String(err.toByteArray(), StandardCharsets.UTF_8);
    String output = new String(out.toByteArray(), StandardCharsets.UTF_8);

    if (!errOutput.matches(string) && !output.matches(string)) {
      fail("Expected output to match '" + string +
          "' but err_output was:\n" + errOutput +
          "\n and output was: \n" + output);
    }

    out.reset();
    err.reset();
  }

  private void assertOutputMatches(String outMessage, String errMessage) {
    String errOutput = new String(err.toByteArray(), StandardCharsets.UTF_8);
    String output = new String(out.toByteArray(), StandardCharsets.UTF_8);

    if (!errOutput.matches(errMessage) || !output.matches(outMessage)) {
      fail("Expected output to match '" + outMessage + " and " + errMessage +
              "' but err_output was:\n" + errOutput + "\n and output was: \n" +
              output);
    }

    out.reset();
    err.reset();
  }

  private void setHAConf(Configuration conf, String nn1Addr, String nn2Addr) {
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        "hdfs://" + NSID);
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, NSID);
    conf.set(DFSConfigKeys.DFS_NAMESERVICE_ID, NSID);
    conf.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX, NSID), "nn1,nn2");
    conf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, "nn1");
    conf.set(DFSUtil.addKeySuffixes(
            DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, NSID, "nn1"), nn1Addr);
    conf.set(DFSUtil.addKeySuffixes(
            DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, NSID, "nn2"), nn2Addr);
  }

  private void setUpHaCluster(boolean security) throws Exception {
    conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        security);
    String baseDir = GenericTestUtils.getRandomizedTempPath();
    cluster = new MiniQJMHACluster.Builder(conf).baseDir(baseDir).build();
    setHAConf(conf, cluster.getDfsCluster().getNameNode(0).getHostAndPort(),
        cluster.getDfsCluster().getNameNode(1).getHostAndPort());
    cluster.getDfsCluster().getNameNode(0).getHostAndPort();
    admin = new DFSAdmin();
    admin.setConf(conf);
    assertTrue(HAUtil.isHAEnabled(conf, "ns1"));

    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));

    // Reduce the number of retries to speed up the tests.
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 3);
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY,
        500);
    conf.setInt(HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_KEY, 2);
    conf.setInt(HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_KEY, 2);
    conf.setInt(HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_KEY, 0);
    conf.setInt(HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_KEY, 0);
  }

  @After
  public void tearDown() throws Exception {
    try {
      System.out.flush();
      System.err.flush();
    } finally {
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
    if (admin != null) {
      admin.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
    out.reset();
    err.reset();
  }

  @Test(timeout = 30000)
  public void testSetSafeMode() throws Exception {
    setUpHaCluster(false);
    // Enter safemode
    int exitCode = admin.run(new String[] {"-safemode", "enter"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Safe mode is ON in.*";
    assertOutputMatches(message + newLine + message + newLine);

    // Get safemode
    exitCode = admin.run(new String[] {"-safemode", "get"});
    assertEquals(err.toString().trim(), 0, exitCode);
    message = "Safe mode is ON in.*";
    assertOutputMatches(message + newLine + message + newLine);

    // Leave safemode
    exitCode = admin.run(new String[] {"-safemode", "leave"});
    assertEquals(err.toString().trim(), 0, exitCode);
    message = "Safe mode is OFF in.*";
    assertOutputMatches(message + newLine + message + newLine);

    // Get safemode
    exitCode = admin.run(new String[] {"-safemode", "get"});
    assertEquals(err.toString().trim(), 0, exitCode);
    message = "Safe mode is OFF in.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test (timeout = 30000)
  public void testSaveNamespace() throws Exception {
    setUpHaCluster(false);
    // Safe mode should be turned ON in order to create namespace image.
    int exitCode = admin.run(new String[] {"-safemode", "enter"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Safe mode is ON in.*";
    assertOutputMatches(message + newLine + message + newLine);

    exitCode = admin.run(new String[] {"-saveNamespace"});
    assertEquals(err.toString().trim(), 0, exitCode);
    message = "Save namespace successful for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test (timeout = 30000)
  public void testSaveNamespaceNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    // Safe mode should be turned ON in order to create namespace image.
    int exitCode = admin.run(new String[] {"-safemode", "enter"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Safe mode is ON in.*";
    assertOutputMatches(message + newLine + message + newLine);

    cluster.getDfsCluster().shutdownNameNode(1);
//
    exitCode = admin.run(new String[] {"-saveNamespace"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String outMessage = "Save namespace successful for.*" + newLine;
    String errMessage = "Save namespace failed for ([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testSaveNamespaceNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    // Safe mode should be turned ON in order to create namespace image.
    int exitCode = admin.run(new String[] {"-safemode", "enter"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Safe mode is ON in.*";
    assertOutputMatches(message + newLine + message + newLine);

    cluster.getDfsCluster().shutdownNameNode(0);

    exitCode = admin.run(new String[] {"-saveNamespace"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String errMessage = "Save namespace failed for ([\\s\\S]*)" + newLine;
    String outMessage = "Save namespace successful for.*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testSaveNamespaceNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    // Safe mode should be turned ON in order to create namespace image.
    int exitCode = admin.run(new String[] {"-safemode", "enter"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Safe mode is ON in.*";
    assertOutputMatches(message + newLine + message + newLine);

    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);

    exitCode = admin.run(new String[] {"-saveNamespace"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    message = "Save namespace failed for ([\\s\\S]*)";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test (timeout = 30000)
  public void testRestoreFailedStorage() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-restoreFailedStorage", "check"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "restoreFailedStorage is set to false for.*";
    // Default is false
    assertOutputMatches(message + newLine + message + newLine);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "true"});
    assertEquals(err.toString().trim(), 0, exitCode);
    message = "restoreFailedStorage is set to true for.*";
    assertOutputMatches(message + newLine + message + newLine);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "false"});
    assertEquals(err.toString().trim(), 0, exitCode);
    message = "restoreFailedStorage is set to false for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test (timeout = 30000)
  public void testRestoreFailedStorageNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-restoreFailedStorage", "check"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String outMessage = "restoreFailedStorage is set to false for.*" + newLine;
    String errMessage = "restoreFailedStorage failed for ([\\s\\S]*)" + newLine;
    // Default is false
    assertOutputMatches(outMessage, errMessage);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "true"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    outMessage = "restoreFailedStorage is set to true for.*" + newLine;
    errMessage = "restoreFailedStorage failed for ([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "false"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    outMessage = "restoreFailedStorage is set to false for.*" + newLine;
    errMessage = "restoreFailedStorage failed for ([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testRestoreFailedStorageNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-restoreFailedStorage", "check"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String errMessage = "restoreFailedStorage failed for ([\\s\\S]*)" + newLine;
    String outMessage = "restoreFailedStorage is set to false for.*" + newLine;
    // Default is false
    assertOutputMatches(outMessage, errMessage);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "true"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    errMessage = "restoreFailedStorage failed for ([\\s\\S]*)" + newLine;
    outMessage = "restoreFailedStorage is set to true for.*" + newLine;
    assertOutputMatches(outMessage, errMessage);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "false"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    errMessage = "restoreFailedStorage failed for ([\\s\\S]*)" + newLine;
    outMessage = "restoreFailedStorage is set to false for.*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testRestoreFailedStorageNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-restoreFailedStorage", "check"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "restoreFailedStorage failed for ([\\s\\S]*)";
    // Default is false
    assertOutputMatches(message + newLine + message + newLine);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "true"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    message = "restoreFailedStorage failed for ([\\s\\S]*)";
    assertOutputMatches(message + newLine + message + newLine);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "false"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    message = "restoreFailedStorage failed for ([\\s\\S]*)";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test (timeout = 30000)
  public void testRefreshNodes() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-refreshNodes"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh nodes successful for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test (timeout = 30000)
  public void testRefreshTopology() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[]{"-refreshTopology", NSID});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh topology successful for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test (timeout = 30000)
  public void testRefreshTopologyUnknownNsid() throws Exception {
    setUpHaCluster(false);
    String unknownNsid = "unknown_nsid";
    int exitCode = admin.run(new String[]{"-refreshTopology", unknownNsid});
    assertEquals(err.toString().trim(), -1, exitCode);
    String message = "refreshTopology: Can not get proxy for nameservice.*[\\s]*";
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testRefreshNodesNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshNodes"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String outMessage = "Refresh nodes successful for .*" + newLine;
    String errMessage = "Refresh nodes failed for ([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testRefreshNodesNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-refreshNodes"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String errMessage = "Refresh nodes failed for ([\\s\\S]*)" + newLine;
    String outMessage = "Refresh nodes successful for .*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testRefreshNodesNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshNodes"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh nodes failed for ([\\s\\S]*)";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test (timeout = 30000)
  public void testSetBalancerBandwidth() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().transitionToActive(0);

    int exitCode = admin.run(new String[] {"-setBalancerBandwidth", "10"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Balancer bandwidth is set to 10";
    assertOutputMatches(message + newLine);
  }

  @Test (timeout = 30000)
  public void testSetBalancerBandwidthNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    cluster.getDfsCluster().transitionToActive(0);
    int exitCode = admin.run(new String[] {"-setBalancerBandwidth", "10"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Balancer bandwidth is set to 10";
    assertOutputMatches(message + newLine);
  }

  @Test (timeout = 30000)
  public void testSetBalancerBandwidthNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().transitionToActive(1);
    int exitCode = admin.run(new String[] {"-setBalancerBandwidth", "10"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Balancer bandwidth is set to 10";
    assertOutputMatches(message + newLine);
  }

  @Test
  public void testSetBalancerBandwidthNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-setBalancerBandwidth", "10"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Balancer bandwidth is set failed." + newLine
        + ".*" + newLine;
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testSetNegativeBalancerBandwidth() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-setBalancerBandwidth", "-10"});
    assertEquals("Negative bandwidth value must fail the command", -1, exitCode);
  }

  @Test (timeout = 30000)
  public void testMetaSave() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().transitionToActive(0);
    int exitCode = admin.run(new String[] {"-metasave", "dfs.meta"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String messageFromActiveNN = "Created metasave file dfs.meta "
        + "in the log directory of namenode.*";
    String messageFromStandbyNN = "Skip Standby NameNode, since it "
        + "cannot perform metasave operation";
    assertOutputMatches(messageFromActiveNN + newLine +
        messageFromStandbyNN + newLine);
  }

  @Test (timeout = 30000)
  public void testMetaSaveNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().transitionToActive(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-metasave", "dfs.meta"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String outMessage = "Created metasave file dfs.meta in the log " +
            "directory of namenode.*" + newLine;
    String errMessage = "Created metasave file dfs.meta in the log " +
            "directory of namenode.*failed" + newLine + ".*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testMetaSaveNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().transitionToActive(1);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-metasave", "dfs.meta"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String errMessage = "Created metasave file dfs.meta in the log " +
            "directory of namenode.*failed" + newLine + ".*" + newLine;
    String outMessage = "Created metasave file dfs.meta in the log " +
            "directory of namenode.*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testMetaSaveNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-metasave", "dfs.meta"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "([\\s\\S]*)2 exceptions([\\s\\S]*)";
    assertOutputMatches(message + newLine);
  }

  @Test (timeout = 30000)
  public void testRefreshServiceAcl() throws Exception {
    setUpHaCluster(true);
    int exitCode = admin.run(new String[] {"-refreshServiceAcl"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh service acl successful for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test (timeout = 30000)
  public void testRefreshServiceAclNN1UpNN2Down() throws Exception {
    setUpHaCluster(true);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshServiceAcl"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String outMessage = "Refresh service acl successful for.*" + newLine;
    String errMessage = "Refresh service acl failed for([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testRefreshServiceAclNN1DownNN2Up() throws Exception {
    setUpHaCluster(true);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-refreshServiceAcl"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String errMessage = "Refresh service acl failed for([\\s\\S]*)" + newLine;
    String outMessage = "Refresh service acl successful for.*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testRefreshServiceAclNN1DownNN2Down() throws Exception {
    setUpHaCluster(true);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshServiceAcl"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "([\\s\\S]*)2 exceptions([\\s\\S]*)";
    assertOutputMatches(message + newLine);
  }


  @Test (timeout = 30000)
  public void testRefreshUserToGroupsMappings() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-refreshUserToGroupsMappings"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh user to groups mapping successful for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test (timeout = 30000)
  public void testRefreshUserToGroupsMappingsNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshUserToGroupsMappings"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String outMessage = "Refresh user to groups mapping successful for.*" + newLine;
    String errMessage = "Refresh user to groups mapping failed for([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testRefreshUserToGroupsMappingsNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-refreshUserToGroupsMappings"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String errMessage = "Refresh user to groups mapping failed for([\\s\\S]*)" + newLine;
    String outMessage = "Refresh user to groups mapping successful for.*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testRefreshUserToGroupsMappingsNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshUserToGroupsMappings"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "([\\s\\S]*)2 exceptions([\\s\\S]*)";
    assertOutputMatches(message + newLine);
  }

  @Test (timeout = 30000)
  public void testRefreshSuperUserGroupsConfiguration() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(
        new String[] {"-refreshSuperUserGroupsConfiguration"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh super user groups configuration successful for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test (timeout = 30000)
  public void testRefreshSuperUserGroupsConfigurationNN1UpNN2Down()
      throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(
        new String[] {"-refreshSuperUserGroupsConfiguration"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String outMessage = "Refresh super user groups configuration successful for.*"
            + newLine;
    String errMessage = "Refresh super user groups configuration failed for([\\s\\S]*)"
            + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testRefreshSuperUserGroupsConfigurationNN1DownNN2Up()
      throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(
        new String[] {"-refreshSuperUserGroupsConfiguration"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String errMessage = "Refresh super user groups configuration failed for([\\s\\S]*)"
            + newLine;
    String outMessage = "Refresh super user groups configuration successful for.*"
            + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testRefreshSuperUserGroupsConfigurationNN1DownNN2Down()
      throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(
        new String[] {"-refreshSuperUserGroupsConfiguration"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "([\\s\\S]*)2 exceptions([\\s\\S]*)";
    assertOutputMatches(message + newLine);
  }

  @Test (timeout = 30000)
  public void testRefreshCallQueue() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-refreshCallQueue"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh call queue successful for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test (timeout = 30000)
  public void testRefreshCallQueueNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshCallQueue"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String outMessage = "Refresh call queue successful for.*" + newLine;
    String errMessage = "Refresh call queue failed for([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testRefreshCallQueueNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-refreshCallQueue"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String errMessage = "Refresh call queue failed for([\\s\\S]*)" + newLine;
    String outMessage = "Refresh call queue successful for.*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testRefreshCallQueueNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshCallQueue"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "([\\s\\S]*)2 exceptions([\\s\\S]*)";
    assertOutputMatches(message + newLine);
  }

  @Test (timeout = 30000)
  public void testFinalizeUpgrade() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-finalizeUpgrade"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = ".*Cannot finalize with no NameNode active";
    assertOutputMatches(message + newLine);

    cluster.getDfsCluster().transitionToActive(0);
    exitCode = admin.run(new String[] {"-finalizeUpgrade"});
    assertEquals(err.toString().trim(), 0, exitCode);
    message = "Finalize upgrade successful for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test (timeout = 30000)
  public void testFinalizeUpgradeNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    cluster.getDfsCluster().transitionToActive(0);
    int exitCode = admin.run(new String[] {"-finalizeUpgrade"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String outMessage = "Finalize upgrade successful for .*" + newLine;
    String errMessage = "Finalize upgrade failed for ([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testFinalizeUpgradeNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().transitionToActive(1);
    int exitCode = admin.run(new String[] {"-finalizeUpgrade"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String errMessage = "Finalize upgrade failed for ([\\s\\S]*)" + newLine;
    String outMessage = "Finalize upgrade successful for .*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test (timeout = 30000)
  public void testFinalizeUpgradeNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-finalizeUpgrade"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = ".*2 exceptions.*";
    assertOutputMatches(message + newLine);
  }

  @Test (timeout = 300000)
  public void testUpgradeCommand() throws Exception {
    final String finalizedMsg = "Upgrade finalized for.*";
    final String notFinalizedMsg = "Upgrade not finalized for.*";
    final String failMsg = "Getting upgrade status failed for.*" + newLine +
        "upgrade: .*";
    final String finalizeSuccessMsg = "Finalize upgrade successful for.*";

    setUpHaCluster(false);
    MiniDFSCluster dfsCluster = cluster.getDfsCluster();

    // Before upgrade is initialized, the query should return upgrade
    // finalized (as no upgrade is in progress)
    String message = finalizedMsg + newLine + finalizedMsg + newLine;
    verifyUpgradeQueryOutput(message, 0);

    // Shutdown the NNs
    dfsCluster.shutdownNameNode(0);
    dfsCluster.shutdownNameNode(1);

    // Start NN1 with -upgrade option
    dfsCluster.getNameNodeInfos()[0].setStartOpt(
        HdfsServerConstants.StartupOption.UPGRADE);
    dfsCluster.restartNameNode(0, true);

    // Running -upgrade query should return "not finalized" for NN1 and
    // connection exception for NN2 (as NN2 is down)
    message = notFinalizedMsg + newLine;
    verifyUpgradeQueryOutput(message, -1);
    String errorMsg =  failMsg + newLine;
    verifyUpgradeQueryOutput(errorMsg, -1);

    // Bootstrap the standby (NN2) with the upgraded info.
    int rc = BootstrapStandby.run(
        new String[]{"-force"},
        dfsCluster.getConfiguration(1));
    assertEquals(0, rc);
    out.reset();

    // Restart NN2.
    dfsCluster.restartNameNode(1);

    // Both NNs should return "not finalized" msg for -upgrade query
    message = notFinalizedMsg + newLine + notFinalizedMsg + newLine;
    verifyUpgradeQueryOutput(message, 0);

    // Finalize the upgrade
    int exitCode = admin.run(new String[] {"-upgrade", "finalize"});
    assertEquals(err.toString().trim(), 0, exitCode);
    message = finalizeSuccessMsg + newLine + finalizeSuccessMsg + newLine;
    assertOutputMatches(message);

    // NNs should return "upgrade finalized" msg
    message = finalizedMsg + newLine + finalizedMsg + newLine;
    verifyUpgradeQueryOutput(message, 0);
  }

  private void verifyUpgradeQueryOutput(String message, int expected) throws
      Exception {
    int exitCode = admin.run(new String[] {"-upgrade", "query"});
    assertEquals(err.toString().trim(), expected, exitCode);
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testListOpenFilesNN1UpNN2Down() throws Exception{
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    cluster.getDfsCluster().transitionToActive(0);
    int exitCode = admin.run(new String[] {"-listOpenFiles"});
    assertEquals(err.toString().trim(), 0, exitCode);
  }

  @Test (timeout = 30000)
  public void testListOpenFilesNN1DownNN2Up() throws Exception{
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().transitionToActive(1);
    int exitCode = admin.run(new String[] {"-listOpenFiles"});
    assertEquals(err.toString().trim(), 0, exitCode);
  }

  @Test
  public void testListOpenFilesNN1DownNN2Down() throws Exception{
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-listOpenFiles"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "List open files failed." + newLine
            + ".*" + newLine;
    assertOutputMatches(message);
  }
}
