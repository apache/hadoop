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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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

  @AfterEach
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

  @Test
  @Timeout(value = 30)
  public void testSetSafeMode() throws Exception {
    setUpHaCluster(false);
    // Enter safemode
    int exitCode = admin.run(new String[] {"-safemode", "enter"});
    assertEquals(0, exitCode, err.toString().trim());
    String message = "Safe mode is ON in.*";
    assertOutputMatches(message + newLine + message + newLine);

    // Get safemode
    exitCode = admin.run(new String[] {"-safemode", "get"});
    assertEquals(0, exitCode, err.toString().trim());
    message = "Safe mode is ON in.*";
    assertOutputMatches(message + newLine + message + newLine);

    // Leave safemode
    exitCode = admin.run(new String[] {"-safemode", "leave"});
    assertEquals(0, exitCode, err.toString().trim());
    message = "Safe mode is OFF in.*";
    assertOutputMatches(message + newLine + message + newLine);

    // Get safemode
    exitCode = admin.run(new String[] {"-safemode", "get"});
    assertEquals(0, exitCode, err.toString().trim());
    message = "Safe mode is OFF in.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testSaveNamespace() throws Exception {
    setUpHaCluster(false);
    // Safe mode should be turned ON in order to create namespace image.
    int exitCode = admin.run(new String[] {"-safemode", "enter"});
    assertEquals(0, exitCode, err.toString().trim());
    String message = "Safe mode is ON in.*";
    assertOutputMatches(message + newLine + message + newLine);

    exitCode = admin.run(new String[] {"-saveNamespace"});
    assertEquals(0, exitCode, err.toString().trim());
    message = "Save namespace successful for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testSaveNamespaceNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    // Safe mode should be turned ON in order to create namespace image.
    int exitCode = admin.run(new String[] {"-safemode", "enter"});
    assertEquals(0, exitCode, err.toString().trim());
    String message = "Safe mode is ON in.*";
    assertOutputMatches(message + newLine + message + newLine);

    cluster.getDfsCluster().shutdownNameNode(1);
//
    exitCode = admin.run(new String[] {"-saveNamespace"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String outMessage = "Save namespace successful for.*" + newLine;
    String errMessage = "Save namespace failed for ([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testSaveNamespaceNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    // Safe mode should be turned ON in order to create namespace image.
    int exitCode = admin.run(new String[] {"-safemode", "enter"});
    assertEquals(0, exitCode, err.toString().trim());
    String message = "Safe mode is ON in.*";
    assertOutputMatches(message + newLine + message + newLine);

    cluster.getDfsCluster().shutdownNameNode(0);

    exitCode = admin.run(new String[] {"-saveNamespace"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String errMessage = "Save namespace failed for ([\\s\\S]*)" + newLine;
    String outMessage = "Save namespace successful for.*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testSaveNamespaceNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    // Safe mode should be turned ON in order to create namespace image.
    int exitCode = admin.run(new String[] {"-safemode", "enter"});
    assertEquals(0, exitCode, err.toString().trim());
    String message = "Safe mode is ON in.*";
    assertOutputMatches(message + newLine + message + newLine);

    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);

    exitCode = admin.run(new String[] {"-saveNamespace"});
    assertNotEquals(0, exitCode, err.toString().trim());
    message = "Save namespace failed for ([\\s\\S]*)";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testRestoreFailedStorage() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-restoreFailedStorage", "check"});
    assertEquals(0, exitCode, err.toString().trim());
    String message = "restoreFailedStorage is set to false for.*";
    // Default is false
    assertOutputMatches(message + newLine + message + newLine);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "true"});
    assertEquals(0, exitCode, err.toString().trim());
    message = "restoreFailedStorage is set to true for.*";
    assertOutputMatches(message + newLine + message + newLine);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "false"});
    assertEquals(0, exitCode, err.toString().trim());
    message = "restoreFailedStorage is set to false for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testRestoreFailedStorageNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-restoreFailedStorage", "check"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String outMessage = "restoreFailedStorage is set to false for.*" + newLine;
    String errMessage = "restoreFailedStorage failed for ([\\s\\S]*)" + newLine;
    // Default is false
    assertOutputMatches(outMessage, errMessage);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "true"});
    assertNotEquals(0, exitCode, err.toString().trim());
    outMessage = "restoreFailedStorage is set to true for.*" + newLine;
    errMessage = "restoreFailedStorage failed for ([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "false"});
    assertNotEquals(0, exitCode, err.toString().trim());
    outMessage = "restoreFailedStorage is set to false for.*" + newLine;
    errMessage = "restoreFailedStorage failed for ([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testRestoreFailedStorageNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-restoreFailedStorage", "check"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String errMessage = "restoreFailedStorage failed for ([\\s\\S]*)" + newLine;
    String outMessage = "restoreFailedStorage is set to false for.*" + newLine;
    // Default is false
    assertOutputMatches(outMessage, errMessage);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "true"});
    assertNotEquals(0, exitCode, err.toString().trim());
    errMessage = "restoreFailedStorage failed for ([\\s\\S]*)" + newLine;
    outMessage = "restoreFailedStorage is set to true for.*" + newLine;
    assertOutputMatches(outMessage, errMessage);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "false"});
    assertNotEquals(0, exitCode, err.toString().trim());
    errMessage = "restoreFailedStorage failed for ([\\s\\S]*)" + newLine;
    outMessage = "restoreFailedStorage is set to false for.*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testRestoreFailedStorageNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-restoreFailedStorage", "check"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String message = "restoreFailedStorage failed for ([\\s\\S]*)";
    // Default is false
    assertOutputMatches(message + newLine + message + newLine);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "true"});
    assertNotEquals(0, exitCode, err.toString().trim());
    message = "restoreFailedStorage failed for ([\\s\\S]*)";
    assertOutputMatches(message + newLine + message + newLine);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "false"});
    assertNotEquals(0, exitCode, err.toString().trim());
    message = "restoreFailedStorage failed for ([\\s\\S]*)";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshNodes() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-refreshNodes"});
    assertEquals(0, exitCode, err.toString().trim());
    String message = "Refresh nodes successful for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshNodesNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshNodes"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String outMessage = "Refresh nodes successful for .*" + newLine;
    String errMessage = "Refresh nodes failed for ([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshNodesNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-refreshNodes"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String errMessage = "Refresh nodes failed for ([\\s\\S]*)" + newLine;
    String outMessage = "Refresh nodes successful for .*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshNodesNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshNodes"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String message = "Refresh nodes failed for ([\\s\\S]*)";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testSetBalancerBandwidth() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().transitionToActive(0);

    int exitCode = admin.run(new String[] {"-setBalancerBandwidth", "10"});
    assertEquals(0, exitCode, err.toString().trim());
    String message = "Balancer bandwidth is set to 10";
    assertOutputMatches(message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testSetBalancerBandwidthNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    cluster.getDfsCluster().transitionToActive(0);
    int exitCode = admin.run(new String[] {"-setBalancerBandwidth", "10"});
    assertEquals(0, exitCode, err.toString().trim());
    String message = "Balancer bandwidth is set to 10";
    assertOutputMatches(message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testSetBalancerBandwidthNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().transitionToActive(1);
    int exitCode = admin.run(new String[] {"-setBalancerBandwidth", "10"});
    assertEquals(0, exitCode, err.toString().trim());
    String message = "Balancer bandwidth is set to 10";
    assertOutputMatches(message + newLine);
  }

  @Test
  public void testSetBalancerBandwidthNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-setBalancerBandwidth", "10"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String message = "Balancer bandwidth is set failed." + newLine
        + ".*" + newLine;
    assertOutputMatches(message);
  }

  @Test
  @Timeout(value = 30)
  public void testSetNegativeBalancerBandwidth() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-setBalancerBandwidth", "-10"});
    assertEquals(-1, exitCode, "Negative bandwidth value must fail the command");
  }

  @Test
  @Timeout(value = 30)
  public void testMetaSave() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().transitionToActive(0);
    int exitCode = admin.run(new String[] {"-metasave", "dfs.meta"});
    assertEquals(0, exitCode, err.toString().trim());
    String messageFromActiveNN = "Created metasave file dfs.meta "
        + "in the log directory of namenode.*";
    String messageFromStandbyNN = "Skip Standby NameNode, since it "
        + "cannot perform metasave operation";
    assertOutputMatches(messageFromActiveNN + newLine +
        messageFromStandbyNN + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testMetaSaveNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().transitionToActive(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-metasave", "dfs.meta"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String outMessage = "Created metasave file dfs.meta in the log " +
            "directory of namenode.*" + newLine;
    String errMessage = "Created metasave file dfs.meta in the log " +
            "directory of namenode.*failed" + newLine + ".*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testMetaSaveNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().transitionToActive(1);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-metasave", "dfs.meta"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String errMessage = "Created metasave file dfs.meta in the log " +
            "directory of namenode.*failed" + newLine + ".*" + newLine;
    String outMessage = "Created metasave file dfs.meta in the log " +
            "directory of namenode.*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testMetaSaveNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-metasave", "dfs.meta"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String message = "([\\s\\S]*)2 exceptions([\\s\\S]*)";
    assertOutputMatches(message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshServiceAcl() throws Exception {
    setUpHaCluster(true);
    int exitCode = admin.run(new String[] {"-refreshServiceAcl"});
    assertEquals(0, exitCode, err.toString().trim());
    String message = "Refresh service acl successful for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshServiceAclNN1UpNN2Down() throws Exception {
    setUpHaCluster(true);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshServiceAcl"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String outMessage = "Refresh service acl successful for.*" + newLine;
    String errMessage = "Refresh service acl failed for([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshServiceAclNN1DownNN2Up() throws Exception {
    setUpHaCluster(true);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-refreshServiceAcl"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String errMessage = "Refresh service acl failed for([\\s\\S]*)" + newLine;
    String outMessage = "Refresh service acl successful for.*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshServiceAclNN1DownNN2Down() throws Exception {
    setUpHaCluster(true);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshServiceAcl"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String message = "([\\s\\S]*)2 exceptions([\\s\\S]*)";
    assertOutputMatches(message + newLine);
  }


  @Test
  @Timeout(value = 30)
  public void testRefreshUserToGroupsMappings() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-refreshUserToGroupsMappings"});
    assertEquals(0, exitCode, err.toString().trim());
    String message = "Refresh user to groups mapping successful for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshUserToGroupsMappingsNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshUserToGroupsMappings"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String outMessage = "Refresh user to groups mapping successful for.*" + newLine;
    String errMessage = "Refresh user to groups mapping failed for([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshUserToGroupsMappingsNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-refreshUserToGroupsMappings"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String errMessage = "Refresh user to groups mapping failed for([\\s\\S]*)" + newLine;
    String outMessage = "Refresh user to groups mapping successful for.*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshUserToGroupsMappingsNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshUserToGroupsMappings"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String message = "([\\s\\S]*)2 exceptions([\\s\\S]*)";
    assertOutputMatches(message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshSuperUserGroupsConfiguration() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(
        new String[] {"-refreshSuperUserGroupsConfiguration"});
    assertEquals(0, exitCode, err.toString().trim());
    String message = "Refresh super user groups configuration successful for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshSuperUserGroupsConfigurationNN1UpNN2Down()
      throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(
        new String[] {"-refreshSuperUserGroupsConfiguration"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String outMessage = "Refresh super user groups configuration successful for.*"
            + newLine;
    String errMessage = "Refresh super user groups configuration failed for([\\s\\S]*)"
            + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshSuperUserGroupsConfigurationNN1DownNN2Up()
      throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(
        new String[] {"-refreshSuperUserGroupsConfiguration"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String errMessage = "Refresh super user groups configuration failed for([\\s\\S]*)"
            + newLine;
    String outMessage = "Refresh super user groups configuration successful for.*"
            + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshSuperUserGroupsConfigurationNN1DownNN2Down()
      throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(
        new String[] {"-refreshSuperUserGroupsConfiguration"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String message = "([\\s\\S]*)2 exceptions([\\s\\S]*)";
    assertOutputMatches(message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshCallQueue() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-refreshCallQueue"});
    assertEquals(0, exitCode, err.toString().trim());
    String message = "Refresh call queue successful for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshCallQueueNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshCallQueue"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String outMessage = "Refresh call queue successful for.*" + newLine;
    String errMessage = "Refresh call queue failed for([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshCallQueueNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-refreshCallQueue"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String errMessage = "Refresh call queue failed for([\\s\\S]*)" + newLine;
    String outMessage = "Refresh call queue successful for.*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testRefreshCallQueueNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshCallQueue"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String message = "([\\s\\S]*)2 exceptions([\\s\\S]*)";
    assertOutputMatches(message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testFinalizeUpgrade() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-finalizeUpgrade"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String message = ".*Cannot finalize with no NameNode active";
    assertOutputMatches(message + newLine);

    cluster.getDfsCluster().transitionToActive(0);
    exitCode = admin.run(new String[] {"-finalizeUpgrade"});
    assertEquals(0, exitCode, err.toString().trim());
    message = "Finalize upgrade successful for.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test
  @Timeout(value = 30)
  public void testFinalizeUpgradeNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    cluster.getDfsCluster().transitionToActive(0);
    int exitCode = admin.run(new String[] {"-finalizeUpgrade"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String outMessage = "Finalize upgrade successful for .*" + newLine;
    String errMessage = "Finalize upgrade failed for ([\\s\\S]*)" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testFinalizeUpgradeNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().transitionToActive(1);
    int exitCode = admin.run(new String[] {"-finalizeUpgrade"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String errMessage = "Finalize upgrade failed for ([\\s\\S]*)" + newLine;
    String outMessage = "Finalize upgrade successful for .*" + newLine;
    assertOutputMatches(outMessage, errMessage);
  }

  @Test
  @Timeout(value = 30)
  public void testFinalizeUpgradeNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-finalizeUpgrade"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String message = ".*2 exceptions.*";
    assertOutputMatches(message + newLine);
  }

  @Test
  @Timeout(value = 300)
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
    assertEquals(0, exitCode, err.toString().trim());
    message = finalizeSuccessMsg + newLine + finalizeSuccessMsg + newLine;
    assertOutputMatches(message);

    // NNs should return "upgrade finalized" msg
    message = finalizedMsg + newLine + finalizedMsg + newLine;
    verifyUpgradeQueryOutput(message, 0);
  }

  private void verifyUpgradeQueryOutput(String message, int expected) throws
      Exception {
    int exitCode = admin.run(new String[] {"-upgrade", "query"});
    assertEquals(expected, exitCode, err.toString().trim());
    assertOutputMatches(message);
  }

  @Test
  @Timeout(value = 30)
  public void testListOpenFilesNN1UpNN2Down() throws Exception{
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    cluster.getDfsCluster().transitionToActive(0);
    int exitCode = admin.run(new String[] {"-listOpenFiles"});
    assertEquals(0, exitCode, err.toString().trim());
  }

  @Test
  @Timeout(value = 30)
  public void testListOpenFilesNN1DownNN2Up() throws Exception{
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().transitionToActive(1);
    int exitCode = admin.run(new String[] {"-listOpenFiles"});
    assertEquals(0, exitCode, err.toString().trim());
  }

  @Test
  public void testListOpenFilesNN1DownNN2Down() throws Exception{
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-listOpenFiles"});
    assertNotEquals(0, exitCode, err.toString().trim());
    String message = "List open files failed." + newLine
            + ".*" + newLine;
    assertOutputMatches(message);
  }
}
