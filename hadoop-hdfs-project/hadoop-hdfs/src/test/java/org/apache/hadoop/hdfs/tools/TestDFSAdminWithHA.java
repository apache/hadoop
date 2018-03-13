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

import com.google.common.base.Charsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
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
    String errOutput = new String(err.toByteArray(), Charsets.UTF_8);
    String output = new String(out.toByteArray(), Charsets.UTF_8);

    if (!errOutput.matches(string) && !output.matches(string)) {
      fail("Expected output to match '" + string +
          "' but err_output was:\n" + errOutput +
          "\n and output was: \n" + output);
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
    cluster = new MiniQJMHACluster.Builder(conf).build();
    setHAConf(conf, cluster.getDfsCluster().getNameNode(0).getHostAndPort(),
        cluster.getDfsCluster().getNameNode(1).getHostAndPort());
    cluster.getDfsCluster().getNameNode(0).getHostAndPort();
    admin = new DFSAdmin();
    admin.setConf(conf);
    assertTrue(HAUtil.isHAEnabled(conf, "ns1"));

    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
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
    message = "Save namespace successful for.*" + newLine
        + "Save namespace failed for.*" + newLine;
    assertOutputMatches(message);
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
    message = "Save namespace failed for.*" + newLine
        + "Save namespace successful for.*" + newLine;
    assertOutputMatches(message);
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
    message = "Save namespace failed for.*";
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
    String message = "restoreFailedStorage is set to false for.*" + newLine
        + "restoreFailedStorage failed for.*" + newLine;
    // Default is false
    assertOutputMatches(message);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "true"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    message = "restoreFailedStorage is set to true for.*" + newLine
        + "restoreFailedStorage failed for.*" + newLine;
    assertOutputMatches(message);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "false"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    message = "restoreFailedStorage is set to false for.*" + newLine
        + "restoreFailedStorage failed for.*" + newLine;
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testRestoreFailedStorageNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-restoreFailedStorage", "check"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "restoreFailedStorage failed for.*" + newLine
        + "restoreFailedStorage is set to false for.*" + newLine;
    // Default is false
    assertOutputMatches(message);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "true"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    message = "restoreFailedStorage failed for.*" + newLine
        + "restoreFailedStorage is set to true for.*" + newLine;
    assertOutputMatches(message);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "false"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    message = "restoreFailedStorage failed for.*" + newLine
        + "restoreFailedStorage is set to false for.*" + newLine;
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testRestoreFailedStorageNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-restoreFailedStorage", "check"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "restoreFailedStorage failed for.*";
    // Default is false
    assertOutputMatches(message + newLine + message + newLine);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "true"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    message = "restoreFailedStorage failed for.*";
    assertOutputMatches(message + newLine + message + newLine);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "false"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    message = "restoreFailedStorage failed for.*";
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
  public void testRefreshNodesNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshNodes"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh nodes successful for.*" + newLine
        + "Refresh nodes failed for.*" + newLine;
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testRefreshNodesNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-refreshNodes"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh nodes failed for.*" + newLine
        + "Refresh nodes successful for.*" + newLine;
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testRefreshNodesNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshNodes"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh nodes failed for.*";
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
    int exitCode = admin.run(new String[] {"-metasave", "dfs.meta"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Created metasave file dfs.meta in the log directory"
        + " of namenode.*";
    assertOutputMatches(message + newLine + message + newLine);
  }

  @Test (timeout = 30000)
  public void testMetaSaveNN1UpNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-metasave", "dfs.meta"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Created metasave file dfs.meta in the log directory"
        + " of namenode.*" + newLine
        + "Created metasave file dfs.meta in the log directory"
        + " of namenode.*failed" + newLine;
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testMetaSaveNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-metasave", "dfs.meta"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Created metasave file dfs.meta in the log directory"
        + " of namenode.*failed" + newLine
        + "Created metasave file dfs.meta in the log directory"
        + " of namenode.*" + newLine;
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testMetaSaveNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-metasave", "dfs.meta"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Created metasave file dfs.meta in the log directory"
        + " of namenode.*failed";
    assertOutputMatches(message + newLine + message + newLine);
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
    String message = "Refresh service acl successful for.*" + newLine
        + "Refresh service acl failed for.*" + newLine;
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testRefreshServiceAclNN1DownNN2Up() throws Exception {
    setUpHaCluster(true);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-refreshServiceAcl"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh service acl failed for.*" + newLine
        + "Refresh service acl successful for.*" + newLine;
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testRefreshServiceAclNN1DownNN2Down() throws Exception {
    setUpHaCluster(true);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshServiceAcl"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh service acl failed for.*";
    assertOutputMatches(message + newLine + message + newLine);
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
    String message = "Refresh user to groups mapping successful for.*"
        + newLine
        + "Refresh user to groups mapping failed for.*"
        + newLine;
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testRefreshUserToGroupsMappingsNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-refreshUserToGroupsMappings"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh user to groups mapping failed for.*"
        + newLine
        + "Refresh user to groups mapping successful for.*"
        + newLine;
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testRefreshUserToGroupsMappingsNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshUserToGroupsMappings"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh user to groups mapping failed for.*";
    assertOutputMatches(message + newLine + message + newLine);
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
    String message = "Refresh super user groups configuration successful for.*"
        + newLine
        + "Refresh super user groups configuration failed for.*"
        + newLine;
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testRefreshSuperUserGroupsConfigurationNN1DownNN2Up()
      throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(
        new String[] {"-refreshSuperUserGroupsConfiguration"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh super user groups configuration failed for.*"
        + newLine
        + "Refresh super user groups configuration successful for.*"
        + newLine;
    assertOutputMatches(message);
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
    String message = "Refresh super user groups configuration failed for.*";
    assertOutputMatches(message + newLine + message + newLine);
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
    String message = "Refresh call queue successful for.*" + newLine
        + "Refresh call queue failed for.*" + newLine;
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testRefreshCallQueueNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    int exitCode = admin.run(new String[] {"-refreshCallQueue"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh call queue failed for.*" + newLine
        + "Refresh call queue successful for.*" + newLine;
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testRefreshCallQueueNN1DownNN2Down() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().shutdownNameNode(1);
    int exitCode = admin.run(new String[] {"-refreshCallQueue"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh call queue failed for.*";
    assertOutputMatches(message + newLine + message + newLine);
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
    String message = "Finalize upgrade successful for .*" + newLine
        + "Finalize upgrade failed for .*" + newLine;
    assertOutputMatches(message);
  }

  @Test (timeout = 30000)
  public void testFinalizeUpgradeNN1DownNN2Up() throws Exception {
    setUpHaCluster(false);
    cluster.getDfsCluster().shutdownNameNode(0);
    cluster.getDfsCluster().transitionToActive(1);
    int exitCode = admin.run(new String[] {"-finalizeUpgrade"});
    assertNotEquals(err.toString().trim(), 0, exitCode);
    String message = "Finalize upgrade failed for .*" + newLine
        + "Finalize upgrade successful for .*" + newLine;
    assertOutputMatches(message);
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
    String message = ".*" + newLine + "List open files failed." + newLine;
    assertOutputMatches(message);
  }
}
