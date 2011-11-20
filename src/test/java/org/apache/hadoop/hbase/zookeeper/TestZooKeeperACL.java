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
package org.apache.hadoop.hbase.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TestZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestZooKeeperACL {
  private final static Log LOG = LogFactory.getLog(TestZooKeeperACL.class);
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private static ZooKeeperWatcher zkw;
  private static boolean secureZKAvailable;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    File saslConfFile = File.createTempFile("tmp", "jaas.conf");
    FileWriter fwriter = new FileWriter(saslConfFile);

    fwriter.write("" +
      "Server {\n" +
        "org.apache.zookeeper.server.auth.DigestLoginModule required\n" +
        "user_hbase=\"secret\";\n" +
      "};\n" +
      "Client {\n" +
        "org.apache.zookeeper.server.auth.DigestLoginModule required\n" +
        "username=\"hbase\"\n" +
        "password=\"secret\";\n" +
      "};" + "\n");
    fwriter.close();
    System.setProperty("java.security.auth.login.config",
        saslConfFile.getAbsolutePath());
    System.setProperty("zookeeper.authProvider.1",
        "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");

    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    TEST_UTIL.getConfiguration().setInt("hbase.zookeeper.property.maxClientCnxns", 1000);

    // If Hadoop is missing HADOOP-7070 the cluster will fail to start due to
    // the JAAS configuration required by ZK being clobbered by Hadoop 
    try {
      TEST_UTIL.startMiniCluster();
    } catch (IOException e) {
      LOG.warn("Hadoop is missing HADOOP-7070", e);
      secureZKAvailable = false;
      return;
    }
    zkw = new ZooKeeperWatcher(
      new Configuration(TEST_UTIL.getConfiguration()),
        TestZooKeeper.class.getName(), null);
    ZKUtil.waitForZKConnectionIfAuthenticating(zkw);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (!secureZKAvailable) {
      return;
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    if (!secureZKAvailable) {
      return;
    }
    TEST_UTIL.ensureSomeRegionServersAvailable(2);
  }

  /**
   * Create a node and check its ACL. When authentication is enabled on 
   * Zookeeper, all nodes (except /hbase/root-region-server, /hbase/master
   * and /hbase/hbaseid) should be created so that only the hbase server user
   * (master or region server user) that created them can access them, and
   * this user should have all permissions on this node. For
   * /hbase/root-region-server, /hbase/master, and /hbase/hbaseid the
   * permissions should be as above, but should also be world-readable. First
   * we check the general case of /hbase nodes in the following test, and
   * then check the subset of world-readable nodes in the three tests after
   * that.
   */
  @Test (timeout=30000)
  public void testHBaseRootZNodeACL() throws Exception {
    if (!secureZKAvailable) {
      return;
    }

    List<ACL> acls = zkw.getRecoverableZooKeeper().getZooKeeper()
        .getACL("/hbase", new Stat());
    assertEquals(acls.size(),1);
    assertEquals(acls.get(0).getId().getScheme(),"sasl");
    assertEquals(acls.get(0).getId().getId(),"hbase");
    assertEquals(acls.get(0).getPerms(), ZooDefs.Perms.ALL);
  }

  /**
   * When authentication is enabled on Zookeeper, /hbase/root-region-server
   * should be created with 2 ACLs: one specifies that the hbase user has
   * full access to the node; the other, that it is world-readable.
   */
  @Test (timeout=30000)
  public void testHBaseRootRegionServerZNodeACL() throws Exception {
    if (!secureZKAvailable) {
      return;
    }

    List<ACL> acls = zkw.getRecoverableZooKeeper().getZooKeeper()
        .getACL("/hbase/root-region-server", new Stat());
    assertEquals(acls.size(),2);

    boolean foundWorldReadableAcl = false;
    boolean foundHBaseOwnerAcl = false;
    for(int i = 0; i < 2; i++) {
      if (acls.get(i).getId().getScheme().equals("world") == true) {
        assertEquals(acls.get(0).getId().getId(),"anyone");
        assertEquals(acls.get(0).getPerms(), ZooDefs.Perms.READ);
        foundWorldReadableAcl = true;
      }
      else {
        if (acls.get(i).getId().getScheme().equals("sasl") == true) {
          assertEquals(acls.get(1).getId().getId(),"hbase");
          assertEquals(acls.get(1).getId().getScheme(),"sasl");
          foundHBaseOwnerAcl = true;
        } else { // error: should not get here: test fails.
          assertTrue(false);
        }
      }
    }
    assertTrue(foundWorldReadableAcl);
    assertTrue(foundHBaseOwnerAcl);
  }

  /**
   * When authentication is enabled on Zookeeper, /hbase/master should be
   * created with 2 ACLs: one specifies that the hbase user has full access
   * to the node; the other, that it is world-readable.
   */
  @Test (timeout=30000)
  public void testHBaseMasterServerZNodeACL() throws Exception {
    if (!secureZKAvailable) {
      return;
    }

    List<ACL> acls = zkw.getRecoverableZooKeeper().getZooKeeper()
        .getACL("/hbase/master", new Stat());
    assertEquals(acls.size(),2);

    boolean foundWorldReadableAcl = false;
    boolean foundHBaseOwnerAcl = false;
    for(int i = 0; i < 2; i++) {
      if (acls.get(i).getId().getScheme().equals("world") == true) {
        assertEquals(acls.get(0).getId().getId(),"anyone");
        assertEquals(acls.get(0).getPerms(), ZooDefs.Perms.READ);
        foundWorldReadableAcl = true;
      } else {
        if (acls.get(i).getId().getScheme().equals("sasl") == true) {
          assertEquals(acls.get(1).getId().getId(),"hbase");
          assertEquals(acls.get(1).getId().getScheme(),"sasl");
          foundHBaseOwnerAcl = true;
        } else { // error: should not get here: test fails.
          assertTrue(false);
        }
      }
    }
    assertTrue(foundWorldReadableAcl);
    assertTrue(foundHBaseOwnerAcl);
  }

  /**
   * When authentication is enabled on Zookeeper, /hbase/hbaseid should be
   * created with 2 ACLs: one specifies that the hbase user has full access
   * to the node; the other, that it is world-readable.
   */
  @Test (timeout=30000)
  public void testHBaseIDZNodeACL() throws Exception {
    if (!secureZKAvailable) {
      return;
    }

    List<ACL> acls = zkw.getRecoverableZooKeeper().getZooKeeper()
        .getACL("/hbase/hbaseid", new Stat());
    assertEquals(acls.size(),2);

    boolean foundWorldReadableAcl = false;
    boolean foundHBaseOwnerAcl = false;
    for(int i = 0; i < 2; i++) {
      if (acls.get(i).getId().getScheme().equals("world") == true) {
        assertEquals(acls.get(0).getId().getId(),"anyone");
        assertEquals(acls.get(0).getPerms(), ZooDefs.Perms.READ);
        foundWorldReadableAcl = true;
      } else {
        if (acls.get(i).getId().getScheme().equals("sasl") == true) {
          assertEquals(acls.get(1).getId().getId(),"hbase");
          assertEquals(acls.get(1).getId().getScheme(),"sasl");
          foundHBaseOwnerAcl = true;
        } else { // error: should not get here: test fails.
          assertTrue(false);
        }
      }
    }
    assertTrue(foundWorldReadableAcl);
    assertTrue(foundHBaseOwnerAcl);
  }

  /**
   * Finally, we check the ACLs of a node outside of the /hbase hierarchy and
   * verify that its ACL is simply 'hbase:Perms.ALL'.
   */
  @Test
  public void testOutsideHBaseNodeACL() throws Exception {
    if (!secureZKAvailable) {
      return;
    }

    ZKUtil.createWithParents(zkw, "/testACLNode");
    List<ACL> acls = zkw.getRecoverableZooKeeper().getZooKeeper()
        .getACL("/testACLNode", new Stat());
    assertEquals(acls.size(),1);
    assertEquals(acls.get(0).getId().getScheme(),"sasl");
    assertEquals(acls.get(0).getId().getId(),"hbase");
    assertEquals(acls.get(0).getPerms(), ZooDefs.Perms.ALL);
  }
}
