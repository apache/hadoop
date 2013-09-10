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
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.BackupNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.test.PathUtils;
import org.junit.Test;

/**
 * This test checks correctness of port usage by hdfs components:
 * NameNode, DataNode, SecondaryNamenode and BackupNode.
 * 
 * The correct behavior is:<br> 
 * - when a specific port is provided the server must either start on that port 
 * or fail by throwing {@link java.net.BindException}.<br>
 * - if the port = 0 (ephemeral) then the server should choose 
 * a free port and start on it.
 */
public class TestHDFSServerPorts {
  public static final Log LOG = LogFactory.getLog(TestHDFSServerPorts.class);
  
  // reset default 0.0.0.0 addresses in order to avoid IPv6 problem
  static final String THIS_HOST = getFullHostName() + ":0";
  
  private static final File TEST_DATA_DIR = PathUtils.getTestDir(TestHDFSServerPorts.class);
  
  static {
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  Configuration config;
  File hdfsDir;

  /**
   * Attempt to determine the fully qualified domain name for this host 
   * to compare during testing.
   * 
   * This is necessary because in order for the BackupNode test to correctly 
   * work, the namenode must have its http server started with the fully 
   * qualified address, as this is the one the backupnode will attempt to start
   * on as well.
   * 
   * @return Fully qualified hostname, or 127.0.0.1 if can't determine
   */
  public static String getFullHostName() {
    try {
      return DNS.getDefaultHost("default");
    } catch (UnknownHostException e) {
      LOG.warn("Unable to determine hostname.  May interfere with obtaining " +
          "valid test results.");
      return "127.0.0.1";
    }
  }
  
  public NameNode startNameNode() throws IOException {
    return startNameNode(false);
  }
  /**
   * Start the namenode.
   */
  public NameNode startNameNode(boolean withService) throws IOException {
    hdfsDir = new File(TEST_DATA_DIR, "dfs");
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
    }
    config = new HdfsConfiguration();
    config.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        fileAsURI(new File(hdfsDir, "name1")).toString());
    FileSystem.setDefaultUri(config, "hdfs://" + THIS_HOST);
    if (withService) {
      NameNode.setServiceAddress(config, THIS_HOST);      
    }
    config.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, THIS_HOST);
    DFSTestUtil.formatNameNode(config);

    String[] args = new String[] {};
    // NameNode will modify config with the ports it bound to
    return NameNode.createNameNode(args, config);
  }

  /**
   * Start the BackupNode
   */
  public BackupNode startBackupNode(Configuration conf) throws IOException {
    // Set up testing environment directories
    hdfsDir = new File(TEST_DATA_DIR, "backupNode");
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
    }
    File currDir = new File(hdfsDir, "name2");
    File currDir2 = new File(currDir, "current");
    File currDir3 = new File(currDir, "image");
    
    assertTrue(currDir.mkdirs());
    assertTrue(currDir2.mkdirs());
    assertTrue(currDir3.mkdirs());
    
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        fileAsURI(new File(hdfsDir, "name2")).toString());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        "${" + DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY + "}");
    
    // Start BackupNode
    String[] args = new String [] { StartupOption.BACKUP.getName() };
    BackupNode bu = (BackupNode)NameNode.createNameNode(args, conf);

    return bu;
  }
  
  /**
   * Start the datanode.
   */
  public DataNode startDataNode(int index, Configuration config) 
  throws IOException {
    File dataNodeDir = new File(TEST_DATA_DIR, "data-" + index);
    config.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataNodeDir.getPath());

    String[] args = new String[] {};
    // NameNode will modify config with the ports it bound to
    return DataNode.createDataNode(args, config);
  }

  /**
   * Stop the datanode.
   */
  public void stopDataNode(DataNode dn) {
    if (dn != null) {
      dn.shutdown();
    }
  }

  public void stopNameNode(NameNode nn) {
    if (nn != null) {
      nn.stop();
    }
  }

  public Configuration getConfig() {
    return this.config;
  }

  /**
   * Check whether the namenode can be started.
   */
  private boolean canStartNameNode(Configuration conf) throws IOException {
    NameNode nn2 = null;
    try {
      nn2 = NameNode.createNameNode(new String[]{}, conf);
    } catch(IOException e) {
      if (e instanceof java.net.BindException)
        return false;
      throw e;
    } finally {
      stopNameNode(nn2);
    }
    return true;
  }

  /**
   * Check whether the datanode can be started.
   */
  private boolean canStartDataNode(Configuration conf) throws IOException {
    DataNode dn = null;
    try {
      dn = DataNode.createDataNode(new String[]{}, conf);
    } catch(IOException e) {
      if (e instanceof java.net.BindException)
        return false;
      throw e;
    } finally {
      if(dn != null) dn.shutdown();
    }
    return true;
  }

  /**
   * Check whether the secondary name-node can be started.
   */
  @SuppressWarnings("deprecation")
  private boolean canStartSecondaryNode(Configuration conf) throws IOException {
    // Using full name allows us not to have to add deprecation tag to
    // entire source file.
    org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode sn = null;
    try {
      sn = new org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode(conf);
    } catch(IOException e) {
      if (e instanceof java.net.BindException)
        return false;
      throw e;
    } finally {
      if(sn != null) sn.shutdown();
    }
    return true;
  }
  
  /**
   * Check whether the BackupNode can be started.
   */
  private boolean canStartBackupNode(Configuration conf) throws IOException {
    BackupNode bn = null;

    try {
      bn = startBackupNode(conf);
    } catch(IOException e) {
      if (e instanceof java.net.BindException)
        return false;
      throw e;
    } finally {
      if(bn != null) bn.stop();
    }

    return true;
  }

  @Test
  public void testNameNodePorts() throws Exception {
    runTestNameNodePorts(false);
    runTestNameNodePorts(true);
  }
  /**
   * Verify namenode port usage.
   */
  public void runTestNameNodePorts(boolean withService) throws Exception {
    NameNode nn = null;
    try {
      nn = startNameNode(withService);

      // start another namenode on the same port
      Configuration conf2 = new HdfsConfiguration(config);
      conf2.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
          fileAsURI(new File(hdfsDir, "name2")).toString());
      DFSTestUtil.formatNameNode(conf2);
      boolean started = canStartNameNode(conf2);
      assertFalse(started); // should fail

      // start on a different main port
      FileSystem.setDefaultUri(conf2, "hdfs://" + THIS_HOST);
      started = canStartNameNode(conf2);
      assertFalse(started); // should fail again

      // reset conf2 since NameNode modifies it
      FileSystem.setDefaultUri(conf2, "hdfs://" + THIS_HOST);
      // different http port
      conf2.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, THIS_HOST);
      started = canStartNameNode(conf2);

      if (withService) {
        assertFalse("Should've failed on service port", started);

        // reset conf2 since NameNode modifies it
        FileSystem.setDefaultUri(conf2, "hdfs://" + THIS_HOST);
        conf2.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, THIS_HOST);
        // Set Service address      
        conf2.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,  THIS_HOST);
        started = canStartNameNode(conf2);        
      }
      assertTrue(started);
    } finally {
      stopNameNode(nn);
    }
  }

  /**
   * Verify datanode port usage.
   */
  @Test
  public void testDataNodePorts() throws Exception {
    NameNode nn = null;
    try {
      nn = startNameNode();

      // start data-node on the same port as name-node
      Configuration conf2 = new HdfsConfiguration(config);
      conf2.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, new File(hdfsDir, "data").getPath());
      conf2.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY,
                FileSystem.getDefaultUri(config).getAuthority());
      conf2.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, THIS_HOST);
      boolean started = canStartDataNode(conf2);
      assertFalse(started); // should fail

      // bind http server to the same port as name-node
      conf2.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, THIS_HOST);
      conf2.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY,
                config.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY));
      started = canStartDataNode(conf2);
      assertFalse(started); // should fail
    
      // both ports are different from the name-node ones
      conf2.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, THIS_HOST);
      conf2.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, THIS_HOST);
      conf2.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, THIS_HOST);
      started = canStartDataNode(conf2);
      assertTrue(started); // should start now
    } finally {
      stopNameNode(nn);
    }
  }

  /**
   * Verify secondary namenode port usage.
   */
  @Test
  public void testSecondaryNodePorts() throws Exception {
    NameNode nn = null;
    try {
      nn = startNameNode();

      // bind http server to the same port as name-node
      Configuration conf2 = new HdfsConfiguration(config);
      conf2.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, 
                config.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY));
      LOG.info("= Starting 1 on: " + 
                                 conf2.get(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY));
      boolean started = canStartSecondaryNode(conf2);
      assertFalse(started); // should fail

      // bind http server to a different port
      conf2.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, THIS_HOST);
      LOG.info("= Starting 2 on: " + 
                                 conf2.get(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY));
      started = canStartSecondaryNode(conf2);
      assertTrue(started); // should start now
    } finally {
      stopNameNode(nn);
    }
  }
    
    /**
     * Verify BackupNode port usage.
     */
  @Test
    public void testBackupNodePorts() throws Exception {
      NameNode nn = null;
      try {
        nn = startNameNode();

        Configuration backup_config = new HdfsConfiguration(config);
        backup_config.set(
            DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY, THIS_HOST);
        // bind http server to the same port as name-node
        backup_config.set(DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY, 
            backup_config.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY));

        LOG.info("= Starting 1 on: " + backup_config.get(
            DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY));

        assertFalse("Backup started on same port as Namenode", 
                           canStartBackupNode(backup_config)); // should fail

        // bind http server to a different port
        backup_config.set(
            DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY, THIS_HOST);
        LOG.info("= Starting 2 on: " + backup_config.get(
            DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY));

        boolean started = canStartBackupNode(backup_config);
        assertTrue("Backup Namenode should've started", started); // should start now
      } finally {
        stopNameNode(nn);
      }
  }
}
