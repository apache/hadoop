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

import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.DATA_NODE;
import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.NAME_NODE;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.TestParallelImageWrite;
import org.junit.Test;
import static org.junit.Assert.*;

/**
* This test ensures the appropriate response (successful or failure) from
* the system when the system is upgraded under various storage state and
* version conditions.
*/
public class TestDFSUpgrade {
 
  private static final Log LOG = LogFactory.getLog(
                                                   "org.apache.hadoop.hdfs.TestDFSUpgrade");
  private Configuration conf;
  private int testCounter = 0;
  private MiniDFSCluster cluster = null;
    
  /**
   * Writes an INFO log message containing the parameters.
   */
  void log(String label, int numDirs) {
    LOG.info("============================================================");
    LOG.info("***TEST " + (testCounter++) + "*** " 
             + label + ":"
             + " numDirs="+numDirs);
  }
  
  /**
   * For namenode, Verify that the current and previous directories exist.
   * Verify that previous hasn't been modified by comparing the checksum of all
   * its files with their original checksum. It is assumed that the
   * server has recovered and upgraded.
   */
  void checkNameNode(String[] baseDirs) throws IOException {
    for (int i = 0; i < baseDirs.length; i++) {
      assertTrue(new File(baseDirs[i],"current").isDirectory());
      assertTrue(new File(baseDirs[i],"current/VERSION").isFile());
      assertTrue(new File(baseDirs[i],"current/edits").isFile());
      assertTrue(new File(baseDirs[i],"current/fsimage").isFile());
      assertTrue(new File(baseDirs[i],"current/fstime").isFile());
      
      File previous = new File(baseDirs[i], "previous");
      assertTrue(previous.isDirectory());
      assertEquals(UpgradeUtilities.checksumContents(NAME_NODE, previous),
          UpgradeUtilities.checksumMasterNameNodeContents());
    }
  }
 
  /**
   * For datanode, for a block pool, verify that the current and previous
   * directories exist. Verify that previous hasn't been modified by comparing
   * the checksum of all its files with their original checksum. It
   * is assumed that the server has recovered and upgraded.
   */
  void checkDataNode(String[] baseDirs, String bpid) throws IOException {
    for (int i = 0; i < baseDirs.length; i++) {
      File current = new File(baseDirs[i], "current/" + bpid + "/current");
      assertEquals(UpgradeUtilities.checksumContents(DATA_NODE, current),
        UpgradeUtilities.checksumMasterDataNodeContents());
      
      // block files are placed under <sd>/current/<bpid>/current/finalized
      File currentFinalized = 
        MiniDFSCluster.getFinalizedDir(new File(baseDirs[i]), bpid);
      assertEquals(UpgradeUtilities.checksumContents(DATA_NODE, currentFinalized),
          UpgradeUtilities.checksumMasterBlockPoolFinalizedContents());
      
      File previous = new File(baseDirs[i], "current/" + bpid + "/previous");
      assertTrue(previous.isDirectory());
      assertEquals(UpgradeUtilities.checksumContents(DATA_NODE, previous),
          UpgradeUtilities.checksumMasterDataNodeContents());
      
      File previousFinalized = 
        new File(baseDirs[i], "current/" + bpid + "/previous"+"/finalized");
      assertEquals(UpgradeUtilities.checksumContents(DATA_NODE, previousFinalized),
          UpgradeUtilities.checksumMasterBlockPoolFinalizedContents());
      
    }
  }
  /**
   * Attempts to start a NameNode with the given operation.  Starting
   * the NameNode should throw an exception.
   */
  void startNameNodeShouldFail(StartupOption operation) {
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
                                                .startupOption(operation)
                                                .format(false)
                                                .manageDataDfsDirs(false)
                                                .manageNameDfsDirs(false)
                                                .build(); // should fail
      throw new AssertionError("NameNode should have failed to start");
    } catch (Exception expected) {
      // expected
    }
  }
  
  /**
   * Attempts to start a DataNode with the given operation. Starting
   * the given block pool should fail.
   * @param operation startup option
   * @param bpid block pool Id that should fail to start
   * @throws IOException 
   */
  void startBlockPoolShouldFail(StartupOption operation, String bpid) throws IOException {
    cluster.startDataNodes(conf, 1, false, operation, null); // should fail
    assertFalse("Block pool " + bpid + " should have failed to start",
        cluster.getDataNodes().get(0).isBPServiceAlive(bpid));
  }
 
  /**
   * Create an instance of a newly configured cluster for testing that does
   * not manage its own directories or files
   */
  private MiniDFSCluster createCluster() throws IOException {
    return new MiniDFSCluster.Builder(conf).numDataNodes(0)
                                           .format(false)
                                           .manageDataDfsDirs(false)
                                           .manageNameDfsDirs(false)
                                           .startupOption(StartupOption.UPGRADE)
                                           .build();
  }
  
  /**
   * This test attempts to upgrade the NameNode and DataNode under
   * a number of valid and invalid conditions.
   */
  @Test
  public void testUpgrade() throws Exception {
    File[] baseDirs;
    UpgradeUtilities.initialize();
    
    StorageInfo storageInfo = null;
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new HdfsConfiguration();
      conf.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);      
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      String[] nameNodeDirs = conf.getStrings(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
      String[] dataNodeDirs = conf.getStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
      
      log("Normal NameNode upgrade", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      cluster = createCluster();
      checkNameNode(nameNodeDirs);
      if (numDirs > 1)
        TestParallelImageWrite.checkImages(cluster.getNamesystem(), numDirs);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("Normal DataNode upgrade", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      cluster = createCluster();
      UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "current");
      cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
      checkDataNode(dataNodeDirs, UpgradeUtilities.getCurrentBlockPoolID(null));
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      
      log("NameNode upgrade with existing previous dir", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "previous");
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("DataNode upgrade with existing previous dir", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      cluster = createCluster();
      UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "current");
      UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "previous");
      cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
      checkDataNode(dataNodeDirs, UpgradeUtilities.getCurrentBlockPoolID(null));
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);

      log("DataNode upgrade with future stored layout version in current", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      cluster = createCluster();
      baseDirs = UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "current");
      storageInfo = new StorageInfo(Integer.MIN_VALUE, 
          UpgradeUtilities.getCurrentNamespaceID(cluster),
          UpgradeUtilities.getCurrentClusterID(cluster),
          UpgradeUtilities.getCurrentFsscTime(cluster));
      
      UpgradeUtilities.createDataNodeVersionFile(baseDirs, storageInfo,
          UpgradeUtilities.getCurrentBlockPoolID(cluster));
      
      startBlockPoolShouldFail(StartupOption.REGULAR, UpgradeUtilities
          .getCurrentBlockPoolID(null));
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      
      log("DataNode upgrade with newer fsscTime in current", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      cluster = createCluster();
      baseDirs = UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "current");
      storageInfo = new StorageInfo(UpgradeUtilities.getCurrentLayoutVersion(), 
          UpgradeUtilities.getCurrentNamespaceID(cluster),
          UpgradeUtilities.getCurrentClusterID(cluster), Long.MAX_VALUE);
          
      UpgradeUtilities.createDataNodeVersionFile(baseDirs, storageInfo, 
          UpgradeUtilities.getCurrentBlockPoolID(cluster));
      // Ensure corresponding block pool failed to initialized
      startBlockPoolShouldFail(StartupOption.REGULAR, UpgradeUtilities
          .getCurrentBlockPoolID(null));
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);

      log("NameNode upgrade with no edits file", numDirs);
      baseDirs = UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      for (File f : baseDirs) { 
        FileUtil.fullyDelete(new File(f,"edits"));
      }
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with no image file", numDirs);
      baseDirs = UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      for (File f : baseDirs) { 
        FileUtil.fullyDelete(new File(f,"fsimage")); 
      }
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with corrupt version file", numDirs);
      baseDirs = UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      for (File f : baseDirs) { 
        UpgradeUtilities.corruptFile(new File(f,"VERSION")); 
      }
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with old layout version in current", numDirs);
      baseDirs = UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      storageInfo = new StorageInfo(Storage.LAST_UPGRADABLE_LAYOUT_VERSION + 1, 
          UpgradeUtilities.getCurrentNamespaceID(null),
          UpgradeUtilities.getCurrentClusterID(null),
          UpgradeUtilities.getCurrentFsscTime(null));
      
      UpgradeUtilities.createNameNodeVersionFile(conf, baseDirs, storageInfo,
          UpgradeUtilities.getCurrentBlockPoolID(cluster));
      
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with future layout version in current", numDirs);
      baseDirs = UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      storageInfo = new StorageInfo(Integer.MIN_VALUE, 
          UpgradeUtilities.getCurrentNamespaceID(null),
          UpgradeUtilities.getCurrentClusterID(null),
          UpgradeUtilities.getCurrentFsscTime(null));
      
      UpgradeUtilities.createNameNodeVersionFile(conf, baseDirs, storageInfo,
          UpgradeUtilities.getCurrentBlockPoolID(cluster));
      
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
    } // end numDir loop
    
    // One more check: normal NN upgrade with 4 directories, concurrent write
    int numDirs = 4;
    {
      conf = new HdfsConfiguration();
      conf.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);      
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      String[] nameNodeDirs = conf.getStrings(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
      
      log("Normal NameNode upgrade", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      cluster = createCluster();
      checkNameNode(nameNodeDirs);
      TestParallelImageWrite.checkImages(cluster.getNamesystem(), numDirs);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
    }
  }
 
  @Test(expected=IOException.class)
  public void testUpgradeFromPreUpgradeLVFails() throws IOException {
    // Upgrade from versions prior to Storage#LAST_UPGRADABLE_LAYOUT_VERSION
    // is not allowed
    Storage.checkVersionUpgradable(Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION + 1);
    fail("Expected IOException is not thrown");
  }
  
  public void test203LayoutVersion() {
    for (int lv : Storage.LAYOUT_VERSIONS_203) {
      assertTrue(Storage.is203LayoutVersion(lv));
    }
  }
  
  public static void main(String[] args) throws Exception {
    new TestDFSUpgrade().testUpgrade();
  }
}


