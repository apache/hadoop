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
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;

/**
* This test ensures the appropriate response (successful or failure) from
* the system when the system is upgraded under various storage state and
* version conditions.
*/
public class TestDFSUpgrade extends TestCase {
 
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
          UpgradeUtilities.checksumMasterContents(NAME_NODE));
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
        UpgradeUtilities.checksumMasterContents(DATA_NODE));
      
      File previous = new File(baseDirs[i], "current/" + bpid + "/previous");
      assertTrue(previous.isDirectory());
      assertEquals(UpgradeUtilities.checksumContents(DATA_NODE, previous),
          UpgradeUtilities.checksumMasterContents(DATA_NODE));
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
  public void testUpgrade() throws Exception {
    File[] baseDirs;
    UpgradeUtilities.initialize();
    
    StorageInfo storageInfo = null;
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new HdfsConfiguration();
      conf.setInt("dfs.datanode.scan.period.hours", -1);      
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      String[] nameNodeDirs = conf.getStrings(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
      String[] dataNodeDirs = conf.getStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
      
      log("Normal NameNode upgrade", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      cluster = createCluster();
      checkNameNode(nameNodeDirs);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("Normal DataNode upgrade", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      cluster = createCluster();
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
      checkDataNode(dataNodeDirs, UpgradeUtilities.getCurrentBlockPoolID(null));
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      
      log("NameNode upgrade with existing previous dir", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("DataNode upgrade with existing previous dir", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      cluster = createCluster();
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "previous");
      cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
      checkDataNode(dataNodeDirs, UpgradeUtilities.getCurrentBlockPoolID(null));
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);

      log("DataNode upgrade with future stored layout version in current", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      cluster = createCluster();
      baseDirs = UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
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
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      cluster = createCluster();
      baseDirs = UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
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
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      for (File f : baseDirs) { 
        FileUtil.fullyDelete(new File(f,"edits"));
      }
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with no image file", numDirs);
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      for (File f : baseDirs) { 
        FileUtil.fullyDelete(new File(f,"fsimage")); 
      }
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with corrupt version file", numDirs);
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      for (File f : baseDirs) { 
        UpgradeUtilities.corruptFile(new File(f,"VERSION")); 
      }
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with old layout version in current", numDirs);
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      storageInfo = new StorageInfo(Storage.LAST_UPGRADABLE_LAYOUT_VERSION + 1, 
          UpgradeUtilities.getCurrentNamespaceID(null),
          UpgradeUtilities.getCurrentClusterID(null),
          UpgradeUtilities.getCurrentFsscTime(null));
      
      UpgradeUtilities.createNameNodeVersionFile(baseDirs, storageInfo,
          UpgradeUtilities.getCurrentBlockPoolID(cluster));
      
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with future layout version in current", numDirs);
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      storageInfo = new StorageInfo(Integer.MIN_VALUE, 
          UpgradeUtilities.getCurrentNamespaceID(null),
          UpgradeUtilities.getCurrentClusterID(null),
          UpgradeUtilities.getCurrentFsscTime(null));
      
      UpgradeUtilities.createNameNodeVersionFile(baseDirs, storageInfo,
          UpgradeUtilities.getCurrentBlockPoolID(cluster));
      
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
    } // end numDir loop
  }
 
  protected void tearDown() throws Exception {
    LOG.info("Shutting down MiniDFSCluster");
    if (cluster != null) cluster.shutdown();
  }
    
  public static void main(String[] args) throws Exception {
    new TestDFSUpgrade().testUpgrade();
  }
  
}


