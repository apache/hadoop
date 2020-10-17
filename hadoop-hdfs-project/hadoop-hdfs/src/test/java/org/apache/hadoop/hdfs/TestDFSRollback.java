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

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType.DATA_NODE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType.NAME_NODE;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

/**
* This test ensures the appropriate response (successful or failure) from
* the system when the system is rolled back under various storage state and
* version conditions.
*/
public class TestDFSRollback {
 
  private static final Logger LOG = LoggerFactory.getLogger(
                                                   "org.apache.hadoop.hdfs.TestDFSRollback");
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
   * Verify that the new current directory is the old previous.  
   * It is assumed that the server has recovered and rolled back.
   */
  void checkResult(NodeType nodeType, String[] baseDirs) throws Exception {
    List<File> curDirs = Lists.newArrayList();
    for (String baseDir : baseDirs) {
      File curDir = new File(baseDir, "current");
      curDirs.add(curDir);
      switch (nodeType) {
      case NAME_NODE:
        FSImageTestUtil.assertReasonableNameCurrentDir(curDir);
        break;
      case DATA_NODE:
        assertEquals(
            UpgradeUtilities.checksumContents(nodeType, curDir, false),
            UpgradeUtilities.checksumMasterDataNodeContents());
        break;
      }
    }
    
    FSImageTestUtil.assertParallelFilesAreIdentical(
        curDirs, Collections.<String>emptySet());

    for (int i = 0; i < baseDirs.length; i++) {
      assertFalse(new File(baseDirs[i],"previous").isDirectory());
    }
  }
 
  /**
   * Attempts to start a NameNode with the given operation.  Starting
   * the NameNode should throw an exception.
   */
  void startNameNodeShouldFail(String searchString) {
    try {
      NameNode.doRollback(conf, false);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
                                                .format(false)
                                                .manageDataDfsDirs(false)
                                                .manageNameDfsDirs(false)
                                                .build(); // should fail
      throw new AssertionError("NameNode should have failed to start");
    } catch (Exception expected) {
      if (!expected.getMessage().contains(searchString)) {
        fail("Expected substring '" + searchString + "' in exception " +
            "but got: " + StringUtils.stringifyException(expected));
      }
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
  void startBlockPoolShouldFail(StartupOption operation, String bpid)
      throws IOException {
    cluster.startDataNodes(conf, 1, false, operation, null); // should fail
    assertFalse("Block pool " + bpid + " should have failed to start", 
        cluster.getDataNodes().get(0).isBPServiceAlive(bpid));
  }
 
  /**
   * This test attempts to rollback the NameNode and DataNode under
   * a number of valid and invalid conditions.
   */
  @Test
  public void testRollback() throws Exception {
    File[] baseDirs;
    UpgradeUtilities.initialize();
    
    StorageInfo storageInfo = null;
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new HdfsConfiguration();
      conf.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);      
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      String[] nameNodeDirs = conf.getStrings(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
      String[] dataNodeDirs = conf.getStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
      
      log("Normal NameNode rollback", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "previous");
      NameNode.doRollback(conf, false);
      checkResult(NAME_NODE, nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("Normal DataNode rollback", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "previous");
      NameNode.doRollback(conf, false);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
                                                .format(false)
                                                .manageDataDfsDirs(false)
                                                .manageNameDfsDirs(false)
                                                .dnStartupOption(StartupOption.ROLLBACK)
                                                .build();
      UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "current");
      UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "previous");
      cluster.startDataNodes(conf, 1, false, StartupOption.ROLLBACK, null);
      checkResult(DATA_NODE, dataNodeDirs);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      
      log("Normal BlockPool rollback", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "previous");
      NameNode.doRollback(conf, false);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
                                                .format(false)
                                                .manageDataDfsDirs(false)
                                                .manageNameDfsDirs(false)
                                                .dnStartupOption(StartupOption.ROLLBACK)
                                                .build();
      UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "current");
      UpgradeUtilities.createBlockPoolStorageDirs(dataNodeDirs, "current",
          UpgradeUtilities.getCurrentBlockPoolID(cluster));
      // Create a previous snapshot for the blockpool
      UpgradeUtilities.createBlockPoolStorageDirs(dataNodeDirs, "previous",
          UpgradeUtilities.getCurrentBlockPoolID(cluster));
      // Put newer layout version in current.
      storageInfo = new StorageInfo(
          HdfsServerConstants.DATANODE_LAYOUT_VERSION - 1,
          UpgradeUtilities.getCurrentNamespaceID(cluster),
          UpgradeUtilities.getCurrentClusterID(cluster),
          UpgradeUtilities.getCurrentFsscTime(cluster),
          NodeType.DATA_NODE);

      // Overwrite VERSION file in the current directory of
      // volume directories and block pool slice directories
      // with a layout version from future.
      File[] dataCurrentDirs = new File[dataNodeDirs.length];
      for (int i=0; i<dataNodeDirs.length; i++) {
        dataCurrentDirs[i] = new File((new Path(dataNodeDirs[i] 
            + "/current")).toString());
      }
      UpgradeUtilities.createDataNodeVersionFile(
          dataCurrentDirs,
          storageInfo,
          UpgradeUtilities.getCurrentBlockPoolID(cluster), conf);

      cluster.startDataNodes(conf, 1, false, StartupOption.ROLLBACK, null);
      assertTrue(cluster.isDataNodeUp());

      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      
      log("NameNode rollback without existing previous dir", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      startNameNodeShouldFail(
          "None of the storage directories contain previous fs state");
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("DataNode rollback without existing previous dir", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
                                                .format(false)
                                                .manageDataDfsDirs(false)
                                                .manageNameDfsDirs(false)
                                                .startupOption(StartupOption.UPGRADE)
                                                .build();
      UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "current");
      cluster.startDataNodes(conf, 1, false, StartupOption.ROLLBACK, null);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      
      log("DataNode rollback with future stored layout version in previous", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "previous");
      NameNode.doRollback(conf, false);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
                                                .format(false)
                                                .manageDataDfsDirs(false)
                                                .manageNameDfsDirs(false)
                                                .dnStartupOption(StartupOption.ROLLBACK)
                                                .build();
      UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "current");
      baseDirs = UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "previous");
      storageInfo = new StorageInfo(Integer.MIN_VALUE, 
          UpgradeUtilities.getCurrentNamespaceID(cluster), 
          UpgradeUtilities.getCurrentClusterID(cluster), 
          UpgradeUtilities.getCurrentFsscTime(cluster),
          NodeType.DATA_NODE);
      
      UpgradeUtilities.createDataNodeVersionFile(baseDirs, storageInfo,
          UpgradeUtilities.getCurrentBlockPoolID(cluster), conf);
      
      startBlockPoolShouldFail(StartupOption.ROLLBACK, 
          cluster.getNamesystem().getBlockPoolId());
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      
      log("DataNode rollback with newer fsscTime in previous", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "previous");
      NameNode.doRollback(conf, false);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
                                                .format(false)
                                                .manageDataDfsDirs(false)
                                                .manageNameDfsDirs(false)
                                                .dnStartupOption(StartupOption.ROLLBACK)
                                                .build();
      
      UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "current");
      baseDirs = UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "previous");
      storageInfo = new StorageInfo(HdfsServerConstants.DATANODE_LAYOUT_VERSION,
          UpgradeUtilities.getCurrentNamespaceID(cluster),
          UpgradeUtilities.getCurrentClusterID(cluster), Long.MAX_VALUE,
          NodeType.DATA_NODE);
     
      UpgradeUtilities.createDataNodeVersionFile(baseDirs, storageInfo,
          UpgradeUtilities.getCurrentBlockPoolID(cluster), conf);
      
      startBlockPoolShouldFail(StartupOption.ROLLBACK, 
          cluster.getNamesystem().getBlockPoolId());
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      
      log("NameNode rollback with no edits file", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      baseDirs = UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "previous");
      deleteMatchingFiles(baseDirs, "edits.*");
      startNameNodeShouldFail("Gap in transactions");
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode rollback with no image file", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      baseDirs = UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "previous");
      deleteMatchingFiles(baseDirs, "fsimage_.*");
      startNameNodeShouldFail("No valid image files found");
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode rollback with corrupt version file", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      baseDirs = UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "previous");
      for (File f : baseDirs) { 
        UpgradeUtilities.corruptFile(
            new File(f,"VERSION"),
            "layoutVersion".getBytes(Charsets.UTF_8),
            "xxxxxxxxxxxxx".getBytes(Charsets.UTF_8));
      }
      startNameNodeShouldFail("file VERSION has layoutVersion missing");

      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode rollback with old layout version in previous", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      baseDirs = UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "previous");
      storageInfo = new StorageInfo(1,
          UpgradeUtilities.getCurrentNamespaceID(null),
          UpgradeUtilities.getCurrentClusterID(null),
          UpgradeUtilities.getCurrentFsscTime(null), NodeType.NAME_NODE);
      
      UpgradeUtilities.createNameNodeVersionFile(conf, baseDirs,
          storageInfo, UpgradeUtilities.getCurrentBlockPoolID(cluster));
      startNameNodeShouldFail("Cannot rollback to storage version 1 using this version");
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
    } // end numDir loop
  }
 
  private void deleteMatchingFiles(File[] baseDirs, String regex) {
    for (File baseDir : baseDirs) {
      for (File f : baseDir.listFiles()) {
        if (f.getName().matches(regex)) {
          f.delete();
        }
      }
    }
  }

  @After
  public void tearDown() throws Exception {
    LOG.info("Shutting down MiniDFSCluster");
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
  
  public static void main(String[] args) throws Exception {
    new TestDFSRollback().testRollback();
  }
  
}


