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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.junit.After;
import org.junit.Test;

/**
 * This test ensures the appropriate response (successful or failure) from 
 * a Datanode when the system is started with differing version combinations. 
 */
public class TestDFSStartupVersions {
  
  private static final Logger LOG = LoggerFactory.getLogger(
                                                   "org.apache.hadoop.hdfs.TestDFSStartupVersions");
  private MiniDFSCluster cluster = null;
  
  /**
   * Writes an INFO log message containing the parameters.
   */
  void log(String label, NodeType nodeType, Integer testCase,
      StorageData sd) {
    String testCaseLine = "";
    if (testCase != null) {
      testCaseLine = " testCase="+testCase;
    }
    LOG.info("============================================================");
    LOG.info("***TEST*** " + label + ":"
             + testCaseLine
             + " nodeType="+nodeType
             + " layoutVersion="+sd.storageInfo.getLayoutVersion()
             + " namespaceID="+sd.storageInfo.getNamespaceID()
             + " fsscTime="+sd.storageInfo.getCTime()
             + " clusterID="+sd.storageInfo.getClusterID()
             + " BlockPoolID="+sd.blockPoolId);
  }
  
  /**
   * Class used for initializing version information for tests
   */
  private static class StorageData {
    private final StorageInfo storageInfo;
    private final String blockPoolId;
    
    StorageData(int layoutVersion, int namespaceId, String clusterId,
        long cTime, String bpid) {
      storageInfo = new StorageInfo(layoutVersion, namespaceId, clusterId,
          cTime, NodeType.DATA_NODE);
      blockPoolId = bpid;
    }
  }
  
  /**
   * Initialize the versions array.  This array stores all combinations 
   * of cross product:
   *  {oldLayoutVersion,currentLayoutVersion,futureLayoutVersion} X
   *    {currentNamespaceId,incorrectNamespaceId} X
   *      {pastFsscTime,currentFsscTime,futureFsscTime}
   */
  private StorageData[] initializeVersions() throws Exception {
    int layoutVersionOld = Storage.LAST_UPGRADABLE_LAYOUT_VERSION;
    int layoutVersionCur = HdfsServerConstants.DATANODE_LAYOUT_VERSION;
    int layoutVersionNew = Integer.MIN_VALUE;
    int namespaceIdCur = UpgradeUtilities.getCurrentNamespaceID(null);
    int namespaceIdOld = Integer.MIN_VALUE;
    long fsscTimeOld = Long.MIN_VALUE;
    long fsscTimeCur = UpgradeUtilities.getCurrentFsscTime(null);
    long fsscTimeNew = Long.MAX_VALUE;
    String clusterID = "testClusterID";
    String invalidClusterID = "testClusterID";
    String bpid = UpgradeUtilities.getCurrentBlockPoolID(null);
    String invalidBpid = "invalidBpid";
    
    return new StorageData[] {
        new StorageData(layoutVersionOld, namespaceIdCur, clusterID,
            fsscTimeOld, bpid), // 0
        new StorageData(layoutVersionOld, namespaceIdCur, clusterID,
            fsscTimeCur, bpid), // 1
        new StorageData(layoutVersionOld, namespaceIdCur, clusterID,
            fsscTimeNew, bpid), // 2
        new StorageData(layoutVersionOld, namespaceIdOld, clusterID,
            fsscTimeOld, bpid), // 3
        new StorageData(layoutVersionOld, namespaceIdOld, clusterID,
            fsscTimeCur, bpid), // 4
        new StorageData(layoutVersionOld, namespaceIdOld, clusterID,
            fsscTimeNew, bpid), // 5
        new StorageData(layoutVersionCur, namespaceIdCur, clusterID,
            fsscTimeOld, bpid), // 6
        new StorageData(layoutVersionCur, namespaceIdCur, clusterID,
            fsscTimeCur, bpid), // 7
        new StorageData(layoutVersionCur, namespaceIdCur, clusterID,
            fsscTimeNew, bpid), // 8
        new StorageData(layoutVersionCur, namespaceIdOld, clusterID,
            fsscTimeOld, bpid), // 9
        new StorageData(layoutVersionCur, namespaceIdOld, clusterID,
            fsscTimeCur, bpid), // 10
        new StorageData(layoutVersionCur, namespaceIdOld, clusterID,
            fsscTimeNew, bpid), // 11
        new StorageData(layoutVersionNew, namespaceIdCur, clusterID,
            fsscTimeOld, bpid), // 12
        new StorageData(layoutVersionNew, namespaceIdCur, clusterID,
            fsscTimeCur, bpid), // 13
        new StorageData(layoutVersionNew, namespaceIdCur, clusterID,
            fsscTimeNew, bpid), // 14
        new StorageData(layoutVersionNew, namespaceIdOld, clusterID,
            fsscTimeOld, bpid), // 15
        new StorageData(layoutVersionNew, namespaceIdOld, clusterID,
            fsscTimeCur, bpid), // 16
        new StorageData(layoutVersionNew, namespaceIdOld, clusterID,
            fsscTimeNew, bpid), // 17
        // Test with invalid clusterId
        new StorageData(layoutVersionCur, namespaceIdCur, invalidClusterID,
            fsscTimeCur, bpid), // 18
        // Test with invalid block pool Id
        new StorageData(layoutVersionCur, namespaceIdCur, clusterID,
            fsscTimeCur, invalidBpid) // 19
    };
  }
  
  /**
   * Determines if the given Namenode version and Datanode version
   * are compatible with each other. Compatibility in this case mean
   * that the Namenode and Datanode will successfully start up and
   * will work together. The rules for compatibility,
   * taken from the DFS Upgrade Design, are as follows:
   * <pre>
   * <ol>
   * <li>Check 0: Datanode namespaceID != Namenode namespaceID the startup fails
   * </li>
   * <li>Check 1: Datanode clusterID != Namenode clusterID the startup fails
   * </li>
   * <li>Check 2: Datanode blockPoolID != Namenode blockPoolID the startup fails
   * </li>
   * <li>Check 3: The data-node does regular startup (no matter which options 
   *    it is started with) if
   *       softwareLV == storedLV AND 
   *       DataNode.FSSCTime == NameNode.FSSCTime
   * </li>
   * <li>Check 4: The data-node performs an upgrade if it is started without any 
   *    options and
   *       |softwareLV| > |storedLV| OR 
   *       (softwareLV == storedLV AND
   *        DataNode.FSSCTime < NameNode.FSSCTime)
   * </li>
   * <li>NOT TESTED: The data-node rolls back if it is started with
   *    the -rollback option and
   *       |softwareLV| >= |previous.storedLV| AND 
   *       DataNode.previous.FSSCTime <= NameNode.FSSCTime
   * </li>
   * <li>Check 5: In all other cases the startup fails.</li>
   * </ol>
   * </pre>
   */
  boolean isVersionCompatible(StorageData namenodeSd, StorageData datanodeSd) {
    final StorageInfo namenodeVer = namenodeSd.storageInfo;
    final StorageInfo datanodeVer = datanodeSd.storageInfo;
    // check #0
    if (namenodeVer.getNamespaceID() != datanodeVer.getNamespaceID()) {
      LOG.info("namespaceIDs are not equal: isVersionCompatible=false");
      return false;
    }
    // check #1
    if (!namenodeVer.getClusterID().equals(datanodeVer.getClusterID())) {
      LOG.info("clusterIDs are not equal: isVersionCompatible=false");
      return false;
    }
    // check #2
    if (!namenodeSd.blockPoolId.equals(datanodeSd.blockPoolId)) {
      LOG.info("blockPoolIDs are not equal: isVersionCompatible=false");
      return false;
    }
    // check #3
    int softwareLV = HdfsServerConstants.DATANODE_LAYOUT_VERSION;
    int storedLV = datanodeVer.getLayoutVersion();
    if (softwareLV == storedLV &&  
        datanodeVer.getCTime() == namenodeVer.getCTime()) 
      {
        LOG.info("layoutVersions and cTimes are equal: isVersionCompatible=true");
        return true;
      }
    // check #4
    long absSoftwareLV = Math.abs((long)softwareLV);
    long absStoredLV = Math.abs((long)storedLV);
    if (absSoftwareLV > absStoredLV ||
        (softwareLV == storedLV &&
         datanodeVer.getCTime() < namenodeVer.getCTime())) 
      {
        LOG.info("softwareLayoutVersion is newer OR namenode cTime is newer: isVersionCompatible=true");
        return true;
      }
    // check #5
    LOG.info("default case: isVersionCompatible=false");
    return false;
  }
  
  /**
   * This test ensures the appropriate response (successful or failure) from 
   * a Datanode when the system is started with differing version combinations. 
   * <pre>
   * For each 3-tuple in the cross product
   *   ({oldLayoutVersion,currentLayoutVersion,futureLayoutVersion},
   *    {currentNamespaceId,incorrectNamespaceId},
   *    {pastFsscTime,currentFsscTime,futureFsscTime})
   *      1. Startup Namenode with version file containing 
   *         (currentLayoutVersion,currentNamespaceId,currentFsscTime)
   *      2. Attempt to startup Datanode with version file containing 
   *         this iterations version 3-tuple
   * </pre>
   */
  @Test (timeout=300000)
  public void testVersions() throws Exception {
    UpgradeUtilities.initialize();
    Configuration conf = UpgradeUtilities.initializeStorageStateConf(1, 
                                                      new HdfsConfiguration());
    StorageData[] versions = initializeVersions();
    UpgradeUtilities.createNameNodeStorageDirs(
        conf.getStrings(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY), "current");
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
                                              .format(false)
                                              .manageDataDfsDirs(false)
                                              .manageNameDfsDirs(false)
                                              .startupOption(StartupOption.REGULAR)
                                              .build();
    StorageData nameNodeVersion = new StorageData(
        HdfsServerConstants.NAMENODE_LAYOUT_VERSION,
        UpgradeUtilities.getCurrentNamespaceID(cluster),
        UpgradeUtilities.getCurrentClusterID(cluster),
        UpgradeUtilities.getCurrentFsscTime(cluster),
        UpgradeUtilities.getCurrentBlockPoolID(cluster));
    
    log("NameNode version info", NAME_NODE, null, nameNodeVersion);
    String bpid = UpgradeUtilities.getCurrentBlockPoolID(cluster);
    for (int i = 0; i < versions.length; i++) {
      File[] storage = UpgradeUtilities.createDataNodeStorageDirs(
          conf.getStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY), "current");
      log("DataNode version info", DATA_NODE, i, versions[i]);
      UpgradeUtilities.createDataNodeVersionFile(storage,
          versions[i].storageInfo, bpid, versions[i].blockPoolId, conf);
      try {
        cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
      } catch (Exception ignore) {
        // Ignore.  The asserts below will check for problems.
        // ignore.printStackTrace();
      }
      assertTrue(cluster.getNameNode() != null);
      assertEquals(isVersionCompatible(nameNodeVersion, versions[i]),
                   cluster.isDataNodeUp());
      cluster.shutdownDataNodes();
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
    new TestDFSStartupVersions().testVersions();
  }
  
}

