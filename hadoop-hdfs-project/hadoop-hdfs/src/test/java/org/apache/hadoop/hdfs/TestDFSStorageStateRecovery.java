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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
* This test ensures the appropriate response (successful or failure) from
* the system when the system is started under various storage state and
* version conditions.
*/
public class TestDFSStorageStateRecovery {
 
  private static final Log LOG = LogFactory.getLog(
                                                   "org.apache.hadoop.hdfs.TestDFSStorageStateRecovery");
  private Configuration conf = null;
  private int testCounter = 0;
  private MiniDFSCluster cluster = null;
  
  // Constants for indexes into test case table below.
  private static final int CURRENT_EXISTS = 0;
  private static final int PREVIOUS_EXISTS = 1;
  private static final int PREVIOUS_TMP_EXISTS = 2;
  private static final int REMOVED_TMP_EXISTS = 3;
  private static final int SHOULD_RECOVER = 4;
  private static final int CURRENT_SHOULD_EXIST_AFTER_RECOVER = 5;
  private static final int PREVIOUS_SHOULD_EXIST_AFTER_RECOVER = 6;
  
  /**
   * The test case table.  Each row represents a test case.  This table is
   * taken from the table in Apendix A of the HDFS Upgrade Test Plan
   * (TestPlan-HdfsUpgrade.html) attached to
   * http://issues.apache.org/jira/browse/HADOOP-702
   * 
   * It has been slightly modified since previouscheckpoint.tmp no longer
   * exists.
   * 
   * The column meanings are:
   *  0) current directory exists
   *  1) previous directory exists
   *  2) previous.tmp directory exists
   *  3) removed.tmp directory exists
   *  4) node should recover and startup
   *  5) current directory should exist after recovery but before startup
   *  6) previous directory should exist after recovery but before startup
   */
  static final boolean[][] testCases = new boolean[][] {
    new boolean[] {true,  false, false, false, true,  true,  false}, // 1
    new boolean[] {true,  true,  false, false, true,  true,  true }, // 2
    new boolean[] {true,  false, true,  false, true,  true,  true }, // 3
    new boolean[] {true,  true,  true,  true,  false, false, false}, // 4
    new boolean[] {true,  true,  true,  false, false, false, false}, // 4
    new boolean[] {false, true,  true,  true,  false, false, false}, // 4
    new boolean[] {false, true,  true,  false, false, false, false}, // 4
    new boolean[] {false, false, false, false, false, false, false}, // 5
    new boolean[] {false, true,  false, false, false, false, false}, // 6
    new boolean[] {false, false, true,  false, true,  true,  false}, // 7
    new boolean[] {true,  false, false, true,  true,  true,  false}, // 8
    new boolean[] {true,  true,  false, true,  false, false, false}, // 9
    new boolean[] {true,  true,  true,  true,  false, false, false}, // 10
    new boolean[] {true,  false, true,  true,  false, false, false}, // 10
    new boolean[] {false, true,  true,  true,  false, false, false}, // 10
    new boolean[] {false, false, true,  true,  false, false, false}, // 10
    new boolean[] {false, false, false, true,  false, false, false}, // 11
    new boolean[] {false, true,  false, true,  true,  true,  true }, // 12
    // name-node specific cases
    new boolean[] {true,  true,  false, false, true,  true,  false}, // 13
  };

  private static final int NUM_NN_TEST_CASES = testCases.length;
  private static final int NUM_DN_TEST_CASES = 18;

  /**
   * Writes an INFO log message containing the parameters. Only
   * the first 4 elements of the state array are included in the message.
   */
  void log(String label, int numDirs, int testCaseNum, boolean[] state) {
    LOG.info("============================================================");
    LOG.info("***TEST " + (testCounter++) + "*** " 
             + label + ":"
             + " numDirs="+numDirs
             + " testCase="+testCaseNum
             + " current="+state[CURRENT_EXISTS]
             + " previous="+state[PREVIOUS_EXISTS]
             + " previous.tmp="+state[PREVIOUS_TMP_EXISTS]
             + " removed.tmp="+state[REMOVED_TMP_EXISTS]
             + " should recover="+state[SHOULD_RECOVER]
             + " current exists after="+state[CURRENT_SHOULD_EXIST_AFTER_RECOVER]
             + " previous exists after="+state[PREVIOUS_SHOULD_EXIST_AFTER_RECOVER]);
  }
  
  /**
   * Sets up the storage directories for namenode as defined by
   * {@link DFSConfigKeys#DFS_NAMENODE_NAME_DIR_KEY}. For each element 
   * in {@link DFSConfigKeys#DFS_NAMENODE_NAME_DIR_KEY}, the subdirectories 
   * represented by the first four elements of the <code>state</code> array
   * will be created and populated.
   * 
   * See {@link UpgradeUtilities#createNameNodeStorageDirs()}
   * 
   * @param state
   *   a row from the testCases table which indicates which directories
   *   to setup for the node
   * @return file paths representing namenode storage directories
   */
  String[] createNameNodeStorageState(boolean[] state) throws Exception {
    String[] baseDirs = conf.getStrings(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
    UpgradeUtilities.createEmptyDirs(baseDirs);
    if (state[CURRENT_EXISTS])  // current
      UpgradeUtilities.createNameNodeStorageDirs(baseDirs, "current");
    if (state[PREVIOUS_EXISTS])  // previous
      UpgradeUtilities.createNameNodeStorageDirs(baseDirs, "previous");
    if (state[PREVIOUS_TMP_EXISTS])  // previous.tmp
      UpgradeUtilities.createNameNodeStorageDirs(baseDirs, "previous.tmp");
    if (state[REMOVED_TMP_EXISTS])  // removed.tmp
      UpgradeUtilities.createNameNodeStorageDirs(baseDirs, "removed.tmp");

    return baseDirs;
  }
  
  /**
   * Sets up the storage directories for a datanode under
   * {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY}. For each element in 
   * {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY}, the subdirectories 
   * represented by the first four elements of the <code>state</code> array 
   * will be created and populated. 
   * See {@link UpgradeUtilities#createDataNodeStorageDirs()}
   * 
   * @param state
   *   a row from the testCases table which indicates which directories
   *   to setup for the node
   * @return file paths representing datanode storage directories
   */
  String[] createDataNodeStorageState(boolean[] state) throws Exception {
    String[] baseDirs = conf.getStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
    UpgradeUtilities.createEmptyDirs(baseDirs);
    if (state[CURRENT_EXISTS])  // current
      UpgradeUtilities.createDataNodeStorageDirs(baseDirs, "current");
    if (state[PREVIOUS_EXISTS])  // previous
      UpgradeUtilities.createDataNodeStorageDirs(baseDirs, "previous");
    if (state[PREVIOUS_TMP_EXISTS])  // previous.tmp
      UpgradeUtilities.createDataNodeStorageDirs(baseDirs, "previous.tmp");
    if (state[REMOVED_TMP_EXISTS])  // removed.tmp
      UpgradeUtilities.createDataNodeStorageDirs(baseDirs, "removed.tmp");

    return baseDirs;
  }
  
  /**
   * Sets up the storage directories for a block pool under
   * {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY}. For each element 
   * in {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY}, the subdirectories 
   * represented by the first four elements of the <code>state</code> array 
   * will be created and populated. 
   * See {@link UpgradeUtilities#createBlockPoolStorageDirs()}
   * 
   * @param bpid block pool Id
   * @param state
   *   a row from the testCases table which indicates which directories
   *   to setup for the node
   * @return file paths representing block pool storage directories
   */
  String[] createBlockPoolStorageState(String bpid, boolean[] state) throws Exception {
    String[] baseDirs = conf.getStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
    UpgradeUtilities.createEmptyDirs(baseDirs);
    UpgradeUtilities.createDataNodeStorageDirs(baseDirs, "current");
    
    // After copying the storage directories from master datanode, empty
    // the block pool storage directories
    String[] bpDirs = UpgradeUtilities.createEmptyBPDirs(baseDirs, bpid);
    if (state[CURRENT_EXISTS]) // current
      UpgradeUtilities.createBlockPoolStorageDirs(baseDirs, "current", bpid);
    if (state[PREVIOUS_EXISTS]) // previous
      UpgradeUtilities.createBlockPoolStorageDirs(baseDirs, "previous", bpid);
    if (state[PREVIOUS_TMP_EXISTS]) // previous.tmp
      UpgradeUtilities.createBlockPoolStorageDirs(baseDirs, "previous.tmp",
          bpid);
    if (state[REMOVED_TMP_EXISTS]) // removed.tmp
      UpgradeUtilities
          .createBlockPoolStorageDirs(baseDirs, "removed.tmp", bpid);
    return bpDirs;
  }
  
  /**
   * For NameNode, verify that the current and/or previous exist as indicated by 
   * the method parameters.  If previous exists, verify that
   * it hasn't been modified by comparing the checksum of all it's
   * containing files with their original checksum.  It is assumed that
   * the server has recovered.
   */
  void checkResultNameNode(String[] baseDirs, 
                   boolean currentShouldExist, boolean previousShouldExist) 
    throws IOException
  {
    if (currentShouldExist) {
      for (int i = 0; i < baseDirs.length; i++) {
        assertTrue(new File(baseDirs[i],"current").isDirectory());
        assertTrue(new File(baseDirs[i],"current/VERSION").isFile());
        assertNotNull(FSImageTestUtil.findNewestImageFile(
            baseDirs[i] + "/current"));
        assertTrue(new File(baseDirs[i],"current/seen_txid").isFile());
      }
    }
    if (previousShouldExist) {
      for (int i = 0; i < baseDirs.length; i++) {
        assertTrue(new File(baseDirs[i],"previous").isDirectory());
        assertEquals(
                     UpgradeUtilities.checksumContents(
                     NAME_NODE, new File(baseDirs[i],"previous"), false),
                     UpgradeUtilities.checksumMasterNameNodeContents());
      }
    }
  }
  
  /**
   * For datanode, verify that the current and/or previous exist as indicated by 
   * the method parameters.  If previous exists, verify that
   * it hasn't been modified by comparing the checksum of all it's
   * containing files with their original checksum.  It is assumed that
   * the server has recovered.
   */
  void checkResultDataNode(String[] baseDirs, 
                   boolean currentShouldExist, boolean previousShouldExist) 
    throws IOException
  {
    if (currentShouldExist) {
      for (int i = 0; i < baseDirs.length; i++) {
        assertEquals(
                     UpgradeUtilities.checksumContents(DATA_NODE,
                     new File(baseDirs[i],"current"), false),
                     UpgradeUtilities.checksumMasterDataNodeContents());
      }
    }
    if (previousShouldExist) {
      for (int i = 0; i < baseDirs.length; i++) {
        assertTrue(new File(baseDirs[i],"previous").isDirectory());
        assertEquals(
                     UpgradeUtilities.checksumContents(DATA_NODE,
                     new File(baseDirs[i],"previous"), false),
                     UpgradeUtilities.checksumMasterDataNodeContents());
      }
    }
  }
 
  /**
   * For block pool, verify that the current and/or previous exist as indicated
   * by the method parameters.  If previous exists, verify that
   * it hasn't been modified by comparing the checksum of all it's
   * containing files with their original checksum.  It is assumed that
   * the server has recovered.
   * @param baseDirs directories pointing to block pool storage
   * @param bpid block pool Id
   * @param currentShouldExist current directory exists under storage
   * @param currentShouldExist previous directory exists under storage
   */
  void checkResultBlockPool(String[] baseDirs, boolean currentShouldExist,
      boolean previousShouldExist) throws IOException
  {
    if (currentShouldExist) {
      for (int i = 0; i < baseDirs.length; i++) {
        File bpCurDir = new File(baseDirs[i], Storage.STORAGE_DIR_CURRENT);
        assertEquals(UpgradeUtilities.checksumContents(DATA_NODE, bpCurDir,
                false), UpgradeUtilities.checksumMasterBlockPoolContents());
      }
    }
    if (previousShouldExist) {
      for (int i = 0; i < baseDirs.length; i++) {
        File bpPrevDir = new File(baseDirs[i], Storage.STORAGE_DIR_PREVIOUS);
        assertTrue(bpPrevDir.isDirectory());
        assertEquals(
                     UpgradeUtilities.checksumContents(DATA_NODE, bpPrevDir,
                     false), UpgradeUtilities.checksumMasterBlockPoolContents());
      }
    }
  }
  
  private MiniDFSCluster createCluster(Configuration c) throws IOException {
    return new MiniDFSCluster.Builder(c)
                             .numDataNodes(0)
                             .startupOption(StartupOption.REGULAR)
                             .format(false)
                             .manageDataDfsDirs(false)
                             .manageNameDfsDirs(false)
                             .build();
  }
  /**
   * This test iterates over the testCases table and attempts
   * to startup the NameNode normally.
   */
  @Test
  public void testNNStorageStates() throws Exception {
    String[] baseDirs;

    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new HdfsConfiguration();
      conf.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);      
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      for (int i = 0; i < NUM_NN_TEST_CASES; i++) {
        boolean[] testCase = testCases[i];
        boolean shouldRecover = testCase[SHOULD_RECOVER];
        boolean curAfterRecover = testCase[CURRENT_SHOULD_EXIST_AFTER_RECOVER];
        boolean prevAfterRecover = testCase[PREVIOUS_SHOULD_EXIST_AFTER_RECOVER];

        log("NAME_NODE recovery", numDirs, i, testCase);
        baseDirs = createNameNodeStorageState(testCase);
        if (shouldRecover) {
          cluster = createCluster(conf);
          checkResultNameNode(baseDirs, curAfterRecover, prevAfterRecover);
          cluster.shutdown();
        } else {
          try {
            cluster = createCluster(conf);
            throw new AssertionError("NameNode should have failed to start");
          } catch (IOException expected) {
            // the exception is expected
            // check that the message says "not formatted" 
            // when storage directory is empty (case #5)
            if(!testCases[i][CURRENT_EXISTS] && !testCases[i][PREVIOUS_TMP_EXISTS] 
                  && !testCases[i][PREVIOUS_EXISTS] && !testCases[i][REMOVED_TMP_EXISTS]) {
              assertTrue(expected.getLocalizedMessage().contains(
                  "NameNode is not formatted"));
            }
          }
        }
        cluster.shutdown();
      } // end testCases loop
    } // end numDirs loop
  }

  /**
   * This test iterates over the testCases table for Datanode storage and
   * attempts to startup the DataNode normally.
   */
  @Test
  public void testDNStorageStates() throws Exception {
    String[] baseDirs;

    // First setup the datanode storage directory
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new HdfsConfiguration();
      conf.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);      
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      for (int i = 0; i < NUM_DN_TEST_CASES; i++) {
        boolean[] testCase = testCases[i];
        boolean shouldRecover = testCase[SHOULD_RECOVER];
        boolean curAfterRecover = testCase[CURRENT_SHOULD_EXIST_AFTER_RECOVER];
        boolean prevAfterRecover = testCase[PREVIOUS_SHOULD_EXIST_AFTER_RECOVER];

        log("DATA_NODE recovery", numDirs, i, testCase);
        createNameNodeStorageState(new boolean[] { true, true, false, false,
            false });
        cluster = createCluster(conf);
        baseDirs = createDataNodeStorageState(testCase);
        if (!testCase[CURRENT_EXISTS] && !testCase[PREVIOUS_EXISTS] && !testCase[PREVIOUS_TMP_EXISTS] && !testCase[REMOVED_TMP_EXISTS]) {
          // DataNode will create and format current if no directories exist
          cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
        } else {
          if (shouldRecover) {
            cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
            checkResultDataNode(baseDirs, curAfterRecover, prevAfterRecover);
          } else {
            cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
            assertFalse(cluster.getDataNodes().get(0).isDatanodeUp());
          }
        }
        cluster.shutdown();
      } // end testCases loop
    } // end numDirs loop
  }

  /**
   * This test iterates over the testCases table for block pool storage and
   * attempts to startup the DataNode normally.
   */
  @Test
  public void testBlockPoolStorageStates() throws Exception {
    String[] baseDirs;

    // First setup the datanode storage directory
    String bpid = UpgradeUtilities.getCurrentBlockPoolID(null);
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new HdfsConfiguration();
      conf.setInt("dfs.datanode.scan.period.hours", -1);      
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      for (int i = 0; i < NUM_DN_TEST_CASES; i++) {
        boolean[] testCase = testCases[i];
        boolean shouldRecover = testCase[SHOULD_RECOVER];
        boolean curAfterRecover = testCase[CURRENT_SHOULD_EXIST_AFTER_RECOVER];
        boolean prevAfterRecover = testCase[PREVIOUS_SHOULD_EXIST_AFTER_RECOVER];

        log("BLOCK_POOL recovery", numDirs, i, testCase);
        createNameNodeStorageState(new boolean[] { true, true, false, false,
            false });
        cluster = createCluster(conf);
        baseDirs = createBlockPoolStorageState(bpid, testCase);
        if (!testCase[CURRENT_EXISTS] && !testCase[PREVIOUS_EXISTS] && !testCase[PREVIOUS_TMP_EXISTS] && !testCase[REMOVED_TMP_EXISTS]) {
          // DataNode will create and format current if no directories exist
          cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
        } else {
          if (shouldRecover) {
            cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
            checkResultBlockPool(baseDirs, curAfterRecover, prevAfterRecover);
          } else {
            cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
            assertFalse(cluster.getDataNodes().get(0).isBPServiceAlive(bpid));
          }
        }
        cluster.shutdown();
      } // end testCases loop
    } // end numDirs loop
  }

  @Before
  public void setUp() throws Exception {
    LOG.info("Setting up the directory structures.");
    UpgradeUtilities.initialize();
  }

  @After
  public void tearDown() throws Exception {
    LOG.info("Shutting down MiniDFSCluster");
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
}
