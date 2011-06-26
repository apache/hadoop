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

import java.io.File;
import java.io.IOException;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;

import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.NAME_NODE;
import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.DATA_NODE;

/**
* This test ensures the appropriate response (successful or failure) from
* the system when the system is started under various storage state and
* version conditions.
*/
public class TestDFSStorageStateRecovery extends TestCase {
 
  private static final Log LOG = LogFactory.getLog(
                                                   "org.apache.hadoop.hdfs.TestDFSStorageStateRecovery");
  private Configuration conf = null;
  private int testCounter = 0;
  private MiniDFSCluster cluster = null;
  
  /**
   * The test case table.  Each row represents a test case.  This table is
   * taken from the table in Apendix A of the HDFS Upgrade Test Plan
   * (TestPlan-HdfsUpgrade.html) attached to
   * http://issues.apache.org/jira/browse/HADOOP-702
   * The column meanings are:
   *  0) current directory exists
   *  1) previous directory exists
   *  2) previous.tmp directory exists
   *  3) removed.tmp directory exists
   *  4) lastcheckpoint.tmp directory exists
   *  5) node should recover and startup
   *  6) current directory should exist after recovery but before startup
   *  7) previous directory should exist after recovery but before startup
   */
  static boolean[][] testCases = new boolean[][] {
    new boolean[] {true,  false, false, false, false, true,  true,  false}, // 1
    new boolean[] {true,  true,  false, false, false, true,  true,  true }, // 2
    new boolean[] {true,  false, true,  false, false, true,  true,  true }, // 3
    new boolean[] {true,  true,  true,  true,  false, false, false, false}, // 4
    new boolean[] {true,  true,  true,  false, false, false, false, false}, // 4
    new boolean[] {false, true,  true,  true,  false, false, false, false}, // 4
    new boolean[] {false, true,  true,  false, false, false, false, false}, // 4
    new boolean[] {false, false, false, false, false, false, false, false}, // 5
    new boolean[] {false, true,  false, false, false, false, false, false}, // 6
    new boolean[] {false, false, true,  false, false, true,  true,  false}, // 7
    new boolean[] {true,  false, false, true,  false, true,  true,  false}, // 8
    new boolean[] {true,  true,  false, true,  false, false, false, false}, // 9
    new boolean[] {true,  true,  true,  true,  false, false, false, false}, // 10
    new boolean[] {true,  false, true,  true,  false, false, false, false}, // 10
    new boolean[] {false, true,  true,  true,  false, false, false, false}, // 10
    new boolean[] {false, false, true,  true,  false, false, false, false}, // 10
    new boolean[] {false, false, false, true,  false, false, false, false}, // 11
    new boolean[] {false, true,  false, true,  false, true,  true,  true }, // 12
    // name-node specific cases
    new boolean[] {true,  false, false, false, true,  true,  true,  false}, // 13
    new boolean[] {true,  true,  false, false, true,  true,  true,  false}, // 13
    new boolean[] {false, false, false, false, true,  true,  true,  false}, // 14
    new boolean[] {false, true,  false, false, true,  true,  true,  false}, // 14
    new boolean[] {true,  false, true,  false, true,  false, false, false}, // 15
    new boolean[] {true,  true,  false, true,  true,  false, false, false}  // 16
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
             + " current="+state[0]
             + " previous="+state[1]
             + " previous.tmp="+state[2]
             + " removed.tmp="+state[3]
             + " lastcheckpoint.tmp="+state[4]
             + " should recover="+state[5]
             + " current exists after="+state[6]
             + " previous exists after="+state[7]);
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
    if (state[0])  // current
      UpgradeUtilities.createNameNodeStorageDirs(baseDirs, "current");
    if (state[1])  // previous
      UpgradeUtilities.createNameNodeStorageDirs(baseDirs, "previous");
    if (state[2])  // previous.tmp
      UpgradeUtilities.createNameNodeStorageDirs(baseDirs, "previous.tmp");
    if (state[3])  // removed.tmp
      UpgradeUtilities.createNameNodeStorageDirs(baseDirs, "removed.tmp");
    if (state[4])  // lastcheckpoint.tmp
      UpgradeUtilities.createNameNodeStorageDirs(baseDirs, "lastcheckpoint.tmp");
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
    if (state[0])  // current
      UpgradeUtilities.createDataNodeStorageDirs(baseDirs, "current");
    if (state[1])  // previous
      UpgradeUtilities.createDataNodeStorageDirs(baseDirs, "previous");
    if (state[2])  // previous.tmp
      UpgradeUtilities.createDataNodeStorageDirs(baseDirs, "previous.tmp");
    if (state[3])  // removed.tmp
      UpgradeUtilities.createDataNodeStorageDirs(baseDirs, "removed.tmp");
    if (state[4])  // lastcheckpoint.tmp
      UpgradeUtilities.createDataNodeStorageDirs(baseDirs, "lastcheckpoint.tmp");
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
    if (state[0]) // current
      UpgradeUtilities.createBlockPoolStorageDirs(baseDirs, "current", bpid);
    if (state[1]) // previous
      UpgradeUtilities.createBlockPoolStorageDirs(baseDirs, "previous", bpid);
    if (state[2]) // previous.tmp
      UpgradeUtilities.createBlockPoolStorageDirs(baseDirs, "previous.tmp",
          bpid);
    if (state[3]) // removed.tmp
      UpgradeUtilities
          .createBlockPoolStorageDirs(baseDirs, "removed.tmp", bpid);
    if (state[4]) // lastcheckpoint.tmp
      UpgradeUtilities.createBlockPoolStorageDirs(baseDirs,
          "lastcheckpoint.tmp", bpid);
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
        assertTrue(new File(baseDirs[i],"current/edits").isFile());
        assertTrue(new File(baseDirs[i],"current/fsimage").isFile());
        assertTrue(new File(baseDirs[i],"current/fstime").isFile());
      }
    }
    if (previousShouldExist) {
      for (int i = 0; i < baseDirs.length; i++) {
        assertTrue(new File(baseDirs[i],"previous").isDirectory());
        assertEquals(
                     UpgradeUtilities.checksumContents(
                                                       NAME_NODE, new File(baseDirs[i],"previous")),
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
                     UpgradeUtilities.checksumContents(DATA_NODE, new File(baseDirs[i],"current")),
                     UpgradeUtilities.checksumMasterDataNodeContents());
      }
    }
    if (previousShouldExist) {
      for (int i = 0; i < baseDirs.length; i++) {
        assertTrue(new File(baseDirs[i],"previous").isDirectory());
        assertEquals(
                     UpgradeUtilities.checksumContents(DATA_NODE, new File(baseDirs[i],"previous")),
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
        assertEquals(UpgradeUtilities.checksumContents(DATA_NODE, bpCurDir),
                     UpgradeUtilities.checksumMasterBlockPoolContents());
      }
    }
    if (previousShouldExist) {
      for (int i = 0; i < baseDirs.length; i++) {
        File bpPrevDir = new File(baseDirs[i], Storage.STORAGE_DIR_PREVIOUS);
        assertTrue(bpPrevDir.isDirectory());
        assertEquals(
                     UpgradeUtilities.checksumContents(DATA_NODE, bpPrevDir),
                     UpgradeUtilities.checksumMasterBlockPoolContents());
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
  public void testNNStorageStates() throws Exception {
    String[] baseDirs;

    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new HdfsConfiguration();
      conf.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);      
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      for (int i = 0; i < NUM_NN_TEST_CASES; i++) {
        boolean[] testCase = testCases[i];
        boolean shouldRecover = testCase[5];
        boolean curAfterRecover = testCase[6];
        boolean prevAfterRecover = testCase[7];

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
            if(!testCases[i][0] && !testCases[i][2] 
                  && !testCases[i][1] && !testCases[i][3] && !testCases[i][4]) {
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
  public void testDNStorageStates() throws Exception {
    String[] baseDirs;

    // First setup the datanode storage directory
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new HdfsConfiguration();
      conf.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);      
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      for (int i = 0; i < NUM_DN_TEST_CASES; i++) {
        boolean[] testCase = testCases[i];
        boolean shouldRecover = testCase[5];
        boolean curAfterRecover = testCase[6];
        boolean prevAfterRecover = testCase[7];

        log("DATA_NODE recovery", numDirs, i, testCase);
        createNameNodeStorageState(new boolean[] { true, true, false, false,
            false });
        cluster = createCluster(conf);
        baseDirs = createDataNodeStorageState(testCase);
        if (!testCase[0] && !testCase[1] && !testCase[2] && !testCase[3]) {
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
        boolean shouldRecover = testCase[5];
        boolean curAfterRecover = testCase[6];
        boolean prevAfterRecover = testCase[7];

        log("BLOCK_POOL recovery", numDirs, i, testCase);
        createNameNodeStorageState(new boolean[] { true, true, false, false,
            false });
        cluster = createCluster(conf);
        baseDirs = createBlockPoolStorageState(bpid, testCase);
        if (!testCase[0] && !testCase[1] && !testCase[2] && !testCase[3]) {
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

  protected void setUp() throws Exception {
    LOG.info("Setting up the directory structures.");
    UpgradeUtilities.initialize();
  }

  protected void tearDown() throws Exception {
    LOG.info("Shutting down MiniDFSCluster");
    if (cluster != null) cluster.shutdown();
  }
}