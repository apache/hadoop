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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.junit.After;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * This test ensures the appropriate response from the system when 
 * the system is finalized.
 */
public class TestDFSFinalize {
 
  private static final Log LOG = LogFactory.getLog(
                                                   "org.apache.hadoop.hdfs.TestDFSFinalize");
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
   * Verify that the current directory exists and that the previous directory
   * does not exist.  Verify that current hasn't been modified by comparing 
   * the checksum of all it's containing files with their original checksum.
   * Note that we do not check that previous is removed on the DataNode
   * because its removal is asynchronous therefore we have no reliable
   * way to know when it will happen.  
   */
  static void checkResult(String[] nameNodeDirs, String[] dataNodeDirs) throws Exception {
    List<File> dirs = Lists.newArrayList();
    for (int i = 0; i < nameNodeDirs.length; i++) {
      File curDir = new File(nameNodeDirs[i], "current");
      dirs.add(curDir);
      FSImageTestUtil.assertReasonableNameCurrentDir(curDir);
    }
    
    FSImageTestUtil.assertParallelFilesAreIdentical(
        dirs, Collections.<String>emptySet());
    
    for (int i = 0; i < dataNodeDirs.length; i++) {
      assertEquals(
                   UpgradeUtilities.checksumContents(
                                                     DATA_NODE, new File(dataNodeDirs[i],"current")),
                   UpgradeUtilities.checksumMasterDataNodeContents());
    }
    for (int i = 0; i < nameNodeDirs.length; i++) {
      assertFalse(new File(nameNodeDirs[i],"previous").isDirectory());
    }
  }
 
  /**
   * This test attempts to finalize the NameNode and DataNode.
   */
  @Test
  public void testFinalize() throws Exception {
    UpgradeUtilities.initialize();
    
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      /* This test requires that "current" directory not change after
       * the upgrade. Actually it is ok for those contents to change.
       * For now disabling block verification so that the contents are 
       * not changed.
       */
      conf = new HdfsConfiguration();
      conf.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      String[] nameNodeDirs = conf.getStrings(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
      String[] dataNodeDirs = conf.getStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
      
      log("Finalize with existing previous dir", numDirs);
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "current");
      UpgradeUtilities.createNameNodeStorageDirs(nameNodeDirs, "previous");
      UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "current");
      UpgradeUtilities.createDataNodeStorageDirs(dataNodeDirs, "previous");
      cluster = new MiniDFSCluster.Builder(conf)
                                  .format(false)
                                  .manageDataDfsDirs(false)
                                  .manageNameDfsDirs(false)
                                  .startupOption(StartupOption.REGULAR)
                                  .build();
      cluster.finalizeCluster(conf);
      checkResult(nameNodeDirs, dataNodeDirs);

      log("Finalize without existing previous dir", numDirs);
      cluster.finalizeCluster(conf);
      checkResult(nameNodeDirs, dataNodeDirs);

      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
    } // end numDir loop
  }
 
  @After
  public void tearDown() throws Exception {
    LOG.info("Shutting down MiniDFSCluster");
    if (cluster != null) cluster.shutdown();
  }
  
  public static void main(String[] args) throws Exception {
    new TestDFSFinalize().testFinalize();
  }
  
}


