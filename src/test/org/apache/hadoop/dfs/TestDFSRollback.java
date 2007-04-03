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
package org.apache.hadoop.dfs;

import java.io.File;
import java.io.IOException;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.FSConstants.NodeType;
import static org.apache.hadoop.dfs.FSConstants.NodeType.NAME_NODE;
import static org.apache.hadoop.dfs.FSConstants.NodeType.DATA_NODE;
import org.apache.hadoop.dfs.FSConstants.StartupOption;
import org.apache.hadoop.fs.Path;

/**
* This test ensures the appropriate response (successful or failure) from
* the system when the system is rolled back under various storage state and
* version conditions.
*
* @author Nigel Daley
*/
public class TestDFSRollback extends TestCase {
 
  private static final Log LOG = LogFactory.getLog(
    "org.apache.hadoop.dfs.TestDFSRollback");
  Configuration conf;
  private int testCounter = 0;
  
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
  void checkResult(NodeType nodeType, String[] baseDirs) throws IOException {
    switch (nodeType) {
      case NAME_NODE:
        for (int i = 0; i < baseDirs.length; i++) {
          assertTrue(new File(baseDirs[i],"current").isDirectory());
          assertTrue(new File(baseDirs[i],"current/VERSION").isFile());
          assertTrue(new File(baseDirs[i],"current/edits").isFile());
          assertTrue(new File(baseDirs[i],"current/fsimage").isFile());
          assertTrue(new File(baseDirs[i],"current/fstime").isFile());
        }
        break;
      case DATA_NODE:
        for (int i = 0; i < baseDirs.length; i++) {
          assertEquals(
            UpgradeUtilities.checksumContents(
              nodeType, new File(baseDirs[i],"current")),
            UpgradeUtilities.checksumMasterContents(nodeType));
        }
        break;
    }
    for (int i = 0; i < baseDirs.length; i++) {
      assertFalse(new File(baseDirs[i],"previous").isDirectory());
    }
  }
 
  /**
   * Starts the given nodeType with the given operation.  The remaining 
   * parameters are used to verify the expected result.
   * 
   * @param nodeType must not be null
   */
  void runTest(NodeType nodeType, StartupOption operation, boolean shouldStart) 
    throws Exception 
  {
    if (shouldStart) {
      UpgradeUtilities.startCluster(nodeType, operation, conf);
      UpgradeUtilities.stopCluster(nodeType);
    } else {
      try {
        UpgradeUtilities.startCluster(nodeType, operation, conf); // should fail
        throw new AssertionError("Cluster should have failed to start");
      } catch (Exception expected) {
        // expected
        //expected.printStackTrace();
        assertFalse(UpgradeUtilities.isNodeRunning(nodeType));
      } finally {
        UpgradeUtilities.stopCluster(nodeType);
      }
    }
  }
 
  /**
   * This test attempts to rollback the NameNode and DataNode under
   * a number of valid and invalid conditions.
   */
  public void testRollback() throws Exception {
    File[] baseDirs;
    UpgradeUtilities.initialize();
    
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs);
      String[] nameNodeDirs = conf.getStrings("dfs.name.dir");
      String[] dataNodeDirs = conf.getStrings("dfs.data.dir");
      
      log("Normal NameNode rollback",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      runTest(NAME_NODE, StartupOption.ROLLBACK, true);
      checkResult(NAME_NODE, nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("Normal DataNode rollback",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      UpgradeUtilities.startCluster(NAME_NODE,StartupOption.ROLLBACK,conf);
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "previous");
      runTest(DATA_NODE, StartupOption.ROLLBACK, true);
      checkResult(DATA_NODE, dataNodeDirs);
      UpgradeUtilities.stopCluster(null);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);

      log("NameNode rollback without existing previous dir",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      runTest(NAME_NODE, StartupOption.ROLLBACK, false);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("DataNode rollback without existing previous dir",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.startCluster(NAME_NODE,StartupOption.UPGRADE,conf);
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      runTest(DATA_NODE, StartupOption.ROLLBACK, true);
      UpgradeUtilities.stopCluster(null);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);

      log("DataNode rollback with future stored layout version in previous",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      UpgradeUtilities.startCluster(NAME_NODE,StartupOption.ROLLBACK,conf);
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      baseDirs = UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "previous");
      UpgradeUtilities.createVersionFile(DATA_NODE,baseDirs,
        new StorageInfo(Integer.MIN_VALUE,
                        UpgradeUtilities.getCurrentNamespaceID(),
                        UpgradeUtilities.getCurrentFsscTime()));
      runTest(DATA_NODE, StartupOption.ROLLBACK, false);
      UpgradeUtilities.stopCluster(null);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      
      log("DataNode rollback with newer fsscTime in previous",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      UpgradeUtilities.startCluster(NAME_NODE,StartupOption.ROLLBACK,conf);
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      baseDirs = UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "previous");
      UpgradeUtilities.createVersionFile(DATA_NODE,baseDirs,
        new StorageInfo(UpgradeUtilities.getCurrentLayoutVersion(),
                        UpgradeUtilities.getCurrentNamespaceID(),
                        Long.MAX_VALUE));
      runTest(DATA_NODE, StartupOption.ROLLBACK, false);
      UpgradeUtilities.stopCluster(null);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);

      log("NameNode rollback with no edits file",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      for (File f : baseDirs) { 
        UpgradeUtilities.remove(new File(f,"edits"));
      }
      runTest(NAME_NODE, StartupOption.ROLLBACK, false);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode rollback with no image file",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      for (File f : baseDirs) { 
        UpgradeUtilities.remove(new File(f,"fsimage")); 
      }
      runTest(NAME_NODE, StartupOption.ROLLBACK, false);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode rollback with corrupt version file",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      for (File f : baseDirs) { 
        UpgradeUtilities.corruptFile(new File(f,"VERSION")); 
      }
      runTest(NAME_NODE, StartupOption.ROLLBACK, false);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode rollback with old layout version in previous",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      UpgradeUtilities.createVersionFile(NAME_NODE,baseDirs,
        new StorageInfo(1,
                        UpgradeUtilities.getCurrentNamespaceID(),
                        UpgradeUtilities.getCurrentFsscTime()));
      runTest(NAME_NODE, StartupOption.UPGRADE, false);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
    } // end numDir loop
  }
 
  public static void main(String[] args) throws Exception {
    new TestDFSRollback().testRollback();
  }
  
}


