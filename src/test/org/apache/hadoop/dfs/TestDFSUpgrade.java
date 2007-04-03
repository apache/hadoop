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
* the system when the system is upgraded under various storage state and
* version conditions.
*
* @author Nigel Daley
*/
public class TestDFSUpgrade extends TestCase {
 
  private static final Log LOG = LogFactory.getLog(
    "org.apache.hadoop.dfs.TestDFSUpgrade");
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
   * Verify that the current and previous directories exist.  Verify that 
   * previous hasn't been modified by comparing the checksum of all it's
   * containing files with their original checksum.  It is assumed that
   * the server has recovered and upgraded.
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
      assertTrue(new File(baseDirs[i],"previous").isDirectory());
      assertEquals(
        UpgradeUtilities.checksumContents(
          nodeType, new File(baseDirs[i],"previous")),
        UpgradeUtilities.checksumMasterContents(nodeType));
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
   * This test attempts to upgrade the NameNode and DataNode under
   * a number of valid and invalid conditions.
   */
  public void testUpgrade() throws Exception {
    File[] baseDirs;
    UpgradeUtilities.initialize();
    
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs);
      String[] nameNodeDirs = conf.getStrings("dfs.name.dir");
      String[] dataNodeDirs = conf.getStrings("dfs.data.dir");
      
      log("Normal NameNode upgrade",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      runTest(NAME_NODE, StartupOption.UPGRADE, true);
      checkResult(NAME_NODE, nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("Normal DataNode upgrade",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.startCluster(NAME_NODE,StartupOption.UPGRADE,conf);
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      runTest(DATA_NODE, StartupOption.REGULAR, true);
      checkResult(DATA_NODE, dataNodeDirs);
      UpgradeUtilities.stopCluster(null);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      
      log("NameNode upgrade with existing previous dir",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      runTest(NAME_NODE, StartupOption.UPGRADE, false);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("DataNode upgrade with existing previous dir",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.startCluster(NAME_NODE,StartupOption.UPGRADE,conf);
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "previous");
      runTest(DATA_NODE, StartupOption.REGULAR, true);
      checkResult(DATA_NODE, dataNodeDirs);
      UpgradeUtilities.stopCluster(null);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);

      log("DataNode upgrade with future stored layout version in current",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.startCluster(NAME_NODE,StartupOption.UPGRADE,conf);
      baseDirs = UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      UpgradeUtilities.createVersionFile(DATA_NODE,baseDirs,
        new StorageInfo(Integer.MIN_VALUE,
                        UpgradeUtilities.getCurrentNamespaceID(),
                        UpgradeUtilities.getCurrentFsscTime()));
      runTest(DATA_NODE, StartupOption.REGULAR, false);
      UpgradeUtilities.stopCluster(null);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      
      log("DataNode upgrade with newer fsscTime in current",numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.startCluster(NAME_NODE,StartupOption.UPGRADE,conf);
      baseDirs = UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      UpgradeUtilities.createVersionFile(DATA_NODE,baseDirs,
        new StorageInfo(UpgradeUtilities.getCurrentLayoutVersion(),
                        UpgradeUtilities.getCurrentNamespaceID(),
                        Long.MAX_VALUE));
      runTest(DATA_NODE, StartupOption.REGULAR, false);
      UpgradeUtilities.stopCluster(null);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);

      log("NameNode upgrade with no edits file",numDirs);
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      for (File f : baseDirs) { 
        UpgradeUtilities.remove(new File(f,"edits"));
      }
      runTest(NAME_NODE, StartupOption.UPGRADE, false);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with no image file",numDirs);
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      for (File f : baseDirs) { 
        UpgradeUtilities.remove(new File(f,"fsimage")); 
      }
      runTest(NAME_NODE, StartupOption.UPGRADE, false);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with corrupt version file",numDirs);
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      for (File f : baseDirs) { 
        UpgradeUtilities.corruptFile(new File(f,"VERSION")); 
      }
      runTest(NAME_NODE, StartupOption.UPGRADE, false);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with future layout version in current",numDirs);
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.createVersionFile(NAME_NODE,baseDirs,
        new StorageInfo(Integer.MIN_VALUE,
                        UpgradeUtilities.getCurrentNamespaceID(),
                        UpgradeUtilities.getCurrentFsscTime()));
      runTest(NAME_NODE, StartupOption.UPGRADE, false);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
    } // end numDir loop
  }
 
  public static void main(String[] args) throws Exception {
    new TestDFSUpgrade().testUpgrade();
  }
  
}


