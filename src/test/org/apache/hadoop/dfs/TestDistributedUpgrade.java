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

import static org.apache.hadoop.dfs.FSConstants.NodeType.DATA_NODE;
import static org.apache.hadoop.dfs.FSConstants.NodeType.NAME_NODE;
import static org.apache.hadoop.dfs.FSConstants.LAYOUT_VERSION;
import org.apache.hadoop.dfs.FSConstants.StartupOption;

/**
 */
public class TestDistributedUpgrade extends TestCase {
  private static final Log LOG = LogFactory.getLog(
                             "org.apache.hadoop.dfs.TestDistributedUpgrade");
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
   * Attempts to start a NameNode with the given operation.  Starting
   * the NameNode should throw an exception.
   */
  void startNameNodeShouldFail(StartupOption operation) {
    try {
      cluster = new MiniDFSCluster(conf, 0, operation); // should fail
      throw new AssertionError("NameNode should have failed to start");
    } catch (Exception expected) {
      expected = null;
      // expected
    }
  }
  
  /**
   * Attempts to start a DataNode with the given operation.  Starting
   * the DataNode should throw an exception.
   */
  void startDataNodeShouldFail(StartupOption operation) {
    try {
      cluster.startDataNodes(conf, 1, false, operation, null); // should fail
      throw new AssertionError("DataNode should have failed to start");
    } catch (Exception expected) {
      // expected
      assertFalse(cluster.isDataNodeUp());
    }
  }
 
  /**
   */
  public void testDistributedUpgrade() throws Exception {
    File[] baseDirs;
    int numDirs = 1;
    UpgradeUtilities.initialize();

    // register new upgrade objects (ignore all existing)
    UpgradeObjectCollection.initialize();
    UpgradeObjectCollection.registerUpgrade(new UpgradeObject_Test_Datanode());
    UpgradeObjectCollection.registerUpgrade(new UpgradeObject_Test_Namenode());

    conf = UpgradeUtilities.initializeStorageStateConf(numDirs);
    String[] nameNodeDirs = conf.getStrings("dfs.name.dir");
    String[] dataNodeDirs = conf.getStrings("dfs.data.dir");
    DFSAdmin dfsAdmin = new DFSAdmin();
    dfsAdmin.setConf(conf);
    String[] pars = {"-safemode", "wait"};

    log("NameNode start in regular mode when dustributed upgrade is required", numDirs);
    baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
    UpgradeUtilities.createVersionFile(NAME_NODE, baseDirs,
        new StorageInfo(LAYOUT_VERSION+2,
                        UpgradeUtilities.getCurrentNamespaceID(cluster),
                        UpgradeUtilities.getCurrentFsscTime(cluster)));
    startNameNodeShouldFail(StartupOption.REGULAR);

    log("Start NameNode only distributed upgrade", numDirs);
    cluster = new MiniDFSCluster(conf, 0, StartupOption.UPGRADE);
    cluster.shutdown();

    log("NameNode start in regular mode when dustributed upgrade has been started", numDirs);
    startNameNodeShouldFail(StartupOption.REGULAR);

    log("NameNode rollback to the old version that require a dustributed upgrade", numDirs);
    startNameNodeShouldFail(StartupOption.ROLLBACK);

    log("Normal distributed upgrade for the cluster", numDirs);
    cluster = new MiniDFSCluster(conf, 0, StartupOption.UPGRADE);
    UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
    cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
    dfsAdmin.run(pars);
    cluster.shutdown();

    // it should be ok to start in regular mode
    log("NameCluster regular startup after the upgrade", numDirs);
    cluster = new MiniDFSCluster(conf, 0, StartupOption.REGULAR);
    cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
    cluster.shutdown();
    UpgradeUtilities.createEmptyDirs(nameNodeDirs);
    UpgradeUtilities.createEmptyDirs(dataNodeDirs);
  }

  public static void main(String[] args) throws Exception {
    new TestDistributedUpgrade().testDistributedUpgrade();
    LOG.info("=== DONE ===");
  }
}

/**
 * Upgrade object for data-node
 */
class UpgradeObject_Test_Datanode extends UpgradeObjectDatanode {
  public int getVersion() {
    return LAYOUT_VERSION+1;
  }

  public void doUpgrade() throws IOException {
    this.status = (short)100;
    getDatanode().namenode.processUpgradeCommand(
        new UpgradeCommand(UpgradeCommand.UC_ACTION_REPORT_STATUS, 
            getVersion(), getUpgradeStatus()));
  }

  public UpgradeCommand startUpgrade() throws IOException {
    this.status = (short)0;
    return null;
  }
}

/**
 * Upgrade object for name-node
 */
class UpgradeObject_Test_Namenode extends UpgradeObjectNamenode {
  public int getVersion() {
    return LAYOUT_VERSION+1;
  }

  synchronized public UpgradeCommand processUpgradeCommand(
                                  UpgradeCommand command) throws IOException {
    switch(command.action) {
      case UpgradeCommand.UC_ACTION_REPORT_STATUS:
        this.status += command.getCurrentStatus()/2;  // 2 reports needed
        break;
      default:
        this.status++;
    }
    return null;
  }

  public UpgradeCommand completeUpgrade() throws IOException {
    return null;
  }
}
