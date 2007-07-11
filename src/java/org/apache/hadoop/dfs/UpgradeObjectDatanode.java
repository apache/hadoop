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

import org.apache.hadoop.util.StringUtils;
import java.io.IOException;

/**
 * Base class for data-node upgrade objects.
 * Data-node upgrades are run in separate threads.
 */
abstract class UpgradeObjectDatanode extends UpgradeObject implements Runnable {
  private DataNode dataNode = null;

  public FSConstants.NodeType getType() {
    return FSConstants.NodeType.DATA_NODE;
  }

  protected DataNode getDatanode() {
    return dataNode;
  }

  void setDatanode(DataNode dataNode) {
    this.dataNode = dataNode;
  }

  /**
   * Specifies how the upgrade is performed. 
   * @throws IOException
   */
  abstract void doUpgrade() throws IOException;

  public void run() {
    assert dataNode != null : "UpgradeObjectDatanode.dataNode is null";
    while(dataNode.shouldRun) {
      try {
        doUpgrade();
      } catch(Exception e) {
        DataNode.LOG.error(StringUtils.stringifyException(e));
      }
      break;
    }

    // report results
    if(getUpgradeStatus() < 100) {
      DataNode.LOG.info("\n   Distributed upgrade for DataNode version " 
          + getVersion() + " to current LV " 
          + FSConstants.LAYOUT_VERSION + " cannot be completed.");
    }

    // Complete the upgrade by calling the manager method
    try {
      dataNode.upgradeManager.completeUpgrade();
    } catch(IOException e) {
      DataNode.LOG.error(StringUtils.stringifyException(e));
    }
  }

  /**
   * Complete upgrade and return a status complete command for broadcasting.
   * 
   * Data-nodes finish upgrade at different times.
   * The data-node needs to re-confirm with the name-node that the upgrade
   * is complete while other nodes are still upgrading.
   */
  public UpgradeCommand completeUpgrade() throws IOException {
    return new UpgradeCommand(UpgradeCommand.UC_ACTION_REPORT_STATUS,
                              getVersion(), (short)100);
  }
}
