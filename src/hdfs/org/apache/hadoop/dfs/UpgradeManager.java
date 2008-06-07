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

import java.io.IOException;
import java.util.SortedSet;

/**
 * Generic upgrade manager.
 * 
 * {@link #broadcastCommand} is the command that should be 
 *
 */
abstract class UpgradeManager {
  protected SortedSet<Upgradeable> currentUpgrades = null;
  protected boolean upgradeState = false; // true if upgrade is in progress
  protected int upgradeVersion = 0;
  protected UpgradeCommand broadcastCommand = null;

  synchronized UpgradeCommand getBroadcastCommand() {
    return this.broadcastCommand;
  }

  boolean getUpgradeState() {
    return this.upgradeState;
  }

  int getUpgradeVersion(){
    return this.upgradeVersion;
  }

  void setUpgradeState(boolean uState, int uVersion) {
    this.upgradeState = uState;
    this.upgradeVersion = uVersion;
  }

  SortedSet<Upgradeable> getDistributedUpgrades() throws IOException {
    return UpgradeObjectCollection.getDistributedUpgrades(
                                            getUpgradeVersion(), getType());
  }

  short getUpgradeStatus() {
    if(currentUpgrades == null)
      return 100;
    return currentUpgrades.first().getUpgradeStatus();
  }

  boolean initializeUpgrade() throws IOException {
    currentUpgrades = getDistributedUpgrades();
    if(currentUpgrades == null) {
      // set new upgrade state
      setUpgradeState(false, FSConstants.LAYOUT_VERSION);
      return false;
    }
    Upgradeable curUO = currentUpgrades.first();
    // set and write new upgrade state into disk
    setUpgradeState(true, curUO.getVersion());
    return true;
  }

  boolean isUpgradeCompleted() {
    if (currentUpgrades == null) {
      return true;
    }
    return false;
  }

  abstract FSConstants.NodeType getType();
  abstract boolean startUpgrade() throws IOException;
  abstract void completeUpgrade() throws IOException;
}
