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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This as a generic distributed upgrade command.
 * 
 * During the upgrade cluster components send upgrade commands to each other
 * in order to obtain or share information with them.
 * It is supposed that each upgrade defines specific upgrade command by
 * deriving them from this class.
 * The upgrade command contains version of the upgrade, which is verified 
 * on the receiving side and current status of the upgrade.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class UpgradeCommand extends DatanodeCommand {
  public final static int UC_ACTION_UNKNOWN = DatanodeProtocol.DNA_UNKNOWN;
  public final static int UC_ACTION_REPORT_STATUS = 
      DatanodeProtocol.DNA_UC_ACTION_REPORT_STATUS;
  public final static int UC_ACTION_START_UPGRADE =
      DatanodeProtocol.DNA_UC_ACTION_START_UPGRADE;

  private int version;
  private short upgradeStatus;

  public UpgradeCommand() {
    super(UC_ACTION_UNKNOWN);
    this.version = 0;
    this.upgradeStatus = 0;
  }

  public UpgradeCommand(int action, int version, short status) {
    super(action);
    this.version = version;
    this.upgradeStatus = status;
  }

  public int getVersion() {
    return this.version;
  }

  public short getCurrentStatus() {
    return this.upgradeStatus;
  }
}
