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
package org.apache.hadoop.hdfs.server.protocolR23Compatible;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.protocol.BalancerBandwidthCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.FinalizeCommand;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;

/**
 * Class for translating DatanodeCommandWritable to and from DatanodeCommand.
 */
class DatanodeCommandHelper {
  public static final Log LOG = LogFactory.getLog(DatanodeCommandHelper.class);
  
  private DatanodeCommandHelper() {
    /* Private constructor to prevent instantiation */
  }
  
  static DatanodeCommand convert(DatanodeCommandWritable cmd) {
    return cmd.convert();
  }
  
  /**
   * Given a subclass of {@link DatanodeCommand} return the corresponding
   * writable type.
   */
  static DatanodeCommandWritable convert(DatanodeCommand cmd) {
    switch (cmd.getAction()) {
    case DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE:
      return BalancerBandwidthCommandWritable
          .convert((BalancerBandwidthCommand) cmd);
    
    case DatanodeProtocol.DNA_FINALIZE:
      return FinalizeCommandWritable.convert((FinalizeCommand)cmd);
    case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
      return KeyUpdateCommandWritable.convert((KeyUpdateCommand)cmd);
    case DatanodeProtocol.DNA_REGISTER:
      return RegisterCommandWritable.REGISTER;
    case DatanodeProtocol.DNA_TRANSFER:
    case DatanodeProtocol.DNA_INVALIDATE:
      return BlockCommandWritable.convert((BlockCommand)cmd);
    case UpgradeCommand.UC_ACTION_START_UPGRADE:
      return UpgradeCommandWritable.convert((UpgradeCommand)cmd);
    case DatanodeProtocol.DNA_RECOVERBLOCK:
      return BlockRecoveryCommandWritable.convert((BlockRecoveryCommand)cmd);
    default:
      LOG.warn("Unknown DatanodeCommand action - " + cmd.getAction());
      return null;
    }
  }
}
