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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

import java.util.EnumSet;

/**
 * AddBlockFlag provides hints for new block allocation and placement.
 * Users can use this flag to control <em>per DFSOutputStream</em>
 * @see ClientProtocol#addBlock(String, String, ExtendedBlock, DatanodeInfo[],
 *      long, String[], EnumSet)
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public enum AddBlockFlag {

  /**
   * Advise that a block replica NOT be written to the local DataNode where
   * 'local' means the same host as the client is being run on.
   *
   * @see CreateFlag#NO_LOCAL_WRITE
   */
  NO_LOCAL_WRITE((short) 0x01),

  /**
   * Advise that the first block replica NOT take into account DataNode
   * locality. The first block replica should be placed randomly within the
   * cluster. Subsequent block replicas should follow DataNode locality rules.
   *
   * @see CreateFlag#IGNORE_CLIENT_LOCALITY
   */
  IGNORE_CLIENT_LOCALITY((short) 0x02);

  private final short mode;

  AddBlockFlag(short mode) {
    this.mode = mode;
  }

  public static AddBlockFlag valueOf(short mode) {
    for (AddBlockFlag flag : AddBlockFlag.values()) {
      if (flag.getMode() == mode) {
        return flag;
      }
    }
    return null;
  }

  public short getMode() {
    return mode;
  }
}
