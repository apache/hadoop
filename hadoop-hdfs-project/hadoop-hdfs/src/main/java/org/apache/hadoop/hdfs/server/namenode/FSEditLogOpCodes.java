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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Op codes for edits file
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum FSEditLogOpCodes {
  // last op code in file
  OP_INVALID                    ((byte) -1),
  OP_ADD                        ((byte)  0),
  OP_RENAME_OLD                 ((byte)  1), // deprecated operation
  OP_DELETE                     ((byte)  2),
  OP_MKDIR                      ((byte)  3),
  OP_SET_REPLICATION            ((byte)  4),
  @Deprecated OP_DATANODE_ADD   ((byte)  5), // obsolete
  @Deprecated OP_DATANODE_REMOVE((byte)  6), // obsolete
  OP_SET_PERMISSIONS            ((byte)  7),
  OP_SET_OWNER                  ((byte)  8),
  OP_CLOSE                      ((byte)  9),
  OP_SET_GENSTAMP_V1            ((byte) 10),
  OP_SET_NS_QUOTA               ((byte) 11), // obsolete
  OP_CLEAR_NS_QUOTA             ((byte) 12), // obsolete
  OP_TIMES                      ((byte) 13), // set atime, mtime
  OP_SET_QUOTA                  ((byte) 14),
  OP_RENAME                     ((byte) 15), // filecontext rename
  OP_CONCAT_DELETE              ((byte) 16), // concat files
  OP_SYMLINK                    ((byte) 17),
  OP_GET_DELEGATION_TOKEN       ((byte) 18),
  OP_RENEW_DELEGATION_TOKEN     ((byte) 19),
  OP_CANCEL_DELEGATION_TOKEN    ((byte) 20),
  OP_UPDATE_MASTER_KEY          ((byte) 21),
  OP_REASSIGN_LEASE             ((byte) 22),
  OP_END_LOG_SEGMENT            ((byte) 23),
  OP_START_LOG_SEGMENT          ((byte) 24),
  OP_UPDATE_BLOCKS              ((byte) 25),
  OP_CREATE_SNAPSHOT            ((byte) 26),
  OP_DELETE_SNAPSHOT            ((byte) 27),
  OP_RENAME_SNAPSHOT            ((byte) 28),
  OP_ALLOW_SNAPSHOT             ((byte) 29),
  OP_DISALLOW_SNAPSHOT          ((byte) 30),
  OP_SET_GENSTAMP_V2            ((byte) 31),
  OP_ALLOCATE_BLOCK_ID          ((byte) 32);
  private byte opCode;

  /**
   * Constructor
   *
   * @param opCode byte value of constructed enum
   */
  FSEditLogOpCodes(byte opCode) {
    this.opCode = opCode;
  }

  /**
   * return the byte value of the enum
   *
   * @return the byte value of the enum
   */
  public byte getOpCode() {
    return opCode;
  }

  private static final Map<Byte, FSEditLogOpCodes> byteToEnum =
    new HashMap<Byte, FSEditLogOpCodes>();

  static {
    // initialize byte to enum map
    for(FSEditLogOpCodes opCode : values())
      byteToEnum.put(opCode.getOpCode(), opCode);
  }

  /**
   * Converts byte to FSEditLogOpCodes enum value
   *
   * @param opCode get enum for this opCode
   * @return enum with byte value of opCode
   */
  public static FSEditLogOpCodes fromByte(byte opCode) {
    return byteToEnum.get(opCode);
  }
}
