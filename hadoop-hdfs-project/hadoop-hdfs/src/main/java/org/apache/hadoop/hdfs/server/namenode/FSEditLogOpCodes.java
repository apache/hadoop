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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Op codes for edits file
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum FSEditLogOpCodes {
  // last op code in file
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
  OP_ALLOCATE_BLOCK_ID          ((byte) 32),
  OP_ADD_BLOCK                  ((byte) 33),
  OP_ADD_CACHE_DIRECTIVE       ((byte) 34),
  OP_REMOVE_CACHE_DIRECTIVE    ((byte) 35),
  OP_ADD_CACHE_POOL                       ((byte) 36),
  OP_MODIFY_CACHE_POOL                    ((byte) 37),
  OP_REMOVE_CACHE_POOL                    ((byte) 38),
  OP_MODIFY_CACHE_DIRECTIVE     ((byte) 39),
  OP_SET_ACL                    ((byte) 40),
  OP_ROLLING_UPGRADE_START      ((byte) 41),
  OP_ROLLING_UPGRADE_FINALIZE   ((byte) 42),
  OP_SET_XATTR                  ((byte) 43),
  OP_REMOVE_XATTR               ((byte) 44),
  OP_SET_STORAGE_POLICY         ((byte) 45),
  OP_TRUNCATE                   ((byte) 46),
  OP_APPEND                     ((byte) 47),
  OP_SET_QUOTA_BY_STORAGETYPE   ((byte) 48),

  // Note that the current range of the valid OP code is 0~127
  OP_INVALID                    ((byte) -1);

  private final byte opCode;

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

  private static final FSEditLogOpCodes[] VALUES;
  
  static {
    byte max = 0;
    for (FSEditLogOpCodes code : FSEditLogOpCodes.values()) {
      if (code.getOpCode() > max) {
        max = code.getOpCode();
      }
    }
    VALUES = new FSEditLogOpCodes[max + 1];
    for (FSEditLogOpCodes code : FSEditLogOpCodes.values()) {
      if (code.getOpCode() >= 0) {
        VALUES[code.getOpCode()] = code;
      }
    }
  }

  /**
   * Converts byte to FSEditLogOpCodes enum value
   *
   * @param opCode get enum for this opCode
   * @return enum with byte value of opCode
   */
  public static FSEditLogOpCodes fromByte(byte opCode) {
    if (opCode >= 0 && opCode < VALUES.length) {
      return VALUES[opCode];
    }
    return opCode == -1 ? OP_INVALID : null;
  }
}
