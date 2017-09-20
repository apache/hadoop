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
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.*;

/**
 * Op codes for edits file
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum FSEditLogOpCodes {
  // last op code in file
  OP_ADD                        ((byte)  0, AddOp.class),
  // deprecated operation
  OP_RENAME_OLD                 ((byte)  1, RenameOldOp.class),
  OP_DELETE                     ((byte)  2, DeleteOp.class),
  OP_MKDIR                      ((byte)  3, MkdirOp.class),
  OP_SET_REPLICATION            ((byte)  4, SetReplicationOp.class),
  @Deprecated OP_DATANODE_ADD   ((byte)  5), // obsolete
  @Deprecated OP_DATANODE_REMOVE((byte)  6), // obsolete
  OP_SET_PERMISSIONS            ((byte)  7, SetPermissionsOp.class),
  OP_SET_OWNER                  ((byte)  8, SetOwnerOp.class),
  OP_CLOSE                      ((byte)  9, CloseOp.class),
  OP_SET_GENSTAMP_V1            ((byte) 10, SetGenstampV1Op.class),
  OP_SET_NS_QUOTA               ((byte) 11, SetNSQuotaOp.class), // obsolete
  OP_CLEAR_NS_QUOTA             ((byte) 12, ClearNSQuotaOp.class), // obsolete
  OP_TIMES                      ((byte) 13, TimesOp.class), // set atime, mtime
  OP_SET_QUOTA                  ((byte) 14, SetQuotaOp.class),
  // filecontext rename
  OP_RENAME                     ((byte) 15, RenameOp.class),
  // concat files
  OP_CONCAT_DELETE              ((byte) 16, ConcatDeleteOp.class),
  OP_SYMLINK                    ((byte) 17, SymlinkOp.class),
  OP_GET_DELEGATION_TOKEN       ((byte) 18, GetDelegationTokenOp.class),
  OP_RENEW_DELEGATION_TOKEN     ((byte) 19, RenewDelegationTokenOp.class),
  OP_CANCEL_DELEGATION_TOKEN    ((byte) 20, CancelDelegationTokenOp.class),
  OP_UPDATE_MASTER_KEY          ((byte) 21, UpdateMasterKeyOp.class),
  OP_REASSIGN_LEASE             ((byte) 22, ReassignLeaseOp.class),
  OP_END_LOG_SEGMENT            ((byte) 23, EndLogSegmentOp.class),
  OP_START_LOG_SEGMENT          ((byte) 24, StartLogSegmentOp.class),
  OP_UPDATE_BLOCKS              ((byte) 25, UpdateBlocksOp.class),
  OP_CREATE_SNAPSHOT            ((byte) 26, CreateSnapshotOp.class),
  OP_DELETE_SNAPSHOT            ((byte) 27, DeleteSnapshotOp.class),
  OP_RENAME_SNAPSHOT            ((byte) 28, RenameSnapshotOp.class),
  OP_ALLOW_SNAPSHOT             ((byte) 29, AllowSnapshotOp.class),
  OP_DISALLOW_SNAPSHOT          ((byte) 30, DisallowSnapshotOp.class),
  OP_SET_GENSTAMP_V2            ((byte) 31, SetGenstampV2Op.class),
  OP_ALLOCATE_BLOCK_ID          ((byte) 32, AllocateBlockIdOp.class),
  OP_ADD_BLOCK                  ((byte) 33, AddBlockOp.class),
  OP_ADD_CACHE_DIRECTIVE        ((byte) 34, AddCacheDirectiveInfoOp.class),
  OP_REMOVE_CACHE_DIRECTIVE     ((byte) 35, RemoveCacheDirectiveInfoOp.class),
  OP_ADD_CACHE_POOL             ((byte) 36, AddCachePoolOp.class),
  OP_MODIFY_CACHE_POOL          ((byte) 37, ModifyCachePoolOp.class),
  OP_REMOVE_CACHE_POOL          ((byte) 38, RemoveCachePoolOp.class),
  OP_MODIFY_CACHE_DIRECTIVE     ((byte) 39, ModifyCacheDirectiveInfoOp.class),
  OP_SET_ACL                    ((byte) 40, SetAclOp.class),
  OP_ROLLING_UPGRADE_START      ((byte) 41, RollingUpgradeStartOp.class),
  OP_ROLLING_UPGRADE_FINALIZE   ((byte) 42, RollingUpgradeFinalizeOp.class),
  OP_SET_XATTR                  ((byte) 43, SetXAttrOp.class),
  OP_REMOVE_XATTR               ((byte) 44, RemoveXAttrOp.class),
  OP_SET_STORAGE_POLICY         ((byte) 45, SetStoragePolicyOp.class),
  OP_TRUNCATE                   ((byte) 46, TruncateOp.class),
  OP_APPEND                     ((byte) 47, AppendOp.class),
  OP_SET_QUOTA_BY_STORAGETYPE   ((byte) 48, SetQuotaByStorageTypeOp.class),
  OP_ADD_ERASURE_CODING_POLICY  ((byte) 49, AddErasureCodingPolicyOp.class),
  OP_ENABLE_ERASURE_CODING_POLICY((byte) 50, EnableErasureCodingPolicyOp.class),
  OP_DISABLE_ERASURE_CODING_POLICY((byte) 51,
      DisableErasureCodingPolicyOp.class),
  OP_REMOVE_ERASURE_CODING_POLICY((byte) 52, RemoveErasureCodingPolicyOp.class),

  // Note that the current range of the valid OP code is 0~127
  OP_INVALID                    ((byte) -1);

  private final byte opCode;
  private final Class<? extends FSEditLogOp> opClass;

  /**
   * Constructor
   *
   * @param opCode byte value of constructed enum
   */
  FSEditLogOpCodes(byte opCode) {
    this(opCode, null);
  }

  FSEditLogOpCodes(byte opCode, Class<? extends FSEditLogOp> opClass) {
    this.opCode = opCode;
    this.opClass = opClass;
  }

  /**
   * return the byte value of the enum
   *
   * @return the byte value of the enum
   */
  public byte getOpCode() {
    return opCode;
  }

  public Class<? extends FSEditLogOp> getOpClass() {
    return opClass;
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
