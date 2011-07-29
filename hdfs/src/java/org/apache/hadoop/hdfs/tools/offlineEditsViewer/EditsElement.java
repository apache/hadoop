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
package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Structural elements of an EditLog that may be encountered within the
 * file. EditsVisitor is able to process these elements.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum EditsElement {
  EDITS,
  EDITS_VERSION,
  RECORD,
  OPCODE,
  TRANSACTION_ID,
  DATA,
    // elements in the data part of the editLog records
    LENGTH,
    // for OP_SET_GENSTAMP
    GENERATION_STAMP,
    // for OP_ADD, OP_CLOSE
    PATH,
    REPLICATION,
    MTIME,
    ATIME,
    BLOCKSIZE,
    NUMBLOCKS,
    BLOCK,
      BLOCK_ID,
      BLOCK_NUM_BYTES,
      BLOCK_GENERATION_STAMP,
    PERMISSION_STATUS,
      FS_PERMISSIONS,
    CLIENT_NAME,
    CLIENT_MACHINE,
    // for OP_RENAME_OLD
    SOURCE,
    DESTINATION,
    TIMESTAMP,
    // for OP_SET_OWNER
    USERNAME,
    GROUPNAME,
    // for OP_SET_QUOTA
    NS_QUOTA,
    DS_QUOTA,
    // for OP_RENAME
    RENAME_OPTIONS,
    // for OP_CONCAT_DELETE
    CONCAT_TARGET,
    CONCAT_SOURCE,
    // for OP_GET_DELEGATION_TOKEN
    T_VERSION,
    T_OWNER,
    T_RENEWER,
    T_REAL_USER,
    T_ISSUE_DATE,
    T_MAX_DATE,
    T_SEQUENCE_NUMBER,
    T_MASTER_KEY_ID,
    T_EXPIRY_TIME,
    // for OP_UPDATE_MASTER_KEY
    KEY_ID,
    KEY_EXPIRY_DATE,
    KEY_LENGTH,
    KEY_BLOB,
    CHECKSUM
}
