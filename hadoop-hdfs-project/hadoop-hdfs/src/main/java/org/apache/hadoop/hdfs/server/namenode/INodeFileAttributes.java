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
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.namenode.INodeFile.HeaderFormat;

/**
 * The attributes of a file.
 */
@InterfaceAudience.Private
public interface INodeFileAttributes extends INodeAttributes {
  /** @return the file replication. */
  short getFileReplication();

  /** @return whether the file is striped (instead of contiguous) */
  boolean isStriped();

  /** @return whether the file is striped (instead of contiguous) */
  BlockType getBlockType();

  /** @return the ID of the ErasureCodingPolicy */
  byte getErasureCodingPolicyID();

  /** @return preferred block size in bytes */
  long getPreferredBlockSize();

  /** @return the header as a long. */
  long getHeaderLong();

  boolean metadataEquals(INodeFileAttributes other);

  byte getLocalStoragePolicyID();

  /** A copy of the inode file attributes */
  static class SnapshotCopy extends INodeAttributes.SnapshotCopy
      implements INodeFileAttributes {
    private final long header;

    public SnapshotCopy(byte[] name, PermissionStatus permissions,
        AclFeature aclFeature, long modificationTime, long accessTime,
        Short replication, Byte ecPolicyID, long preferredBlockSize,
        byte storagePolicyID, XAttrFeature xAttrsFeature, BlockType blockType) {
      super(name, permissions, aclFeature, modificationTime, accessTime, 
          xAttrsFeature);
      final long layoutRedundancy = HeaderFormat.getBlockLayoutRedundancy(
          blockType, replication, ecPolicyID);
      header = HeaderFormat.toLong(preferredBlockSize, layoutRedundancy,
          storagePolicyID);
    }

    public SnapshotCopy(INodeFile file) {
      super(file);
      this.header = file.getHeaderLong();
    }

    @Override
    public boolean isDirectory() {
      return false;
    }

    @Override
    public short getFileReplication() {
      return HeaderFormat.getReplication(header);
    }

    @Override
    public boolean isStriped() {
      return HeaderFormat.isStriped(header);
    }

    @Override
    public BlockType getBlockType() {
      return HeaderFormat.getBlockType(header);
    }

    @Override
    public byte getErasureCodingPolicyID() {
      if (isStriped()) {
        return HeaderFormat.getECPolicyID(header);
      }
      return -1;
    }

    @Override
    public long getPreferredBlockSize() {
      return HeaderFormat.getPreferredBlockSize(header);
    }

    @Override
    public byte getLocalStoragePolicyID() {
      return HeaderFormat.getStoragePolicyID(header);
    }

    @Override
    public long getHeaderLong() {
      return header;
    }

    @Override
    public boolean metadataEquals(INodeFileAttributes other) {
      return other != null
          && getHeaderLong()== other.getHeaderLong()
          && getPermissionLong() == other.getPermissionLong()
          && getAclFeature() == other.getAclFeature()
          && getXAttrFeature() == other.getXAttrFeature();
    }
  }
}
