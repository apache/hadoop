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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields.PermissionStatusFormat;
import org.apache.hadoop.hdfs.server.namenode.XAttrFeature;

/**
 * The attributes of an inode.
 */
@InterfaceAudience.Private
public interface INodeAttributes {
  /**
   * @return null if the local name is null;
   *         otherwise, return the local name byte array.
   */
  public byte[] getLocalNameBytes();

  /** @return the user name. */
  public String getUserName();

  /** @return the group name. */
  public String getGroupName();
  
  /** @return the permission. */
  public FsPermission getFsPermission();

  /** @return the permission as a short. */
  public short getFsPermissionShort();
  
  /** @return the permission information as a long. */
  public long getPermissionLong();

  /** @return the ACL feature. */
  public AclFeature getAclFeature();
  
  /** @return the XAttrs feature. */
  public XAttrFeature getXAttrFeature();

  /** @return the modification time. */
  public long getModificationTime();

  /** @return the access time. */
  public long getAccessTime();

  /** A read-only copy of the inode attributes. */
  public static abstract class SnapshotCopy implements INodeAttributes {
    private final byte[] name;
    private final long permission;
    private final AclFeature aclFeature;
    private final long modificationTime;
    private final long accessTime;
    private XAttrFeature xAttrFeature;

    SnapshotCopy(byte[] name, PermissionStatus permissions,
        AclFeature aclFeature, long modificationTime, long accessTime, 
        XAttrFeature xAttrFeature) {
      this.name = name;
      this.permission = PermissionStatusFormat.toLong(permissions);
      this.aclFeature = aclFeature;
      this.modificationTime = modificationTime;
      this.accessTime = accessTime;
      this.xAttrFeature = xAttrFeature;
    }

    SnapshotCopy(INode inode) {
      this.name = inode.getLocalNameBytes();
      this.permission = inode.getPermissionLong();
      this.aclFeature = inode.getAclFeature();
      this.modificationTime = inode.getModificationTime();
      this.accessTime = inode.getAccessTime();
      this.xAttrFeature = inode.getXAttrFeature();
    }

    @Override
    public final byte[] getLocalNameBytes() {
      return name;
    }

    @Override
    public final String getUserName() {
      return PermissionStatusFormat.getUser(permission);
    }

    @Override
    public final String getGroupName() {
      return PermissionStatusFormat.getGroup(permission);
    }

    @Override
    public final FsPermission getFsPermission() {
      return new FsPermission(getFsPermissionShort());
    }

    @Override
    public final short getFsPermissionShort() {
      return PermissionStatusFormat.getMode(permission);
    }
    
    @Override
    public long getPermissionLong() {
      return permission;
    }

    @Override
    public AclFeature getAclFeature() {
      return aclFeature;
    }

    @Override
    public final long getModificationTime() {
      return modificationTime;
    }

    @Override
    public final long getAccessTime() {
      return accessTime;
    }
    
    @Override
    public final XAttrFeature getXAttrFeature() {
      return xAttrFeature;
    }
  }
}
