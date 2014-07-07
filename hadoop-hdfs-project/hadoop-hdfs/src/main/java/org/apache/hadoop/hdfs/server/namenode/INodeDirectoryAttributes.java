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
import org.apache.hadoop.hdfs.server.namenode.XAttrFeature;

import com.google.common.base.Preconditions;

/**
 * The attributes of an inode.
 */
@InterfaceAudience.Private
public interface INodeDirectoryAttributes extends INodeAttributes {
  public Quota.Counts getQuotaCounts();

  public boolean metadataEquals(INodeDirectoryAttributes other);
  
  /** A copy of the inode directory attributes */
  public static class SnapshotCopy extends INodeAttributes.SnapshotCopy
      implements INodeDirectoryAttributes {
    public SnapshotCopy(byte[] name, PermissionStatus permissions,
        AclFeature aclFeature, long modificationTime, 
        XAttrFeature xAttrsFeature) {
      super(name, permissions, aclFeature, modificationTime, 0L, xAttrsFeature);
    }

    public SnapshotCopy(INodeDirectory dir) {
      super(dir);
    }

    @Override
    public Quota.Counts getQuotaCounts() {
      return Quota.Counts.newInstance(-1, -1);
    }

    @Override
    public boolean metadataEquals(INodeDirectoryAttributes other) {
      return other != null
          && getQuotaCounts().equals(other.getQuotaCounts())
          && getPermissionLong() == other.getPermissionLong()
          && getAclFeature() == other.getAclFeature()
          && getXAttrFeature() == other.getXAttrFeature();
    }
  }

  public static class CopyWithQuota extends INodeDirectoryAttributes.SnapshotCopy {
    private final long nsQuota;
    private final long dsQuota;


    public CopyWithQuota(byte[] name, PermissionStatus permissions,
        AclFeature aclFeature, long modificationTime, long nsQuota,
        long dsQuota, XAttrFeature xAttrsFeature) {
      super(name, permissions, aclFeature, modificationTime, xAttrsFeature);
      this.nsQuota = nsQuota;
      this.dsQuota = dsQuota;
    }

    public CopyWithQuota(INodeDirectory dir) {
      super(dir);
      Preconditions.checkArgument(dir.isQuotaSet());
      final Quota.Counts q = dir.getQuotaCounts();
      this.nsQuota = q.get(Quota.NAMESPACE);
      this.dsQuota = q.get(Quota.DISKSPACE);
    }
    
    @Override
    public Quota.Counts getQuotaCounts() {
      return Quota.Counts.newInstance(nsQuota, dsQuota);
    }
  }
}
