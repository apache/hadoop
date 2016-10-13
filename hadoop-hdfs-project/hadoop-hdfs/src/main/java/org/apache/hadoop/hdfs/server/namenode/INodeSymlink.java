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

import java.io.PrintWriter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

/**
 * An {@link INode} representing a symbolic link.
 */
@InterfaceAudience.Private
public class INodeSymlink extends INodeWithAdditionalFields {
  private final byte[] symlink; // The target URI

  INodeSymlink(long id, byte[] name, PermissionStatus permissions,
      long mtime, long atime, String symlink) {
    super(id, name, permissions, mtime, atime);
    this.symlink = DFSUtil.string2Bytes(symlink);
  }
  
  INodeSymlink(INodeSymlink that) {
    super(that);
    this.symlink = that.symlink;
  }

  @Override
  void recordModification(int latestSnapshotId) {
    if (isInLatestSnapshot(latestSnapshotId)) {
      INodeDirectory parent = getParent();
      parent.saveChild2Snapshot(this, latestSnapshotId, new INodeSymlink(this));
    }
  }

  /** @return true unconditionally. */
  @Override
  public boolean isSymlink() {
    return true;
  }

  /** @return this object. */
  @Override
  public INodeSymlink asSymlink() {
    return this;
  }

  public String getSymlinkString() {
    return DFSUtil.bytes2String(symlink);
  }

  public byte[] getSymlink() {
    return symlink;
  }
  
  @Override
  public void cleanSubtree(ReclaimContext reclaimContext, final int snapshotId,
      int priorSnapshotId) {
    if (snapshotId == Snapshot.CURRENT_STATE_ID
        && priorSnapshotId == Snapshot.NO_SNAPSHOT_ID) {
      destroyAndCollectBlocks(reclaimContext);
    }
  }
  
  @Override
  public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
    reclaimContext.removedINodes.add(this);
    reclaimContext.quotaDelta().add(
        new QuotaCounts.Builder().nameSpace(1).build());
  }

  @Override
  public QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
      byte blockStoragePolicyId, boolean useCache, int lastSnapshotId) {
    return new QuotaCounts.Builder().nameSpace(1).build();
  }

  @Override
  public ContentSummaryComputationContext computeContentSummary(int snapshotId,
      final ContentSummaryComputationContext summary) {
    summary.nodeIncluded(this);
    summary.getCounts().addContent(Content.SYMLINK, 1);
    return summary;
  }

  @Override
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix,
      final int snapshot) {
    super.dumpTreeRecursively(out, prefix, snapshot);
    out.println();
  }

  @Override
  public void removeAclFeature() {
    throw new UnsupportedOperationException("ACLs are not supported on symlinks");
  }

  @Override
  public void addAclFeature(AclFeature f) {
    throw new UnsupportedOperationException("ACLs are not supported on symlinks");
  }

  @Override
  final XAttrFeature getXAttrFeature(int snapshotId) {
    throw new UnsupportedOperationException("XAttrs are not supported on symlinks");
  }
  
  @Override
  public void removeXAttrFeature() {
    throw new UnsupportedOperationException("XAttrs are not supported on symlinks");
  }
  
  @Override
  public void addXAttrFeature(XAttrFeature f) {
    throw new UnsupportedOperationException("XAttrs are not supported on symlinks");
  }

  @Override
  public byte getStoragePolicyID() {
    throw new UnsupportedOperationException(
        "Storage policy are not supported on symlinks");
  }

  @Override
  public byte getLocalStoragePolicyID() {
    throw new UnsupportedOperationException(
        "Storage policy are not supported on symlinks");
  }
}
