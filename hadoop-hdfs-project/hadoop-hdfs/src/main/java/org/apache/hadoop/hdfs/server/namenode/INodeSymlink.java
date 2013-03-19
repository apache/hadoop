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
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.Content.CountsMap.Key;
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
  INode recordModification(Snapshot latest) throws QuotaExceededException {
    return isInLatestSnapshot(latest)?
        getParent().saveChild2Snapshot(this, latest, new INodeSymlink(this))
        : this;
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
  public Quota.Counts cleanSubtree(final Snapshot snapshot, Snapshot prior,
      final BlocksMapUpdateInfo collectedBlocks) {
    return Quota.Counts.newInstance();
  }
  
  @Override
  public void destroyAndCollectBlocks(
      final BlocksMapUpdateInfo collectedBlocks) {
    // do nothing
  }

  @Override
  public Quota.Counts computeQuotaUsage(Quota.Counts counts,
      boolean updateCache) {
    counts.add(Quota.NAMESPACE, 1);
    return counts;
  }

  @Override
  public Content.CountsMap computeContentSummary(
      final Content.CountsMap countsMap) {
    computeContentSummary(countsMap.getCounts(Key.CURRENT));
    return countsMap;
  }

  @Override
  public Content.Counts computeContentSummary(final Content.Counts counts) {
    counts.add(Content.SYMLINK, 1);
    return counts;
  }

  @Override
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix,
      final Snapshot snapshot) {
    super.dumpTreeRecursively(out, prefix, snapshot);
    out.println();
  }
}
