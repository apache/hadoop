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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * Represent an {@link INodeFile} that is snapshotted.
 * Note that snapshot files are represented by {@link INodeFileSnapshot}.
 */
@InterfaceAudience.Private
public class INodeFileWithSnapshot extends INodeFile
    implements FileWithSnapshot {
  private final FileDiffList diffs;

  public INodeFileWithSnapshot(INodeFile f) {
    super(f);
    this.diffs = new FileDiffList(this, f instanceof FileWithSnapshot?
        ((FileWithSnapshot)f).getFileDiffList().asList(): null);
  }

  @Override
  public INodeFileUnderConstructionWithSnapshot toUnderConstruction(
      final String clientName,
      final String clientMachine,
      final DatanodeDescriptor clientNode) {
    return new INodeFileUnderConstructionWithSnapshot(this,
        clientName, clientMachine, clientNode);
  }

  @Override
  public boolean isCurrentFileDeleted() {
    return getParent() == null;
  }

  @Override
  public INodeFileWithSnapshot recordModification(final Snapshot latest) {
    // if this object is NOT the latest snapshot copy, this object is created
    // after the latest snapshot, then do NOT record modification.
    if (this == getParent().getChild(getLocalNameBytes(), latest)) {
      diffs.saveSelf2Snapshot(latest, null);
    }
    return this;
  }

  @Override
  public INodeFile asINodeFile() {
    return this;
  }

  @Override
  public FileDiffList getFileDiffList() {
    return diffs;
  }

  @Override
  public short getFileReplication(Snapshot snapshot) {
    final INodeFile inode = diffs.getSnapshotINode(snapshot);
    return inode != null? inode.getFileReplication()
        : super.getFileReplication(null);
  }

  @Override
  public short getBlockReplication() {
    return Util.getBlockReplication(this);
  }

  @Override
  public long computeFileSize(boolean includesBlockInfoUnderConstruction,
      Snapshot snapshot) {
    final FileDiff diff = diffs.getDiff(snapshot);
    return diff != null? diff.fileSize
        : super.computeFileSize(includesBlockInfoUnderConstruction, null);
  }

  @Override
  public int destroySubtreeAndCollectBlocks(final Snapshot snapshot,
      final BlocksMapUpdateInfo collectedBlocks) {
    if (snapshot == null) {
      clearReferences();
    } else {
      if (diffs.deleteSnapshotDiff(snapshot, collectedBlocks) == null) {
        //snapshot diff not found and nothing is deleted.
        return 0;
      }
    }

    Util.collectBlocksAndClear(this, collectedBlocks);
    return 1;
  }

  @Override
  public String getUserName(Snapshot snapshot) {
    final INodeFile inode = diffs.getSnapshotINode(snapshot);
    return inode != null? inode.getUserName(): super.getUserName(null);
  }

  @Override
  public String getGroupName(Snapshot snapshot) {
    final INodeFile inode = diffs.getSnapshotINode(snapshot);
    return inode != null? inode.getGroupName(): super.getGroupName(null);
  }

  @Override
  public FsPermission getFsPermission(Snapshot snapshot) {
    final INodeFile inode = diffs.getSnapshotINode(snapshot);
    return inode != null? inode.getFsPermission(): super.getFsPermission(null);
  }

  @Override
  public long getAccessTime(Snapshot snapshot) {
    final INodeFile inode = diffs.getSnapshotINode(snapshot);
    return inode != null? inode.getAccessTime(): super.getAccessTime(null);
  }

  @Override
  public long getModificationTime(Snapshot snapshot) {
    final INodeFile inode = diffs.getSnapshotINode(snapshot);
    return inode != null? inode.getModificationTime()
        : super.getModificationTime(null);
  }

  @Override
  public String toDetailString() {
    return super.toDetailString()
        + (isCurrentFileDeleted()? "(DELETED), ": ", ") + diffs;
  }
}
