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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileUnderConstruction;

/**
 * Represent an {@link INodeFileUnderConstruction} that is snapshotted.
 * Note that snapshot files are represented by
 * {@link INodeFileUnderConstructionSnapshot}.
 */
@InterfaceAudience.Private
public class INodeFileUnderConstructionWithSnapshot
    extends INodeFileUnderConstruction implements FileWithSnapshot {
  /**
   * The difference of an {@link INodeFileUnderConstruction} between two snapshots.
   */
  static class FileUcDiff extends FileDiff {
    private FileUcDiff(Snapshot snapshot, INodeFile file) {
      super(snapshot, file);
    }

    @Override
    INodeFileUnderConstruction createSnapshotCopyOfCurrentINode(INodeFile file) {
      final INodeFileUnderConstruction uc = (INodeFileUnderConstruction)file;
      final INodeFileUnderConstruction copy = new INodeFileUnderConstruction(
          uc, uc.getClientName(), uc.getClientMachine(), uc.getClientNode());
      copy.setBlocks(null);
      return copy;
    }
  }

  /**
   * A list of file diffs.
   */
  static class FileUcDiffList extends FileDiffList {
    private FileUcDiffList(INodeFile currentINode, final List<FileDiff> diffs) {
      super(currentINode, diffs);
    }

    @Override
    FileDiff addSnapshotDiff(Snapshot snapshot) {
      return addLast(new FileUcDiff(snapshot, getCurrentINode()));
    }
  }

  private final FileUcDiffList diffs;

  INodeFileUnderConstructionWithSnapshot(final INodeFile f,
      final String clientName,
      final String clientMachine,
      final DatanodeDescriptor clientNode) {
    super(f, clientName, clientMachine, clientNode);
    this.diffs = new FileUcDiffList(this, f instanceof FileWithSnapshot?
        ((FileWithSnapshot)f).getFileDiffList().asList(): null);
  }

  /**
   * Construct an {@link INodeFileUnderConstructionWithSnapshot} based on an
   * {@link INodeFileUnderConstruction}.
   * 
   * @param f The given {@link INodeFileUnderConstruction} instance
   */
  public INodeFileUnderConstructionWithSnapshot(INodeFileUnderConstruction f) {
    this(f, f.getClientName(), f.getClientMachine(), f.getClientNode());
  }
  
  @Override
  protected INodeFileWithSnapshot toINodeFile(final long mtime) {
    assertAllBlocksComplete();
    final long atime = getModificationTime();
    final INodeFileWithSnapshot f = new INodeFileWithSnapshot(this);
    f.setModificationTime(mtime, null);
    f.setAccessTime(atime, null);
    return f;
  }

  @Override
  public boolean isCurrentFileDeleted() {
    return getParent() == null;
  }

  @Override
  public INodeFileUnderConstructionWithSnapshot recordModification(
      final Snapshot latest) {
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
        + (isCurrentFileDeleted()? " (DELETED), ": ", ") + diffs;
  }
}
