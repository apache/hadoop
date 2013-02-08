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

/**
 * Represent an {@link INodeFile} that is snapshotted.
 * Note that snapshot files are represented by {@link INodeFileSnapshot}.
 */
@InterfaceAudience.Private
public class INodeFileWithSnapshot extends INodeFile
    implements FileWithSnapshot {
  /**
   * A list of file diffs.
   */
  static class FileDiffList extends AbstractINodeDiffList<INodeFile, FileDiff> {
    final INodeFile currentINode;

    FileDiffList(INodeFile currentINode, List<FileDiff> diffs) {
      super(diffs);
      this.currentINode = currentINode;
    }

    @Override
    INodeFile getCurrentINode() {
      return currentINode;
    }

    @Override
    FileDiff addSnapshotDiff(Snapshot snapshot) {
      return addLast(new FileDiff(snapshot, getCurrentINode()));
    }
  }

  private final FileDiffList diffs;
  private FileWithSnapshot next;

  public INodeFileWithSnapshot(INodeFile f, FileDiffList diffs) {
    super(f);
    this.diffs = new FileDiffList(this, diffs == null? null: diffs.asList());
    setNext(this);
  }

  @Override
  public INodeFileUnderConstructionWithSnapshot toUnderConstruction(
      final String clientName,
      final String clientMachine,
      final DatanodeDescriptor clientNode) {
    final INodeFileUnderConstructionWithSnapshot f
        = new INodeFileUnderConstructionWithSnapshot(this,
            clientName, clientMachine, clientNode, diffs);
    this.insertBefore(f);
    return f;
  }

  @Override
  public boolean isCurrentFileDeleted() {
    return getParent() == null;
  }

  @Override
  public boolean isEverythingDeleted() {
    return isCurrentFileDeleted() && diffs.asList().isEmpty();
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
  public FileWithSnapshot getNext() {
    return next;
  }

  @Override
  public void setNext(FileWithSnapshot next) {
    this.next = next;
  }

  @Override
  public void insertAfter(FileWithSnapshot inode) {
    inode.setNext(this.getNext());
    this.setNext(inode);
  }
  
  @Override
  public void insertBefore(FileWithSnapshot inode) {
    inode.setNext(this);
    if (this.next == null || this.next == this) {
      this.next = inode;
      return;
    }
    FileWithSnapshot previous = Util.getPrevious(this);
    previous.setNext(inode);
  }

  @Override
  public void removeSelf() {
    if (this.next != null && this.next != this) {
      FileWithSnapshot previous = Util.getPrevious(this);
      previous.setNext(next);
    }
    this.next = null;
  }

  @Override
  public short getFileReplication(Snapshot snapshot) {
    final INodeFile inode = diffs.getSnapshotINode(snapshot);
    return inode != null? inode.getFileReplication()
        : super.getFileReplication(null);
  }

  @Override
  public short getMaxFileReplication() {
    final short max = isCurrentFileDeleted()? 0: getFileReplication();
    return Util.getMaxFileReplication(max, diffs);
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
  public long computeMaxFileSize() {
    if (isCurrentFileDeleted()) {
      final FileDiff last = diffs.getLast();
      return last == null? 0: last.fileSize;
    } else { 
      return super.computeFileSize(true, null);
    }
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
