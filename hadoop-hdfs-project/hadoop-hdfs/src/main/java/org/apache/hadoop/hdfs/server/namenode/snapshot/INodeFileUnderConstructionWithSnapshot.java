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
  private FileWithSnapshot next;

  INodeFileUnderConstructionWithSnapshot(final INodeFile f,
      final String clientName,
      final String clientMachine,
      final DatanodeDescriptor clientNode) {
    super(f, clientName, clientMachine, clientNode);
    next = this;
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
    // link f with this
    this.insertBefore(f);
    return f;
  }

  @Override
  public Pair<? extends INodeFileUnderConstruction,
      INodeFileUnderConstructionSnapshot> createSnapshotCopy() {
    return new Pair<INodeFileUnderConstructionWithSnapshot,
        INodeFileUnderConstructionSnapshot>(
            this, new INodeFileUnderConstructionSnapshot(this));
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
  public short getBlockReplication() {
    return Util.getBlockReplication(this);
  }

  @Override
  public int destroySubtreeAndCollectBlocks(final Snapshot snapshot,
      final BlocksMapUpdateInfo collectedBlocks) {
    if (snapshot != null) {
      return 0;
    }
    if (next == null || next == this) {
      // this is the only remaining inode.
      return super.destroySubtreeAndCollectBlocks(null, collectedBlocks);
    } else {
      return Util.collectSubtreeBlocksAndClear(this, collectedBlocks);
    }
  }
}
