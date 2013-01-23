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

/**
 * Represent an {@link INodeFile} that is snapshotted.
 * Note that snapshot files are represented by {@link INodeFileSnapshot}.
 */
@InterfaceAudience.Private
public class INodeFileWithSnapshot extends INodeFile
    implements FileWithSnapshot {
  private FileWithSnapshot next;

  public INodeFileWithSnapshot(INodeFile f) {
    super(f);
    setNext(this);
  }

  @Override
  public INodeFileUnderConstructionWithSnapshot toUnderConstruction(
      final String clientName,
      final String clientMachine,
      final DatanodeDescriptor clientNode) {
    final INodeFileUnderConstructionWithSnapshot f
        = new INodeFileUnderConstructionWithSnapshot(this,
            clientName, clientMachine, clientNode);
    Util.replace(this, f);
    return f;
  }

  @Override
  public Pair<INodeFileWithSnapshot, INodeFileSnapshot> createSnapshotCopy() {
    return new Pair<INodeFileWithSnapshot, INodeFileSnapshot>(this,
        new INodeFileSnapshot(this));
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
  public int collectSubtreeBlocksAndClear(BlocksMapUpdateInfo info) {
    if (next == null || next == this) {
      // this is the only remaining inode.
      return super.collectSubtreeBlocksAndClear(info);
    } else {
      return Util.collectSubtreeBlocksAndClear(this, info);
    }
  }
}
