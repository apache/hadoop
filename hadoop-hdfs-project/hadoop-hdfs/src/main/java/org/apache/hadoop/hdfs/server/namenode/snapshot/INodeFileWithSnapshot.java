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
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeMap;
import org.apache.hadoop.hdfs.server.namenode.Quota;

/**
 * Represent an {@link INodeFile} that is snapshotted.
 */
@InterfaceAudience.Private
public class INodeFileWithSnapshot extends INodeFile
    implements FileWithSnapshot {
  private final FileDiffList diffs;
  private boolean isCurrentFileDeleted = false;

  public INodeFileWithSnapshot(INodeFile f) {
    this(f, f instanceof FileWithSnapshot?
        ((FileWithSnapshot)f).getDiffs(): null);
  }

  public INodeFileWithSnapshot(INodeFile f, FileDiffList diffs) {
    super(f);
    this.diffs = diffs != null? diffs: new FileDiffList();
  }

  @Override
  public INodeFileUnderConstructionWithSnapshot toUnderConstruction(
      final String clientName,
      final String clientMachine,
      final DatanodeDescriptor clientNode) {
    return new INodeFileUnderConstructionWithSnapshot(this,
        clientName, clientMachine, clientNode, getDiffs());
  }

  @Override
  public boolean isCurrentFileDeleted() {
    return isCurrentFileDeleted;
  }
  
  @Override
  public void deleteCurrentFile() {
    isCurrentFileDeleted = true;
  }

  @Override
  public INodeFileAttributes getSnapshotINode(Snapshot snapshot) {
    return diffs.getSnapshotINode(snapshot, this);
  }

  @Override
  public INodeFileWithSnapshot recordModification(final Snapshot latest,
      final INodeMap inodeMap) throws QuotaExceededException {
    if (isInLatestSnapshot(latest) && !shouldRecordInSrcSnapshot(latest)) {
      diffs.saveSelf2Snapshot(latest, this, null);
    }
    return this;
  }

  @Override
  public INodeFile asINodeFile() {
    return this;
  }

  @Override
  public FileDiffList getDiffs() {
    return diffs;
  }

  @Override
  public Quota.Counts cleanSubtree(final Snapshot snapshot, Snapshot prior,
      final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, final boolean countDiffChange) 
      throws QuotaExceededException {
    if (snapshot == null) { // delete the current file
      recordModification(prior, null);
      isCurrentFileDeleted = true;
      Util.collectBlocksAndClear(this, collectedBlocks, removedINodes);
      return Quota.Counts.newInstance();
    } else { // delete a snapshot
      prior = getDiffs().updatePrior(snapshot, prior);
      return diffs.deleteSnapshotDiff(snapshot, prior, this, collectedBlocks,
          removedINodes, countDiffChange);
    }
  }

  @Override
  public String toDetailString() {
    return super.toDetailString()
        + (isCurrentFileDeleted()? "(DELETED), ": ", ") + diffs;
  }
}
