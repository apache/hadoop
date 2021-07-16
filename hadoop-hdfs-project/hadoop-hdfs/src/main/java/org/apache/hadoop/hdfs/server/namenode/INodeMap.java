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

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LatchLock;
import org.apache.hadoop.util.LightWeightGSet;
import org.apache.hadoop.util.PartitionedGSet;

/**
 * Storing all the {@link INode}s and maintaining the mapping between INode ID
 * and INode.  
 */
public class INodeMap {
  static final int NAMESPACE_KEY_DEBTH = 2;
  static final int NUM_RANGES_STATIC = 256;  // power of 2

  public static class INodeKeyComparator implements Comparator<INode> {
    INodeKeyComparator() {
      FSDirectory.LOG.info("Namespace key debth = {}", NAMESPACE_KEY_DEBTH);
    }

    @Override
    public int compare(INode i1, INode i2) {
      if (i1 == null || i2 == null) {
        throw new NullPointerException("Cannot compare null INodes");
      }
      long[] key1 = i1.getNamespaceKey(NAMESPACE_KEY_DEBTH);
      long[] key2 = i2.getNamespaceKey(NAMESPACE_KEY_DEBTH);
      for(int l = 0; l < NAMESPACE_KEY_DEBTH; l++) {
        if(key1[l] == key2[l]) continue;
        return (key1[l] < key2[l] ? -1 : 1);
      }
      return 0;
    }
  }

  /**
   * INodeKeyComparator with Hashed Parent
   *
   */
  public static class HPINodeKeyComparator implements Comparator<INode> {
    HPINodeKeyComparator() {
      FSDirectory.LOG.info("Namespace key debth = {}", NAMESPACE_KEY_DEBTH);
    }

    @Override
    public int compare(INode i1, INode i2) {
      if (i1 == null || i2 == null) {
        throw new NullPointerException("Cannot compare null INodes");
      }
      long[] key1 = i1.getNamespaceKey(NAMESPACE_KEY_DEBTH);
      long[] key2 = i2.getNamespaceKey(NAMESPACE_KEY_DEBTH);
      long key1_0 = INode.indexOf(key1);
      long key2_0 = INode.indexOf(key2);
      if(key1_0 != key2_0)
        return (key1_0 < key2_0 ? -1 : 1);
      for(int l = 1; l < NAMESPACE_KEY_DEBTH; l++) {
        if(key1[l] == key2[l]) continue;
        return (key1[l] < key2[l] ? -1 : 1);
      }
      return 0;
    }
  }

  public static class INodeIdComparator implements Comparator<INode> {
    @Override
    public int compare(INode i1, INode i2) {
      if (i1 == null || i2 == null) {
        throw new NullPointerException("Cannot compare null INodesl");
      }
      long id1 = i1.getId();
      long id2 = i2.getId();
      return id1 < id2 ? -1 : id1 == id2 ? 0 : 1;
    }
  }

  public class INodeMapLock extends LatchLock<ReentrantReadWriteLock> {
    private ReentrantReadWriteLock childLock;

    INodeMapLock() {
      this(null);
    }

    private INodeMapLock(ReentrantReadWriteLock childLock) {
      assert namesystem != null : "namesystem is null";
      this.childLock = childLock;
    }

    @Override
    protected boolean isReadTopLocked() {
      return namesystem.getFSLock().isReadLocked();
    }

    @Override
    protected boolean isWriteTopLocked() {
      return namesystem.getFSLock().isWriteLocked();
    }

    @Override
    protected void readTopUnlock() {
      namesystem.getFSLock().readUnlock("INodeMap", null, false);
    }

    @Override
    protected void writeTopUnlock() {
      namesystem.getFSLock().writeUnlock("INodeMap", false, null, false);
    }

    @Override
    protected boolean hasReadChildLock() {
      return this.childLock.getReadHoldCount() > 0 || hasWriteChildLock();
    }

    @Override
    protected void readChildLock() {
      // LOG.info("readChildLock: thread = {}, {}", Thread.currentThread().getId(), Thread.currentThread().getName());
      this.childLock.readLock().lock();
      namesystem.getFSLock().addChildLock(this);
      // LOG.info("readChildLock: done");
    }

    @Override
    protected void readChildUnlock() {
      // LOG.info("readChildUnlock: thread = {}, {}", Thread.currentThread().getId(), Thread.currentThread().getName());
      this.childLock.readLock().unlock();
      // LOG.info("readChildUnlock: done");
    }

    @Override
    protected boolean hasWriteChildLock() {
      return this.childLock.isWriteLockedByCurrentThread();
    }

    @Override
    protected void writeChildLock() {
      // LOG.info("writeChildLock: thread = {}, {}", Thread.currentThread().getId(), Thread.currentThread().getName());
      this.childLock.writeLock().lock();
      namesystem.getFSLock().addChildLock(this);
      // LOG.info("writeChildLock: done");
    }

    @Override
    protected void writeChildUnlock() {
      // LOG.info("writeChildUnlock: thread = {}, {}", Thread.currentThread().getId(), Thread.currentThread().getName());
      this.childLock.writeLock().unlock();
      // LOG.info("writeChildUnlock: done");
    }

    @Override
    protected LatchLock<ReentrantReadWriteLock> clone() {
      return new INodeMapLock(new ReentrantReadWriteLock(false)); // not fair
    }
  }

  static INodeMap newInstance(INodeDirectory rootDir,
      FSNamesystem ns) {
    return new INodeMap(rootDir, ns);
  }

  /** Synchronized by external lock. */
  private final GSet<INode, INodeWithAdditionalFields> map;
  private FSNamesystem namesystem;

  public Iterator<INodeWithAdditionalFields> getMapIterator() {
    return map.iterator();
  }

  private INodeMap(INodeDirectory rootDir, FSNamesystem ns) {
    this.namesystem = ns;
    // Compute the map capacity by allocating 1% of total memory
    int capacity = LightWeightGSet.computeCapacity(1, "INodeMap");
    this.map = new PartitionedGSet<>(capacity, new INodeKeyComparator(),
            new INodeMapLock());

    // Pre-populate initial empty partitions
    PartitionedGSet<INode, INodeWithAdditionalFields> pgs =
        (PartitionedGSet<INode, INodeWithAdditionalFields>) map;
    PermissionStatus perm = new PermissionStatus(
        "", "", new FsPermission((short) 0));
    for(int p = 0; p < NUM_RANGES_STATIC; p++) {
      INodeDirectory key = new INodeDirectory(
          INodeId.ROOT_INODE_ID, "range key".getBytes(), perm, 0);
      key.setParent(new INodeDirectory((long)p, null, perm, 0));
      pgs.addNewPartition(key);
    }

    map.put(rootDir);
  }

  /**
   * Add an {@link INode} into the {@link INode} map. Replace the old value if 
   * necessary. 
   * @param inode The {@link INode} to be added to the map.
   */
  public final void put(INode inode) {
    if (inode instanceof INodeWithAdditionalFields) {
      map.put((INodeWithAdditionalFields)inode);
    }
  }
  
  /**
   * Remove a {@link INode} from the map.
   * @param inode The {@link INode} to be removed.
   */
  public final void remove(INode inode) {
    map.remove(inode);
  }
  
  /**
   * @return The size of the map.
   */
  public int size() {
    return map.size();
  }
  
  /**
   * Get the {@link INode} with the given id from the map.
   * @param id ID of the {@link INode}.
   * @return The {@link INode} in the map with the given id. Return null if no 
   *         such {@link INode} in the map.
   */
  public INode get(long id) {
    INode inode = new INodeWithAdditionalFields(id, null, new PermissionStatus(
        "", "", new FsPermission((short) 0)), 0, 0) {
      
      @Override
      void recordModification(int latestSnapshotId) {
      }
      
      @Override
      public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
        // Nothing to do
      }

      @Override
      public QuotaCounts computeQuotaUsage(
          BlockStoragePolicySuite bsps, byte blockStoragePolicyId,
          boolean useCache, int lastSnapshotId) {
        return null;
      }

      @Override
      public ContentSummaryComputationContext computeContentSummary(
          int snapshotId, ContentSummaryComputationContext summary) {
        return null;
      }
      
      @Override
      public void cleanSubtree(
          ReclaimContext reclaimContext, int snapshotId, int priorSnapshotId) {
      }

      @Override
      public byte getStoragePolicyID(){
        return HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      }

      @Override
      public byte getLocalStoragePolicyID() {
        return HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      }
    };
      
    return map.get(inode);
  }
  
  /**
   * Clear the {@link #map}
   */
  public void clear() {
    map.clear();
  }

  public void latchWriteLock(INodesInPath iip, INode[] missing) {
    assert namesystem.hasReadLock() : "must have namesysem lock";
    assert iip.length() > 0 : "INodesInPath has 0 length";
    if(!(map instanceof PartitionedGSet)) {
      return;
    }
    // Locks partitions along the path starting from the first existing parent
    // Locking is in the hierarchical order
    INode[] allINodes = new INode[Math.min(1, iip.length()) + missing.length];
    allINodes[0] = iip.getLastINode();
    System.arraycopy(missing, 0, allINodes, 1, missing.length);
    /*
    // Locks all the partitions along the path in the hierarchical order
    INode[] allINodes = new INode[iip.length() + missing.length];
    INode[] existing = iip.getINodesArray();
    System.arraycopy(existing, 0, allINodes, 0, existing.length);
    System.arraycopy(missing, 0, allINodes, existing.length, missing.length);
    */

    ((PartitionedGSet<INode, INodeWithAdditionalFields>)
        map).latchWriteLock(allINodes);
  }
}
