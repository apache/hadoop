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

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
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
  private static int numSpaceKeyDepth;
  private static long numRanges;

  public static class INodeKeyComparator implements Comparator<INode> {
    INodeKeyComparator() {
      FSDirectory.LOG.info("Namespace key depth = {}", numSpaceKeyDepth);
    }

    @Override
    public int compare(INode i1, INode i2) {
      if (i1 == null || i2 == null) {
        throw new NullPointerException("Cannot compare null INodes");
      }
      long[] key1 = i1.getNamespaceKey(numSpaceKeyDepth);
      long[] key2 = i2.getNamespaceKey(numSpaceKeyDepth);
      for(int l = 0; l < numSpaceKeyDepth; l++) {
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
      FSDirectory.LOG.info("Namespace key depth = {}", numSpaceKeyDepth);
    }

    @Override
    public int compare(INode i1, INode i2) {
      if (i1 == null || i2 == null) {
        throw new NullPointerException("Cannot compare null INodes");
      }
      long[] key1 = i1.getNamespaceKey(numSpaceKeyDepth);
      long[] key2 = i2.getNamespaceKey(numSpaceKeyDepth);
      long key1_0 = INode.indexOf(key1);
      long key2_0 = INode.indexOf(key2);
      if(key1_0 != key2_0)
        return (key1_0 < key2_0 ? -1 : 1);
      for(int l = 1; l < numSpaceKeyDepth; l++) {
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

  public static long getNumRanges() {
    return numRanges;
  }

  public static int getNumSpaceKeyDepth() {
    return numSpaceKeyDepth;
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
      return this.childLock.isWriteLockedByCurrentThread() || namesystem
          .getFSLock().hasWriteChildLock();
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
      FSNamesystem ns, Configuration conf) {
    return new INodeMap(rootDir, ns, conf);
  }

  /** Synchronized by external lock. */
  private final GSet<INode, INodeWithAdditionalFields> map;
  private FSNamesystem namesystem;

  public Iterator<INodeWithAdditionalFields> getMapIterator() {
    return map.iterator();
  }

  public Set<INode> rangeKeys() {
    return ((PartitionedGSet)map).entryKeySet();
  }

  /**
   * Check if a number is a power of two.
   * @param n The number to be checked.
   * @return true if it is a power of two, else false.
   */
  private static boolean checkPowerOfTwo(long n) {
    if (n < 1) {
      return false;
    }
    return (n&(n - 1)) == 0;
  }

  private static void setStaticField(Configuration conf) {
    numSpaceKeyDepth = conf.getInt(DFSConfigKeys.DFS_NAMENODE_INOD_NAMESPACE_KEY_DEPTH,
        DFSConfigKeys.DFS_NAMENODE_INOD_NAMESPACE_KEY_DEPTH_DEFAULT);
    if (numSpaceKeyDepth < 1) {
      numSpaceKeyDepth = DFSConfigKeys.DFS_NAMENODE_INOD_NAMESPACE_KEY_DEPTH_DEFAULT;
    }

    numRanges = conf.getLong(DFSConfigKeys.DFS_NAMENODE_INOD_NUM_RANGES,
        DFSConfigKeys.DFS_NAMENODE_INOD_NUM_RANGES_DEFAULT);
    boolean isPowerOfTwo = checkPowerOfTwo(numRanges);
    if (!isPowerOfTwo) {
      FSDirectory.LOG.warn("Num of ranges = {}. It should be a power of two.",
          numRanges);
    }
    if ((numRanges < 1) || !isPowerOfTwo) {
      numRanges = DFSConfigKeys.DFS_NAMENODE_INOD_NUM_RANGES_DEFAULT;
    }
  }

  private INodeMap(INodeDirectory rootDir, FSNamesystem ns, Configuration conf) {
    this.namesystem = ns;
    setStaticField(conf);

    // Compute the map capacity by allocating 1% of total memory
    int capacity = LightWeightGSet.computeCapacity(1, "INodeMap");
    this.map = new PartitionedGSet<>(capacity, new INodeKeyComparator(),
            new INodeMapLock());

    // Pre-populate initial empty partitions
    PartitionedGSet<INode, INodeWithAdditionalFields> pgs =
        (PartitionedGSet<INode, INodeWithAdditionalFields>) map;
    PermissionStatus perm = new PermissionStatus(
        "", "", new FsPermission((short) 0));
    for(int p = 0; p < numRanges; p++) {
      INodeDirectory key = new INodeDirectory(INodeId.ROOT_INODE_ID,
          "range key".getBytes(StandardCharsets.UTF_8), perm, 0);
      INodeDirectory ppKey = key;
      if (numSpaceKeyDepth > 2) {
        for (int pp = 0; pp < (numSpaceKeyDepth - 2); pp++) {
          INodeDirectory tmpKey = new INodeDirectory((long)0, null, perm, 0);
          ppKey.setParent(tmpKey);
          ppKey = tmpKey;
        }
      }
      ppKey.setParent(new INodeDirectory((long)p, null, perm, 0));
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
    PartitionedGSet<INode, INodeWithAdditionalFields> pgs =
        (PartitionedGSet<INode, INodeWithAdditionalFields>) map;
    /*
     * Convert a long inode id into an INode object. We only need to compare
     * two inodes by inode id. So, it can be any type of INode object.
     */
    INode inode = new INodeDirectory(id, null,
        new PermissionStatus("", "", new FsPermission((short) 0)), 0);

    /*
     * Iterate all partitions of PGSet and return the INode.
     * Just for fallback.
     */
    PermissionStatus perm =
        new PermissionStatus("", "", new FsPermission((short) 0));
    // TODO: create a static array, to avoid creation of keys each time.
    for (int p = 0; p < numRanges; p++) {
      INodeDirectory key = new INodeDirectory(INodeId.ROOT_INODE_ID,
          "range key".getBytes(StandardCharsets.UTF_8), perm, 0);
      INodeDirectory ppKey = key;
      if (numSpaceKeyDepth > 2) {
        for (int pp = 0; pp < (numSpaceKeyDepth - 2); pp++) {
          INodeDirectory tmpKey = new INodeDirectory((long)0, null, perm, 0);
          ppKey.setParent(tmpKey);
          ppKey = tmpKey;
        }
      }
      ppKey.setParent(new INodeDirectory((long)p, null, perm, 0));
      PartitionedGSet.PartitionEntry e = pgs.getPartition(key);
      
      if (e.contains(inode)) {
        return (INode) e.get(inode);
      }
    }

    return null;
  }

  public INode get(INode inode) {

    /*
     * Check whether the Inode has (numSpaceKeyDepth - 1) levels of parent
     * dirs
     */
    int i = numSpaceKeyDepth - 1;
    INode tmpInode = inode;
    while (i > 0 && tmpInode.getParent() != null) {
      tmpInode = tmpInode.getParent();
      i--;
    }

    /*
     * If the Inode has (numSpaceKeyDepth - 1) levels of parent dirs,
     * use map.get(); else, fall back to get INode based on Inode ID.
     */
    if (i == 0) {
      return map.get(inode);
    } else {
      return get(inode.getId());
    }
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
