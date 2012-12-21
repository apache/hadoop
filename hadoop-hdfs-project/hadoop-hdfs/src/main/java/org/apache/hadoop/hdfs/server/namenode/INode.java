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
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.SignedBytes;

/**
 * We keep an in-memory representation of the file/block hierarchy.
 * This is a base INode class containing common fields for file and 
 * directory inodes.
 */
@InterfaceAudience.Private
public abstract class INode implements Comparable<byte[]> {
  public static final Log LOG = LogFactory.getLog(INode.class);

  static final ReadOnlyList<INode> EMPTY_READ_ONLY_LIST
      = ReadOnlyList.Util.emptyList();
  
  /**
   * Assert that the snapshot parameter must be null since this class only take
   * care current state. Subclasses should override the methods for handling the
   * snapshot states.
   */
  static void assertNull(Snapshot snapshot) {
    if (snapshot != null) {
      throw new AssertionError("snapshot is not null: " + snapshot);
    }
  }

  /** A pair of objects. */
  public static class Pair<L, R> {
    public final L left;
    public final R right;

    public Pair(L left, R right) {
      this.left = left;
      this.right = right;
    }
  }

  /** A triple of objects. */
  public static class Triple<L, M, R> extends Pair<L, R> {
    public final M middle;
    
    public Triple(L left, M middle, R right) {
      super(left, right);
      this.middle = middle;
    }
  }

  /** Wrapper of two counters for namespace consumed and diskspace consumed. */
  static class DirCounts {
    /** namespace count */
    long nsCount = 0;
    /** diskspace count */
    long dsCount = 0;
    
    /** returns namespace count */
    long getNsCount() {
      return nsCount;
    }
    /** returns diskspace count */
    long getDsCount() {
      return dsCount;
    }
  }
  
  private static enum PermissionStatusFormat {
    MODE(0, 16),
    GROUP(MODE.OFFSET + MODE.LENGTH, 25),
    USER(GROUP.OFFSET + GROUP.LENGTH, 23);

    final int OFFSET;
    final int LENGTH; //bit length
    final long MASK;

    PermissionStatusFormat(int offset, int length) {
      OFFSET = offset;
      LENGTH = length;
      MASK = ((-1L) >>> (64 - LENGTH)) << OFFSET;
    }

    long retrieve(long record) {
      return (record & MASK) >>> OFFSET;
    }

    long combine(long bits, long record) {
      return (record & ~MASK) | (bits << OFFSET);
    }

    /** Encode the {@link PermissionStatus} to a long. */
    static long toLong(PermissionStatus ps) {
      long permission = 0L;
      final int user = SerialNumberManager.INSTANCE.getUserSerialNumber(
          ps.getUserName());
      permission = USER.combine(user, permission);
      final int group = SerialNumberManager.INSTANCE.getGroupSerialNumber(
          ps.getGroupName());
      permission = GROUP.combine(group, permission);
      final int mode = ps.getPermission().toShort();
      permission = MODE.combine(mode, permission);
      return permission;
    }
  }

  /**
   *  The inode name is in java UTF8 encoding; 
   *  The name in HdfsFileStatus should keep the same encoding as this.
   *  if this encoding is changed, implicitly getFileInfo and listStatus in
   *  clientProtocol are changed; The decoding at the client
   *  side should change accordingly.
   */
  private byte[] name = null;
  /** 
   * Permission encoded using {@link PermissionStatusFormat}.
   * Codes other than {@link #clonePermissionStatus(INode)}
   * and {@link #updatePermissionStatus(PermissionStatusFormat, long)}
   * should not modify it.
   */
  private long permission = 0L;
  INodeDirectory parent = null;
  private long modificationTime = 0L;
  private long accessTime = 0L;

  private INode(byte[] name, long permission, INodeDirectory parent,
      long modificationTime, long accessTime) {
    this.name = name;
    this.permission = permission;
    this.parent = parent;
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
  }

  INode(byte[] name, PermissionStatus permissions, INodeDirectory parent,
      long modificationTime, long accessTime) {
    this(name, PermissionStatusFormat.toLong(permissions), parent,
        modificationTime, accessTime);
  }

  INode(PermissionStatus permissions, long mtime, long atime) {
    this(null, permissions, null, mtime, atime);
  }

  protected INode(String name, PermissionStatus permissions) {
    this(DFSUtil.string2Bytes(name), permissions, null, 0L, 0L);
  }
  
  /** @param other Other node to be copied */
  INode(INode other) {
    this(other.name, other.permission, other.parent, 
        other.modificationTime, other.accessTime);
  }

  /**
   * Create a copy of this inode for snapshot.
   * 
   * @return a pair of inodes, where the left inode is the current inode and
   *         the right inode is the snapshot copy. The current inode usually is
   *         the same object of this inode. However, in some cases, the inode
   *         may be replaced with a new inode for maintaining snapshot data.
   *         Then, the current inode is the new inode.
   */
  public Pair<? extends INode, ? extends INode> createSnapshotCopy() {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " does not support createSnapshotCopy().");
  }

  /**
   * Check whether this is the root inode.
   */
  boolean isRoot() {
    return name.length == 0;
  }

  /** Clone the {@link PermissionStatus}. */
  void clonePermissionStatus(INode that) {
    this.permission = that.permission;
  }
  /** Get the {@link PermissionStatus} */
  public PermissionStatus getPermissionStatus(Snapshot snapshot) {
    return new PermissionStatus(getUserName(snapshot), getGroupName(snapshot),
        getFsPermission(snapshot));
  }
  /** The same as getPermissionStatus(null). */
  public PermissionStatus getPermissionStatus() {
    return getPermissionStatus(null);
  }
  private void updatePermissionStatus(PermissionStatusFormat f, long n,
      Snapshot latest) {
    recordModification(latest);
    permission = f.combine(n, permission);
  }
  /**
   * @param snapshot
   *          if it is not null, get the result from the given snapshot;
   *          otherwise, get the result from the current inode.
   * @return user name
   */
  public String getUserName(Snapshot snapshot) {
    int n = (int)PermissionStatusFormat.USER.retrieve(permission);
    return SerialNumberManager.INSTANCE.getUser(n);
  }
  /** The same as getUserName(null). */
  public String getUserName() {
    return getUserName(null);
  }
  /** Set user */
  protected void setUser(String user, Snapshot latest) {
    int n = SerialNumberManager.INSTANCE.getUserSerialNumber(user);
    updatePermissionStatus(PermissionStatusFormat.USER, n, latest);
  }
  /**
   * @param snapshot
   *          if it is not null, get the result from the given snapshot;
   *          otherwise, get the result from the current inode.
   * @return group name
   */
  public String getGroupName(Snapshot snapshot) {
    int n = (int)PermissionStatusFormat.GROUP.retrieve(permission);
    return SerialNumberManager.INSTANCE.getGroup(n);
  }
  /** The same as getGroupName(null). */
  public String getGroupName() {
    return getGroupName(null);
  }
  /** Set group */
  protected void setGroup(String group, Snapshot latest) {
    int n = SerialNumberManager.INSTANCE.getGroupSerialNumber(group);
    updatePermissionStatus(PermissionStatusFormat.GROUP, n, latest);
  }
  /**
   * @param snapshot
   *          if it is not null, get the result from the given snapshot;
   *          otherwise, get the result from the current inode.
   * @return permission.
   */
  public FsPermission getFsPermission(Snapshot snapshot) {
    return new FsPermission(
        (short)PermissionStatusFormat.MODE.retrieve(permission));
  }
  /** The same as getFsPermission(null). */
  public FsPermission getFsPermission() {
    return getFsPermission(null);
  }
  protected short getFsPermissionShort() {
    return (short)PermissionStatusFormat.MODE.retrieve(permission);
  }
  /** Set the {@link FsPermission} of this {@link INode} */
  void setPermission(FsPermission permission, Snapshot latest) {
    final short mode = permission.toShort();
    updatePermissionStatus(PermissionStatusFormat.MODE, mode, latest);
  }

  /**
   * This inode is being modified.  The previous version of the inode needs to
   * be recorded in the latest snapshot.
   *
   * @param latest the latest snapshot that has been taken.
   *        Note that it is null if no snapshots have been taken.
   * @return see {@link #createSnapshotCopy()}. 
   */
  Pair<? extends INode, ? extends INode> recordModification(Snapshot latest) {
    Preconditions.checkState(!isDirectory(),
        "this is an INodeDirectory, this=%s", this);
    return latest == null? null: parent.saveChild2Snapshot(this, latest);
  }

  /**
   * Check whether it's a file.
   */
  public boolean isFile() {
    return false;
  }

  /**
   * Check whether it's a directory
   */
  public boolean isDirectory() {
    return false;
  }

  /**
   * Collect all the blocks in all children of this INode. Count and return the
   * number of files in the sub tree. Also clears references since this INode is
   * deleted.
   * 
   * @param info
   *          Containing all the blocks collected from the children of this
   *          INode. These blocks later should be removed/updated in the
   *          blocksMap.
   */
  abstract int collectSubtreeBlocksAndClear(BlocksMapUpdateInfo info);

  /** Compute {@link ContentSummary}. */
  public final ContentSummary computeContentSummary() {
    long[] a = computeContentSummary(new long[]{0,0,0,0});
    return new ContentSummary(a[0], a[1], a[2], getNsQuota(), 
                              a[3], getDsQuota());
  }
  /**
   * @return an array of three longs. 
   * 0: length, 1: file count, 2: directory count 3: disk space
   */
  abstract long[] computeContentSummary(long[] summary);
  
  /**
   * Get the quota set for this inode
   * @return the quota if it is set; -1 otherwise
   */
  public long getNsQuota() {
    return -1;
  }

  public long getDsQuota() {
    return -1;
  }
  
  boolean isQuotaSet() {
    return getNsQuota() >= 0 || getDsQuota() >= 0;
  }
  
  /**
   * Adds total number of names and total disk space taken under 
   * this tree to counts.
   * Returns updated counts object.
   */
  abstract DirCounts spaceConsumedInTree(DirCounts counts);
  
  /**
   * @return null if the local name is null; otherwise, return the local name.
   */
  public String getLocalName() {
    return name == null? null: DFSUtil.bytes2String(name);
  }


  String getLocalParentDir() {
    INode inode = isRoot() ? this : getParent();
    return (inode != null) ? inode.getFullPathName() : "";
  }

  /**
   * @return null if the local name is null;
   *         otherwise, return the local name byte array.
   */
  public byte[] getLocalNameBytes() {
    return name;
  }

  /**
   * Set local file name
   */
  public void setLocalName(String name) {
    setLocalName(DFSUtil.string2Bytes(name));
  }

  /**
   * Set local file name
   */
  public void setLocalName(byte[] name) {
    this.name = name;
  }

  public String getFullPathName() {
    // Get the full path name of this inode.
    return FSDirectory.getFullPathName(this);
  }

  @Override
  public String toString() {
    return name == null? "<name==null>": getFullPathName();
  }

  @VisibleForTesting
  public String getObjectString() {
    final String s = super.toString();
    return s.substring(s.lastIndexOf(getClass().getSimpleName()));
  }

  @VisibleForTesting
  public String toStringWithObjectType() {
    return toString() + "(" + getObjectString() + ")";
  }

  @VisibleForTesting
  public String toDetailString() {
    return toStringWithObjectType()
        + ", parent=" + (parent == null? null: parent.toStringWithObjectType());
  }

  /**
   * Get parent directory 
   * @return parent INode
   */
  public INodeDirectory getParent() {
    return this.parent;
  }

  /** Set parent directory */
  public void setParent(INodeDirectory parent) {
    this.parent = parent;
  }
  
  /**
   * @param snapshot
   *          if it is not null, get the result from the given snapshot;
   *          otherwise, get the result from the current inode.
   * @return modification time.
   */
  public long getModificationTime(Snapshot snapshot) {
    return this.modificationTime;
  }

  /** The same as getModificationTime(null). */
  public long getModificationTime() {
    return getModificationTime(null);
  }

  /** Update modification time if it is larger than the current value. */
  public void updateModificationTime(long mtime, Snapshot latest) {
    assert isDirectory();
    if (mtime > modificationTime) {
      setModificationTime(mtime, latest);
    }
  }

  void cloneModificationTime(INode that) {
    this.modificationTime = that.modificationTime;
  }

  /**
   * Always set the last modification time of inode.
   */
  public void setModificationTime(long modtime, Snapshot latest) {
    recordModification(latest);
    this.modificationTime = modtime;
  }

  /**
   * @param snapshot
   *          if it is not null, get the result from the given snapshot;
   *          otherwise, get the result from the current inode.
   * @return access time
   */
  public long getAccessTime(Snapshot snapshot) {
    return accessTime;
  }

  /** The same as getAccessTime(null). */
  public long getAccessTime() {
    return getAccessTime(null);
  }

  /**
   * Set last access time of inode.
   */
  void setAccessTime(long atime, Snapshot latest) {
    recordModification(latest);
    accessTime = atime;
  }

  /**
   * Is this inode being constructed?
   */
  public boolean isUnderConstruction() {
    return false;
  }

  /**
   * Check whether it's a symlink
   */
  public boolean isSymlink() {
    return false;
  }

  /**
   * Breaks file path into components.
   * @param path
   * @return array of byte arrays each of which represents 
   * a single path component.
   */
  static byte[][] getPathComponents(String path) {
    return getPathComponents(getPathNames(path));
  }

  /** Convert strings to byte arrays for path components. */
  static byte[][] getPathComponents(String[] strings) {
    if (strings.length == 0) {
      return new byte[][]{null};
    }
    byte[][] bytes = new byte[strings.length][];
    for (int i = 0; i < strings.length; i++)
      bytes[i] = DFSUtil.string2Bytes(strings[i]);
    return bytes;
  }

  /**
   * Splits an absolute path into an array of path components.
   * @param path
   * @throws AssertionError if the given path is invalid.
   * @return array of path components.
   */
  static String[] getPathNames(String path) {
    if (path == null || !path.startsWith(Path.SEPARATOR)) {
      throw new AssertionError("Absolute path required");
    }
    return StringUtils.split(path, Path.SEPARATOR_CHAR);
  }

  /**
   * Given some components, create a path name.
   * @param components The path components
   * @param start index
   * @param end index
   * @return concatenated path
   */
  static String constructPath(byte[][] components, int start, int end) {
    StringBuilder buf = new StringBuilder();
    for (int i = start; i < end; i++) {
      buf.append(DFSUtil.bytes2String(components[i]));
      if (i < end - 1) {
        buf.append(Path.SEPARATOR);
      }
    }
    return buf.toString();
  }

  private static final byte[] EMPTY_BYTES = {};

  @Override
  public final int compareTo(byte[] bytes) {
    final byte[] left = name == null? EMPTY_BYTES: name;
    final byte[] right = bytes == null? EMPTY_BYTES: bytes;
    return SignedBytes.lexicographicalComparator().compare(left, right);
  }

  @Override
  public final boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || !(that instanceof INode)) {
      return false;
    }
    return Arrays.equals(this.name, ((INode)that).name);
  }

  @Override
  public final int hashCode() {
    return Arrays.hashCode(this.name);
  }
  
  /**
   * Create an INode; the inode's name is not set yet
   * 
   * @param permissions permissions
   * @param blocks blocks if a file
   * @param symlink symblic link if a symbolic link
   * @param replication replication factor
   * @param modificationTime modification time
   * @param atime access time
   * @param nsQuota namespace quota
   * @param dsQuota disk quota
   * @param preferredBlockSize block size
   * @return an inode
   */
  static INode newINode(PermissionStatus permissions,
                        BlockInfo[] blocks,
                        String symlink,
                        short replication,
                        long modificationTime,
                        long atime,
                        long nsQuota,
                        long dsQuota,
                        long preferredBlockSize) {
    if (symlink.length() != 0) { // check if symbolic link
      return new INodeSymlink(symlink, modificationTime, atime, permissions);
    }  else if (blocks == null) { //not sym link and blocks null? directory!
      if (nsQuota >= 0 || dsQuota >= 0) {
        return new INodeDirectoryWithQuota(
            permissions, modificationTime, nsQuota, dsQuota);
      } 
      // regular directory
      return new INodeDirectory(permissions, modificationTime);
    }
    // file
    return new INodeFile(permissions, blocks, replication,
        modificationTime, atime, preferredBlockSize);
  }

  /**
   * Dump the subtree starting from this inode.
   * @return a text representation of the tree.
   */
  @VisibleForTesting
  public final StringBuffer dumpTreeRecursively() {
    final StringWriter out = new StringWriter(); 
    dumpTreeRecursively(new PrintWriter(out, true), new StringBuilder(), null);
    return out.getBuffer();
  }

  /**
   * Dump tree recursively.
   * @param prefix The prefix string that each line should print.
   */
  @VisibleForTesting
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix, Snapshot snapshot) {
    out.print(prefix);
    out.print(" ");
    out.print(getLocalName());
    out.print("   (");
    out.print(getObjectString());
    out.print("), parent=");
    out.print(parent == null? null: parent.getLocalName() + "/");
    if (!this.isDirectory()) {
      out.println();
    } else {
      final INodeDirectory dir = (INodeDirectory)this;
      out.println(", size=" + dir.getChildrenList(snapshot).size());
    }
  }
  
  /**
   * Information used for updating the blocksMap when deleting files.
   */
  public static class BlocksMapUpdateInfo implements
      Iterable<Map.Entry<Block, BlocksMapINodeUpdateEntry>> {
    private final Map<Block, BlocksMapINodeUpdateEntry> updateMap;
    
    public BlocksMapUpdateInfo() {
      updateMap = new HashMap<Block, BlocksMapINodeUpdateEntry>();
    }
    
    /**
     * Add a to-be-deleted block. This block should belongs to a file without
     * snapshots. We thus only need to put a block-null pair into the updateMap.
     * 
     * @param toDelete the to-be-deleted block
     */
    public void addDeleteBlock(Block toDelete) {
      if (toDelete != null) {
        updateMap.put(toDelete, null);
      }
    }
    
    /**
     * Add a given block, as well as its old and new BlockCollection
     * information, into the updateMap.
     * 
     * @param toUpdateBlock
     *          The given block
     * @param entry
     *          The BlocksMapINodeUpdateEntry instance containing both the
     *          original BlockCollection of the given block and the new
     *          BlockCollection of the given block for updating the blocksMap.
     *          The new BlockCollection should be the INode of one of the
     *          corresponding file's snapshot.
     */
    public void addUpdateBlock(Block toUpdateBlock,
        BlocksMapINodeUpdateEntry entry) {
      updateMap.put(toUpdateBlock, entry);
    }

    /**
     * Clear {@link BlocksMapUpdateInfo#updateMap}
     */
    public void clear() {
      updateMap.clear();
    }

    @Override
    public Iterator<Map.Entry<Block, BlocksMapINodeUpdateEntry>> iterator() {
      return updateMap.entrySet().iterator();
    }
  }
  
  /**
   * When deleting a file with snapshot, we cannot directly remove its record
   * from blocksMap. Instead, we should consider replacing the original record
   * in blocksMap with INode of snapshot.
   */
  public static class BlocksMapINodeUpdateEntry {
    /**
     * The BlockCollection of the file to be deleted
     */
    private final BlockCollection toDelete;
    /**
     * The BlockCollection of the to-be-deleted file's snapshot
     */
    private final BlockCollection toReplace;

    public BlocksMapINodeUpdateEntry(BlockCollection toDelete,
        BlockCollection toReplace) {
      this.toDelete = toDelete;
      this.toReplace = toReplace;
    }

    public BlockCollection getToDelete() {
      return toDelete;
    }

    public BlockCollection getToReplace() {
      return toReplace;
    }
  }
}
