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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.diff.Diff;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.SignedBytes;

/**
 * We keep an in-memory representation of the file/block hierarchy.
 * This is a base INode class containing common fields for file and 
 * directory inodes.
 */
@InterfaceAudience.Private
public abstract class INode implements Diff.Element<byte[]> {
  public static final Log LOG = LogFactory.getLog(INode.class);

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
   * The inode id
   */
  final private long id;

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

  private INode(long id, byte[] name, long permission, INodeDirectory parent,
      long modificationTime, long accessTime) {
    this.id = id;
    this.name = name;
    this.permission = permission;
    this.parent = parent;
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
  }

  INode(long id, byte[] name, PermissionStatus permissions,
      long modificationTime, long accessTime) {
    this(id, name, PermissionStatusFormat.toLong(permissions), null,
        modificationTime, accessTime);
  }
  
  /** @param other Other node to be copied */
  INode(INode other) {
    this(other.id, other.name, other.permission, other.parent, 
        other.modificationTime, other.accessTime);
  }

  /** Get inode id */
  public long getId() {
    return this.id;
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
  public final PermissionStatus getPermissionStatus(Snapshot snapshot) {
    return new PermissionStatus(getUserName(snapshot), getGroupName(snapshot),
        getFsPermission(snapshot));
  }
  /** The same as getPermissionStatus(null). */
  public final PermissionStatus getPermissionStatus() {
    return getPermissionStatus(null);
  }
  private INode updatePermissionStatus(PermissionStatusFormat f, long n,
      Snapshot latest) {
    final INode nodeToUpdate = recordModification(latest);
    nodeToUpdate.permission = f.combine(n, permission);
    return nodeToUpdate;
  }
  /**
   * @param snapshot
   *          if it is not null, get the result from the given snapshot;
   *          otherwise, get the result from the current inode.
   * @return user name
   */
  public final String getUserName(Snapshot snapshot) {
    if (snapshot != null) {
      return getSnapshotINode(snapshot).getUserName();
    }

    int n = (int)PermissionStatusFormat.USER.retrieve(permission);
    return SerialNumberManager.INSTANCE.getUser(n);
  }
  /** The same as getUserName(null). */
  public final String getUserName() {
    return getUserName(null);
  }
  /** Set user */
  protected INode setUser(String user, Snapshot latest) {
    int n = SerialNumberManager.INSTANCE.getUserSerialNumber(user);
    return updatePermissionStatus(PermissionStatusFormat.USER, n, latest);
  }
  /**
   * @param snapshot
   *          if it is not null, get the result from the given snapshot;
   *          otherwise, get the result from the current inode.
   * @return group name
   */
  public final String getGroupName(Snapshot snapshot) {
    if (snapshot != null) {
      return getSnapshotINode(snapshot).getGroupName();
    }

    int n = (int)PermissionStatusFormat.GROUP.retrieve(permission);
    return SerialNumberManager.INSTANCE.getGroup(n);
  }
  /** The same as getGroupName(null). */
  public final String getGroupName() {
    return getGroupName(null);
  }
  /** Set group */
  protected INode setGroup(String group, Snapshot latest) {
    int n = SerialNumberManager.INSTANCE.getGroupSerialNumber(group);
    return updatePermissionStatus(PermissionStatusFormat.GROUP, n, latest);
  }
  /**
   * @param snapshot
   *          if it is not null, get the result from the given snapshot;
   *          otherwise, get the result from the current inode.
   * @return permission.
   */
  public final FsPermission getFsPermission(Snapshot snapshot) {
    if (snapshot != null) {
      return getSnapshotINode(snapshot).getFsPermission();
    }

    return new FsPermission(
        (short)PermissionStatusFormat.MODE.retrieve(permission));
  }
  /** The same as getFsPermission(null). */
  public final FsPermission getFsPermission() {
    return getFsPermission(null);
  }
  protected short getFsPermissionShort() {
    return (short)PermissionStatusFormat.MODE.retrieve(permission);
  }
  /** Set the {@link FsPermission} of this {@link INode} */
  INode setPermission(FsPermission permission, Snapshot latest) {
    final short mode = permission.toShort();
    return updatePermissionStatus(PermissionStatusFormat.MODE, mode, latest);
  }

  /**
   * @return if the given snapshot is null, return this;
   *     otherwise return the corresponding snapshot inode.
   */
  public INode getSnapshotINode(final Snapshot snapshot) {
    return this;
  }

  /** Is this inode in the latest snapshot? */
  public final boolean isInLatestSnapshot(final Snapshot latest) {
    return latest != null
        && (parent == null
            || (parent.isInLatestSnapshot(latest)
                && this == parent.getChild(getLocalNameBytes(), latest)));
  }

  /**
   * This inode is being modified.  The previous version of the inode needs to
   * be recorded in the latest snapshot.
   *
   * @param latest the latest snapshot that has been taken.
   *        Note that it is null if no snapshots have been taken.
   * @return The current inode, which usually is the same object of this inode.
   *         However, in some cases, this inode may be replaced with a new inode
   *         for maintaining snapshots. The current inode is then the new inode.
   */
  abstract INode recordModification(final Snapshot latest);

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
   * Destroy the subtree under this inode and collect the blocks from the
   * descents for further block deletion/update. If snapshot is null, the
   * subtree resides in the current state; otherwise, the subtree resides in the
   * given snapshot. The method also clears the references in the deleted inodes
   * and remove the corresponding snapshot information, if there is any.
   * 
   * @param snapshot the snapshot to be deleted; null means the current state.
   * @param collectedBlocks blocks collected from the descents for further block
   *                        deletion/update will be added to the given map.
   * @return the number of deleted inodes in the subtree.
   */
  public abstract int destroySubtreeAndCollectBlocks(Snapshot snapshot,
      BlocksMapUpdateInfo collectedBlocks);

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

  @Override
  public byte[] getKey() {
    return getLocalNameBytes();
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
    return getLocalName();
  }

  @VisibleForTesting
  public String getObjectString() {
    return getClass().getSimpleName() + "@"
        + Integer.toHexString(super.hashCode());
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
  public final INodeDirectory getParent() {
    return this.parent;
  }

  /** Set parent directory */
  public void setParent(INodeDirectory parent) {
    this.parent = parent;
  }

  /** Clear references to other objects. */
  public void clearReferences() {
    setParent(null);
  }

  /**
   * @param snapshot
   *          if it is not null, get the result from the given snapshot;
   *          otherwise, get the result from the current inode.
   * @return modification time.
   */
  public final long getModificationTime(Snapshot snapshot) {
    if (snapshot != null) {
      return getSnapshotINode(snapshot).modificationTime;
    }

    return this.modificationTime;
  }

  /** The same as getModificationTime(null). */
  public final long getModificationTime() {
    return getModificationTime(null);
  }

  /** Update modification time if it is larger than the current value. */
  public final INode updateModificationTime(long mtime, Snapshot latest) {
    assert isDirectory();
    if (mtime <= modificationTime) {
      return this;
    }
    return setModificationTime(mtime, latest);
  }

  void cloneModificationTime(INode that) {
    this.modificationTime = that.modificationTime;
  }

  /**
   * Always set the last modification time of inode.
   */
  public final INode setModificationTime(long modtime, Snapshot latest) {
    final INode nodeToUpdate = recordModification(latest);
    nodeToUpdate.modificationTime = modtime;
    return nodeToUpdate;
  }

  /**
   * @param snapshot
   *          if it is not null, get the result from the given snapshot;
   *          otherwise, get the result from the current inode.
   * @return access time
   */
  public final long getAccessTime(Snapshot snapshot) {
    if (snapshot != null) {
      return getSnapshotINode(snapshot).accessTime;
    }

    return accessTime;
  }

  /** The same as getAccessTime(null). */
  public final long getAccessTime() {
    return getAccessTime(null);
  }

  /**
   * Set last access time of inode.
   */
  public INode setAccessTime(long atime, Snapshot latest) {
    final INode nodeToUpdate = recordModification(latest);
    nodeToUpdate.accessTime = atime;
    return nodeToUpdate;
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

  public static final byte[] EMPTY_BYTES = {};

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
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix,
      Snapshot snapshot) {
    out.print(prefix);
    out.print(" ");
    out.print(getLocalName());
    out.print("   (");
    out.print(getObjectString());
    out.print("), parent=");
    out.print(parent == null? null: parent.getLocalName() + "/");
    out.print(", " + getPermissionStatus(snapshot));
  }
  
  /**
   * Information used for updating the blocksMap when deleting files.
   */
  public static class BlocksMapUpdateInfo {
    /**
     * The list of blocks that need to be removed from blocksMap
     */
    private List<Block> toDeleteList;
    
    public BlocksMapUpdateInfo(List<Block> toDeleteList) {
      this.toDeleteList = toDeleteList == null ? new ArrayList<Block>()
          : toDeleteList;
    }
    
    public BlocksMapUpdateInfo() {
      toDeleteList = new ArrayList<Block>();
    }
    
    /**
     * @return The list of blocks that need to be removed from blocksMap
     */
    public List<Block> getToDeleteList() {
      return toDeleteList;
    }
    
    /**
     * Add a to-be-deleted block into the
     * {@link BlocksMapUpdateInfo#toDeleteList}
     * @param toDelete the to-be-deleted block
     */
    public void addDeleteBlock(Block toDelete) {
      if (toDelete != null) {
        toDeleteList.add(toDelete);
      }
    }
    
    /**
     * Clear {@link BlocksMapUpdateInfo#toDeleteList}
     */
    public void clear() {
      toDeleteList.clear();
    }
  }
}
