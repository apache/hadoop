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

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import com.google.common.base.Preconditions;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.ID_INTEGER_COMPARATOR;

/**
 * Contains INodes information resolved from a given path.
 */
public class INodesInPath {
  public static final Log LOG = LogFactory.getLog(INodesInPath.class);

  /**
   * @return true if path component is {@link HdfsConstants#DOT_SNAPSHOT_DIR}
   */
  private static boolean isDotSnapshotDir(byte[] pathComponent) {
    return pathComponent != null &&
        Arrays.equals(HdfsServerConstants.DOT_SNAPSHOT_DIR_BYTES, pathComponent);
  }

  private static INode[] getINodes(final INode inode) {
    int depth = 0, index;
    INode tmp = inode;
    while (tmp != null) {
      depth++;
      tmp = tmp.getParent();
    }
    INode[] inodes = new INode[depth];
    tmp = inode;
    index = depth;
    while (tmp != null) {
      index--;
      inodes[index] = tmp;
      tmp = tmp.getParent();
    }
    return inodes;
  }

  private static byte[][] getPaths(final INode[] inodes) {
    byte[][] paths = new byte[inodes.length][];
    for (int i = 0; i < inodes.length; i++) {
      paths[i] = inodes[i].getKey();
    }
    return paths;
  }

  /**
   * Construct {@link INodesInPath} from {@link INode}.
   *
   * @param inode to construct from
   * @return INodesInPath
   */
  static INodesInPath fromINode(INode inode) {
    INode[] inodes = getINodes(inode);
    byte[][] paths = getPaths(inodes);
    return new INodesInPath(inodes, paths);
  }

  /**
   * Construct {@link INodesInPath} from {@link INode} and its root
   * {@link INodeDirectory}. INodesInPath constructed this way will
   * each have its snapshot and latest snapshot id filled in.
   *
   * This routine is specifically for
   * {@link LeaseManager#getINodeWithLeases(INodeDirectory)} to get
   * open files along with their snapshot details which is used during
   * new snapshot creation to capture their meta data.
   *
   * @param rootDir the root {@link INodeDirectory} under which inode
   *                needs to be resolved
   * @param inode the {@link INode} to be resolved
   * @return INodesInPath
   */
  static INodesInPath fromINode(final INodeDirectory rootDir, INode inode) {
    byte[][] paths = getPaths(getINodes(inode));
    return resolve(rootDir, paths);
  }

  static INodesInPath fromComponents(byte[][] components) {
    return new INodesInPath(new INode[components.length], components);
  }

  /**
   * Retrieve existing INodes from a path.  The number of INodes is equal
   * to the number of path components.  For a snapshot path
   * (e.g. /foo/.snapshot/s1/bar), the ".snapshot/s1" will be represented in
   * one path component corresponding to its Snapshot.Root inode.  This 1-1
   * mapping ensures the path can always be properly reconstructed.
   *
   * <p>
   * Example: <br>
   * Given the path /c1/c2/c3 where only /c1/c2 exists, resulting in the
   * following path components: ["","c1","c2","c3"]
   * 
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"])</code> should fill
   * the array with [rootINode,c1,c2], <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"])</code> should
   * fill the array with [rootINode,c1,c2,null]
   * 
   * @param startingDir the starting directory
   * @param components array of path component name
   * @return the specified number of existing INodes in the path
   */
  static INodesInPath resolve(final INodeDirectory startingDir,
      final byte[][] components) {
    return resolve(startingDir, components, false);
  }

  static INodesInPath resolve(final INodeDirectory startingDir,
      byte[][] components, final boolean isRaw) {
    Preconditions.checkArgument(startingDir.compareTo(components[0]) == 0);

    INode curNode = startingDir;
    int count = 0;
    int inodeNum = 0;
    INode[] inodes = new INode[components.length];
    boolean isSnapshot = false;
    int snapshotId = CURRENT_STATE_ID;

    while (count < components.length && curNode != null) {
      final boolean lastComp = (count == components.length - 1);
      inodes[inodeNum++] = curNode;
      final boolean isRef = curNode.isReference();
      final boolean isDir = curNode.isDirectory();
      final INodeDirectory dir = isDir? curNode.asDirectory(): null;
      if (!isRef && isDir && dir.isWithSnapshot()) {
        //if the path is a non-snapshot path, update the latest snapshot.
        if (!isSnapshot && shouldUpdateLatestId(
            dir.getDirectoryWithSnapshotFeature().getLastSnapshotId(),
            snapshotId)) {
          snapshotId = dir.getDirectoryWithSnapshotFeature().getLastSnapshotId();
        }
      } else if (isRef && isDir && !lastComp) {
        // If the curNode is a reference node, need to check its dstSnapshot:
        // 1. if the existing snapshot is no later than the dstSnapshot (which
        // is the latest snapshot in dst before the rename), the changes 
        // should be recorded in previous snapshots (belonging to src).
        // 2. however, if the ref node is already the last component, we still 
        // need to know the latest snapshot among the ref node's ancestors, 
        // in case of processing a deletion operation. Thus we do not overwrite
        // the latest snapshot if lastComp is true. In case of the operation is
        // a modification operation, we do a similar check in corresponding 
        // recordModification method.
        if (!isSnapshot) {
          int dstSnapshotId = curNode.asReference().getDstSnapshotId();
          if (snapshotId == CURRENT_STATE_ID || // no snapshot in dst tree of rename
              (dstSnapshotId != CURRENT_STATE_ID &&
               dstSnapshotId >= snapshotId)) { // the above scenario
            int lastSnapshot = CURRENT_STATE_ID;
            DirectoryWithSnapshotFeature sf;
            if (curNode.isDirectory() && 
                (sf = curNode.asDirectory().getDirectoryWithSnapshotFeature()) != null) {
              lastSnapshot = sf.getLastSnapshotId();
            }
            snapshotId = lastSnapshot;
          }
        }
      }
      if (lastComp || !isDir) {
        break;
      }

      final byte[] childName = components[++count];
      // check if the next byte[] in components is for ".snapshot"
      if (isDotSnapshotDir(childName) && dir.isSnapshottable()) {
        isSnapshot = true;
        // check if ".snapshot" is the last element of components
        if (count == components.length - 1) {
          break;
        }
        // Resolve snapshot root
        final Snapshot s = dir.getSnapshot(components[count + 1]);
        if (s == null) {
          curNode = null; // snapshot not found
        } else {
          curNode = s.getRoot();
          snapshotId = s.getId();
        }
        // combine .snapshot & name into 1 component element to ensure
        // 1-to-1 correspondence between components and inodes arrays is
        // preserved so a path can be reconstructed.
        byte[][] componentsCopy =
            Arrays.copyOf(components, components.length - 1);
        componentsCopy[count] = DFSUtil.string2Bytes(
            DFSUtil.byteArray2PathString(components, count, 2));
        // shift the remaining components after snapshot name
        int start = count + 2;
        System.arraycopy(components, start, componentsCopy, count + 1,
            components.length - start);
        components = componentsCopy;
        // reduce the inodes array to compensate for reduction in components
        inodes = Arrays.copyOf(inodes, components.length);
      } else {
        // normal case, and also for resolving file/dir under snapshot root
        curNode = dir.getChild(childName,
            isSnapshot ? snapshotId : CURRENT_STATE_ID);
      }
    }
    return new INodesInPath(inodes, components, isRaw, isSnapshot, snapshotId);
  }

  private static boolean shouldUpdateLatestId(int sid, int snapshotId) {
    return snapshotId == CURRENT_STATE_ID || (sid != CURRENT_STATE_ID &&
        ID_INTEGER_COMPARATOR.compare(snapshotId, sid) < 0);
  }

  /**
   * Replace an inode of the given INodesInPath in the given position. We do a
   * deep copy of the INode array.
   * @param pos the position of the replacement
   * @param inode the new inode
   * @return a new INodesInPath instance
   */
  public static INodesInPath replace(INodesInPath iip, int pos, INode inode) {
    Preconditions.checkArgument(iip.length() > 0 && pos > 0 // no for root
        && pos < iip.length());
    if (iip.getINode(pos) == null) {
      Preconditions.checkState(iip.getINode(pos - 1) != null);
    }
    INode[] inodes = new INode[iip.inodes.length];
    System.arraycopy(iip.inodes, 0, inodes, 0, inodes.length);
    inodes[pos] = inode;
    return new INodesInPath(inodes, iip.path, iip.isRaw,
        iip.isSnapshot, iip.snapshotId);
  }

  /**
   * Extend a given INodesInPath with a child INode. The child INode will be
   * appended to the end of the new INodesInPath.
   */
  public static INodesInPath append(INodesInPath iip, INode child,
      byte[] childName) {
    Preconditions.checkArgument(iip.length() > 0);
    Preconditions.checkArgument(iip.getLastINode() != null && iip
        .getLastINode().isDirectory());
    INode[] inodes = new INode[iip.length() + 1];
    System.arraycopy(iip.inodes, 0, inodes, 0, inodes.length - 1);
    inodes[inodes.length - 1] = child;
    byte[][] path = new byte[iip.path.length + 1][];
    System.arraycopy(iip.path, 0, path, 0, path.length - 1);
    path[path.length - 1] = childName;
    return new INodesInPath(inodes, path, iip.isRaw,
        iip.isSnapshot, iip.snapshotId);
  }

  private final byte[][] path;
  private volatile String pathname;

  /**
   * Array with the specified number of INodes resolved for a given path.
   */
  private final INode[] inodes;
  /**
   * true if this path corresponds to a snapshot
   */
  private final boolean isSnapshot;

  /**
   * true if this is a /.reserved/raw path.  path component resolution strips
   * it from the path so need to track it separately.
   */
  private final boolean isRaw;

  /**
   * For snapshot paths, it is the id of the snapshot; or 
   * {@link Snapshot#CURRENT_STATE_ID} if the snapshot does not exist. For 
   * non-snapshot paths, it is the id of the latest snapshot found in the path;
   * or {@link Snapshot#CURRENT_STATE_ID} if no snapshot is found.
   */
  private final int snapshotId;

  private INodesInPath(INode[] inodes, byte[][] path, boolean isRaw,
      boolean isSnapshot,int snapshotId) {
    Preconditions.checkArgument(inodes != null && path != null);
    this.inodes = inodes;
    this.path = path;
    this.isRaw = isRaw;
    this.isSnapshot = isSnapshot;
    this.snapshotId = snapshotId;
  }

  private INodesInPath(INode[] inodes, byte[][] path) {
    this(inodes, path, false, false, CURRENT_STATE_ID);
  }

  /**
   * For non-snapshot paths, return the latest snapshot id found in the path.
   */
  public int getLatestSnapshotId() {
    Preconditions.checkState(!isSnapshot);
    return snapshotId;
  }
  
  /**
   * For snapshot paths, return the id of the snapshot specified in the path.
   * For non-snapshot paths, return {@link Snapshot#CURRENT_STATE_ID}.
   */
  public int getPathSnapshotId() {
    return isSnapshot ? snapshotId : CURRENT_STATE_ID;
  }

  /**
   * @return the i-th inode if i >= 0;
   *         otherwise, i < 0, return the (length + i)-th inode.
   */
  public INode getINode(int i) {
    return inodes[(i < 0) ? inodes.length + i : i];
  }

  /** @return the last inode. */
  public INode getLastINode() {
    return getINode(-1);
  }

  byte[] getLastLocalName() {
    return path[path.length - 1];
  }

  public byte[][] getPathComponents() {
    return path;
  }

  public byte[] getPathComponent(int i) {
    return path[i];
  }

  /** @return the full path in string form */
  public String getPath() {
    if (pathname == null) {
      pathname = DFSUtil.byteArray2PathString(path);
    }
    return pathname;
  }

  public String getParentPath() {
    return getPath(path.length - 2);
  }

  public String getPath(int pos) {
    return DFSUtil.byteArray2PathString(path, 0, pos + 1); // it's a length...
  }

  public int length() {
    return inodes.length;
  }

  public INode[] getINodesArray() {
    INode[] retArr = new INode[inodes.length];
    System.arraycopy(inodes, 0, retArr, 0, inodes.length);
    return retArr;
  }

  /**
   * @param length number of ancestral INodes in the returned INodesInPath
   *               instance
   * @return the INodesInPath instance containing ancestral INodes. Note that
   * this method only handles non-snapshot paths.
   */
  private INodesInPath getAncestorINodesInPath(int length) {
    Preconditions.checkArgument(length >= 0 && length < inodes.length);
    Preconditions.checkState(isDotSnapshotDir() || !isSnapshot());
    final INode[] anodes = new INode[length];
    final byte[][] apath = new byte[length][];
    System.arraycopy(this.inodes, 0, anodes, 0, length);
    System.arraycopy(this.path, 0, apath, 0, length);
    return new INodesInPath(anodes, apath, isRaw, false, snapshotId);
  }

  /**
   * @return an INodesInPath instance containing all the INodes in the parent
   *         path. We do a deep copy here.
   */
  public INodesInPath getParentINodesInPath() {
    return inodes.length > 1 ? getAncestorINodesInPath(inodes.length - 1) :
        null;
  }

  /**
   * Verify if this {@link INodesInPath} is a descendant of the
   * requested {@link INodeDirectory}.
   *
   * @param inodeDirectory the ancestor directory
   * @return true if this INodesInPath is a descendant of inodeDirectory
   */
  public boolean isDescendant(final INodeDirectory inodeDirectory) {
    final INodesInPath dirIIP = fromINode(inodeDirectory);
    return isDescendant(dirIIP);
  }

  private boolean isDescendant(final INodesInPath ancestorDirIIP) {
    int ancestorDirINodesLength = ancestorDirIIP.length();
    int myParentINodesLength = length() - 1;
    if (myParentINodesLength < ancestorDirINodesLength) {
      return false;
    }

    int index = 0;
    while (index < ancestorDirINodesLength) {
      if (inodes[index] != ancestorDirIIP.getINode(index)) {
        return false;
      }
      index++;
    }
    return true;
  }


  /**
   * @return a new INodesInPath instance that only contains existing INodes.
   * Note that this method only handles non-snapshot paths.
   */
  public INodesInPath getExistingINodes() {
    Preconditions.checkState(!isSnapshot());
    for (int i = inodes.length; i > 0; i--) {
      if (inodes[i - 1] != null) {
        return (i == inodes.length) ? this : getAncestorINodesInPath(i);
      }
    }
    return null;
  }

  /**
   * @return isSnapshot true for a snapshot path
   */
  boolean isSnapshot() {
    return this.isSnapshot;
  }

  /**
   * @return if .snapshot is the last path component.
   */
  boolean isDotSnapshotDir() {
    return isDotSnapshotDir(getLastLocalName());
  }

  /**
   * @return if this is a /.reserved/raw path.
   */
  public boolean isRaw() {
    return isRaw;
  }

  private static String toString(INode inode) {
    return inode == null? null: inode.getLocalName();
  }

  @Override
  public String toString() {
    return toString(true);
  }

  private String toString(boolean vaildateObject) {
    if (vaildateObject) {
      validate();
    }

    final StringBuilder b = new StringBuilder(getClass().getSimpleName())
        .append(": path = ").append(DFSUtil.byteArray2PathString(path))
        .append("\n  inodes = ");
    if (inodes == null) {
      b.append("null");
    } else if (inodes.length == 0) {
      b.append("[]");
    } else {
      b.append("[").append(toString(inodes[0]));
      for(int i = 1; i < inodes.length; i++) {
        b.append(", ").append(toString(inodes[i]));
      }
      b.append("], length=").append(inodes.length);
    }
    b.append("\n  isSnapshot        = ").append(isSnapshot)
     .append("\n  snapshotId        = ").append(snapshotId);
    return b.toString();
  }

  void validate() {
    // check parent up to snapshotRootIndex if this is a snapshot path
    int i = 0;
    if (inodes[i] != null) {
      for(i++; i < inodes.length && inodes[i] != null; i++) {
        final INodeDirectory parent_i = inodes[i].getParent();
        final INodeDirectory parent_i_1 = inodes[i-1].getParent();
        if (parent_i != inodes[i-1] &&
            (parent_i_1 == null || !parent_i_1.isSnapshottable()
                || parent_i != parent_i_1)) {
          throw new AssertionError(
              "inodes[" + i + "].getParent() != inodes[" + (i-1)
              + "]\n  inodes[" + i + "]=" + inodes[i].toDetailString()
              + "\n  inodes[" + (i-1) + "]=" + inodes[i-1].toDetailString()
              + "\n this=" + toString(false));
        }
      }
    }
    if (i != inodes.length) {
      throw new AssertionError("i = " + i + " != " + inodes.length
          + ", this=" + toString(false));
    }
  }
}
