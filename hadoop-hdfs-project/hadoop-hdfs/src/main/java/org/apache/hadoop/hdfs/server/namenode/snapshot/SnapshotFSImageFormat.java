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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiffList;
import org.apache.hadoop.hdfs.tools.snapshot.SnapshotDiff;
import org.apache.hadoop.hdfs.util.Diff.ListType;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormat.Loader;

/**
 * A helper class defining static methods for reading/writing snapshot related
 * information from/to FSImage.
 */
public class SnapshotFSImageFormat {
  public static FileDiffList loadFileDiffList(DataInput in,
      FSImageFormat.Loader loader) throws IOException {
    final int size = in.readInt();
    if (size == -1) {
      return null;
    } else {
      final FileDiffList diffs = new FileDiffList();
      FileDiff posterior = null;
      for(int i = 0; i < size; i++) {
        final FileDiff d = loadFileDiff(posterior, in, loader);
        diffs.addFirst(d);
        posterior = d;
      }
      return diffs;
    }
  }

  private static FileDiff loadFileDiff(FileDiff posterior, DataInput in,
      FSImageFormat.Loader loader) throws IOException {
    // 1. Read the id of the Snapshot root to identify the Snapshot
    final Snapshot snapshot = loader.getSnapshot(in);

    // 2. Load file size
    final long fileSize = in.readLong();
    
    // 3. Load snapshotINode 
    final INodeFileAttributes snapshotINode = in.readBoolean()?
        loader.loadINodeFileAttributes(in): null;
    
    return new FileDiff(snapshot.getId(), snapshotINode, posterior, fileSize);
  }

  /**
   * Load a node stored in the created list from fsimage.
   * @param createdNodeName The name of the created node.
   * @param parent The directory that the created list belongs to.
   * @return The created node.
   */
  public static INode loadCreated(byte[] createdNodeName,
      INodeDirectory parent) throws IOException {
    // the INode in the created list should be a reference to another INode
    // in posterior SnapshotDiffs or one of the current children
    for (DirectoryDiff postDiff : parent.getDiffs()) {
      final INode d = postDiff.getChildrenDiff().search(ListType.DELETED,
          createdNodeName);
      if (d != null) {
        return d;
      } // else go to the next SnapshotDiff
    } 
    // use the current child
    INode currentChild = parent.getChild(createdNodeName,
        Snapshot.CURRENT_STATE_ID);
    if (currentChild == null) {
      throw new IOException("Cannot find an INode associated with the INode "
          + DFSUtil.bytes2String(createdNodeName)
          + " in created list while loading FSImage.");
    }
    return currentChild;
  }
  
  /**
   * Load the created list from fsimage.
   * @param parent The directory that the created list belongs to.
   * @param in The {@link DataInput} to read.
   * @return The created list.
   */
  private static List<INode> loadCreatedList(INodeDirectory parent,
      DataInput in) throws IOException {
    // read the size of the created list
    int createdSize = in.readInt();
    List<INode> createdList = new ArrayList<INode>(createdSize);
    for (int i = 0; i < createdSize; i++) {
      byte[] createdNodeName = FSImageSerialization.readLocalName(in);
      INode created = loadCreated(createdNodeName, parent);
      createdList.add(created);
    }
    return createdList;
  }
    
  /**
   * Load the deleted list from the fsimage.
   * 
   * @param parent The directory that the deleted list belongs to.
   * @param createdList The created list associated with the deleted list in 
   *                    the same Diff.
   * @param in The {@link DataInput} to read.
   * @param loader The {@link Loader} instance.
   * @return The deleted list.
   */
  private static List<INode> loadDeletedList(INodeDirectory parent,
      List<INode> createdList, DataInput in, FSImageFormat.Loader loader)
      throws IOException {
    int deletedSize = in.readInt();
    List<INode> deletedList = new ArrayList<INode>(deletedSize);
    for (int i = 0; i < deletedSize; i++) {
      final INode deleted = loader.loadINodeWithLocalName(true, in, true);
      deletedList.add(deleted);
      // set parent: the parent field of an INode in the deleted list is not 
      // useful, but set the parent here to be consistent with the original 
      // fsdir tree.
      deleted.setParent(parent);
      if (deleted.isFile()) {
        loader.updateBlocksMap(deleted.asFile());
      }
    }
    return deletedList;
  }
  
  /**
   * Load snapshots and snapshotQuota for a Snapshottable directory.
   *
   * @param snapshottableParent
   *          The snapshottable directory for loading.
   * @param numSnapshots
   *          The number of snapshots that the directory has.
   * @param loader
   *          The loader
   */
  public static void loadSnapshotList(
      INodeDirectorySnapshottable snapshottableParent, int numSnapshots,
      DataInput in, FSImageFormat.Loader loader) throws IOException {
    for (int i = 0; i < numSnapshots; i++) {
      // read snapshots
      final Snapshot s = loader.getSnapshot(in);
      s.getRoot().setParent(snapshottableParent);
      snapshottableParent.addSnapshot(s);
    }
    int snapshotQuota = in.readInt();
    snapshottableParent.setSnapshotQuota(snapshotQuota);
  }
  
  /**
   * Load the {@link SnapshotDiff} list for the INodeDirectoryWithSnapshot
   * directory.
   *
   * @param dir
   *          The snapshottable directory for loading.
   * @param in
   *          The {@link DataInput} instance to read.
   * @param loader
   *          The loader
   */
  public static void loadDirectoryDiffList(INodeDirectory dir,
      DataInput in, FSImageFormat.Loader loader) throws IOException {
    final int size = in.readInt();
    if (dir.isWithSnapshot()) {
      DirectoryDiffList diffs = dir.getDiffs();
      for (int i = 0; i < size; i++) {
        diffs.addFirst(loadDirectoryDiff(dir, in, loader));
      }
    }
  }

  /**
   * Load the snapshotINode field of {@link AbstractINodeDiff}.
   * @param snapshot The Snapshot associated with the {@link AbstractINodeDiff}.
   * @param in The {@link DataInput} to read.
   * @param loader The {@link Loader} instance that this loading procedure is
   *               using.
   * @return The snapshotINode.
   */
  private static INodeDirectoryAttributes loadSnapshotINodeInDirectoryDiff(
      Snapshot snapshot, DataInput in, FSImageFormat.Loader loader)
      throws IOException {
    // read the boolean indicating whether snapshotINode == Snapshot.Root
    boolean useRoot = in.readBoolean();      
    if (useRoot) {
      return snapshot.getRoot();
    } else {
      // another boolean is used to indicate whether snapshotINode is non-null
      return in.readBoolean()? loader.loadINodeDirectoryAttributes(in): null;
    }
  }
   
  /**
   * Load {@link DirectoryDiff} from fsimage.
   * @param parent The directory that the SnapshotDiff belongs to.
   * @param in The {@link DataInput} instance to read.
   * @param loader The {@link Loader} instance that this loading procedure is 
   *               using.
   * @return A {@link DirectoryDiff}.
   */
  private static DirectoryDiff loadDirectoryDiff(INodeDirectory parent,
      DataInput in, FSImageFormat.Loader loader) throws IOException {
    // 1. Read the full path of the Snapshot root to identify the Snapshot
    final Snapshot snapshot = loader.getSnapshot(in);

    // 2. Load DirectoryDiff#childrenSize
    int childrenSize = in.readInt();
    
    // 3. Load DirectoryDiff#snapshotINode 
    INodeDirectoryAttributes snapshotINode = loadSnapshotINodeInDirectoryDiff(
        snapshot, in, loader);
    
    // 4. Load the created list in SnapshotDiff#Diff
    List<INode> createdList = loadCreatedList(parent, in);
    
    // 5. Load the deleted list in SnapshotDiff#Diff
    List<INode> deletedList = loadDeletedList(parent, createdList, in, loader);
    
    // 6. Compose the SnapshotDiff
    List<DirectoryDiff> diffs = parent.getDiffs().asList();
    DirectoryDiff sdiff = new DirectoryDiff(snapshot.getId(), snapshotINode,
        diffs.isEmpty() ? null : diffs.get(0), childrenSize, createdList,
        deletedList, snapshotINode == snapshot.getRoot());
    return sdiff;
  }
  

  /** A reference map for fsimage serialization. */
  public static class ReferenceMap {
    /**
     * Used to indicate whether the reference node itself has been saved
     */
    private final Map<Long, INodeReference.WithCount> referenceMap
        = new HashMap<Long, INodeReference.WithCount>();
    /**
     * Used to record whether the subtree of the reference node has been saved 
     */
    private final Map<Long, Long> dirMap = new HashMap<Long, Long>();
    
    public boolean toProcessSubtree(long id) {
      if (dirMap.containsKey(id)) {
        return false;
      } else {
        dirMap.put(id, id);
        return true;
      }
    }
    
    public INodeReference.WithCount loadINodeReferenceWithCount(
        boolean isSnapshotINode, DataInput in, FSImageFormat.Loader loader
        ) throws IOException {
      final boolean firstReferred = in.readBoolean();

      final INodeReference.WithCount withCount;
      if (firstReferred) {
        final INode referred = loader.loadINodeWithLocalName(isSnapshotINode,
            in, true);
        withCount = new INodeReference.WithCount(null, referred);
        referenceMap.put(withCount.getId(), withCount);
      } else {
        final long id = in.readLong();
        withCount = referenceMap.get(id);
      }
      return withCount;
    }
  }
}
