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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormat.Loader;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshot.FileDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshot.FileDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot.DirectoryDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot.DirectoryDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.Root;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

/**
 * A helper class defining static methods for reading/writing snapshot related
 * information from/to FSImage.
 */
public class SnapshotFSImageFormat {

  /**
   * Save snapshots and snapshot quota for a snapshottable directory.
   * @param current The directory that the snapshots belongs to.
   * @param out The {@link DataOutputStream} to write.
   * @throws IOException
   */
  public static void saveSnapshots(INodeDirectorySnapshottable current,
      DataOutputStream out) throws IOException {
    // list of snapshots in snapshotsByNames
    ReadOnlyList<Snapshot> snapshots = current.getSnapshotsByNames();
    out.writeInt(snapshots.size());
    for (Snapshot ss : snapshots) {
      // write the snapshot
      ss.write(out);
    }
    // snapshot quota
    out.writeInt(current.getSnapshotQuota());
  }
  
  /**
   * Save SnapshotDiff list for an INodeDirectoryWithSnapshot.
   * @param sNode The directory that the SnapshotDiff list belongs to.
   * @param out The {@link DataOutputStream} to write.
   */
  private static <N extends INode, D extends AbstractINodeDiff<N, D>>
      void saveINodeDiffs(final AbstractINodeDiffList<N, D> diffs,
      final DataOutputStream out) throws IOException {
    // Record the diffs in reversed order, so that we can find the correct
    // reference for INodes in the created list when loading the FSImage
    if (diffs == null) {
      out.writeInt(-1); // no diffs
    } else {
      final List<D> list = diffs.asList();
      final int size = list.size();
      out.writeInt(size);
      for (int i = size - 1; i >= 0; i--) {
        list.get(i).write(out);
      }
    }
  }
  
  public static void saveDirectoryDiffList(final INodeDirectory dir,
      final DataOutputStream out) throws IOException {
    saveINodeDiffs(dir instanceof INodeDirectoryWithSnapshot?
        ((INodeDirectoryWithSnapshot)dir).getDiffs(): null, out);
  }
  
  public static void saveFileDiffList(final INodeFile file,
      final DataOutputStream out) throws IOException {
    saveINodeDiffs(file instanceof FileWithSnapshot?
        ((FileWithSnapshot)file).getDiffs(): null, out);
  }

  public static FileDiffList loadFileDiffList(DataInputStream in,
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

  private static FileDiff loadFileDiff(FileDiff posterior, DataInputStream in,
      FSImageFormat.Loader loader) throws IOException {
    // 1. Read the full path of the Snapshot root to identify the Snapshot
    Snapshot snapshot = findSnapshot(FSImageSerialization.readString(in),
        loader.getFSDirectoryInLoading());

    // 2. Load file size
    final long fileSize = in.readLong();
    
    // 3. Load snapshotINode 
    final INodeFile snapshotINode = in.readBoolean()?
        (INodeFile) loader.loadINodeWithLocalName(true, in): null;
    
    return new FileDiff(snapshot, snapshotINode, posterior, fileSize);
  }

  /**
   * Load a node stored in the created list from fsimage.
   * @param createdNodeName The name of the created node.
   * @param parent The directory that the created list belongs to.
   * @return The created node.
   */
  private static INode loadCreated(byte[] createdNodeName,
      INodeDirectoryWithSnapshot parent) throws IOException {
    // the INode in the created list should be a reference to another INode
    // in posterior SnapshotDiffs or one of the current children
    for (DirectoryDiff postDiff : parent.getDiffs()) {
      INode d = postDiff.getChildrenDiff().searchDeleted(createdNodeName);
      if (d != null) {
        return d;
      } // else go to the next SnapshotDiff
    } 
    // use the current child
    INode currentChild = parent.getChild(createdNodeName, null);
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
   * @param in The {@link DataInputStream} to read.
   * @return The created list.
   */
  private static List<INode> loadCreatedList(INodeDirectoryWithSnapshot parent,
      DataInputStream in) throws IOException {
    // read the size of the created list
    int createdSize = in.readInt();
    List<INode> createdList = new ArrayList<INode>(createdSize);
    for (int i = 0; i < createdSize; i++) {
      byte[] createdNodeName = new byte[in.readShort()];
      in.readFully(createdNodeName);
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
   * @param in The {@link DataInputStream} to read.
   * @param loader The {@link Loader} instance.
   * @return The deleted list.
   */
  private static List<INode> loadDeletedList(INodeDirectoryWithSnapshot parent,
      List<INode> createdList, DataInputStream in, FSImageFormat.Loader loader)
      throws IOException {
    int deletedSize = in.readInt();
    List<INode> deletedList = new ArrayList<INode>(deletedSize);
    for (int i = 0; i < deletedSize; i++) {
      final INode deleted = loader.loadINodeWithLocalName(true, in);
      deletedList.add(deleted);
      // set parent: the parent field of an INode in the deleted list is not 
      // useful, but set the parent here to be consistent with the original 
      // fsdir tree.
      deleted.setParent(parent);
      if (deleted instanceof INodeFile
          && ((INodeFile) deleted).getBlocks() == null) {
        // if deleted is an INodeFile, and its blocks is null, then deleted
        // must be an INodeFileWithLink, and we need to rebuild its next link
        int c = Collections.binarySearch(createdList, deleted.getLocalNameBytes());
        if (c < 0) {
          throw new IOException(
              "Cannot find the INode linked with the INode "
                  + deleted.getLocalName()
                  + " in deleted list while loading FSImage.");
        }
        // deleted must be an FileWithSnapshot (INodeFileSnapshot or 
        // INodeFileUnderConstructionSnapshot)
        INodeFile cNode = (INodeFile) createdList.get(c);
        ((INodeFile) deleted).setBlocks(cNode.getBlocks());
      }
    }
    return deletedList;
  }
  
  /**
   * Load snapshots and snapshotQuota for a Snapshottable directory.
   * @param snapshottableParent The snapshottable directory for loading.
   * @param numSnapshots The number of snapshots that the directory has.
   * @param in The {@link DataInputStream} instance to read.
   * @param loader The {@link Loader} instance that this loading procedure is 
   *               using.
   */
  public static void loadSnapshotList(
      INodeDirectorySnapshottable snapshottableParent, int numSnapshots,
      DataInputStream in, FSImageFormat.Loader loader) throws IOException {
    for (int i = 0; i < numSnapshots; i++) {
      // read snapshots
      Snapshot ss = loadSnapshot(snapshottableParent, in, loader);
      snapshottableParent.addSnapshot(ss);
    }
    int snapshotQuota = in.readInt();
    snapshottableParent.setSnapshotQuota(snapshotQuota);
  }
  
  /**
   * Load a {@link Snapshot} from fsimage.
   * @param parent The directory that the snapshot belongs to.
   * @param in The {@link DataInputStream} instance to read.
   * @param loader The {@link Loader} instance that this loading procedure is 
   *               using.
   * @return The snapshot.
   */
  private static Snapshot loadSnapshot(INodeDirectorySnapshottable parent,
      DataInputStream in, FSImageFormat.Loader loader) throws IOException {
    int snapshotId = in.readInt();
    INodeDirectory rootNode = (INodeDirectory)loader.loadINodeWithLocalName(
        false, in);
    return new Snapshot(snapshotId, rootNode, parent);
  }
  
  /**
   * Load the {@link SnapshotDiff} list for the INodeDirectoryWithSnapshot
   * directory.
   * @param dir The snapshottable directory for loading.
   * @param numSnapshotDiffs The number of {@link SnapshotDiff} that the 
   *                         directory has.
   * @param in The {@link DataInputStream} instance to read.
   * @param loader The {@link Loader} instance that this loading procedure is 
   *               using.
   */
  public static void loadDirectoryDiffList(INodeDirectory dir,
      DataInputStream in, FSImageFormat.Loader loader) throws IOException {
    final int size = in.readInt();
    if (size != -1) {
      INodeDirectoryWithSnapshot withSnapshot = (INodeDirectoryWithSnapshot)dir;
      DirectoryDiffList diffs = withSnapshot.getDiffs();
      for (int i = 0; i < size; i++) {
        diffs.addFirst(loadDirectoryDiff(withSnapshot, in, loader));
      }
    }
  }
  
  /**
   * Use the given full path to a {@link Root} directory to find the
   * associated snapshot.
   */
  private static Snapshot findSnapshot(String sRootFullPath, FSDirectory fsdir)
      throws IOException {
    // find the root
    INode root = fsdir.getINode(sRootFullPath);
    INodeDirectorySnapshottable snapshotRoot = INodeDirectorySnapshottable
        .valueOf(root.getParent(), root.getParent().getFullPathName());
    // find the snapshot
    return snapshotRoot.getSnapshot(root.getLocalNameBytes());
  }
  
  /**
   * Load the snapshotINode field of {@link SnapshotDiff}.
   * @param snapshot The Snapshot associated with the {@link SnapshotDiff}.
   * @param in The {@link DataInputStream} to read.
   * @param loader The {@link Loader} instance that this loading procedure is 
   *               using.
   * @return The snapshotINode.
   */
  private static INodeDirectory loadSnapshotINodeInDirectoryDiff(
      Snapshot snapshot, DataInputStream in, FSImageFormat.Loader loader)
      throws IOException {
    // read the boolean indicating whether snapshotINode == Snapshot.Root
    boolean useRoot = in.readBoolean();      
    if (useRoot) {
      return snapshot.getRoot();
    } else {
      // another boolean is used to indicate whether snapshotINode is non-null
      return in.readBoolean()?
          (INodeDirectory) loader.loadINodeWithLocalName(true, in): null;
    }
  }
   
  /**
   * Load {@link DirectoryDiff} from fsimage.
   * @param parent The directory that the SnapshotDiff belongs to.
   * @param in The {@link DataInputStream} instance to read.
   * @param loader The {@link Loader} instance that this loading procedure is 
   *               using.
   * @return A {@link DirectoryDiff}.
   */
  private static DirectoryDiff loadDirectoryDiff(
      INodeDirectoryWithSnapshot parent, DataInputStream in,
      FSImageFormat.Loader loader) throws IOException {
    // 1. Read the full path of the Snapshot root to identify the Snapshot
    Snapshot snapshot = findSnapshot(FSImageSerialization.readString(in),
        loader.getFSDirectoryInLoading());

    // 2. Load DirectoryDiff#childrenSize
    int childrenSize = in.readInt();
    
    // 3. Load DirectoryDiff#snapshotINode 
    INodeDirectory snapshotINode = loadSnapshotINodeInDirectoryDiff(snapshot,
        in, loader);
    
    // 4. Load the created list in SnapshotDiff#Diff
    List<INode> createdList = loadCreatedList(parent, in);
    
    // 5. Load the deleted list in SnapshotDiff#Diff
    List<INode> deletedList = loadDeletedList(parent, createdList, in, loader);
    
    // 6. Compose the SnapshotDiff
    List<DirectoryDiff> diffs = parent.getDiffs().asList();
    DirectoryDiff sdiff = new DirectoryDiff(snapshot, snapshotINode,
        diffs.isEmpty() ? null : diffs.get(0),
        childrenSize, createdList, deletedList);
    return sdiff;
  }
  
}
