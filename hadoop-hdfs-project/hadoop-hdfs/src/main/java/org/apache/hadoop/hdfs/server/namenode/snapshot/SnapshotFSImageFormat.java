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
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot.ChildrenDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot.SnapshotDiff;
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
  public static void saveSnapshotDiffs(INodeDirectoryWithSnapshot sNode,
      DataOutputStream out) throws IOException {
    // # of SnapshotDiff
    List<SnapshotDiff> diffs = sNode.getSnapshotDiffs();
    // Record the SnapshotDiff in reversed order, so that we can find the
    // correct reference for INodes in the created list when loading the
    // FSImage
    out.writeInt(diffs.size());
    for (int i = diffs.size() - 1; i >= 0; i--) {
      SnapshotDiff sdiff = diffs.get(i);
      sdiff.write(out);
    }
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
    for (SnapshotDiff postDiff : parent.getSnapshotDiffs()) {
      INode created = findCreated(createdNodeName, postDiff.getDiff());
      if (created != null) {
        return created;
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
   * Search the given {@link ChildrenDiff} to find an inode matching the specific name.
   * @param createdNodeName The name of the node for searching.
   * @param diff The given {@link ChildrenDiff} where to search the node.
   * @return The matched inode. Return null if no matched inode can be found.
   */
  private static INode findCreated(byte[] createdNodeName, ChildrenDiff diff) {
    INode c = diff.searchCreated(createdNodeName);
    INode d = diff.searchDeleted(createdNodeName);
    if (c == null && d != null) {
      // if an INode with the same name is only contained in the deleted
      // list, then the node should be the snapshot copy of a deleted
      // node, and the node in the created list should be its reference 
      return d;
    } else if (c != null && d != null) {
      // in a posterior SnapshotDiff, if the created/deleted lists both
      // contains nodes with the same name (c & d), there are two
      // possibilities:
      // 
      // 1) c and d are used to represent a modification, and 
      // 2) d indicates the deletion of the node, while c was originally
      // contained in the created list of a later snapshot, but c was
      // moved here because of the snapshot deletion.
      // 
      // For case 1), c and d should be both INodeFile and should share
      // the same blockInfo list.
      if (c.isFile() && INodeFile.isOfSameFile((INodeFile) c, (INodeFile) d)) {
        return c;
      } else {
        return d;
      }
    }
    return null;
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
   * @param loader The {@link Loader} instance. Used to call the
   *               {@link Loader#loadINode(DataInputStream)} method.
   * @return The deleted list.
   */
  private static List<INode> loadDeletedList(INodeDirectoryWithSnapshot parent,
      List<INode> createdList, DataInputStream in, FSImageFormat.Loader loader)
      throws IOException {
    int deletedSize = in.readInt();
    List<INode> deletedList = new ArrayList<INode>(deletedSize);
    for (int i = 0; i < deletedSize; i++) {
      byte[] deletedNodeName = new byte[in.readShort()];
      in.readFully(deletedNodeName);
      INode deleted = loader.loadINode(in);
      deleted.setLocalName(deletedNodeName);
      deletedList.add(deleted);
      // set parent: the parent field of an INode in the deleted list is not 
      // useful, but set the parent here to be consistent with the original 
      // fsdir tree.
      deleted.setParent(parent);
      if (deleted instanceof INodeFile
          && ((INodeFile) deleted).getBlocks() == null) {
        // if deleted is an INodeFile, and its blocks is null, then deleted
        // must be an INodeFileWithLink, and we need to rebuild its next link
        int c = Collections.binarySearch(createdList, deletedNodeName);
        if (c < 0) {
          throw new IOException(
              "Cannot find the INode linked with the INode "
                  + DFSUtil.bytes2String(deletedNodeName)
                  + " in deleted list while loading FSImage.");
        }
        // deleted must be an FileWithSnapshot (INodeFileSnapshot or 
        // INodeFileUnderConstructionSnapshot)
        FileWithSnapshot deletedWithLink = (FileWithSnapshot) deleted;
        INodeFile cNode = (INodeFile) createdList.get(c);
        INodeFileWithSnapshot cNodeWithLink = (INodeFileWithSnapshot) cNode;
        ((INodeFile) deleted).setBlocks(cNode.getBlocks());
        // insert deleted into the circular list
        cNodeWithLink.insertBefore(deletedWithLink);
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
    byte[] snapshotName = new byte[in.readShort()];
    in.readFully(snapshotName);
    INode rootNode = loader.loadINode(in);
    rootNode.setLocalName(snapshotName);
    rootNode.setParent(parent);
    return new Snapshot(snapshotId, (INodeDirectory) rootNode);
  }
  
  /**
   * Load the {@link SnapshotDiff} list for the INodeDirectoryWithSnapshot
   * directory.
   * @param snapshottableParent The snapshottable directory for loading.
   * @param numSnapshotDiffs The number of {@link SnapshotDiff} that the 
   *                         directory has.
   * @param in The {@link DataInputStream} instance to read.
   * @param loader The {@link Loader} instance that this loading procedure is 
   *               using.
   */
  public static void loadSnapshotDiffList(
      INodeDirectoryWithSnapshot parentWithSnapshot, int numSnapshotDiffs,
      DataInputStream in, FSImageFormat.Loader loader)
      throws IOException {
    for (int i = 0; i < numSnapshotDiffs; i++) {
      SnapshotDiff diff = loadSnapshotDiff(parentWithSnapshot, in, loader);
      parentWithSnapshot.insertDiff(diff);
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
  private static INodeDirectory loadSnapshotINodeInSnapshotDiff(
      Snapshot snapshot, DataInputStream in, FSImageFormat.Loader loader)
      throws IOException {
    // read the boolean indicating whether snapshotINode == Snapshot.Root
    boolean useRoot = in.readBoolean();      
    if (useRoot) {
      return snapshot.getRoot();
    } else {
      // another boolean is used to indicate whether snapshotINode is non-null
      if (in.readBoolean()) {
        byte[] localName = new byte[in.readShort()];
        in.readFully(localName);
        INodeDirectory snapshotINode = (INodeDirectory) loader.loadINode(in);
        snapshotINode.setLocalName(localName);
        return snapshotINode;
      }
    }
    return null;
  }
   
  /**
   * Load {@link SnapshotDiff} from fsimage.
   * @param parent The directory that the SnapshotDiff belongs to.
   * @param in The {@link DataInputStream} instance to read.
   * @param loader The {@link Loader} instance that this loading procedure is 
   *               using.
   * @return A {@link SnapshotDiff}.
   */
  private static SnapshotDiff loadSnapshotDiff(
      INodeDirectoryWithSnapshot parent, DataInputStream in,
      FSImageFormat.Loader loader) throws IOException {
    // 1. Load SnapshotDiff#childrenSize
    int childrenSize = in.readInt();
    // 2. Read the full path of the Snapshot's Root, identify 
    //    SnapshotDiff#Snapshot
    Snapshot snapshot = findSnapshot(FSImageSerialization.readString(in),
        loader.getFSDirectoryInLoading());
    
    // 3. Load SnapshotDiff#snapshotINode 
    INodeDirectory snapshotINode = loadSnapshotINodeInSnapshotDiff(snapshot,
        in, loader);
    
    // 4. Load the created list in SnapshotDiff#Diff
    List<INode> createdList = loadCreatedList(parent, in);
    
    // 5. Load the deleted list in SnapshotDiff#Diff
    List<INode> deletedList = loadDeletedList(parent, createdList, in, loader);
    
    // 6. Compose the SnapshotDiff
    SnapshotDiff sdiff = parent.new SnapshotDiff(snapshot, childrenSize,
        snapshotINode, parent.getSnapshotDiffs().isEmpty() ? null : parent
            .getSnapshotDiffs().get(0), createdList, deletedList);
    return sdiff;
  }
  
}
