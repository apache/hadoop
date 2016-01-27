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

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat.ReferenceMap;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Contains inner classes for reading or writing the on-disk format for
 * FSImages.
 *
 * In particular, the format of the FSImage looks like:
 * <pre>
 * FSImage {
 *   layoutVersion: int, namespaceID: int, numberItemsInFSDirectoryTree: long,
 *   namesystemGenerationStampV1: long, namesystemGenerationStampV2: long,
 *   generationStampAtBlockIdSwitch:long, lastAllocatedBlockId:
 *   long transactionID: long, snapshotCounter: int, numberOfSnapshots: int,
 *   numOfSnapshottableDirs: int,
 *   {FSDirectoryTree, FilesUnderConstruction, SecretManagerState} (can be compressed)
 * }
 *
 * FSDirectoryTree (if {@link Feature#FSIMAGE_NAME_OPTIMIZATION} is supported) {
 *   INodeInfo of root, numberOfChildren of root: int
 *   [list of INodeInfo of root's children],
 *   [list of INodeDirectoryInfo of root's directory children]
 * }
 *
 * FSDirectoryTree (if {@link Feature#FSIMAGE_NAME_OPTIMIZATION} not supported){
 *   [list of INodeInfo of INodes in topological order]
 * }
 *
 * INodeInfo {
 *   {
 *     localName: short + byte[]
 *   } when {@link Feature#FSIMAGE_NAME_OPTIMIZATION} is supported
 *   or
 *   {
 *     fullPath: byte[]
 *   } when {@link Feature#FSIMAGE_NAME_OPTIMIZATION} is not supported
 *   replicationFactor: short, modificationTime: long,
 *   accessTime: long, preferredBlockSize: long,
 *   numberOfBlocks: int (-1 for INodeDirectory, -2 for INodeSymLink),
 *   {
 *     nsQuota: long, dsQuota: long,
 *     {
 *       isINodeSnapshottable: byte,
 *       isINodeWithSnapshot: byte (if isINodeSnapshottable is false)
 *     } (when {@link Feature#SNAPSHOT} is supported),
 *     fsPermission: short, PermissionStatus
 *   } for INodeDirectory
 *   or
 *   {
 *     symlinkString, fsPermission: short, PermissionStatus
 *   } for INodeSymlink
 *   or
 *   {
 *     [list of BlockInfo]
 *     [list of FileDiff]
 *     {
 *       isINodeFileUnderConstructionSnapshot: byte,
 *       {clientName: short + byte[], clientMachine: short + byte[]} (when
 *       isINodeFileUnderConstructionSnapshot is true),
 *     } (when {@link Feature#SNAPSHOT} is supported and writing snapshotINode),
 *     fsPermission: short, PermissionStatus
 *   } for INodeFile
 * }
 *
 * INodeDirectoryInfo {
 *   fullPath of the directory: short + byte[],
 *   numberOfChildren: int, [list of INodeInfo of children INode],
 *   {
 *     numberOfSnapshots: int,
 *     [list of Snapshot] (when NumberOfSnapshots is positive),
 *     numberOfDirectoryDiffs: int,
 *     [list of DirectoryDiff] (NumberOfDirectoryDiffs is positive),
 *     number of children that are directories,
 *     [list of INodeDirectoryInfo of the directory children] (includes
 *     snapshot copies of deleted sub-directories)
 *   } (when {@link Feature#SNAPSHOT} is supported),
 * }
 *
 * Snapshot {
 *   snapshotID: int, root of Snapshot: INodeDirectoryInfo (its local name is
 *   the name of the snapshot)
 * }
 *
 * DirectoryDiff {
 *   full path of the root of the associated Snapshot: short + byte[],
 *   childrenSize: int,
 *   isSnapshotRoot: byte,
 *   snapshotINodeIsNotNull: byte (when isSnapshotRoot is false),
 *   snapshotINode: INodeDirectory (when SnapshotINodeIsNotNull is true), Diff
 * }
 *
 * Diff {
 *   createdListSize: int, [Local name of INode in created list],
 *   deletedListSize: int, [INode in deleted list: INodeInfo]
 * }
 *
 * FileDiff {
 *   full path of the root of the associated Snapshot: short + byte[],
 *   fileSize: long,
 *   snapshotINodeIsNotNull: byte,
 *   snapshotINode: INodeFile (when SnapshotINodeIsNotNull is true), Diff
 * }
 * </pre>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSImageFormat {
  private static final Log LOG = FSImage.LOG;

  // Static-only class
  private FSImageFormat() {}

  interface AbstractLoader {
    MD5Hash getLoadedImageMd5();
    long getLoadedImageTxId();
  }

  static class LoaderDelegator implements AbstractLoader {
    private AbstractLoader impl;
    private final Configuration conf;
    private final FSNamesystem fsn;

    LoaderDelegator(Configuration conf, FSNamesystem fsn) {
      this.conf = conf;
      this.fsn = fsn;
    }

    @Override
    public MD5Hash getLoadedImageMd5() {
      return impl.getLoadedImageMd5();
    }

    @Override
    public long getLoadedImageTxId() {
      return impl.getLoadedImageTxId();
    }

    public void load(File file, boolean requireSameLayoutVersion)
        throws IOException {
      Preconditions.checkState(impl == null, "Image already loaded!");

      FileInputStream is = null;
      try {
        is = new FileInputStream(file);
        byte[] magic = new byte[FSImageUtil.MAGIC_HEADER.length];
        IOUtils.readFully(is, magic, 0, magic.length);
        if (Arrays.equals(magic, FSImageUtil.MAGIC_HEADER)) {
          FSImageFormatProtobuf.Loader loader = new FSImageFormatProtobuf.Loader(
              conf, fsn, requireSameLayoutVersion);
          impl = loader;
          loader.load(file);
        } else {
          Loader loader = new Loader(conf, fsn);
          impl = loader;
          loader.load(file);
        }
      } finally {
        IOUtils.cleanup(LOG, is);
      }
    }
  }

  /**
   * Construct a loader class to load the image. It chooses the loader based on
   * the layout version.
   */
  public static LoaderDelegator newLoader(Configuration conf, FSNamesystem fsn) {
    return new LoaderDelegator(conf, fsn);
  }

  /**
   * A one-shot class responsible for loading an image. The load() function
   * should be called once, after which the getter methods may be used to retrieve
   * information about the image that was loaded, if loading was successful.
   */
  public static class Loader implements AbstractLoader {
    private final Configuration conf;
    /** which namesystem this loader is working for */
    private final FSNamesystem namesystem;

    /** Set to true once a file has been loaded using this loader. */
    private boolean loaded = false;

    /** The transaction ID of the last edit represented by the loaded file */
    private long imgTxId;
    /** The MD5 sum of the loaded file */
    private MD5Hash imgDigest;
    
    private Map<Integer, Snapshot> snapshotMap = null;
    private final ReferenceMap referenceMap = new ReferenceMap();

    Loader(Configuration conf, FSNamesystem namesystem) {
      this.conf = conf;
      this.namesystem = namesystem;
    }

    /**
     * Return the MD5 checksum of the image that has been loaded.
     * @throws IllegalStateException if load() has not yet been called.
     */
    @Override
    public MD5Hash getLoadedImageMd5() {
      checkLoaded();
      return imgDigest;
    }

    @Override
    public long getLoadedImageTxId() {
      checkLoaded();
      return imgTxId;
    }

    /**
     * Throw IllegalStateException if load() has not yet been called.
     */
    private void checkLoaded() {
      if (!loaded) {
        throw new IllegalStateException("Image not yet loaded!");
      }
    }

    /**
     * Throw IllegalStateException if load() has already been called.
     */
    private void checkNotLoaded() {
      if (loaded) {
        throw new IllegalStateException("Image already loaded!");
      }
    }

    public void load(File curFile) throws IOException {
      checkNotLoaded();
      assert curFile != null : "curFile is null";

      StartupProgress prog = NameNode.getStartupProgress();
      Step step = new Step(StepType.INODES);
      prog.beginStep(Phase.LOADING_FSIMAGE, step);
      long startTime = monotonicNow();

      //
      // Load in bits
      //
      MessageDigest digester = MD5Hash.getDigester();
      DigestInputStream fin = new DigestInputStream(
           new FileInputStream(curFile), digester);

      DataInputStream in = new DataInputStream(fin);
      try {
        // read image version: first appeared in version -1
        int imgVersion = in.readInt();
        if (getLayoutVersion() != imgVersion) {
          throw new InconsistentFSStateException(curFile, 
              "imgVersion " + imgVersion +
              " expected to be " + getLayoutVersion());
        }
        boolean supportSnapshot = NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.SNAPSHOT, imgVersion);
        if (NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.ADD_LAYOUT_FLAGS, imgVersion)) {
          LayoutFlags.read(in);
        }

        // read namespaceID: first appeared in version -2
        in.readInt();

        long numFiles = in.readLong();

        // read in the last generation stamp for legacy blocks.
        long genstamp = in.readLong();
        final BlockIdManager blockIdManager = namesystem.getBlockManager()
            .getBlockIdManager();
        blockIdManager.setLegacyGenerationStamp(genstamp);

        if (NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.SEQUENTIAL_BLOCK_ID, imgVersion)) {
          // read the starting generation stamp for sequential block IDs
          genstamp = in.readLong();
          blockIdManager.setGenerationStamp(genstamp);

          // read the last generation stamp for blocks created after
          // the switch to sequential block IDs.
          long stampAtIdSwitch = in.readLong();
          blockIdManager.setLegacyGenerationStampLimit(stampAtIdSwitch);

          // read the max sequential block ID.
          long maxSequentialBlockId = in.readLong();
          blockIdManager.setLastAllocatedContiguousBlockId(maxSequentialBlockId);
        } else {
          long startingGenStamp = blockIdManager.upgradeLegacyGenerationStamp();
          // This is an upgrade.
          LOG.info("Upgrading to sequential block IDs. Generation stamp " +
                   "for new blocks set to " + startingGenStamp);
        }

        // read the transaction ID of the last edit represented by
        // this image
        if (NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.STORED_TXIDS, imgVersion)) {
          imgTxId = in.readLong();
        } else {
          imgTxId = 0;
        }

        // read the last allocated inode id in the fsimage
        if (NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.ADD_INODE_ID, imgVersion)) {
          long lastInodeId = in.readLong();
          namesystem.dir.resetLastInodeId(lastInodeId);
          if (LOG.isDebugEnabled()) {
            LOG.debug("load last allocated InodeId from fsimage:" + lastInodeId);
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Old layout version doesn't have inode id."
                + " Will assign new id for each inode.");
          }
        }
        
        if (supportSnapshot) {
          snapshotMap = namesystem.getSnapshotManager().read(in, this);
        }

        // read compression related info
        FSImageCompression compression;
        if (NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.FSIMAGE_COMPRESSION, imgVersion)) {
          compression = FSImageCompression.readCompressionHeader(conf, in);
        } else {
          compression = FSImageCompression.createNoopCompression();
        }
        in = compression.unwrapInputStream(fin);

        LOG.info("Loading image file " + curFile + " using " + compression);
        
        // load all inodes
        LOG.info("Number of files = " + numFiles);
        prog.setTotal(Phase.LOADING_FSIMAGE, step, numFiles);
        Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, step);
        if (NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.FSIMAGE_NAME_OPTIMIZATION, imgVersion)) {
          if (supportSnapshot) {
            loadLocalNameINodesWithSnapshot(numFiles, in, counter);
          } else {
            loadLocalNameINodes(numFiles, in, counter);
          }
        } else {
          loadFullNameINodes(numFiles, in, counter);
        }

        loadFilesUnderConstruction(in, supportSnapshot, counter);
        prog.endStep(Phase.LOADING_FSIMAGE, step);
        // Now that the step is finished, set counter equal to total to adjust
        // for possible under-counting due to reference inodes.
        prog.setCount(Phase.LOADING_FSIMAGE, step, numFiles);

        loadSecretManagerState(in);

        loadCacheManagerState(in);

        // make sure to read to the end of file
        boolean eof = (in.read() == -1);
        assert eof : "Should have reached the end of image file " + curFile;
      } finally {
        in.close();
      }

      imgDigest = new MD5Hash(digester.digest());
      loaded = true;
      
      LOG.info("Image file " + curFile + " of size " + curFile.length()
          + " bytes loaded in " + (monotonicNow() - startTime) / 1000
          + " seconds.");
    }

  /** Update the root node's attributes */
  private void updateRootAttr(INodeWithAdditionalFields root) {                                                           
    final QuotaCounts q = root.getQuotaCounts();
    final long nsQuota = q.getNameSpace();
    final long dsQuota = q.getStorageSpace();
    FSDirectory fsDir = namesystem.dir;
    if (nsQuota != -1 || dsQuota != -1) {
      fsDir.rootDir.getDirectoryWithQuotaFeature().setQuota(nsQuota, dsQuota);
    }
    fsDir.rootDir.cloneModificationTime(root);
    fsDir.rootDir.clonePermissionStatus(root);    
  }
  
    /**
     * Load fsimage files when 1) only local names are stored, 
     * and 2) snapshot is supported.
     * 
     * @param numFiles number of files expected to be read
     * @param in Image input stream
     * @param counter Counter to increment for namenode startup progress
     */
    private void loadLocalNameINodesWithSnapshot(long numFiles, DataInput in,
        Counter counter) throws IOException {
      assert NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.FSIMAGE_NAME_OPTIMIZATION, getLayoutVersion());
      assert NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.SNAPSHOT, getLayoutVersion());
      
      // load root
      loadRoot(in, counter);
      // load rest of the nodes recursively
      loadDirectoryWithSnapshot(in, counter);
    }
    
  /** 
   * load fsimage files assuming only local names are stored. Used when
   * snapshots are not supported by the layout version.
   *   
   * @param numFiles number of files expected to be read
   * @param in image input stream
   * @param counter Counter to increment for namenode startup progress
   * @throws IOException
   */  
   private void loadLocalNameINodes(long numFiles, DataInput in, Counter counter)
       throws IOException {
     assert NameNodeLayoutVersion.supports(
         LayoutVersion.Feature.FSIMAGE_NAME_OPTIMIZATION, getLayoutVersion());
     assert numFiles > 0;

     // load root
     loadRoot(in, counter);
     // have loaded the first file (the root)
     numFiles--; 

     // load rest of the nodes directory by directory
     while (numFiles > 0) {
       numFiles -= loadDirectory(in, counter);
     }
     if (numFiles != 0) {
       throw new IOException("Read unexpect number of files: " + -numFiles);
     }
   }
   
    /**
     * Load information about root, and use the information to update the root
     * directory of NameSystem.
     * @param in The {@link DataInput} instance to read.
     * @param counter Counter to increment for namenode startup progress
     */
    private void loadRoot(DataInput in, Counter counter)
        throws IOException {
      // load root
      if (in.readShort() != 0) {
        throw new IOException("First node is not root");
      }
      final INodeDirectory root = loadINode(null, false, in, counter)
        .asDirectory();
      // update the root's attributes
      updateRootAttr(root);
    }
   
    /** Load children nodes for the parent directory. */
    private int loadChildren(INodeDirectory parent, DataInput in,
        Counter counter) throws IOException {
      int numChildren = in.readInt();
      for (int i = 0; i < numChildren; i++) {
        // load single inode
        INode newNode = loadINodeWithLocalName(false, in, true, counter);
        addToParent(parent, newNode);
      }
      return numChildren;
    }
    
    /**
     * Load a directory when snapshot is supported.
     * @param in The {@link DataInput} instance to read.
     * @param counter Counter to increment for namenode startup progress
     */
    private void loadDirectoryWithSnapshot(DataInput in, Counter counter)
        throws IOException {
      // Step 1. Identify the parent INode
      long inodeId = in.readLong();
      final INodeDirectory parent = this.namesystem.dir.getInode(inodeId)
          .asDirectory();
      
      // Check if the whole subtree has been saved (for reference nodes)
      boolean toLoadSubtree = referenceMap.toProcessSubtree(parent.getId());
      if (!toLoadSubtree) {
        return;
      }

      // Step 2. Load snapshots if parent is snapshottable
      int numSnapshots = in.readInt();
      if (numSnapshots >= 0) {
        // load snapshots and snapshotQuota
        SnapshotFSImageFormat.loadSnapshotList(parent, numSnapshots, in, this);
        if (parent.getDirectorySnapshottableFeature().getSnapshotQuota() > 0) {
          // add the directory to the snapshottable directory list in 
          // SnapshotManager. Note that we only add root when its snapshot quota
          // is positive.
          this.namesystem.getSnapshotManager().addSnapshottable(parent);
        }
      }

      // Step 3. Load children nodes under parent
      loadChildren(parent, in, counter);
      
      // Step 4. load Directory Diff List
      SnapshotFSImageFormat.loadDirectoryDiffList(parent, in, this);
      
      // Recursively load sub-directories, including snapshot copies of deleted
      // directories
      int numSubTree = in.readInt();
      for (int i = 0; i < numSubTree; i++) {
        loadDirectoryWithSnapshot(in, counter);
      }
    }
    
   /**
    * Load all children of a directory
    * 
    * @param in input to load from
    * @param counter Counter to increment for namenode startup progress
    * @return number of child inodes read
    * @throws IOException
    */
   private int loadDirectory(DataInput in, Counter counter) throws IOException {
     String parentPath = FSImageSerialization.readString(in);
     // Rename .snapshot paths if we're doing an upgrade
     parentPath = renameReservedPathsOnUpgrade(parentPath, getLayoutVersion());
     final INodeDirectory parent = INodeDirectory.valueOf(
         namesystem.dir.getINode(parentPath, true), parentPath);
     return loadChildren(parent, in, counter);
   }

  /**
   * load fsimage files assuming full path names are stored
   * 
   * @param numFiles total number of files to load
   * @param in data input stream
   * @param counter Counter to increment for namenode startup progress
   * @throws IOException if any error occurs
   */
  private void loadFullNameINodes(long numFiles, DataInput in, Counter counter)
      throws IOException {
    byte[][] pathComponents;
    byte[][] parentPath = {{}};      
    FSDirectory fsDir = namesystem.dir;
    INodeDirectory parentINode = fsDir.rootDir;
    for (long i = 0; i < numFiles; i++) {
      pathComponents = FSImageSerialization.readPathComponents(in);
      for (int j=0; j < pathComponents.length; j++) {
        byte[] newComponent = renameReservedComponentOnUpgrade
            (pathComponents[j], getLayoutVersion());
        if (!Arrays.equals(newComponent, pathComponents[j])) {
          String oldPath = DFSUtil.byteArray2PathString(pathComponents);
          pathComponents[j] = newComponent;
          String newPath = DFSUtil.byteArray2PathString(pathComponents);
          LOG.info("Renaming reserved path " + oldPath + " to " + newPath);
        }
      }
      final INode newNode = loadINode(
          pathComponents[pathComponents.length-1], false, in, counter);

      if (isRoot(pathComponents)) { // it is the root
        // update the root's attributes
        updateRootAttr(newNode.asDirectory());
        continue;
      }

      namesystem.dir.addToInodeMap(newNode);
      // check if the new inode belongs to the same parent
      if(!isParent(pathComponents, parentPath)) {
        parentINode = getParentINodeDirectory(pathComponents);
        parentPath = getParent(pathComponents);
      }

      // add new inode
      addToParent(parentINode, newNode);
    }
  }

  private INodeDirectory getParentINodeDirectory(byte[][] pathComponents
      ) throws FileNotFoundException, PathIsNotDirectoryException,
      UnresolvedLinkException {
    if (pathComponents.length < 2) { // root
      return null;
    }
    // Gets the parent INode
    final INodesInPath inodes = namesystem.dir.getExistingPathINodes(
        pathComponents);
    return INodeDirectory.valueOf(inodes.getINode(-2), pathComponents);
  }

  /**
   * Add the child node to parent and, if child is a file, update block map.
   * This method is only used for image loading so that synchronization,
   * modification time update and space count update are not needed.
   */
  private void addToParent(INodeDirectory parent, INode child)
      throws IllegalReservedPathException {
    FSDirectory fsDir = namesystem.dir;
    if (parent == fsDir.rootDir) {
        child.setLocalName(renameReservedRootComponentOnUpgrade(
            child.getLocalNameBytes(), getLayoutVersion()));
    }
    // NOTE: This does not update space counts for parents
    if (!parent.addChild(child)) {
      return;
    }
    namesystem.dir.cacheName(child);

    if (child.isFile()) {
      updateBlocksMap(child.asFile());
    }
  }

    public void updateBlocksMap(INodeFile file) {
      // Add file->block mapping
      final BlockInfo[] blocks = file.getBlocks();
      if (blocks != null) {
        final BlockManager bm = namesystem.getBlockManager();
        for (int i = 0; i < blocks.length; i++) {
          file.setBlock(i, bm.addBlockCollectionWithCheck(blocks[i], file));
        } 
      }
    }

    /** @return The FSDirectory of the namesystem where the fsimage is loaded */
    public FSDirectory getFSDirectoryInLoading() {
      return namesystem.dir;
    }

    public INode loadINodeWithLocalName(boolean isSnapshotINode, DataInput in,
        boolean updateINodeMap) throws IOException {
      return loadINodeWithLocalName(isSnapshotINode, in, updateINodeMap, null);
    }

    public INode loadINodeWithLocalName(boolean isSnapshotINode,
        DataInput in, boolean updateINodeMap, Counter counter)
        throws IOException {
      byte[] localName = FSImageSerialization.readLocalName(in);
      localName =
          renameReservedComponentOnUpgrade(localName, getLayoutVersion());
      INode inode = loadINode(localName, isSnapshotINode, in, counter);
      if (updateINodeMap) {
        namesystem.dir.addToInodeMap(inode);
      }
      return inode;
    }
  
  /**
   * load an inode from fsimage except for its name
   * 
   * @param in data input stream from which image is read
   * @param counter Counter to increment for namenode startup progress
   * @return an inode
   */
  @SuppressWarnings("deprecation")
  INode loadINode(final byte[] localName, boolean isSnapshotINode,
      DataInput in, Counter counter) throws IOException {
    final int imgVersion = getLayoutVersion();
    if (NameNodeLayoutVersion.supports(
        LayoutVersion.Feature.SNAPSHOT, imgVersion)) {
      namesystem.getFSDirectory().verifyINodeName(localName);
    }

    long inodeId = NameNodeLayoutVersion.supports(
        LayoutVersion.Feature.ADD_INODE_ID, imgVersion) ? in.readLong()
        : namesystem.dir.allocateNewInodeId();
    
    final short replication = namesystem.getBlockManager().adjustReplication(
        in.readShort());
    final long modificationTime = in.readLong();
    long atime = 0;
    if (NameNodeLayoutVersion.supports(
        LayoutVersion.Feature.FILE_ACCESS_TIME, imgVersion)) {
      atime = in.readLong();
    }
    final long blockSize = in.readLong();
    final int numBlocks = in.readInt();

    if (numBlocks >= 0) {
      // file
      
      // read blocks
      BlockInfo[] blocks = new BlockInfoContiguous[numBlocks];
      for (int j = 0; j < numBlocks; j++) {
        blocks[j] = new BlockInfoContiguous(replication);
        blocks[j].readFields(in);
      }

      String clientName = "";
      String clientMachine = "";
      boolean underConstruction = false;
      FileDiffList fileDiffs = null;
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.SNAPSHOT, imgVersion)) {
        // read diffs
        fileDiffs = SnapshotFSImageFormat.loadFileDiffList(in, this);

        if (isSnapshotINode) {
          underConstruction = in.readBoolean();
          if (underConstruction) {
            clientName = FSImageSerialization.readString(in);
            clientMachine = FSImageSerialization.readString(in);
            // convert the last block to BlockUC
            if (blocks.length > 0) {
              BlockInfo lastBlk = blocks[blocks.length - 1];
              lastBlk.convertToBlockUnderConstruction(
                  HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION, null);
            }
          }
        }
      }

      final PermissionStatus permissions = PermissionStatus.read(in);

      // return
      if (counter != null) {
        counter.increment();
      }

      INodeFile file = new INodeFile(inodeId, localName, permissions,
          modificationTime, atime, (BlockInfoContiguous[]) blocks,
          replication, blockSize);
      if (underConstruction) {
        file.toUnderConstruction(clientName, clientMachine);
      }
      return fileDiffs == null ? file : new INodeFile(file, fileDiffs);
    } else if (numBlocks == -1) {
      //directory
      
      //read quotas
      final long nsQuota = in.readLong();
      long dsQuota = -1L;
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.DISKSPACE_QUOTA, imgVersion)) {
        dsQuota = in.readLong();
      }

      //read snapshot info
      boolean snapshottable = false;
      boolean withSnapshot = false;
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.SNAPSHOT, imgVersion)) {
        snapshottable = in.readBoolean();
        if (!snapshottable) {
          withSnapshot = in.readBoolean();
        }
      }

      final PermissionStatus permissions = PermissionStatus.read(in);

      //return
      if (counter != null) {
        counter.increment();
      }
      final INodeDirectory dir = new INodeDirectory(inodeId, localName,
          permissions, modificationTime);
      if (nsQuota >= 0 || dsQuota >= 0) {
        dir.addDirectoryWithQuotaFeature(new DirectoryWithQuotaFeature.Builder().
            nameSpaceQuota(nsQuota).storageSpaceQuota(dsQuota).build());
      }
      if (withSnapshot) {
        dir.addSnapshotFeature(null);
      }
      if (snapshottable) {
        dir.addSnapshottableFeature();
      }
      return dir;
    } else if (numBlocks == -2) {
      //symlink
      if (!FileSystem.areSymlinksEnabled()) {
        throw new IOException("Symlinks not supported - please remove symlink before upgrading to this version of HDFS");
      }

      final String symlink = Text.readString(in);
      final PermissionStatus permissions = PermissionStatus.read(in);
      if (counter != null) {
        counter.increment();
      }
      return new INodeSymlink(inodeId, localName, permissions,
          modificationTime, atime, symlink);
    } else if (numBlocks == -3) {
      //reference
      // Intentionally do not increment counter, because it is too difficult at
      // this point to assess whether or not this is a reference that counts
      // toward quota.
      
      final boolean isWithName = in.readBoolean();
      // lastSnapshotId for WithName node, dstSnapshotId for DstReference node
      int snapshotId = in.readInt();
      
      final INodeReference.WithCount withCount
          = referenceMap.loadINodeReferenceWithCount(isSnapshotINode, in, this);

      if (isWithName) {
          return new INodeReference.WithName(null, withCount, localName,
              snapshotId);
      } else {
        final INodeReference ref = new INodeReference.DstReference(null,
            withCount, snapshotId);
        return ref;
      }
    }
    
    throw new IOException("Unknown inode type: numBlocks=" + numBlocks);
  }

    /** Load {@link INodeFileAttributes}. */
    public INodeFileAttributes loadINodeFileAttributes(DataInput in)
        throws IOException {
      final int layoutVersion = getLayoutVersion();
      
      if (!NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.OPTIMIZE_SNAPSHOT_INODES, layoutVersion)) {
        return loadINodeWithLocalName(true, in, false).asFile();
      }
  
      final byte[] name = FSImageSerialization.readLocalName(in);
      final PermissionStatus permissions = PermissionStatus.read(in);
      final long modificationTime = in.readLong();
      final long accessTime = in.readLong();
  
      final short replication = namesystem.getBlockManager().adjustReplication(
          in.readShort());
      final long preferredBlockSize = in.readLong();

      return new INodeFileAttributes.SnapshotCopy(name, permissions, null, modificationTime,
          accessTime, replication, preferredBlockSize, (byte) 0, null, false);
    }

    public INodeDirectoryAttributes loadINodeDirectoryAttributes(DataInput in)
        throws IOException {
      final int layoutVersion = getLayoutVersion();
      
      if (!NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.OPTIMIZE_SNAPSHOT_INODES, layoutVersion)) {
        return loadINodeWithLocalName(true, in, false).asDirectory();
      }
  
      final byte[] name = FSImageSerialization.readLocalName(in);
      final PermissionStatus permissions = PermissionStatus.read(in);
      final long modificationTime = in.readLong();
      
      // Read quotas: quota by storage type does not need to be processed below.
      // It is handled only in protobuf based FsImagePBINode class for newer
      // fsImages. Tools using this class such as legacy-mode of offline image viewer
      // should only load legacy FSImages without newer features.
      final long nsQuota = in.readLong();
      final long dsQuota = in.readLong();

      return nsQuota == -1L && dsQuota == -1L ? new INodeDirectoryAttributes.SnapshotCopy(
          name, permissions, null, modificationTime, null)
        : new INodeDirectoryAttributes.CopyWithQuota(name, permissions,
            null, modificationTime, nsQuota, dsQuota, null, null);
    }
  
    private void loadFilesUnderConstruction(DataInput in,
        boolean supportSnapshot, Counter counter) throws IOException {
      FSDirectory fsDir = namesystem.dir;
      int size = in.readInt();

      LOG.info("Number of files under construction = " + size);

      for (int i = 0; i < size; i++) {
        INodeFile cons = FSImageSerialization.readINodeUnderConstruction(in,
            namesystem, getLayoutVersion());
        counter.increment();

        // verify that file exists in namespace
        String path = cons.getLocalName();
        INodeFile oldnode = null;
        boolean inSnapshot = false;
        if (path != null && FSDirectory.isReservedName(path) && 
            NameNodeLayoutVersion.supports(
                LayoutVersion.Feature.ADD_INODE_ID, getLayoutVersion())) {
          // TODO: for HDFS-5428, we use reserved path for those INodeFileUC in
          // snapshot. If we support INode ID in the layout version, we can use
          // the inode id to find the oldnode.
          oldnode = namesystem.dir.getInode(cons.getId()).asFile();
          inSnapshot = true;
        } else {
          path = renameReservedPathsOnUpgrade(path, getLayoutVersion());
          final INodesInPath iip = fsDir.getINodesInPath(path, true);
          oldnode = INodeFile.valueOf(iip.getLastINode(), path);
        }

        FileUnderConstructionFeature uc = cons.getFileUnderConstructionFeature();
        oldnode.toUnderConstruction(uc.getClientName(), uc.getClientMachine());
        if (oldnode.numBlocks() > 0) {
          BlockInfo ucBlock = cons.getLastBlock();
          // we do not replace the inode, just replace the last block of oldnode
          BlockInfo info = namesystem.getBlockManager()
              .addBlockCollectionWithCheck(ucBlock, oldnode);
          oldnode.setBlock(oldnode.numBlocks() - 1, info);
        }

        if (!inSnapshot) {
          namesystem.leaseManager.addLease(uc.getClientName(), oldnode.getId());
        }
      }
    }

    private void loadSecretManagerState(DataInput in)
        throws IOException {
      int imgVersion = getLayoutVersion();

      if (!NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.DELEGATION_TOKEN, imgVersion)) {
        //SecretManagerState is not available.
        //This must not happen if security is turned on.
        return; 
      }
      namesystem.loadSecretManagerStateCompat(in);
    }

    private void loadCacheManagerState(DataInput in) throws IOException {
      int imgVersion = getLayoutVersion();
      if (!NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.CACHING, imgVersion)) {
        return;
      }
      namesystem.getCacheManager().loadStateCompat(in);
    }

    private int getLayoutVersion() {
      return namesystem.getFSImage().getStorage().getLayoutVersion();
    }

    private boolean isRoot(byte[][] path) {
      return path.length == 1 &&
        path[0] == null;    
    }

    private boolean isParent(byte[][] path, byte[][] parent) {
      if (path == null || parent == null)
        return false;
      if (parent.length == 0 || path.length != parent.length + 1)
        return false;
      boolean isParent = true;
      for (int i = 0; i < parent.length; i++) {
        isParent = isParent && Arrays.equals(path[i], parent[i]); 
      }
      return isParent;
    }

    /**
     * Return string representing the parent of the given path.
     */
    String getParent(String path) {
      return path.substring(0, path.lastIndexOf(Path.SEPARATOR));
    }
    
    byte[][] getParent(byte[][] path) {
      byte[][] result = new byte[path.length - 1][];
      for (int i = 0; i < result.length; i++) {
        result[i] = new byte[path[i].length];
        System.arraycopy(path[i], 0, result[i], 0, path[i].length);
      }
      return result;
    }
    
    public Snapshot getSnapshot(DataInput in) throws IOException {
      return snapshotMap.get(in.readInt());
    }
  }

  @VisibleForTesting
  public static final TreeMap<String, String> renameReservedMap =
      new TreeMap<String, String>();

  /**
   * Use the default key-value pairs that will be used to determine how to
   * rename reserved paths on upgrade.
   */
  @VisibleForTesting
  public static void useDefaultRenameReservedPairs() {
    renameReservedMap.clear();
    for (String key: HdfsServerConstants.RESERVED_PATH_COMPONENTS) {
      renameReservedMap.put(
          key,
          key + "." + HdfsServerConstants.NAMENODE_LAYOUT_VERSION + "."
              + "UPGRADE_RENAMED");
    }
  }

  /**
   * Set the key-value pairs that will be used to determine how to rename
   * reserved paths on upgrade.
   */
  @VisibleForTesting
  public static void setRenameReservedPairs(String renameReserved) {
    // Clear and set the default values
    useDefaultRenameReservedPairs();
    // Overwrite with provided values
    setRenameReservedMapInternal(renameReserved);
  }

  private static void setRenameReservedMapInternal(String renameReserved) {
    Collection<String> pairs =
        StringUtils.getTrimmedStringCollection(renameReserved);
    for (String p : pairs) {
      String[] pair = StringUtils.split(p, '/', '=');
      Preconditions.checkArgument(pair.length == 2,
          "Could not parse key-value pair " + p);
      String key = pair[0];
      String value = pair[1];
      Preconditions.checkArgument(DFSUtil.isReservedPathComponent(key),
          "Unknown reserved path " + key);
      Preconditions.checkArgument(DFSUtil.isValidNameForComponent(value),
          "Invalid rename path for " + key + ": " + value);
      LOG.info("Will rename reserved path " + key + " to " + value);
      renameReservedMap.put(key, value);
    }
  }

  /**
   * When upgrading from an old version, the filesystem could contain paths
   * that are now reserved in the new version (e.g. .snapshot). This renames
   * these new reserved paths to a user-specified value to avoid collisions
   * with the reserved name.
   * 
   * @param path Old path potentially containing a reserved path
   * @return New path with reserved path components renamed to user value
   */
  static String renameReservedPathsOnUpgrade(String path,
      final int layoutVersion) throws IllegalReservedPathException {
    final String oldPath = path;
    // If any known LVs aren't supported, we're doing an upgrade
    if (!NameNodeLayoutVersion.supports(Feature.ADD_INODE_ID, layoutVersion)) {
      String[] components = INode.getPathNames(path);
      // Only need to worry about the root directory
      if (components.length > 1) {
        components[1] = DFSUtil.bytes2String(
            renameReservedRootComponentOnUpgrade(
                DFSUtil.string2Bytes(components[1]),
                layoutVersion));
        path = DFSUtil.strings2PathString(components);
      }
    }
    if (!NameNodeLayoutVersion.supports(Feature.SNAPSHOT, layoutVersion)) {
      String[] components = INode.getPathNames(path);
      // Special case the root path
      if (components.length == 0) {
        return path;
      }
      for (int i=0; i<components.length; i++) {
        components[i] = DFSUtil.bytes2String(
            renameReservedComponentOnUpgrade(
                DFSUtil.string2Bytes(components[i]),
                layoutVersion));
      }
      path = DFSUtil.strings2PathString(components);
    }

    if (!path.equals(oldPath)) {
      LOG.info("Upgrade process renamed reserved path " + oldPath + " to "
          + path);
    }
    return path;
  }

  private final static String RESERVED_ERROR_MSG = 
      FSDirectory.DOT_RESERVED_PATH_PREFIX + " is a reserved path and "
      + HdfsConstants.DOT_SNAPSHOT_DIR + " is a reserved path component in"
      + " this version of HDFS. Please rollback and delete or rename"
      + " this path, or upgrade with the "
      + StartupOption.RENAMERESERVED.getName()
      + " [key-value pairs]"
      + " option to automatically rename these paths during upgrade.";

  /**
   * Same as {@link #renameReservedPathsOnUpgrade}, but for a single
   * byte array path component.
   */
  private static byte[] renameReservedComponentOnUpgrade(byte[] component,
      final int layoutVersion) throws IllegalReservedPathException {
    // If the LV doesn't support snapshots, we're doing an upgrade
    if (!NameNodeLayoutVersion.supports(Feature.SNAPSHOT, layoutVersion)) {
      if (Arrays.equals(component, HdfsServerConstants.DOT_SNAPSHOT_DIR_BYTES)) {
        if (!renameReservedMap.containsKey(HdfsConstants.DOT_SNAPSHOT_DIR)) {
          throw new IllegalReservedPathException(RESERVED_ERROR_MSG);
        }
        component =
            DFSUtil.string2Bytes(renameReservedMap
                .get(HdfsConstants.DOT_SNAPSHOT_DIR));
      }
    }
    return component;
  }

  /**
   * Same as {@link #renameReservedPathsOnUpgrade}, but for a single
   * byte array path component.
   */
  private static byte[] renameReservedRootComponentOnUpgrade(byte[] component,
      final int layoutVersion) throws IllegalReservedPathException {
    // If the LV doesn't support inode IDs, we're doing an upgrade
    if (!NameNodeLayoutVersion.supports(Feature.ADD_INODE_ID, layoutVersion)) {
      if (Arrays.equals(component, FSDirectory.DOT_RESERVED)) {
        if (!renameReservedMap.containsKey(HdfsConstants.DOT_SNAPSHOT_DIR)) {
          throw new IllegalReservedPathException(RESERVED_ERROR_MSG);
        }
        final String renameString = renameReservedMap
            .get(FSDirectory.DOT_RESERVED_STRING);
        component =
            DFSUtil.string2Bytes(renameString);
        LOG.info("Renamed root path " + FSDirectory.DOT_RESERVED_STRING
            + " to " + renameString);
      }
    }
    return component;
  }

  /**
   * A one-shot class responsible for writing an image file.
   * The write() function should be called once, after which the getter
   * functions may be used to retrieve information about the file that was written.
   *
   * This is replaced by the PB-based FSImage. The class is to maintain
   * compatibility for the external fsimage tool.
   */
  @Deprecated
  static class Saver {
    private static final int LAYOUT_VERSION = -51;
    public static final int CHECK_CANCEL_INTERVAL = 4096;
    private final SaveNamespaceContext context;
    /** Set to true once an image has been written */
    private boolean saved = false;
    private long checkCancelCounter = 0;

    /** The MD5 checksum of the file that was written */
    private MD5Hash savedDigest;
    private final ReferenceMap referenceMap = new ReferenceMap();

    private final Map<Long, INodeFile> snapshotUCMap =
        new HashMap<Long, INodeFile>();

    /** @throws IllegalStateException if the instance has not yet saved an image */
    private void checkSaved() {
      if (!saved) {
        throw new IllegalStateException("FSImageSaver has not saved an image");
      }
    }

    /** @throws IllegalStateException if the instance has already saved an image */
    private void checkNotSaved() {
      if (saved) {
        throw new IllegalStateException("FSImageSaver has already saved an image");
      }
    }


    Saver(SaveNamespaceContext context) {
      this.context = context;
    }

    /**
     * Return the MD5 checksum of the image file that was saved.
     */
    MD5Hash getSavedDigest() {
      checkSaved();
      return savedDigest;
    }

    void save(File newFile, FSImageCompression compression) throws IOException {
      checkNotSaved();

      final FSNamesystem sourceNamesystem = context.getSourceNamesystem();
      final INodeDirectory rootDir = sourceNamesystem.dir.rootDir;
      final long numINodes = rootDir.getDirectoryWithQuotaFeature()
          .getSpaceConsumed().getNameSpace();
      String sdPath = newFile.getParentFile().getParentFile().getAbsolutePath();
      Step step = new Step(StepType.INODES, sdPath);
      StartupProgress prog = NameNode.getStartupProgress();
      prog.beginStep(Phase.SAVING_CHECKPOINT, step);
      prog.setTotal(Phase.SAVING_CHECKPOINT, step, numINodes);
      Counter counter = prog.getCounter(Phase.SAVING_CHECKPOINT, step);
      long startTime = monotonicNow();
      //
      // Write out data
      //
      MessageDigest digester = MD5Hash.getDigester();
      FileOutputStream fout = new FileOutputStream(newFile);
      DigestOutputStream fos = new DigestOutputStream(fout, digester);
      DataOutputStream out = new DataOutputStream(fos);
      try {
        out.writeInt(LAYOUT_VERSION);
        LayoutFlags.write(out);
        // We use the non-locked version of getNamespaceInfo here since
        // the coordinating thread of saveNamespace already has read-locked
        // the namespace for us. If we attempt to take another readlock
        // from the actual saver thread, there's a potential of a
        // fairness-related deadlock. See the comments on HDFS-2223.
        out.writeInt(sourceNamesystem.unprotectedGetNamespaceInfo()
            .getNamespaceID());
        out.writeLong(numINodes);
        final BlockIdManager blockIdManager = sourceNamesystem.getBlockManager()
            .getBlockIdManager();
        out.writeLong(blockIdManager.getLegacyGenerationStamp());
        out.writeLong(blockIdManager.getGenerationStamp());
        out.writeLong(blockIdManager.getGenerationStampAtblockIdSwitch());
        out.writeLong(blockIdManager.getLastAllocatedContiguousBlockId());
        out.writeLong(context.getTxId());
        out.writeLong(sourceNamesystem.dir.getLastInodeId());


        sourceNamesystem.getSnapshotManager().write(out);

        // write compression info and set up compressed stream
        out = compression.writeHeaderAndWrapStream(fos);
        LOG.info("Saving image file " + newFile +
                 " using " + compression);

        // save the root
        saveINode2Image(rootDir, out, false, referenceMap, counter);
        // save the rest of the nodes
        saveImage(rootDir, out, true, false, counter);
        prog.endStep(Phase.SAVING_CHECKPOINT, step);
        // Now that the step is finished, set counter equal to total to adjust
        // for possible under-counting due to reference inodes.
        prog.setCount(Phase.SAVING_CHECKPOINT, step, numINodes);
        // save files under construction
        // TODO: for HDFS-5428, since we cannot break the compatibility of
        // fsimage, we store part of the under-construction files that are only
        // in snapshots in this "under-construction-file" section. As a
        // temporary solution, we use "/.reserved/.inodes/<inodeid>" as their
        // paths, so that when loading fsimage we do not put them into the lease
        // map. In the future, we can remove this hack when we can bump the
        // layout version.
        saveFilesUnderConstruction(sourceNamesystem, out, snapshotUCMap);

        context.checkCancelled();
        sourceNamesystem.saveSecretManagerStateCompat(out, sdPath);
        context.checkCancelled();
        sourceNamesystem.getCacheManager().saveStateCompat(out, sdPath);
        context.checkCancelled();
        out.flush();
        context.checkCancelled();
        fout.getChannel().force(true);
      } finally {
        out.close();
      }

      saved = true;
      // set md5 of the saved image
      savedDigest = new MD5Hash(digester.digest());

      LOG.info("Image file " + newFile + " of size " + newFile.length()
          + " bytes saved in " + (monotonicNow() - startTime) / 1000
          + " seconds.");
    }

    /**
     * Save children INodes.
     * @param children The list of children INodes
     * @param out The DataOutputStream to write
     * @param inSnapshot Whether the parent directory or its ancestor is in
     *                   the deleted list of some snapshot (caused by rename or
     *                   deletion)
     * @param counter Counter to increment for namenode startup progress
     * @return Number of children that are directory
     */
    private int saveChildren(ReadOnlyList<INode> children,
        DataOutputStream out, boolean inSnapshot, Counter counter)
        throws IOException {
      // Write normal children INode.
      out.writeInt(children.size());
      int dirNum = 0;
      for(INode child : children) {
        // print all children first
        // TODO: for HDFS-5428, we cannot change the format/content of fsimage
        // here, thus even if the parent directory is in snapshot, we still
        // do not handle INodeUC as those stored in deleted list
        saveINode2Image(child, out, false, referenceMap, counter);
        if (child.isDirectory()) {
          dirNum++;
        } else if (inSnapshot && child.isFile()
            && child.asFile().isUnderConstruction()) {
          this.snapshotUCMap.put(child.getId(), child.asFile());
        }
        if (checkCancelCounter++ % CHECK_CANCEL_INTERVAL == 0) {
          context.checkCancelled();
        }
      }
      return dirNum;
    }

    /**
     * Save file tree image starting from the given root.
     * This is a recursive procedure, which first saves all children and
     * snapshot diffs of a current directory and then moves inside the
     * sub-directories.
     *
     * @param current The current node
     * @param out The DataoutputStream to write the image
     * @param toSaveSubtree Whether or not to save the subtree to fsimage. For
     *                      reference node, its subtree may already have been
     *                      saved before.
     * @param inSnapshot Whether the current directory is in snapshot
     * @param counter Counter to increment for namenode startup progress
     */
    private void saveImage(INodeDirectory current, DataOutputStream out,
        boolean toSaveSubtree, boolean inSnapshot, Counter counter)
        throws IOException {
      // write the inode id of the directory
      out.writeLong(current.getId());

      if (!toSaveSubtree) {
        return;
      }

      final ReadOnlyList<INode> children = current
          .getChildrenList(Snapshot.CURRENT_STATE_ID);
      int dirNum = 0;
      List<INodeDirectory> snapshotDirs = null;
      DirectoryWithSnapshotFeature sf = current.getDirectoryWithSnapshotFeature();
      if (sf != null) {
        snapshotDirs = new ArrayList<INodeDirectory>();
        sf.getSnapshotDirectory(snapshotDirs);
        dirNum += snapshotDirs.size();
      }

      // 2. Write INodeDirectorySnapshottable#snapshotsByNames to record all
      // Snapshots
      if (current.isDirectory() && current.asDirectory().isSnapshottable()) {
        SnapshotFSImageFormat.saveSnapshots(current.asDirectory(), out);
      } else {
        out.writeInt(-1); // # of snapshots
      }

      // 3. Write children INode
      dirNum += saveChildren(children, out, inSnapshot, counter);

      // 4. Write DirectoryDiff lists, if there is any.
      SnapshotFSImageFormat.saveDirectoryDiffList(current, out, referenceMap);

      // Write sub-tree of sub-directories, including possible snapshots of
      // deleted sub-directories
      out.writeInt(dirNum); // the number of sub-directories
      for(INode child : children) {
        if(!child.isDirectory()) {
          continue;
        }
        // make sure we only save the subtree under a reference node once
        boolean toSave = child.isReference() ?
            referenceMap.toProcessSubtree(child.getId()) : true;
        saveImage(child.asDirectory(), out, toSave, inSnapshot, counter);
      }
      if (snapshotDirs != null) {
        for (INodeDirectory subDir : snapshotDirs) {
          // make sure we only save the subtree under a reference node once
          boolean toSave = subDir.getParentReference() != null ?
              referenceMap.toProcessSubtree(subDir.getId()) : true;
          saveImage(subDir, out, toSave, true, counter);
        }
      }
    }

    /**
     * Saves inode and increments progress counter.
     *
     * @param inode INode to save
     * @param out DataOutputStream to receive inode
     * @param writeUnderConstruction boolean true if this is under construction
     * @param referenceMap ReferenceMap containing reference inodes
     * @param counter Counter to increment for namenode startup progress
     * @throws IOException thrown if there is an I/O error
     */
    private void saveINode2Image(INode inode, DataOutputStream out,
        boolean writeUnderConstruction, ReferenceMap referenceMap,
        Counter counter) throws IOException {
      FSImageSerialization.saveINode2Image(inode, out, writeUnderConstruction,
        referenceMap);
      // Intentionally do not increment counter for reference inodes, because it
      // is too difficult at this point to assess whether or not this is a
      // reference that counts toward quota.
      if (!(inode instanceof INodeReference)) {
        counter.increment();
      }
    }

    /**
     * Serializes leases.
     */
    void saveFilesUnderConstruction(FSNamesystem fsn, DataOutputStream out,
                                    Map<Long, INodeFile> snapshotUCMap) throws IOException {
      // This is run by an inferior thread of saveNamespace, which holds a read
      // lock on our behalf. If we took the read lock here, we could block
      // for fairness if a writer is waiting on the lock.
      final LeaseManager leaseManager = fsn.getLeaseManager();
      final FSDirectory dir = fsn.getFSDirectory();
      synchronized (leaseManager) {
        Collection<Long> filesWithUC = leaseManager.getINodeIdWithLeases();
        for (Long id : filesWithUC) {
          // TODO: for HDFS-5428, because of rename operations, some
          // under-construction files that are
          // in the current fs directory can also be captured in the
          // snapshotUCMap. We should remove them from the snapshotUCMap.
          snapshotUCMap.remove(id);
        }
        out.writeInt(filesWithUC.size() + snapshotUCMap.size()); // write the size

        for (Long id : filesWithUC) {
          INodeFile file = dir.getInode(id).asFile();
          String path = file.getFullPathName();
          FSImageSerialization.writeINodeUnderConstruction(
                  out, file, path);
        }

        for (Map.Entry<Long, INodeFile> entry : snapshotUCMap.entrySet()) {
          // for those snapshot INodeFileUC, we use "/.reserved/.inodes/<inodeid>"
          // as their paths
          StringBuilder b = new StringBuilder();
          b.append(FSDirectory.DOT_RESERVED_PATH_PREFIX)
                  .append(Path.SEPARATOR).append(FSDirectory.DOT_INODES_STRING)
                  .append(Path.SEPARATOR).append(entry.getValue().getId());
          FSImageSerialization.writeINodeUnderConstruction(
                  out, entry.getValue(), b.toString());
        }
      }
    }
  }
}
