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

import static org.apache.hadoop.util.Time.now;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat.ReferenceMap;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.annotations.VisibleForTesting;

/**
 * This class loads and stores the FSImage of the NameNode. The file
 * src/main/proto/fsimage.proto describes the on-disk layout of the FSImage.
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

    public void load(File file) throws IOException {
      Preconditions.checkState(impl == null, "Image already loaded!");

      FileInputStream is = null;
      try {
        is = new FileInputStream(file);
        byte[] magic = new byte[FSImageUtil.MAGIC_HEADER.length];
        IOUtils.readFully(is, magic, 0, magic.length);
        if (Arrays.equals(magic, FSImageUtil.MAGIC_HEADER)) {
          FSImageFormatProtobuf.Loader loader = new FSImageFormatProtobuf.Loader(
              conf, fsn);
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
      long startTime = now();

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
        namesystem.setGenerationStampV1(genstamp);
        
        if (NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.SEQUENTIAL_BLOCK_ID, imgVersion)) {
          // read the starting generation stamp for sequential block IDs
          genstamp = in.readLong();
          namesystem.setGenerationStampV2(genstamp);

          // read the last generation stamp for blocks created after
          // the switch to sequential block IDs.
          long stampAtIdSwitch = in.readLong();
          namesystem.setGenerationStampV1Limit(stampAtIdSwitch);

          // read the max sequential block ID.
          long maxSequentialBlockId = in.readLong();
          namesystem.setLastAllocatedBlockId(maxSequentialBlockId);
        } else {
          long startingGenStamp = namesystem.upgradeGenerationStampToV2();
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
          namesystem.resetLastInodeId(lastInodeId);
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
      
      LOG.info("Image file " + curFile + " of size " + curFile.length() +
          " bytes loaded in " + (now() - startTime)/1000 + " seconds.");
    }

  /** Update the root node's attributes */
  private void updateRootAttr(INodeWithAdditionalFields root) {                                                           
    final Quota.Counts q = root.getQuotaCounts();
    final long nsQuota = q.get(Quota.NAMESPACE);
    final long dsQuota = q.get(Quota.DISKSPACE);
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
        final INodeDirectorySnapshottable snapshottableParent
            = INodeDirectorySnapshottable.valueOf(parent, parent.getLocalName());
        // load snapshots and snapshotQuota
        SnapshotFSImageFormat.loadSnapshotList(snapshottableParent,
            numSnapshots, in, this);
        if (snapshottableParent.getSnapshotQuota() > 0) {
          // add the directory to the snapshottable directory list in 
          // SnapshotManager. Note that we only add root when its snapshot quota
          // is positive.
          this.namesystem.getSnapshotManager().addSnapshottable(
              snapshottableParent);
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
    * @param in
    * @param counter Counter to increment for namenode startup progress
    * @return number of child inodes read
    * @throws IOException
    */
   private int loadDirectory(DataInput in, Counter counter) throws IOException {
     String parentPath = FSImageSerialization.readString(in);
     // Rename .snapshot paths if we're doing an upgrade
     parentPath = renameReservedPathsOnUpgrade(parentPath, getLayoutVersion());
     final INodeDirectory parent = INodeDirectory.valueOf(
         namesystem.dir.rootDir.getNode(parentPath, true), parentPath);
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
  private void addToParent(INodeDirectory parent, INode child) {
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
          file.setBlock(i, bm.addBlockCollection(blocks[i], file));
        } 
      }
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
        : namesystem.allocateNewInodeId();
    
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
      BlockInfo[] blocks = new BlockInfo[numBlocks];
      for (int j = 0; j < numBlocks; j++) {
        blocks[j] = new BlockInfo(replication);
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
            if (blocks != null && blocks.length > 0) {
              BlockInfo lastBlk = blocks[blocks.length - 1]; 
              blocks[blocks.length - 1] = new BlockInfoUnderConstruction(
                  lastBlk, replication);
            }
          }
        }
      }

      final PermissionStatus permissions = PermissionStatus.read(in);

      // return
      if (counter != null) {
        counter.increment();
      }
      final INodeFile file = new INodeFile(inodeId, localName, permissions,
          modificationTime, atime, blocks, replication, blockSize);
      if (underConstruction) {
        file.toUnderConstruction(clientName, clientMachine, null);
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
        dir.addDirectoryWithQuotaFeature(nsQuota, dsQuota);
      }
      if (withSnapshot) {
        dir.addSnapshotFeature(null);
      }
      return snapshottable ? new INodeDirectorySnapshottable(dir) : dir;
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
          accessTime, replication, preferredBlockSize);
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
      
      //read quotas
      final long nsQuota = in.readLong();
      final long dsQuota = in.readLong();
  
      return nsQuota == -1L && dsQuota == -1L?
          new INodeDirectoryAttributes.SnapshotCopy(name, permissions, null, modificationTime)
        : new INodeDirectoryAttributes.CopyWithQuota(name, permissions,
            null, modificationTime, nsQuota, dsQuota);
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
          final INodesInPath iip = fsDir.getLastINodeInPath(path);
          oldnode = INodeFile.valueOf(iip.getINode(0), path);
        }

        FileUnderConstructionFeature uc = cons.getFileUnderConstructionFeature();
        oldnode.toUnderConstruction(uc.getClientName(), uc.getClientMachine(),
            uc.getClientNode());
        if (oldnode.numBlocks() > 0) {
          BlockInfo ucBlock = cons.getLastBlock();
          // we do not replace the inode, just replace the last block of oldnode
          BlockInfo info = namesystem.getBlockManager().addBlockCollection(
              ucBlock, oldnode);
          oldnode.setBlock(oldnode.numBlocks() - 1, info);
        }

        if (!inSnapshot) {
          namesystem.leaseManager.addLease(cons
              .getFileUnderConstructionFeature().getClientName(), path);
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
    for (String key: HdfsConstants.RESERVED_PATH_COMPONENTS) {
      renameReservedMap.put(
          key,
          key + "." + HdfsConstants.NAMENODE_LAYOUT_VERSION + "."
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
      final int layoutVersion) {
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
      final int layoutVersion) {
    // If the LV doesn't support snapshots, we're doing an upgrade
    if (!NameNodeLayoutVersion.supports(Feature.SNAPSHOT, layoutVersion)) {
      if (Arrays.equals(component, HdfsConstants.DOT_SNAPSHOT_DIR_BYTES)) {
        Preconditions.checkArgument(
            renameReservedMap != null &&
            renameReservedMap.containsKey(HdfsConstants.DOT_SNAPSHOT_DIR),
            RESERVED_ERROR_MSG);
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
      final int layoutVersion) {
    // If the LV doesn't support inode IDs, we're doing an upgrade
    if (!NameNodeLayoutVersion.supports(Feature.ADD_INODE_ID, layoutVersion)) {
      if (Arrays.equals(component, FSDirectory.DOT_RESERVED)) {
        Preconditions.checkArgument(
            renameReservedMap != null &&
            renameReservedMap.containsKey(FSDirectory.DOT_RESERVED_STRING),
            RESERVED_ERROR_MSG);
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
}
