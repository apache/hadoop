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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.flatbuffer.*;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.LoaderContext;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.*;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotDiffSection.DiffEntry.Type;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.DstReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithName;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.Root;
import org.apache.hadoop.hdfs.util.Diff.ListType;
import org.apache.hadoop.hdfs.util.EnumCounters;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.Loader.*;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.Saver.*;

//import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.Saver.buildIntelINodeDirectory;

@InterfaceAudience.Private
public class FSImageFormatPBSnapshot {
  /**
   * Loading snapshot related information from protobuf based FSImage
   */
  public final static class Loader {
    private final FSNamesystem fsn;
    private final FSDirectory fsDir;
    private final FSImageFormatProtobuf.Loader parent;
    private final Map<Integer, Snapshot> snapshotMap;

    public Loader(FSNamesystem fsn, FSImageFormatProtobuf.Loader parent) {
      this.fsn = fsn;
      this.fsDir = fsn.getFSDirectory();
      this.snapshotMap = new HashMap<Integer, Snapshot>();
      this.parent = parent;
    }

    /**
     * The sequence of the ref node in refList must be strictly the same with
     * the sequence in fsimage
     */
    public void loadIntelINodeReferenceSection(InputStream in) throws IOException {
      final List<INodeReference> refList = parent.getLoaderContext()
          .getRefList();

      while (true) {
        byte[] bytes = parseFrom(in);
        if(bytes==null){
          break;
        }
        IntelINodeReference ie = IntelINodeReference.
            getRootAsIntelINodeReference(ByteBuffer.wrap(bytes));

        INodeReference ref = loadIntelINodeReference(ie); // success
        refList.add(ref);
      }
    }

    /**
     * The sequence of the ref node in refList must be strictly the same with
     * the sequence in fsimage
     */
    public void loadINodeReferenceSection(InputStream in) throws IOException {
      final List<INodeReference> refList = parent.getLoaderContext()
          .getRefList();
      while (true)
      {
        INodeReferenceSection.INodeReference e = INodeReferenceSection
            .INodeReference.parseDelimitedFrom(in);
        if (e == null) {
          break;
        }
        INodeReference ref = loadINodeReference(e);
        refList.add(ref);
      }
    }

    private INodeReference loadIntelINodeReference(IntelINodeReference r) throws IOException {
      long referredId = r.referredId();
      INode referred = fsDir.getInode(referredId);
      WithCount withCount = (WithCount) referred.getParentReference();
      if (withCount == null) {
        withCount = new INodeReference.WithCount(null, referred);
      }
      final INodeReference ref;

      if (r.dstSnapshotId() != 0) { // DstReference
        ref = new INodeReference.DstReference(null, withCount,
            (int)r.dstSnapshotId());
      } else {
        ref = new INodeReference.WithName(null, withCount,
            r.name().getBytes(), (int)r.lastSnapshotId());
      }
      return ref;
    }

    private INodeReference loadINodeReference(INodeReferenceSection.INodeReference r) throws IOException {
      long referredId = r.getReferredId();
      INode referred = fsDir.getInode(referredId);
      WithCount withCount = (WithCount) referred.getParentReference();
      if (withCount == null) {
        withCount = new INodeReference.WithCount(null, referred);
      }
      final INodeReference ref;
      if (r.hasDstSnapshotId()) { // DstReference
        ref = new INodeReference.DstReference(null, withCount,
            r.getDstSnapshotId());
      } else {
        ref = new INodeReference.WithName(null, withCount, r.getName()
            .toByteArray(), r.getLastSnapshotId());
      }
      return ref;
    }

    /**
     * Load the snapshots section from fsimage. Also add snapshottable feature
     * to snapshottable directories.
     */
    public void loadIntelSnapshotSection(InputStream in) throws IOException {
      SnapshotManager sm = fsn.getSnapshotManager();

      IntelSnapshotSection isection = IntelSnapshotSection.
          getRootAsIntelSnapshotSection(ByteBuffer.wrap(parseFrom(in)));

      int snum = (int)isection.numSnapshots();
      sm.setNumSnapshots(snum);

      sm.setSnapshotCounter((int)isection.snapshotCounter());

      for (int i = 0 ;i < isection.snapshottableDirLength() ;i ++) {
        long sdirId = isection.snapshottableDir(i);
        INodeDirectory dir = fsDir.getInode(sdirId).asDirectory();
        if (!dir.isSnapshottable()) {
          dir.addSnapshottableFeature();
        } else {
          // dir is root, and admin set root to snapshottable before
          dir.setSnapshotQuota(DirectorySnapshottableFeature.SNAPSHOT_LIMIT);
        }
        sm.addSnapshottable(dir);
      }
      loadIntelSnapshots(in, snum);
    }

    /**
     * Load the snapshots section from fsimage. Also add snapshottable feature
     * to snapshottable directories.
     */
    public void loadSnapshotSection(InputStream in) throws IOException {
      SnapshotManager sm = fsn.getSnapshotManager();
      SnapshotSection section = SnapshotSection.parseDelimitedFrom(in);
      int snum = section.getNumSnapshots();
      sm.setNumSnapshots(snum);
      sm.setSnapshotCounter(section.getSnapshotCounter());
      for (long sdirId : section.getSnapshottableDirList()) {
        INodeDirectory dir = fsDir.getInode(sdirId).asDirectory();
        if (!dir.isSnapshottable()) {
          dir.addSnapshottableFeature();
        } else {
          // dir is root, and admin set root to snapshottable before
          dir.setSnapshotQuota(DirectorySnapshottableFeature.SNAPSHOT_LIMIT);
        }
        sm.addSnapshottable(dir);
      }
      loadSnapshots(in, snum);
    }

    // finished
    private void loadIntelSnapshots(InputStream in, int size) throws IOException {
      for (int i = 0; i < size; i++)
      {
        IntelSnapshot ipbs = IntelSnapshot.getRootAsIntelSnapshot(ByteBuffer.wrap(parseFrom(in)));
//        SnapshotSection.Snapshot pbs = SnapshotSection.Snapshot
//            .parseDelimitedFrom(in);

        INodeDirectory root = loadIntelINodeDirectory(ipbs.root(),
            parent.getLoaderContext());

        int sid = (int)ipbs.snapshotId();
        INodeDirectory parent = fsDir.getInode(root.getId()).asDirectory();
        Snapshot snapshot = new Snapshot(sid, root, parent);
        // add the snapshot to parent, since we follow the sequence of
        // snapshotsByNames when saving, we do not need to sort when loading
        parent.getDirectorySnapshottableFeature().addSnapshot(snapshot);
        snapshotMap.put(sid, snapshot);
      }
    }

    private void loadSnapshots(InputStream in, int size) throws IOException {
      for (int i = 0; i < size; i++) {
        SnapshotSection.Snapshot pbs = SnapshotSection.Snapshot
            .parseDelimitedFrom(in);
        INodeDirectory root = loadINodeDirectory(pbs.getRoot(),
            parent.getLoaderContext());
        int sid = pbs.getSnapshotId();
        INodeDirectory parent = fsDir.getInode(root.getId()).asDirectory();
        Snapshot snapshot = new Snapshot(sid, root, parent);
        // add the snapshot to parent, since we follow the sequence of
        // snapshotsByNames when saving, we do not need to sort when loading
        parent.getDirectorySnapshottableFeature().addSnapshot(snapshot);
        snapshotMap.put(sid, snapshot);
      }
    }

    /**
     * Load the snapshot diff section from fsimage.
     */
    public void loadIntelSnapshotDiffSection(InputStream in) throws IOException {
      final List<INodeReference> refList = parent.getLoaderContext()
          .getRefList();
      while (true) {
        byte[] bytes = parseFrom(in);
        if (bytes == null) {
          break;
        }
        IntelDiffEntry ientry = IntelDiffEntry.getRootAsIntelDiffEntry(ByteBuffer.wrap(bytes));
        long inodeId = ientry.inodeId();
        INode inode = fsDir.getInode(inodeId);
        int itype = ientry.type();
        switch (itype) {
          case IntelType.FILEDIFF:
            loadIntelFileDiffList(in, inode.asFile(), (int)ientry.numOfDiff()); // finished
            break;
          case IntelType.DIRECTORYDIFF:
            loadIntelDirectoryDiffList(in, inode.asDirectory(), (int)ientry.numOfDiff(), refList); // finished
            break;
        }
      }
    }

    /**
     * Load the snapshot diff section from fsimage.
     */
    public void loadSnapshotDiffSection(InputStream in) throws IOException {
      final List<INodeReference> refList = parent.getLoaderContext()
          .getRefList();
      while (true) {
        SnapshotDiffSection.DiffEntry entry = SnapshotDiffSection.DiffEntry
            .parseDelimitedFrom(in);
        if (entry == null) {
          break;
        }
        long inodeId = entry.getInodeId();
        INode inode = fsDir.getInode(inodeId);
        SnapshotDiffSection.DiffEntry.Type type = entry.getType();
        switch (type) {
        case FILEDIFF:
          loadFileDiffList(in, inode.asFile(), entry.getNumOfDiff());
          break;
        case DIRECTORYDIFF:
          loadDirectoryDiffList(in, inode.asDirectory(), entry.getNumOfDiff(),
              refList);
          break;
        }
      }
    }

    /** Load FileDiff list for a file with snapshot feature */
    private void loadIntelFileDiffList(InputStream in, INodeFile file, int size)
        throws IOException {
      final FileDiffList diffs = new FileDiffList();
      final LoaderContext state = parent.getLoaderContext();
      for (int i = 0; i < size; i++) {
        IntelFileDiff ipbf = IntelFileDiff.getRootAsIntelFileDiff(ByteBuffer.wrap(parseFrom(in)));
        INodeFileAttributes copy = null;
        if (ipbf.snapshotCopy() != null) {
          IntelINodeFile ifileInPb = ipbf.snapshotCopy();
          PermissionStatus permission = loadPermission(
              ifileInPb.permission(), state.getStringTable());
          AclFeature acl = null;

          if (ifileInPb.acl() != null) {
            int[] entries = AclEntryStatusFormat
                .toInt(FSImageFormatPBINode.Loader.loadIntelAclEntries(
                    ifileInPb.acl(), state.getStringTable()));
            acl = new AclFeature(entries);
          }
          XAttrFeature xAttrs = null;

          if (ifileInPb.xAttrs() != null) {
            xAttrs = new XAttrFeature(FSImageFormatPBINode.Loader.loadIntelXAttrs(
                ifileInPb.xAttrs(), state.getStringTable()));
          }

          copy = new INodeFileAttributes.SnapshotCopy(ipbf.name().getBytes()
              , permission, acl, ifileInPb.modificationTime(),
              ifileInPb.accessTime(), (short) ifileInPb.replication(),
              ifileInPb.preferredBlockSize(),
              (byte)ifileInPb.storagePolicyID(), xAttrs);
        }

        FileDiff diff = new FileDiff((int)ipbf.snapshotId(), copy, null,
            ipbf.fileSize());

        List<IntelBlockProto> ibpl = null;
        for (int j = 0; j < ipbf.blocksLength() ; j++) {
          ibpl.add(ipbf.blocks(j));
        }
//        List<BlockProto> bpl = pbf.getBlocksList();
        BlockInfo[] blocks = new BlockInfo[ibpl.size()];
        for(int j = 0, e = ibpl.size(); j < e; ++j) {
          Block blk = PBHelper.convert(ibpl.get(j));
          BlockInfo storedBlock =  fsn.getBlockManager().getStoredBlock(blk);
          if(storedBlock == null) {
            storedBlock = fsn.getBlockManager().addBlockCollection(
                new BlockInfoContiguous(blk, copy.getFileReplication()), file);
          }
          blocks[j] = storedBlock;
        }
        if(blocks.length > 0) {
          diff.setBlocks(blocks);
        }
        diffs.addFirst(diff);
      } // for end
      file.addSnapshotFeature(diffs);
    }

    /** Load FileDiff list for a file with snapshot feature */
    private void loadFileDiffList(InputStream in, INodeFile file, int size)
        throws IOException {
      final FileDiffList diffs = new FileDiffList();
      final LoaderContext state = parent.getLoaderContext();
      for (int i = 0; i < size; i++) {
        SnapshotDiffSection.FileDiff pbf = SnapshotDiffSection.FileDiff
            .parseDelimitedFrom(in);
        INodeFileAttributes copy = null;
        if (pbf.hasSnapshotCopy()) {
          INodeSection.INodeFile fileInPb = pbf.getSnapshotCopy();
          PermissionStatus permission = loadPermission(
              fileInPb.getPermission(), state.getStringTable());

          AclFeature acl = null;
          if (fileInPb.hasAcl()) {
            int[] entries = AclEntryStatusFormat
                .toInt(FSImageFormatPBINode.Loader.loadAclEntries(
                    fileInPb.getAcl(), state.getStringTable()));
            acl = new AclFeature(entries);
          }
          XAttrFeature xAttrs = null;
          if (fileInPb.hasXAttrs()) {
            xAttrs = new XAttrFeature(FSImageFormatPBINode.Loader.loadXAttrs(
                fileInPb.getXAttrs(), state.getStringTable()));
          }

          copy = new INodeFileAttributes.SnapshotCopy(pbf.getName()
              .toByteArray(), permission, acl, fileInPb.getModificationTime(),
              fileInPb.getAccessTime(), (short) fileInPb.getReplication(),
              fileInPb.getPreferredBlockSize(),
              (byte)fileInPb.getStoragePolicyID(), xAttrs);
        }

        FileDiff diff = new FileDiff(pbf.getSnapshotId(), copy, null,
            pbf.getFileSize());
        List<BlockProto> bpl = pbf.getBlocksList();
        BlockInfo[] blocks = new BlockInfo[bpl.size()];
        for(int j = 0, e = bpl.size(); j < e; ++j) {
          Block blk = PBHelper.convert(bpl.get(j));
          BlockInfo storedBlock =  fsn.getBlockManager().getStoredBlock(blk);
          if(storedBlock == null) {
            storedBlock = fsn.getBlockManager().addBlockCollection(
                new BlockInfoContiguous(blk, copy.getFileReplication()), file);
          }
          blocks[j] = storedBlock;
        }
        if(blocks.length > 0) {
          diff.setBlocks(blocks);
        }
        diffs.addFirst(diff);
      }
      file.addSnapshotFeature(diffs);
    }

    /** Load the created list in a DirectoryDiff */
    private List<INode> loadCreatedList(InputStream in, INodeDirectory dir,
        int size) throws IOException {

      List<INode> clist = new ArrayList<INode>(size);
      for (long c = 0; c < size; c++) {

        IntelCreatedListEntry ientry = IntelCreatedListEntry.getRootAsIntelCreatedListEntry(ByteBuffer.wrap(parseFrom(in)));
        INode created = SnapshotFSImageFormat.loadCreated(ientry.name().getBytes(), dir);
        clist.add(created);
      }
      return clist;
    }

    private void addToDeletedList(INode dnode, INodeDirectory parent) {
      dnode.setParent(parent);
      if (dnode.isFile()) {
        updateBlocksMap(dnode.asFile(), fsn.getBlockManager());
      }
    }

    /**
     * Load the deleted list in a DirectoryDiff
     */
    private List<INode> loadDeletedList(final List<INodeReference> refList,
        InputStream in, INodeDirectory dir, List<Long> deletedNodes,
        List<Integer> deletedRefNodes)
        throws IOException {
      List<INode> dlist = new ArrayList<INode>(deletedRefNodes.size()
          + deletedNodes.size());
      // load non-reference inodes
      for (long deletedId : deletedNodes) {
        INode deleted = fsDir.getInode(deletedId);
        dlist.add(deleted);
        addToDeletedList(deleted, dir);
      }
      // load reference nodes in the deleted list
      for (int refId : deletedRefNodes) {
        INodeReference deletedRef = refList.get(refId);
        dlist.add(deletedRef);
        addToDeletedList(deletedRef, dir);
      }

      Collections.sort(dlist, new Comparator<INode>() {
        @Override
        public int compare(INode n1, INode n2) {
          return n1.compareTo(n2.getLocalNameBytes());
        }
      });
      return dlist;
    }

    /** Load DirectoryDiff list for a directory with snapshot feature */
    private void loadIntelDirectoryDiffList(InputStream in, INodeDirectory dir,
                                       int size, final List<INodeReference> refList) throws IOException
    {
      if (!dir.isWithSnapshot()) {
        dir.addSnapshotFeature(null);
      }
      DirectoryDiffList diffs = dir.getDiffs();
      final LoaderContext state = parent.getLoaderContext();

      for (int i = 0; i < size; i++) {
        IntelDirectoryDiff idiffInPb = IntelDirectoryDiff.getRootAsIntelDirectoryDiff(ByteBuffer.wrap(parseFrom(in)));

        final int snapshotId = (int)idiffInPb.snapshotId();
        final Snapshot snapshot = snapshotMap.get(snapshotId);

        int childrenSize = (int)idiffInPb.childrenSize();

        boolean useRoot = idiffInPb.isSnapshotRoot();

        INodeDirectoryAttributes copy = null;

        if (useRoot) {
          copy = snapshot.getRoot();
        } else if (idiffInPb.snapshotCopy() != null) {

          IntelINodeDirectory idirCopyInPb = idiffInPb.snapshotCopy();

//          INodeSection.INodeDirectory dirCopyInPb = diffInPb.getSnapshotCopy();
          final byte[] name = idiffInPb.name().getBytes();

          PermissionStatus permission = loadPermission(
              idirCopyInPb.permission(), state.getStringTable());

          AclFeature acl = null;
          if (idirCopyInPb.acl() != null) {
            int[] entries = AclEntryStatusFormat
                .toInt(FSImageFormatPBINode.Loader.loadIntelAclEntries(
                    idirCopyInPb.acl(), state.getStringTable()));
            acl = new AclFeature(entries);
          }
          XAttrFeature xAttrs = null;
          if (idirCopyInPb.xAttrs() != null) {
            xAttrs = new XAttrFeature(FSImageFormatPBINode.Loader.loadIntelXAttrs(
                idirCopyInPb.xAttrs(), state.getStringTable()));
          }

          long modTime = idirCopyInPb.modificationTime();

          boolean noQuota = idirCopyInPb.nsQuota() == -1
              && idirCopyInPb.dsQuota() == -1
              && (!(idirCopyInPb.typeQuotas() != null));

          if (noQuota) {
            copy = new INodeDirectoryAttributes.SnapshotCopy(name,
                permission, acl, modTime, xAttrs);
          } else {
            EnumCounters<StorageType> typeQuotas = null;

            if (idirCopyInPb.typeQuotas() != null) {

              ImmutableList<QuotaByStorageTypeEntry> qes =
                  FSImageFormatPBINode.Loader.loadIntelQuotaByStorageTypeEntries(
                      idirCopyInPb.typeQuotas());
              typeQuotas = new EnumCounters<StorageType>(StorageType.class,
                  HdfsConstants.QUOTA_RESET);
              for (QuotaByStorageTypeEntry qe : qes) {
                if (qe.getQuota() >= 0 && qe.getStorageType() != null &&
                    qe.getStorageType().supportTypeQuota()) {
                  typeQuotas.set(qe.getStorageType(), qe.getQuota());
                }
              }
            }
            copy = new INodeDirectoryAttributes.CopyWithQuota(name, permission,
                acl, modTime, idirCopyInPb.nsQuota(),
                idirCopyInPb.dsQuota(), typeQuotas, xAttrs);
          }
        }
        // load created list
        List<INode> clist = loadCreatedList(in, dir,
            (int)idiffInPb.createdListSize());

        List<Long> deletedNodes = new ArrayList<>();
        for (int k = 0 ; k < idiffInPb.deletedINodeLength(); k++) {
          deletedNodes.add(idiffInPb.deletedINode(k));
        }
        List<Integer> deletedRefNodes = new ArrayList<>();
        for (int q = 0 ;q < idiffInPb.deletedINodeRefLength() ; q++) {
          deletedRefNodes.add((int)idiffInPb.deletedINodeRef(q));
        }
        // load deleted list
        List<INode> dlist = loadDeletedList(refList, in, dir,
            deletedNodes, deletedRefNodes);

        // create the directory diff
        DirectoryDiff diff = new DirectoryDiff(snapshotId, copy, null,
            childrenSize, clist, dlist, useRoot);
        diffs.addFirst(diff);
      } // for end
    }


    /** Load DirectoryDiff list for a directory with snapshot feature */
    private void loadDirectoryDiffList(InputStream in, INodeDirectory dir,
        int size, final List<INodeReference> refList) throws IOException {
      if (!dir.isWithSnapshot()) {
        dir.addSnapshotFeature(null);
      }
      DirectoryDiffList diffs = dir.getDiffs();
      final LoaderContext state = parent.getLoaderContext();

      for (int i = 0; i < size; i++) {
        // load a directory diff
        SnapshotDiffSection.DirectoryDiff diffInPb = SnapshotDiffSection.
            DirectoryDiff.parseDelimitedFrom(in);
        final int snapshotId = diffInPb.getSnapshotId();
        final Snapshot snapshot = snapshotMap.get(snapshotId);
        int childrenSize = diffInPb.getChildrenSize();
        boolean useRoot = diffInPb.getIsSnapshotRoot();
        INodeDirectoryAttributes copy = null;
        if (useRoot) {
          copy = snapshot.getRoot();
        } else if (diffInPb.hasSnapshotCopy()) {
          INodeSection.INodeDirectory dirCopyInPb = diffInPb.getSnapshotCopy();
          final byte[] name = diffInPb.getName().toByteArray();
          PermissionStatus permission = loadPermission(
              dirCopyInPb.getPermission(), state.getStringTable());
          AclFeature acl = null;
          if (dirCopyInPb.hasAcl()) {
            int[] entries = AclEntryStatusFormat
                .toInt(FSImageFormatPBINode.Loader.loadAclEntries(
                    dirCopyInPb.getAcl(), state.getStringTable()));
            acl = new AclFeature(entries);
          }
          XAttrFeature xAttrs = null;
          if (dirCopyInPb.hasXAttrs()) {
            xAttrs = new XAttrFeature(FSImageFormatPBINode.Loader.loadXAttrs(
                dirCopyInPb.getXAttrs(), state.getStringTable()));
          }

          long modTime = dirCopyInPb.getModificationTime();
          boolean noQuota = dirCopyInPb.getNsQuota() == -1
              && dirCopyInPb.getDsQuota() == -1
              && (!dirCopyInPb.hasTypeQuotas());

          if (noQuota) {
            copy = new INodeDirectoryAttributes.SnapshotCopy(name,
              permission, acl, modTime, xAttrs);
          } else {
            EnumCounters<StorageType> typeQuotas = null;
            if (dirCopyInPb.hasTypeQuotas()) {
              ImmutableList<QuotaByStorageTypeEntry> qes =
                  FSImageFormatPBINode.Loader.loadQuotaByStorageTypeEntries(
                      dirCopyInPb.getTypeQuotas());
              typeQuotas = new EnumCounters<StorageType>(StorageType.class,
                  HdfsConstants.QUOTA_RESET);
              for (QuotaByStorageTypeEntry qe : qes) {
                if (qe.getQuota() >= 0 && qe.getStorageType() != null &&
                    qe.getStorageType().supportTypeQuota()) {
                  typeQuotas.set(qe.getStorageType(), qe.getQuota());
                }
              }
            }
            copy = new INodeDirectoryAttributes.CopyWithQuota(name, permission,
                acl, modTime, dirCopyInPb.getNsQuota(),
                dirCopyInPb.getDsQuota(), typeQuotas, xAttrs);
          }
        }
        // load created list
        List<INode> clist = loadCreatedList(in, dir,
            diffInPb.getCreatedListSize());
        // load deleted list
        List<INode> dlist = loadDeletedList(refList, in, dir,
            diffInPb.getDeletedINodeList(), diffInPb.getDeletedINodeRefList());
        // create the directory diff
        DirectoryDiff diff = new DirectoryDiff(snapshotId, copy, null,
            childrenSize, clist, dlist, useRoot);
        diffs.addFirst(diff);
      }
    }
  }

  /**
   * Saving snapshot related information to protobuf based FSImage
   */
  public final static class Saver {
    private final FSNamesystem fsn;
    private final FileSummary.Builder headers;
    private final FlatBufferBuilder fbb;
    private final FSImageFormatProtobuf.Saver parent;
    private final SaveNamespaceContext context;

    public Saver(FSImageFormatProtobuf.Saver parent,
        FileSummary.Builder headers, FlatBufferBuilder fbb, SaveNamespaceContext context,
        FSNamesystem fsn) {
      this.parent = parent;
      this.headers = headers;
      this.fbb = fbb;
      this.context = context;
      this.fsn = fsn;
    }

    /**
     * save all the snapshottable directories and snapshots to fsimage
     */
    public int serializeIntelSnapshotSection(OutputStream out, FlatBufferBuilder fbb2) throws IOException {
      SnapshotManager sm = fsn.getSnapshotManager();
      FlatBufferBuilder fbb = new FlatBufferBuilder();

      int snapshottableDirOffset = 0;
      ArrayList<Long> list = new ArrayList<>();
      INodeDirectory[] snapshottables = sm.getSnapshottableDirs();
      for (INodeDirectory sdir : snapshottables) {
        list.add(sdir.getId());
      }
      Long[] data = list.toArray(new Long[list.size()]);

      snapshottableDirOffset = IntelSnapshotSection.
          createSnapshottableDirVector(fbb, ArrayUtils.toPrimitive(data));

      IntelSnapshotSection.startIntelSnapshotSection(fbb);
      IntelSnapshotSection.addSnapshotCounter(fbb, sm.getSnapshotCounter());
      IntelSnapshotSection.addNumSnapshots(fbb, sm.getNumSnapshots());
      IntelSnapshotSection.addSnapshottableDir(fbb, snapshottableDirOffset);
      int inv = IntelSnapshotSection.endIntelSnapshotSection(fbb);
      IntelSnapshotSection.finishIntelSnapshotSectionBuffer(fbb, inv);
      byte[] bytes = fbb.sizedByteArray();
      writeTo(bytes, bytes.length, out);
      FlatBufferBuilder fbb1 = new FlatBufferBuilder();
      int i = 0;
      for(INodeDirectory sdir : snapshottables) {
        for (Snapshot s : sdir.getDirectorySnapshottableFeature()
            .getSnapshotList()) {
          Root sroot = s.getRoot();
          int intelINodeDirectory =
              buildIntelINodeDirectory(sroot, parent.getSaverContext(),fbb1);
          int r = IntelINode.createIntelINode(fbb1, IntelTypee.DIRECTORY, sroot.getId(),
              fbb1.createString(sroot.getLocalName()), 0, intelINodeDirectory, 0);
          int offset = 0;

          IntelSnapshot.startIntelSnapshot(fbb1);
          IntelSnapshot.addSnapshotId(fbb1, s.getId());
          IntelSnapshot.addRoot(fbb1, r);
          offset = IntelSnapshot.endIntelSnapshot(fbb1);
          IntelSnapshot.finishIntelSnapshotBuffer(fbb1, offset);
          byte[] bytes1 = fbb1.sizedByteArray();
          writeTo(bytes1, bytes1.length, out);
          i++;
          if (i % FSImageFormatProtobuf.Saver.CHECK_CANCEL_INTERVAL == 0) {
            context.checkCancelled();
          }
        }
      }
      Preconditions.checkState(i == sm.getNumSnapshots());
     return parent.commitIntelSection(FSImageFormatProtobuf.SectionName.SNAPSHOT, fbb2);
    }

    /**
     * abandon method
     *
     */
    public void serializeSnapshotSection(OutputStream out) throws IOException {
      SnapshotManager sm = fsn.getSnapshotManager();
      SnapshotSection.Builder b = SnapshotSection.newBuilder()
          .setSnapshotCounter(sm.getSnapshotCounter())
          .setNumSnapshots(sm.getNumSnapshots());

      INodeDirectory[] snapshottables = sm.getSnapshottableDirs();
      for (INodeDirectory sdir : snapshottables) {
        b.addSnapshottableDir(sdir.getId());
      }
      b.build().writeDelimitedTo(out);
      int i = 0;
      for(INodeDirectory sdir : snapshottables) {
        for (Snapshot s : sdir.getDirectorySnapshottableFeature()
            .getSnapshotList()) {
          Root sroot = s.getRoot();

          SnapshotSection.Snapshot.Builder sb = SnapshotSection.Snapshot
              .newBuilder().setSnapshotId(s.getId());
          INodeSection.INodeDirectory.Builder db = buildINodeDirectory(sroot,
              parent.getSaverContext());
          INodeSection.INode r = INodeSection.INode.newBuilder()
              .setId(sroot.getId())
              .setType(INodeSection.INode.Type.DIRECTORY)
              .setName(ByteString.copyFrom(sroot.getLocalNameBytes()))
              .setDirectory(db).build();
          sb.setRoot(r).build().writeDelimitedTo(out);
          i++;
          if (i % FSImageFormatProtobuf.Saver.CHECK_CANCEL_INTERVAL == 0) {
            context.checkCancelled();
          }
        }
      }
      Preconditions.checkState(i == sm.getNumSnapshots());
      parent.commitSection(headers, FSImageFormatProtobuf.SectionName.SNAPSHOT);
    }


    /**
     * This can only be called after serializing both INode_Dir and SnapshotDiff
     */
    public int serializeIntelINodeReferenceSection(OutputStream out, FlatBufferBuilder fbb1)
        throws IOException {
      FlatBufferBuilder fbb = new FlatBufferBuilder();
      int name = 0;
      long dstSnapshotId = 0, lastSnapshotId = 0;
      final List<INodeReference> refList = parent.getSaverContext()
          .getRefList();
      for (INodeReference ref : refList) {
        if (ref instanceof WithName) {
          name = fbb.createString(bytesToString(ref.getLocalNameBytes()));
          lastSnapshotId = ((WithName) ref).getLastSnapshotId();
        } else if (ref instanceof DstReference) {
          dstSnapshotId = ref.getDstSnapshotId();
        }
        int offset = IntelINodeReference.createIntelINodeReference(fbb, ref.getId(),
            name, dstSnapshotId, lastSnapshotId);
        IntelINodeReference.finishIntelINodeReferenceBuffer(fbb, offset);
        byte[] bytes = fbb.sizedByteArray();
        writeTo(bytes, bytes.length, out);
      }
     return parent.commitIntelSection(SectionName.INODE_REFERENCE, fbb1);
    }


    /**
     * This can only be called after serializing both INode_Dir and SnapshotDiff
     */
    public void serializeINodeReferenceSection(OutputStream out)
        throws IOException {
      final List<INodeReference> refList = parent.getSaverContext()
          .getRefList();
      for (INodeReference ref : refList) {
        INodeReferenceSection.INodeReference.Builder rb = buildINodeReference(ref);
        rb.build().writeDelimitedTo(out);
      }
      parent.commitSection(headers, SectionName.INODE_REFERENCE);
    }

    private INodeReferenceSection.INodeReference.Builder buildINodeReference(
        INodeReference ref) throws IOException {
      INodeReferenceSection.INodeReference.Builder rb =
          INodeReferenceSection.INodeReference.newBuilder().
            setReferredId(ref.getId());
      if (ref instanceof WithName) {
        rb.setLastSnapshotId(((WithName) ref).getLastSnapshotId()).setName(
            ByteString.copyFrom(ref.getLocalNameBytes()));
      } else if (ref instanceof DstReference) {
        rb.setDstSnapshotId(ref.getDstSnapshotId());
      }
      return rb;
    }

    public int serializeIntelSnapshotDiffSection(OutputStream out, FlatBufferBuilder fbb)
        throws IOException {
      INodeMap inodesMap = fsn.getFSDirectory().getINodeMap();
      final List<INodeReference> refList = parent.getSaverContext()
          .getRefList();
      int i = 0;
      Iterator<INodeWithAdditionalFields> iter = inodesMap.getMapIterator();
      while (iter.hasNext()) {
        INodeWithAdditionalFields inode = iter.next();

        if (inode.isFile()) {
          serializeIntelFileDiffList(inode.asFile(), out); // Intel has already finished.
        } else if (inode.isDirectory()) {
          serializeIntelDirDiffList(inode.asDirectory(), refList, out); // Intel has already finished.
        }
        ++i;
        if (i % FSImageFormatProtobuf.Saver.CHECK_CANCEL_INTERVAL == 0) {
          context.checkCancelled();
        }
      }
      return parent.commitIntelSection(FSImageFormatProtobuf.SectionName.SNAPSHOT_DIFF, fbb);
    }

    /**
     * save all the snapshot diff to fsimage
     */
    public void serializeSnapshotDiffSection(OutputStream out)
        throws IOException {
      INodeMap inodesMap = fsn.getFSDirectory().getINodeMap();
      final List<INodeReference> refList = parent.getSaverContext()
          .getRefList();
      int i = 0;
      Iterator<INodeWithAdditionalFields> iter = inodesMap.getMapIterator();
      while (iter.hasNext()) {
        INodeWithAdditionalFields inode = iter.next();
        if (inode.isFile()) {
          serializeFileDiffList(inode.asFile(), out);
        } else if (inode.isDirectory()) {
          serializeDirDiffList(inode.asDirectory(), refList, out);
        }
        ++i;
        if (i % FSImageFormatProtobuf.Saver.CHECK_CANCEL_INTERVAL == 0) {
          context.checkCancelled();
        }
      }
      parent.commitSection(headers,
          FSImageFormatProtobuf.SectionName.SNAPSHOT_DIFF);
    }

    private void saveIntelCreatedList(List<INode> created, OutputStream out)
    throws IOException{
      FlatBufferBuilder fbb = new FlatBufferBuilder();
      // local names of the created list member
      for (INode c : created) {
        int inv =IntelCreatedListEntry.createIntelCreatedListEntry(fbb,
            fbb.createString(bytesToString(c.getLocalNameBytes())));
        IntelCreatedListEntry.finishIntelCreatedListEntryBuffer(fbb, inv);
        byte[] bytes = fbb.sizedByteArray();
        writeTo(bytes, bytes.length, out);
      }
    }

    private void saveCreatedList(List<INode> created, OutputStream out)
        throws IOException {
      // local names of the created list member
      for (INode c : created) {
        SnapshotDiffSection.CreatedListEntry.newBuilder()
            .setName(ByteString.copyFrom(c.getLocalNameBytes())).build()
            .writeDelimitedTo(out);
      }
    }

    private void serializeIntelFileDiffList(INodeFile file, OutputStream out)
        throws IOException {
      FileWithSnapshotFeature sf = file.getFileWithSnapshotFeature();
      if (sf != null) {
        List<FileDiff> diffList = sf.getDiffs().asList();
        FlatBufferBuilder fbb = new FlatBufferBuilder();

        int inv = IntelDiffEntry.createIntelDiffEntry(fbb, IntelType.FILEDIFF, file.getId(), diffList.size());
        IntelDiffEntry.finishIntelDiffEntryBuffer(fbb, inv);
        byte[] bytes = fbb.sizedByteArray();
        writeTo(bytes, bytes.length, out);

        ArrayList<Integer> list = new ArrayList<>();
        FlatBufferBuilder fbb1 = new FlatBufferBuilder();
        for (int i = diffList.size() - 1; i >= 0; i--) {
          FileDiff diff = diffList.get(i);
          if(diff.getBlocks() != null) {
            for(Block block : diff.getBlocks()) {
              list.add(PBHelper.convertIntel(block, fbb1));
            }
          }
          int[] data = ArrayUtils.toPrimitive(list.toArray(new Integer[list.size()]));
          int blocks = IntelFileDiff.createBlocksVector(fbb1, data);
          int name = 0;
          int snapshotcopy = 0;
          INodeFileAttributes copy = diff.snapshotINode;

          if (copy != null) {
            name = fbb1.createString(bytesToString(copy.getLocalNameBytes()));
            snapshotcopy = buildIntelINodeFile(fbb1, copy, parent.getSaverContext());
          }
          int offset = IntelFileDiff.createIntelFileDiff(fbb1, diff.getSnapshotId(),
              diff.getFileSize(), name, snapshotcopy, blocks);
          IntelFileDiff.finishIntelFileDiffBuffer(fbb1, offset);
          byte[] bytes1 = fbb1.sizedByteArray();
          writeTo(bytes1, bytes1.length, out);
        }
      }
    }

    private void serializeFileDiffList(INodeFile file, OutputStream out)
        throws IOException {
      FileWithSnapshotFeature sf = file.getFileWithSnapshotFeature();
      if (sf != null) {
        List<FileDiff> diffList = sf.getDiffs().asList();
        SnapshotDiffSection.DiffEntry entry = SnapshotDiffSection.DiffEntry
            .newBuilder().setInodeId(file.getId()).setType(Type.FILEDIFF)
            .setNumOfDiff(diffList.size()).build();
        entry.writeDelimitedTo(out);
        for (int i = diffList.size() - 1; i >= 0; i--) {
          FileDiff diff = diffList.get(i);
          SnapshotDiffSection.FileDiff.Builder fb = SnapshotDiffSection.FileDiff
              .newBuilder().setSnapshotId(diff.getSnapshotId())
              .setFileSize(diff.getFileSize());
          if(diff.getBlocks() != null) {
            for(Block block : diff.getBlocks()) {
              fb.addBlocks(PBHelper.convert(block));
            }
          }
          INodeFileAttributes copy = diff.snapshotINode;
          if (copy != null) {
            fb.setName(ByteString.copyFrom(copy.getLocalNameBytes()))
                .setSnapshotCopy(buildINodeFile(copy, parent.getSaverContext()));
          }
          fb.build().writeDelimitedTo(out);
        }
      }
    }

    private void serializeIntelDirDiffList(INodeDirectory dir,
        final List<INodeReference> refList, OutputStream out)
        throws IOException {
      DirectoryWithSnapshotFeature sf = dir.getDirectoryWithSnapshotFeature();
      if (sf != null) {
        List<DirectoryDiff> diffList = sf.getDiffs().asList();

        FlatBufferBuilder fbb = new FlatBufferBuilder();
        int offset = IntelDiffEntry.createIntelDiffEntry(fbb, IntelType.DIRECTORYDIFF,
            dir.getId(), diffList.size());
        IntelDiffEntry.finishIntelDiffEntryBuffer(fbb, offset);
        byte[] bytes = fbb.sizedByteArray();
        writeTo(bytes, bytes.length, out);

        FlatBufferBuilder fbb1 = new FlatBufferBuilder();
        int name = 0, snapshotCopy = 0;
        for (int i = diffList.size() - 1; i >= 0; i--) { // reverse order!
          DirectoryDiff diff = diffList.get(i);
          INodeDirectoryAttributes copy = diff.snapshotINode;
          if (!diff.isSnapshotRoot() && copy != null) {
            name = fbb1.createString(bytesToString(copy.getLocalNameBytes()));
            snapshotCopy = buildIntelINodeDirectory(copy, parent.getSaverContext(),fbb1);
          }
          long createdListSize = 0;
          // process created list and deleted list
          List<INode> created = diff.getChildrenDiff()
              .getList(ListType.CREATED);
          createdListSize = created.size();
          int deletedINode = 0, deletedINodeRef = 0;
          List<INode> deleted = diff.getChildrenDiff().getList(ListType.DELETED);
          ArrayList<Integer> list1 = new ArrayList<>();
          ArrayList<Long> list2 = new ArrayList<>();
          for (INode d : deleted) {
            if (d.isReference()) {
              refList.add(d.asReference());
              list1.add(refList.size() - 1);
            } else {
              list2.add(d.getId());
            }
          }
          deletedINodeRef = IntelDirectoryDiff.createDeletedINodeRefVector(fbb1,
              ArrayUtils.toPrimitive(list1.toArray(new Integer[list1.size()])));
          deletedINode = IntelDirectoryDiff.createDeletedINodeVector(fbb1,
              ArrayUtils.toPrimitive(list2.toArray(new Long[list2.size()])));
          int inv = IntelDirectoryDiff.createIntelDirectoryDiff(fbb1, diff.getSnapshotId(),
              diff.getChildrenSize(), diff.isSnapshotRoot(), name, snapshotCopy,
              createdListSize, deletedINode, deletedINodeRef
              );
          IntelDirectoryDiff.finishIntelDirectoryDiffBuffer(fbb1, inv);
          byte[] bytes1 = fbb1.sizedByteArray();
          writeTo(bytes1, bytes1.length, out);
          saveIntelCreatedList(created, out); // intel will have rewrite
        }
      }
    }

    private void serializeDirDiffList(INodeDirectory dir,
        final List<INodeReference> refList, OutputStream out)
        throws IOException {
      DirectoryWithSnapshotFeature sf = dir.getDirectoryWithSnapshotFeature();
      if (sf != null) {
        List<DirectoryDiff> diffList = sf.getDiffs().asList();
        SnapshotDiffSection.DiffEntry entry = SnapshotDiffSection.DiffEntry
            .newBuilder().setInodeId(dir.getId()).setType(Type.DIRECTORYDIFF)
            .setNumOfDiff(diffList.size()).build();
        entry.writeDelimitedTo(out);
        for (int i = diffList.size() - 1; i >= 0; i--) { // reverse order!
          DirectoryDiff diff = diffList.get(i);
          SnapshotDiffSection.DirectoryDiff.Builder db = SnapshotDiffSection.
              DirectoryDiff.newBuilder().setSnapshotId(diff.getSnapshotId())
                           .setChildrenSize(diff.getChildrenSize())
                           .setIsSnapshotRoot(diff.isSnapshotRoot());
          INodeDirectoryAttributes copy = diff.snapshotINode;
          if (!diff.isSnapshotRoot() && copy != null) {
            db.setName(ByteString.copyFrom(copy.getLocalNameBytes()))
                .setSnapshotCopy(
                    buildINodeDirectory(copy, parent.getSaverContext()));
          }
          // process created list and deleted list
          List<INode> created = diff.getChildrenDiff()
              .getList(ListType.CREATED);
          db.setCreatedListSize(created.size());
          List<INode> deleted = diff.getChildrenDiff().getList(ListType.DELETED);
          for (INode d : deleted) {
            if (d.isReference()) {
              refList.add(d.asReference());
              db.addDeletedINodeRef(refList.size() - 1);
            } else {
              db.addDeletedINode(d.getId());
            }
          }
          db.build().writeDelimitedTo(out);
          saveCreatedList(created, out);
        }
      }
    }
  }



  private FSImageFormatPBSnapshot(){}
}
