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

import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.Loader.loadINodeDirectory;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.Loader.loadPermission;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.Loader.updateBlocksMap;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.Saver.buildINodeDirectory;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.Saver.buildINodeFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.namenode.AclEntryStatusFormat;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.LoaderContext;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeReferenceSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotDiffSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotDiffSection.CreatedListEntry;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotDiffSection.DiffEntry.Type;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotSection;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeMap;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.DstReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithName;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.hdfs.server.namenode.QuotaByStorageTypeEntry;
import org.apache.hadoop.hdfs.server.namenode.SaveNamespaceContext;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.Root;
import org.apache.hadoop.hdfs.server.namenode.XAttrFeature;
import org.apache.hadoop.hdfs.util.Diff.ListType;
import org.apache.hadoop.hdfs.util.EnumCounters;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

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
    public void loadINodeReferenceSection(InputStream in) throws IOException {
      final List<INodeReference> refList = parent.getLoaderContext()
          .getRefList();
      while (true) {
        INodeReferenceSection.INodeReference e = INodeReferenceSection
            .INodeReference.parseDelimitedFrom(in);
        if (e == null) {
          break;
        }
        INodeReference ref = loadINodeReference(e);
        refList.add(ref);
      }
    }

    private INodeReference loadINodeReference(
        INodeReferenceSection.INodeReference r) throws IOException {
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
              (byte)fileInPb.getStoragePolicyID(), 
              xAttrs);
        }

        FileDiff diff = new FileDiff(pbf.getSnapshotId(), copy, null,
            pbf.getFileSize());
        List<BlockProto> bpl = pbf.getBlocksList();
        BlockInfoContiguous[] blocks = new BlockInfoContiguous[bpl.size()];
        for(int j = 0, e = bpl.size(); j < e; ++j) {
          Block blk = PBHelper.convert(bpl.get(j));
          BlockInfoContiguous storedBlock =  fsn.getBlockManager().getStoredBlock(blk);
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
        CreatedListEntry entry = CreatedListEntry.parseDelimitedFrom(in);
        INode created = SnapshotFSImageFormat.loadCreated(entry.getName()
            .toByteArray(), dir);
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
    private final FSImageFormatProtobuf.Saver parent;
    private final SaveNamespaceContext context;

    public Saver(FSImageFormatProtobuf.Saver parent,
        FileSummary.Builder headers, SaveNamespaceContext context,
        FSNamesystem fsn) {
      this.parent = parent;
      this.headers = headers;
      this.context = context;
      this.fsn = fsn;
    }

    /**
     * save all the snapshottable directories and snapshots to fsimage
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

    private void saveCreatedList(List<INode> created, OutputStream out)
        throws IOException {
      // local names of the created list member
      for (INode c : created) {
        SnapshotDiffSection.CreatedListEntry.newBuilder()
            .setName(ByteString.copyFrom(c.getLocalNameBytes())).build()
            .writeDelimitedTo(out);
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
