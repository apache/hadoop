/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstructionContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.flatbuffer.*;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.LoaderContext;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SaverContext;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeDirectorySection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.AclFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.XAttrCompactProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.XAttrFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.QuotaByStorageTypeEntryProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.QuotaByStorageTypeFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

@InterfaceAudience.Private
public final class FSImageFormatPBINode {
  private final static long USER_GROUP_STRID_MASK = (1 << 24) - 1;
  private final static int USER_STRID_OFFSET = 40;
  private final static int GROUP_STRID_OFFSET = 16;

  private static final int ACL_ENTRY_NAME_MASK = (1 << 24) - 1;
  private static final int ACL_ENTRY_NAME_OFFSET = 6;
  private static final int ACL_ENTRY_TYPE_OFFSET = 3;
  private static final int ACL_ENTRY_SCOPE_OFFSET = 5;
  private static final int ACL_ENTRY_PERM_MASK = 7;
  private static final int ACL_ENTRY_TYPE_MASK = 3;
  private static final int ACL_ENTRY_SCOPE_MASK = 1;
  private static final FsAction[] FSACTION_VALUES = FsAction.values();
  private static final AclEntryScope[] ACL_ENTRY_SCOPE_VALUES = AclEntryScope
      .values();
  private static final AclEntryType[] ACL_ENTRY_TYPE_VALUES = AclEntryType
      .values();

  private static final int XATTR_NAMESPACE_MASK = 3;
  private static final int XATTR_NAMESPACE_OFFSET = 30;
  private static final int XATTR_NAME_MASK = (1 << 24) - 1;
  private static final int XATTR_NAME_OFFSET = 6;

  /* See the comments in fsimage.proto for an explanation of the following. */
  private static final int XATTR_NAMESPACE_EXT_OFFSET = 5;
  private static final int XATTR_NAMESPACE_EXT_MASK = 1;

  private static final XAttr.NameSpace[] XATTR_NAMESPACE_VALUES =
      XAttr.NameSpace.values();


  private static final Log LOG = LogFactory.getLog(FSImageFormatPBINode.class);

  public final static class Loader {
    public static PermissionStatus loadPermission(long id,
                                                  final String[] stringTable) {
      short perm = (short) (id & ((1 << GROUP_STRID_OFFSET) - 1));
      int gsid = (int) ((id >> GROUP_STRID_OFFSET) & USER_GROUP_STRID_MASK);
      int usid = (int) ((id >> USER_STRID_OFFSET) & USER_GROUP_STRID_MASK);
      return new PermissionStatus(stringTable[usid], stringTable[gsid],
          new FsPermission(perm));
    }

    public static ImmutableList<AclEntry> loadIntelAclEntries(
        IntelAclFeatureProto proto, final String[] stringTable) {
      ImmutableList.Builder<AclEntry> b = ImmutableList.builder();
      Long[] i = new Long[proto.entriesLength()];
      long v;
      for (int j = 0; j < i.length; j++) {
        i[j] = proto.entries(j);
        v = i[j];
        int p = (int) (v & ACL_ENTRY_PERM_MASK);
        int t = (int) ((v >> ACL_ENTRY_TYPE_OFFSET) & ACL_ENTRY_TYPE_MASK);
        int s = (int) ((v >> ACL_ENTRY_SCOPE_OFFSET) & ACL_ENTRY_SCOPE_MASK);
        int nid = (int) ((v >> ACL_ENTRY_NAME_OFFSET) & ACL_ENTRY_NAME_MASK);
        String name = stringTable[nid];
        b.add(new AclEntry.Builder().setName(name)
            .setPermission(FSACTION_VALUES[p])
            .setScope(ACL_ENTRY_SCOPE_VALUES[s])
            .setType(ACL_ENTRY_TYPE_VALUES[t]).build());
      }
      return b.build();
    }

    public static ImmutableList<AclEntry> loadAclEntries(
        AclFeatureProto proto, final String[] stringTable) {
      ImmutableList.Builder<AclEntry> b = ImmutableList.builder();

      for (int v : proto.getEntriesList()) {
        int p = v & ACL_ENTRY_PERM_MASK;
        int t = (v >> ACL_ENTRY_TYPE_OFFSET) & ACL_ENTRY_TYPE_MASK;
        int s = (v >> ACL_ENTRY_SCOPE_OFFSET) & ACL_ENTRY_SCOPE_MASK;
        int nid = (v >> ACL_ENTRY_NAME_OFFSET) & ACL_ENTRY_NAME_MASK;
        String name = stringTable[nid];
        b.add(new AclEntry.Builder().setName(name)
            .setPermission(FSACTION_VALUES[p])
            .setScope(ACL_ENTRY_SCOPE_VALUES[s])
            .setType(ACL_ENTRY_TYPE_VALUES[t]).build());
      }
      return b.build();
    }

    public static ImmutableList<XAttr> loadIntelXAttrs(
        IntelXAttrFeatureProto proto, final String[] stringTable) {

      ImmutableList.Builder<XAttr> b = ImmutableList.builder();
      IntelXAttrCompactProto[] xAttrCompactProtos
          = new IntelXAttrCompactProto[proto.xAttrsLength()];
      for (int i = 0; i < proto.xAttrsLength(); i++) {
        xAttrCompactProtos[i] = proto.xAttrs(i);
        int v = (int) xAttrCompactProtos[i].name();
        int nid = (v >> XATTR_NAME_OFFSET) & XATTR_NAME_MASK;
        int ns = (v >> XATTR_NAMESPACE_OFFSET) & XATTR_NAMESPACE_MASK;
        ns |=
            ((v >> XATTR_NAMESPACE_EXT_OFFSET) & XATTR_NAMESPACE_EXT_MASK) << 2;
        String name = stringTable[nid];
        byte[] value = null;
        if (xAttrCompactProtos[i].value() != null) {
          value = xAttrCompactProtos[i].value().getBytes();
        }
        b.add(new XAttr.Builder().setNameSpace(XATTR_NAMESPACE_VALUES[ns])
            .setName(name).setValue(value).build());
      }
      return b.build();
    }


    public static ImmutableList<XAttr> loadXAttrs(
        XAttrFeatureProto proto, final String[] stringTable) {
      ImmutableList.Builder<XAttr> b = ImmutableList.builder();
      for (XAttrCompactProto xAttrCompactProto : proto.getXAttrsList()) {
        int v = xAttrCompactProto.getName();
        int nid = (v >> XATTR_NAME_OFFSET) & XATTR_NAME_MASK;
        int ns = (v >> XATTR_NAMESPACE_OFFSET) & XATTR_NAMESPACE_MASK;
        ns |=
            ((v >> XATTR_NAMESPACE_EXT_OFFSET) & XATTR_NAMESPACE_EXT_MASK) << 2;
        String name = stringTable[nid];
        byte[] value = null;
        if (xAttrCompactProto.getValue() != null) {
          value = xAttrCompactProto.getValue().toByteArray();
        }
        b.add(new XAttr.Builder().setNameSpace(XATTR_NAMESPACE_VALUES[ns])
            .setName(name).setValue(value).build());
      }

      return b.build();
    }


    public static ImmutableList<QuotaByStorageTypeEntry> loadIntelQuotaByStorageTypeEntries(
        IntelQuotaByStorageTypeFeatureProto proto) {
      ImmutableList.Builder<QuotaByStorageTypeEntry> b = ImmutableList.builder();

      IntelQuotaByStorageTypeEntryProto[] intelquotaEntry
          = new IntelQuotaByStorageTypeEntryProto[proto.quotasLength()];
      for (int i = 0; i < proto.quotasLength(); i++) {
        intelquotaEntry[i] = proto.quotas(i);
        StorageType type = PBHelper.convertIntelStorageType(intelquotaEntry[i].storageType());
        long quota = intelquotaEntry[i].quota();
        b.add(new QuotaByStorageTypeEntry.Builder().setStorageType(type)
            .setQuota(quota).build());
      }
      return b.build();
    }

    public static ImmutableList<QuotaByStorageTypeEntry> loadQuotaByStorageTypeEntries(
        QuotaByStorageTypeFeatureProto proto) {
      ImmutableList.Builder<QuotaByStorageTypeEntry> b = ImmutableList.builder();
      for (QuotaByStorageTypeEntryProto quotaEntry : proto.getQuotasList()) {
        StorageType type = PBHelper.convertStorageType(quotaEntry.getStorageType());
        long quota = quotaEntry.getQuota();
        b.add(new QuotaByStorageTypeEntry.Builder().setStorageType(type)
            .setQuota(quota).build());
      }
      return b.build();
    }

    public static INodeDirectory loadIntelINodeDirectory(IntelINode intelINode,
                                                         LoaderContext state) {
      assert intelINode.type() == IntelTypee.DIRECTORY;
//      assert n.getType() == INodeSection.INode.Type.DIRECTORY;

      IntelINodeDirectory id = intelINode.directory();

//      INodeSection.INodeDirectory d = n.getDirectory();

      final PermissionStatus permissions = loadPermission(id.permission(),
          state.getStringTable());
      final INodeDirectory dir = new INodeDirectory(intelINode.id(),
          intelINode.name().getBytes(), permissions, id.modificationTime());

      final long nsQuota = id.nsQuota(), dsQuota = id.dsQuota();
      if (nsQuota >= 0 || dsQuota >= 0) {
        dir.addDirectoryWithQuotaFeature(new DirectoryWithQuotaFeature.Builder().
            nameSpaceQuota(nsQuota).storageSpaceQuota(dsQuota).build());
      }
      EnumCounters<StorageType> typeQuotas = null;

      if (id.typeQuotas() != null) {
        ImmutableList<QuotaByStorageTypeEntry> qes =
            loadIntelQuotaByStorageTypeEntries(id.typeQuotas());

        typeQuotas = new EnumCounters<StorageType>(StorageType.class,
            HdfsConstants.QUOTA_RESET);
        for (QuotaByStorageTypeEntry qe : qes) {
          if (qe.getQuota() >= 0 && qe.getStorageType() != null &&
              qe.getStorageType().supportTypeQuota()) {
            typeQuotas.set(qe.getStorageType(), qe.getQuota());
          }
        }

        if (typeQuotas.anyGreaterOrEqual(0)) {
          DirectoryWithQuotaFeature q = dir.getDirectoryWithQuotaFeature();
          if (q == null) {
            dir.addDirectoryWithQuotaFeature(new DirectoryWithQuotaFeature.
                Builder().typeQuotas(typeQuotas).build());
          } else {
            q.setQuota(typeQuotas);
          }
        }
      }

      if (id.acl() != null) {
        int[] entries = AclEntryStatusFormat.toInt(loadIntelAclEntries(
            id.acl(), state.getStringTable()));
        dir.addAclFeature(new AclFeature(entries));
      }
      if (id.xAttrs() != null) {
        dir.addXAttrFeature(new XAttrFeature(
            loadIntelXAttrs(id.xAttrs(), state.getStringTable())));
      }
      return dir;
    }

    public static INodeDirectory loadINodeDirectory(INodeSection.INode n,
                                                    LoaderContext state) {
      assert n.getType() == INodeSection.INode.Type.DIRECTORY;
      INodeSection.INodeDirectory d = n.getDirectory();

      final PermissionStatus permissions = loadPermission(d.getPermission(),
          state.getStringTable());
      final INodeDirectory dir = new INodeDirectory(n.getId(), n.getName()
          .toByteArray(), permissions, d.getModificationTime());
      final long nsQuota = d.getNsQuota(), dsQuota = d.getDsQuota();
      if (nsQuota >= 0 || dsQuota >= 0) {
        dir.addDirectoryWithQuotaFeature(new DirectoryWithQuotaFeature.Builder().
            nameSpaceQuota(nsQuota).storageSpaceQuota(dsQuota).build());
      }
      EnumCounters<StorageType> typeQuotas = null;
      if (d.hasTypeQuotas()) {
        ImmutableList<QuotaByStorageTypeEntry> qes =
            loadQuotaByStorageTypeEntries(d.getTypeQuotas());
        typeQuotas = new EnumCounters<StorageType>(StorageType.class,
            HdfsConstants.QUOTA_RESET);
        for (QuotaByStorageTypeEntry qe : qes) {
          if (qe.getQuota() >= 0 && qe.getStorageType() != null &&
              qe.getStorageType().supportTypeQuota()) {
            typeQuotas.set(qe.getStorageType(), qe.getQuota());
          }
        }

        if (typeQuotas.anyGreaterOrEqual(0)) {
          DirectoryWithQuotaFeature q = dir.getDirectoryWithQuotaFeature();
          if (q == null) {
            dir.addDirectoryWithQuotaFeature(new DirectoryWithQuotaFeature.
                Builder().typeQuotas(typeQuotas).build());
          } else {
            q.setQuota(typeQuotas);
          }
        }
      }

      if (d.hasAcl()) {
        int[] entries = AclEntryStatusFormat.toInt(loadAclEntries(
            d.getAcl(), state.getStringTable()));
        dir.addAclFeature(new AclFeature(entries));
      }
      if (d.hasXAttrs()) {
        dir.addXAttrFeature(new XAttrFeature(
            loadXAttrs(d.getXAttrs(), state.getStringTable())));
      }
      return dir;
    }

    public static void updateBlocksMap(INodeFile file, BlockManager bm) {
      // Add file->block mapping
      final BlockInfo[] blocks = file.getBlocks();
      if (blocks != null) {
        for (int i = 0; i < blocks.length; i++) {
          file.setBlock(i, bm.addBlockCollection(blocks[i], file));
        }
      }
    }

    private final FSDirectory dir;
    private final FSNamesystem fsn;
    private final FSImageFormatProtobuf.Loader parent;

    Loader(FSNamesystem fsn, final FSImageFormatProtobuf.Loader parent) {
      this.fsn = fsn;
      this.dir = fsn.dir;
      this.parent = parent;
    }

    void loadIntelINodeDirectorySection(InputStream in) throws IOException {
      final List<INodeReference> refList = parent.getLoaderContext()
          .getRefList();
      while (true) {

        byte[] bytes = parseFrom(in);
        if (bytes == null) {
          break;
        }
        IntelDirEntry ie = IntelDirEntry.getRootAsIntelDirEntry(ByteBuffer.wrap(bytes));
        // note that in is a LimitedInputStream
        INodeDirectory p = dir.getInode(ie.parent()).asDirectory();

        for (int i = 0; i < ie.childrenLength(); i++) {
          INode child = dir.getInode(ie.children(i));
          addToParent(p, child);
        }

        for (int i = 0; i < ie.refChildrenLength(); i++) {
          INodeReference ref = refList.get((int) ie.refChildren(i));
          addToParent(p, ref);
        }
      }
    }

    void loadINodeDirectorySection(InputStream in) throws IOException {
      final List<INodeReference> refList = parent.getLoaderContext()
          .getRefList();
      while (true) {
        INodeDirectorySection.DirEntry e = INodeDirectorySection.DirEntry
            .parseDelimitedFrom(in);
        // note that in is a LimitedInputStream
        if (e == null) {
          break;
        }
        INodeDirectory p = dir.getInode(e.getParent()).asDirectory();
        for (long id : e.getChildrenList()) {
          INode child = dir.getInode(id);
          addToParent(p, child);
        }
        for (int refId : e.getRefChildrenList()) {
          INodeReference ref = refList.get(refId);
          addToParent(p, ref);
        }
      }
    }

    public static byte[] parseFrom(InputStream in) throws IOException {
      DataInputStream inputStream = new DataInputStream(in);
      int len;
      try {
        len = inputStream.readInt();
      } catch (EOFException e) {
        return null;
      }
      byte[] data = new byte[len];
      inputStream.read(data);
      return data;
    }

    void loadIntelINodeSection(InputStream in, StartupProgress prog,
                               Step currentStep) throws IOException {
      IntelINodeSection is =
          IntelINodeSection.getRootAsIntelINodeSection(ByteBuffer.wrap(parseFrom(in)));

      fsn.dir.resetLastInodeId(is.lastInodeId());
      long numInodes = is.numInodes();

      LOG.info("Loading " + numInodes + " INodes.");
      prog.setTotal(Phase.LOADING_FSIMAGE, currentStep, numInodes);
      Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, currentStep);
      // hack here
      for (int i = 0; i < numInodes; ++i) {
        byte[] b = parseFrom(in);
        if (b == null || b.length <= 0) continue;
        IntelINode iNode = IntelINode.getRootAsIntelINode(ByteBuffer.wrap(b));

        if (iNode.id() == INodeId.ROOT_INODE_ID) {
          loadRootIntelINode(iNode);
        } else {
          INode n = loadIntelINode(iNode);
          dir.addToInodeMap(n);
        }
        counter.increment();
      }
    }

    /**
     * Load the under-construction files section, and update the lease map
     */
    void loadIntelFilesUnderConstructionSection(InputStream in) throws IOException {
      while (true) {
        byte[] bytes = parseFrom(in);
        if (bytes == null) {
          break;
        }
        IntelFileUnderConstructionEntry ientry = IntelFileUnderConstructionEntry.
            getRootAsIntelFileUnderConstructionEntry(ByteBuffer.wrap(bytes));
        // update the lease manager
        INodeFile file = dir.getInode(ientry.inodeId()).asFile();
        FileUnderConstructionFeature uc = file.getFileUnderConstructionFeature();
        Preconditions.checkState(uc != null); // file must be under-construction
        fsn.leaseManager.addLease(uc.getClientName(), ientry.inodeId());
      }
    }

    /**
     * Load the under-construction files section, and update the lease map
     */
    void loadFilesUnderConstructionSection(InputStream in) throws IOException {
      while (true) {
        FileUnderConstructionEntry entry = FileUnderConstructionEntry
            .parseDelimitedFrom(in);
        if (entry == null) {
          break;
        }
        // update the lease manager
        INodeFile file = dir.getInode(entry.getInodeId()).asFile();
        FileUnderConstructionFeature uc = file.getFileUnderConstructionFeature();
        Preconditions.checkState(uc != null); // file must be under-construction
        fsn.leaseManager.addLease(uc.getClientName(),
            entry.getInodeId());
      }
    }

    private void addToParent(INodeDirectory parent, INode child) {
      if (parent == dir.rootDir && FSDirectory.isReservedName(child)) {
        throw new HadoopIllegalArgumentException("File name \""
            + child.getLocalName() + "\" is reserved. Please "
            + " change the name of the existing file or directory to another "
            + "name before upgrading to this release.");
      }
      // NOTE: This does not update space counts for parents
      if (!parent.addChild(child)) {
        return;
      }
      dir.cacheName(child);

      if (child.isFile()) {
        updateBlocksMap(child.asFile(), fsn.getBlockManager());
      }
    }

    private INode loadIntelINode(IntelINode in) {
      switch (in.type()) {
        case IntelTypee.FILE:
//          return loadINodeFile(n);
          return loadIntelINodeFile(in);
        case IntelTypee.DIRECTORY:
//          return loadINodeDirectory(n, parent.getLoaderContext());
          return loadIntelINodeDirectory(in, parent.getLoaderContext());
        case IntelTypee.SYMLINK:
//          return loadINodeSymlink(n);
          return loadIntelINodeSymlink(in);
        default:
          break;
      }
      return null;
    }

    private INode loadINode(INodeSection.INode n) {
      switch (n.getType()) {
        case FILE:
          return loadINodeFile(n);
        case DIRECTORY:
          return loadINodeDirectory(n, parent.getLoaderContext());
        case SYMLINK:
          return loadINodeSymlink(n);
        default:
          break;
      }
      return null;
    }

    private INodeFile loadIntelINodeFile(IntelINode in) {
      assert in.type() == IntelTypee.FILE;

      IntelINodeFile intelF = in.file();
      IntelBlockProto[] ibp = new IntelBlockProto[intelF.blocksLength()];
      for (int i = 0; i < intelF.blocksLength(); i++) {
        ibp[i] = intelF.blocks(i);
      }

//      List<BlockProto> bp = f.getBlocksList();
      short replication = (short) intelF.replication();
      LoaderContext state = parent.getLoaderContext();

      BlockInfo[] blocks = new BlockInfo[ibp.length];
      for (int i = 0, e = ibp.length; i < e; ++i) {
        blocks[i] =
            new BlockInfoContiguous(PBHelper.convert(ibp[i]), replication);
      }
      final PermissionStatus permissions = loadPermission(intelF.permission(),
          parent.getLoaderContext().getStringTable());

      final INodeFile file = new INodeFile(in.id(),
          in.name().getBytes(), permissions, intelF.modificationTime(),
          intelF.accessTime(), blocks, replication, intelF.preferredBlockSize(),
          (byte) intelF.storagePolicyID());

      if (intelF.acl() != null) {
        int[] entries = AclEntryStatusFormat.toInt(loadIntelAclEntries(
            intelF.acl(), state.getStringTable()));
        file.addAclFeature(new AclFeature(entries));
      }

      if (intelF.xAttrs() != null) {
        file.addXAttrFeature(new XAttrFeature(
            loadIntelXAttrs(intelF.xAttrs(), state.getStringTable())));
      }

      // under-construction information
      if (intelF.fileUC() != null) {
        IntelFileUnderConstructionFeature iuc = intelF.fileUC();
//        INodeSection.FileUnderConstructionFeature uc = f.getFileUC();
        file.toUnderConstruction(iuc.clientName(), iuc.clientMachine());
        if (blocks.length > 0) {
          BlockInfo lastBlk = file.getLastBlock();
          // replace the last block of file
          file.setBlock(file.numBlocks() - 1,
              new BlockInfoUnderConstructionContiguous(lastBlk, replication));
        }
      }
      return file;
    }

    private INodeFile loadINodeFile(INodeSection.INode n) {
      assert n.getType() == INodeSection.INode.Type.FILE;
      INodeSection.INodeFile f = n.getFile();
      List<BlockProto> bp = f.getBlocksList();
      short replication = (short) f.getReplication();
      LoaderContext state = parent.getLoaderContext();

      BlockInfo[] blocks = new BlockInfo[bp.size()];
      for (int i = 0, e = bp.size(); i < e; ++i) {
        blocks[i] =
            new BlockInfoContiguous(PBHelper.convert(bp.get(i)), replication);
      }
      final PermissionStatus permissions = loadPermission(f.getPermission(),
          parent.getLoaderContext().getStringTable());

      final INodeFile file = new INodeFile(n.getId(),
          n.getName().toByteArray(), permissions, f.getModificationTime(),
          f.getAccessTime(), blocks, replication, f.getPreferredBlockSize(),
          (byte) f.getStoragePolicyID());

      if (f.hasAcl()) {
        int[] entries = AclEntryStatusFormat.toInt(loadAclEntries(
            f.getAcl(), state.getStringTable()));
        file.addAclFeature(new AclFeature(entries));
      }

      if (f.hasXAttrs()) {
        file.addXAttrFeature(new XAttrFeature(
            loadXAttrs(f.getXAttrs(), state.getStringTable())));
      }

      // under-construction information
      if (f.hasFileUC()) {
        INodeSection.FileUnderConstructionFeature uc = f.getFileUC();
        file.toUnderConstruction(uc.getClientName(), uc.getClientMachine());
        if (blocks.length > 0) {
          BlockInfo lastBlk = file.getLastBlock();
          // replace the last block of file
          file.setBlock(file.numBlocks() - 1,
              new BlockInfoUnderConstructionContiguous(lastBlk, replication));
        }
      }
      return file;
    }

    private INodeSymlink loadIntelINodeSymlink(IntelINode in) {
      assert in.type() == IntelTypee.SYMLINK;

      IntelINodeSymlink is = in.symlink();
      final PermissionStatus permissions = loadPermission(is.permission(),
          parent.getLoaderContext().getStringTable());

      INodeSymlink sym = new INodeSymlink(in.id(), in.name().getBytes(),
          permissions, is.modificationTime(), is.accessTime(),
          is.target()); // utf-8 ??? confusing ....

      return sym;
    }

    private INodeSymlink loadINodeSymlink(INodeSection.INode n) {
      assert n.getType() == INodeSection.INode.Type.SYMLINK;
      INodeSection.INodeSymlink s = n.getSymlink();
      final PermissionStatus permissions = loadPermission(s.getPermission(),
          parent.getLoaderContext().getStringTable());

      INodeSymlink sym = new INodeSymlink(n.getId(), n.getName().toByteArray(),
          permissions, s.getModificationTime(), s.getAccessTime(),
          s.getTarget().toStringUtf8());

      return sym;
    }

    private void loadRootINode(INodeSection.INode p) {
      INodeDirectory root = loadINodeDirectory(p, parent.getLoaderContext());
      final QuotaCounts q = root.getQuotaCounts();
      final long nsQuota = q.getNameSpace();
      final long dsQuota = q.getStorageSpace();
      if (nsQuota != -1 || dsQuota != -1) {
        dir.rootDir.getDirectoryWithQuotaFeature().setQuota(nsQuota, dsQuota);
      }
      final EnumCounters<StorageType> typeQuotas = q.getTypeSpaces();
      if (typeQuotas.anyGreaterOrEqual(0)) {
        dir.rootDir.getDirectoryWithQuotaFeature().setQuota(typeQuotas);
      }
      dir.rootDir.cloneModificationTime(root);
      dir.rootDir.clonePermissionStatus(root);
      // root dir supports having extended attributes according to POSIX
      final XAttrFeature f = root.getXAttrFeature();
      if (f != null) {
        dir.rootDir.addXAttrFeature(f);
      }
    }

    private void loadRootIntelINode(IntelINode intelINode) {
      INodeDirectory root = loadIntelINodeDirectory(intelINode, parent.getLoaderContext());

      final QuotaCounts q = root.getQuotaCounts();
      final long nsQuota = q.getNameSpace();
      final long dsQuota = q.getStorageSpace();
      if (nsQuota != -1 || dsQuota != -1) {
        dir.rootDir.getDirectoryWithQuotaFeature().setQuota(nsQuota, dsQuota);
      }
      final EnumCounters<StorageType> typeQuotas = q.getTypeSpaces();
      if (typeQuotas.anyGreaterOrEqual(0)) {
        dir.rootDir.getDirectoryWithQuotaFeature().setQuota(typeQuotas);
      }
      dir.rootDir.cloneModificationTime(root);
      dir.rootDir.clonePermissionStatus(root);
      // root dir supports having extended attributes according to POSIX
      final XAttrFeature f = root.getXAttrFeature();
      if (f != null) {
        dir.rootDir.addXAttrFeature(f);
      }
    }

  }

  public final static class Saver {
    private static long buildPermissionStatus(INodeAttributes n,
                                              final SaverContext.DeduplicationMap<String> stringMap) {
      long userId = stringMap.getId(n.getUserName());
      long groupId = stringMap.getId(n.getGroupName());
      return ((userId & USER_GROUP_STRID_MASK) << USER_STRID_OFFSET)
          | ((groupId & USER_GROUP_STRID_MASK) << GROUP_STRID_OFFSET)
          | n.getFsPermissionShort();
    }

    private static int buildIntelAclEntries(AclFeature f,
                                            final SaverContext.DeduplicationMap<String> map, FlatBufferBuilder fbb) {
      int entries = 0;
      ArrayList<Integer> list = new ArrayList<>();
      for (int pos = 0, e; pos < f.getEntriesSize(); pos++) {
        e = f.getEntryAt(pos);
        int nameId = map.getId(AclEntryStatusFormat.getName(e));
        int v = ((nameId & ACL_ENTRY_NAME_MASK) << ACL_ENTRY_NAME_OFFSET)
            | (AclEntryStatusFormat.getType(e).ordinal() << ACL_ENTRY_TYPE_OFFSET)
            | (AclEntryStatusFormat.getScope(e).ordinal() << ACL_ENTRY_SCOPE_OFFSET)
            | (AclEntryStatusFormat.getPermission(e).ordinal());
        list.add(v);
      }
      entries = IntelAclFeatureProto.
          createEntriesVector(fbb, ArrayUtils.toPrimitive(list.toArray(new Integer[list.size()])));
      return IntelAclFeatureProto.createIntelAclFeatureProto(fbb, entries);
    }

    private static AclFeatureProto.Builder buildAclEntries(AclFeature f,
                                                           final SaverContext.DeduplicationMap<String> map) {
      AclFeatureProto.Builder b = AclFeatureProto.newBuilder();
      for (int pos = 0, e; pos < f.getEntriesSize(); pos++) {
        e = f.getEntryAt(pos);
        int nameId = map.getId(AclEntryStatusFormat.getName(e));
        int v = ((nameId & ACL_ENTRY_NAME_MASK) << ACL_ENTRY_NAME_OFFSET)
            | (AclEntryStatusFormat.getType(e).ordinal() << ACL_ENTRY_TYPE_OFFSET)
            | (AclEntryStatusFormat.getScope(e).ordinal() << ACL_ENTRY_SCOPE_OFFSET)
            | (AclEntryStatusFormat.getPermission(e).ordinal());
        b.addEntries(v);
      }
      return b;
    }

    private static int buildIntelXAttrs(XAttrFeature f,
                                        final SaverContext.DeduplicationMap<String> stringMap, FlatBufferBuilder fbb) {
//      FlatBufferBuilder fbb = new FlatBufferBuilder();
      IntelXAttrCompactProto.startIntelXAttrCompactProto(fbb);
      int value = 0;
      ArrayList<Integer> list = new ArrayList<>();
      for (XAttr a : f.getXAttrs()) {
        int nsOrd = a.getNameSpace().ordinal();
        Preconditions.checkArgument(nsOrd < 8, "Too many namespaces.");
        int v = ((nsOrd & XATTR_NAMESPACE_MASK) << XATTR_NAMESPACE_OFFSET)
            | ((stringMap.getId(a.getName()) & XATTR_NAME_MASK) <<
            XATTR_NAME_OFFSET);
        v |= (((nsOrd >> 2) & XATTR_NAMESPACE_EXT_MASK) <<
            XATTR_NAMESPACE_EXT_OFFSET);
        IntelXAttrCompactProto.addName(fbb, v);
        if (a.getValue() != null) {
          value = fbb.createString(bytesToString(a.getValue()));
        }
        int inv = IntelXAttrCompactProto.createIntelXAttrCompactProto(fbb, v, value);
        list.add(inv);
      }
      int xAttrs = 0;
      xAttrs = IntelXAttrFeatureProto.
          createXAttrsVector(fbb, ArrayUtils.toPrimitive(list.toArray(new Integer[list.size()])));
      return IntelXAttrFeatureProto.createIntelXAttrFeatureProto(fbb, xAttrs);
    }

    private static XAttrFeatureProto.Builder buildXAttrs(XAttrFeature f,
                                                         final SaverContext.DeduplicationMap<String> stringMap) {

      XAttrFeatureProto.Builder b = XAttrFeatureProto.newBuilder();
      for (XAttr a : f.getXAttrs()) {
        XAttrCompactProto.Builder xAttrCompactBuilder = XAttrCompactProto.
            newBuilder();
        int nsOrd = a.getNameSpace().ordinal();
        Preconditions.checkArgument(nsOrd < 8, "Too many namespaces.");
        int v = ((nsOrd & XATTR_NAMESPACE_MASK) << XATTR_NAMESPACE_OFFSET)
            | ((stringMap.getId(a.getName()) & XATTR_NAME_MASK) <<
            XATTR_NAME_OFFSET);
        v |= (((nsOrd >> 2) & XATTR_NAMESPACE_EXT_MASK) <<
            XATTR_NAMESPACE_EXT_OFFSET);
        xAttrCompactBuilder.setName(v);
        if (a.getValue() != null) {
          xAttrCompactBuilder.setValue(PBHelper.getByteString(a.getValue()));
        }
        b.addXAttrs(xAttrCompactBuilder.build());
      }
      return b;
    }

    private static int buildIntelQuotaByStorageTypeEntries(QuotaCounts q, FlatBufferBuilder fbb) {
      ArrayList<Integer> list = null;
      for (StorageType t : StorageType.getTypesSupportingQuota()) {
        if (q.getTypeSpace(t) >= 0) {
          int quotaOffset = IntelQuotaByStorageTypeEntryProto.createIntelQuotaByStorageTypeEntryProto(fbb,
              PBHelper.convertIntelStorageType(t), q.getTypeSpace(t));
          list = new ArrayList<>();
          list.add(quotaOffset);
        }
      }
      Integer[] data = list.toArray(new Integer[list.size()]);
      int quotas = IntelQuotaByStorageTypeFeatureProto.createQuotasVector(fbb, ArrayUtils.toPrimitive(data));
      return IntelQuotaByStorageTypeFeatureProto.createIntelQuotaByStorageTypeFeatureProto(fbb, quotas);
    }

    private static QuotaByStorageTypeFeatureProto.Builder buildQuotaByStorageTypeEntries(QuotaCounts q) {

      QuotaByStorageTypeFeatureProto.Builder b =
          QuotaByStorageTypeFeatureProto.newBuilder();

      for (StorageType t : StorageType.getTypesSupportingQuota()) {
        if (q.getTypeSpace(t) >= 0) {
          QuotaByStorageTypeEntryProto.Builder eb =
              QuotaByStorageTypeEntryProto
                  .newBuilder().setStorageType(PBHelper.convertStorageType(t)).setQuota(q.getTypeSpace(t));
          b.addQuotas(eb);
        }
      }
      return b;
    }

    public static int buildIntelINodeFile(FlatBufferBuilder fbb,
                                          INodeFileAttributes file, final SaverContext state) {
      int acl = 0;
      int xAttrs = 0;
      AclFeature f = file.getAclFeature();
      if (f != null) {
        acl = buildIntelAclEntries(f, state.getStringMap(), fbb);
      }
      XAttrFeature xAttrFeature = file.getXAttrFeature();
      if (xAttrFeature != null) {
        xAttrs = buildIntelXAttrs(xAttrFeature, state.getStringMap(), fbb);
      }
      return IntelINodeFile.createIntelINodeFile(fbb, file.getFileReplication(),
          file.getModificationTime(), file.getAccessTime(),
          file.getPreferredBlockSize(), buildPermissionStatus(file, state.getStringMap()),
          0, 0, acl, xAttrs, file.getLocalStoragePolicyID());
    }

    public static INodeSection.INodeFile.Builder buildINodeFile(
        INodeFileAttributes file, final SaverContext state) {
      INodeSection.INodeFile.Builder b = INodeSection.INodeFile.newBuilder()
          .setAccessTime(file.getAccessTime())
          .setModificationTime(file.getModificationTime())
          .setPermission(buildPermissionStatus(file, state.getStringMap()))
          .setPreferredBlockSize(file.getPreferredBlockSize())
          .setReplication(file.getFileReplication())
          .setStoragePolicyID(file.getLocalStoragePolicyID());

      AclFeature f = file.getAclFeature();
      if (f != null) {
        b.setAcl(buildAclEntries(f, state.getStringMap()));
      }
      XAttrFeature xAttrFeature = file.getXAttrFeature();
      if (xAttrFeature != null) {
        b.setXAttrs(buildXAttrs(xAttrFeature, state.getStringMap()));
      }
      return b;
    }

    public static int buildIntelINodeDirectory(
        INodeDirectoryAttributes dir, final SaverContext state, FlatBufferBuilder fbb) {
      QuotaCounts quota = dir.getQuotaCounts();
      long modifyTime = dir.getModificationTime();
      long nsQuota = quota.getNameSpace();
      long dsQuota = quota.getStorageSpace();
      long permission = buildPermissionStatus(dir, state.getStringMap());
      int aclOffset = 0;
      int xAttrsOffset = 0;
      int typeQuotasOffset = 0;

      if (quota.getTypeSpaces().anyGreaterOrEqual(0)) {
        typeQuotasOffset = buildIntelQuotaByStorageTypeEntries(quota, fbb); // attention
      }

      AclFeature f = dir.getAclFeature();
      if (f != null) {
        aclOffset = buildIntelAclEntries(f, state.getStringMap(), fbb); // attention
      }
      XAttrFeature xAttrFeature = dir.getXAttrFeature();
      if (xAttrFeature != null) {
        xAttrsOffset = buildIntelXAttrs(xAttrFeature, state.getStringMap(), fbb); // attention
      }
      return IntelINodeDirectory.createIntelINodeDirectory(fbb, modifyTime,
          nsQuota, dsQuota, permission, aclOffset, xAttrsOffset, typeQuotasOffset);
    }

    public static INodeSection.INodeDirectory.Builder buildINodeDirectory(
        INodeDirectoryAttributes dir, final SaverContext state) {
      QuotaCounts quota = dir.getQuotaCounts();

      INodeSection.INodeDirectory.Builder b = INodeSection.INodeDirectory
          .newBuilder().setModificationTime(dir.getModificationTime())
          .setNsQuota(quota.getNameSpace())
          .setDsQuota(quota.getStorageSpace())
          .setPermission(buildPermissionStatus(dir, state.getStringMap()));

      if (quota.getTypeSpaces().anyGreaterOrEqual(0)) {
        b.setTypeQuotas(buildQuotaByStorageTypeEntries(quota));
      }

      AclFeature f = dir.getAclFeature();
      if (f != null) {
        b.setAcl(buildAclEntries(f, state.getStringMap()));
      }
      XAttrFeature xAttrFeature = dir.getXAttrFeature();
      if (xAttrFeature != null) {
        b.setXAttrs(buildXAttrs(xAttrFeature, state.getStringMap()));
      }
      return b;
    }

    private final FSNamesystem fsn;
    private final FileSummary.Builder summary;
    private final SaveNamespaceContext context;
    private final FSImageFormatProtobuf.Saver parent;
    private final FlatBufferBuilder fbb;


    Saver(FSImageFormatProtobuf.Saver parent, FileSummary.Builder summary, FlatBufferBuilder fbb) {
      this.parent = parent;
      this.summary = summary;
      this.context = parent.getContext();
      this.fsn = context.getSourceNamesystem();
      this.fbb = fbb;
    }

    int serializeIntelINodeDirectorySection(OutputStream out, FlatBufferBuilder fbb) throws IOException {
      Iterator<INodeWithAdditionalFields> iter = fsn.getFSDirectory()
          .getINodeMap().getMapIterator();
      final ArrayList<INodeReference> refList = parent.getSaverContext()
          .getRefList();
      int i = 0;
      while (iter.hasNext()) {
        INodeWithAdditionalFields n = iter.next();
        if (!n.isDirectory()) {
          continue;
        }
        ReadOnlyList<INode> children = n.asDirectory().getChildrenList(
            Snapshot.CURRENT_STATE_ID);
        if (children.size() > 0) {
          FlatBufferBuilder fbb1 = new FlatBufferBuilder();
          ArrayList<Long> childrenList = new ArrayList<>();
          ArrayList<Integer> refenceList = new ArrayList<>();
          for (INode inode : children) {
            if (!inode.isReference()) {
              childrenList.add(inode.getId());
            } else {
              refList.add(inode.asReference());
              int size = refList.size() - 1;
              refenceList.add(size);
            }
          }

          int childrenOffset = IntelDirEntry.
              createChildrenVector(fbb1, ArrayUtils.toPrimitive(childrenList.toArray(new Long[childrenList.size()])));
          int refChildrenOffset = IntelDirEntry.
              createRefChildrenVector(fbb1, ArrayUtils.toPrimitive(refenceList.toArray(new Integer[refenceList.size()])));
          int end = IntelDirEntry.createIntelDirEntry(fbb1, n.getId(), childrenOffset, refChildrenOffset);
          IntelDirEntry.finishIntelDirEntryBuffer(fbb1, end);
          byte[] bytes = fbb1.sizedByteArray();
          writeTo(bytes, bytes.length, out);
        }
        ++i;
        if (i % FSImageFormatProtobuf.Saver.CHECK_CANCEL_INTERVAL == 0) {
          context.checkCancelled();
        }
      }
      return parent.commitIntelSection(FSImageFormatProtobuf.SectionName.INODE_DIR, fbb);
    }

    void serializeINodeDirectorySection(OutputStream out) throws IOException {
      Iterator<INodeWithAdditionalFields> iter = fsn.getFSDirectory()
          .getINodeMap().getMapIterator();
      final ArrayList<INodeReference> refList = parent.getSaverContext()
          .getRefList();
      int i = 0;
      while (iter.hasNext()) {
        INodeWithAdditionalFields n = iter.next();
        if (!n.isDirectory()) {
          continue;
        }

        ReadOnlyList<INode> children = n.asDirectory().getChildrenList(
            Snapshot.CURRENT_STATE_ID);
        if (children.size() > 0) {
          INodeDirectorySection.DirEntry.Builder b = INodeDirectorySection.
              DirEntry.newBuilder().setParent(n.getId());
          for (INode inode : children) {
            if (!inode.isReference()) {
              b.addChildren(inode.getId());
            } else {
              refList.add(inode.asReference());
              b.addRefChildren(refList.size() - 1);
            }
          }
          INodeDirectorySection.DirEntry e = b.build();
          e.writeDelimitedTo(out);
        }

        ++i;
        if (i % FSImageFormatProtobuf.Saver.CHECK_CANCEL_INTERVAL == 0) {
          context.checkCancelled();
        }
      }
      parent.commitSection(summary,
          FSImageFormatProtobuf.SectionName.INODE_DIR);
    }

    public static void writeTo(byte[] b, int serialLength, OutputStream outputStream) throws IOException {
      DataOutputStream dos = new DataOutputStream(outputStream);
      dos.writeInt(serialLength);
      dos.write(b);
      dos.flush();
    }

    int serializeIntelINodeSection(OutputStream out, FlatBufferBuilder fbb)
        throws IOException {
      INodeMap inodesMap = fsn.dir.getINodeMap();

      FlatBufferBuilder fbb1 = new FlatBufferBuilder();
      int inv = IntelINodeSection.createIntelINodeSection(fbb1, fsn.dir.getLastInodeId(),
          inodesMap.size());
      IntelINodeSection.finishIntelINodeSectionBuffer(fbb1, inv);
      byte[] bytes = fbb1.sizedByteArray();
      writeTo(bytes, bytes.length, out);

      int i = 0;
      Iterator<INodeWithAdditionalFields> iter = inodesMap.getMapIterator();
      while (iter.hasNext()) {
        INodeWithAdditionalFields n = iter.next();
        save(out, n); // finish
        ++i;
        if (i % FSImageFormatProtobuf.Saver.CHECK_CANCEL_INTERVAL == 0) {
          context.checkCancelled();
        }
      }

      return parent.commitIntelSection(FSImageFormatProtobuf.SectionName.INODE, fbb);
    }

    void serializeINodeSection(OutputStream out) throws IOException {
      INodeMap inodesMap = fsn.dir.getINodeMap();

      INodeSection.Builder b = INodeSection.newBuilder()
          .setLastInodeId(fsn.dir.getLastInodeId()).setNumInodes(inodesMap.size());
      INodeSection s = b.build();
      s.writeDelimitedTo(out);

      int i = 0;
      Iterator<INodeWithAdditionalFields> iter = inodesMap.getMapIterator();
      while (iter.hasNext()) {
        INodeWithAdditionalFields n = iter.next();
        save(out, n);
        ++i;
        if (i % FSImageFormatProtobuf.Saver.CHECK_CANCEL_INTERVAL == 0) {
          context.checkCancelled();
        }
      }
      parent.commitSection(summary, FSImageFormatProtobuf.SectionName.INODE);
    }

    int serializeIntelFilesUCSection(OutputStream out, FlatBufferBuilder fbb) throws IOException {
      Collection<Long> filesWithUC = fsn.getLeaseManager()
          .getINodeIdWithLeases();
      for (Long id : filesWithUC) {
        INode inode = fsn.getFSDirectory().getInode(id);
        if (inode == null) {
          LOG.warn("Fail to find inode " + id + " when saving the leases.");
          continue;
        }
        INodeFile file = inode.asFile();
        if (!file.isUnderConstruction()) {
          LOG.warn("Fail to save the lease for inode id " + id
              + " as the file is not under construction");
          continue;
        }
        String path = file.getFullPathName();

        FlatBufferBuilder fbb1 = new FlatBufferBuilder();
        int inv = IntelFileUnderConstructionEntry.createIntelFileUnderConstructionEntry(fbb1,
            file.getId(), fbb1.createString(path));
        IntelFileUnderConstructionEntry.finishIntelFileUnderConstructionEntryBuffer(fbb1, inv);
        byte[] bytes = fbb1.sizedByteArray();
        writeTo(bytes, bytes.length, out);
      }
      return parent.commitIntelSection(
          FSImageFormatProtobuf.SectionName.FILES_UNDERCONSTRUCTION, fbb);
    }

    // abandon method
    void serializeFilesUCSection(OutputStream out) throws IOException {
      Collection<Long> filesWithUC = fsn.getLeaseManager()
          .getINodeIdWithLeases();
      for (Long id : filesWithUC) {
        INode inode = fsn.getFSDirectory().getInode(id);
        if (inode == null) {
          LOG.warn("Fail to find inode " + id + " when saving the leases.");
          continue;
        }
        INodeFile file = inode.asFile();
        if (!file.isUnderConstruction()) {
          LOG.warn("Fail to save the lease for inode id " + id
              + " as the file is not under construction");
          continue;
        }
        String path = file.getFullPathName();
        FileUnderConstructionEntry.Builder b = FileUnderConstructionEntry
            .newBuilder().setInodeId(file.getId()).setFullPath(path);
        FileUnderConstructionEntry e = b.build();
        e.writeDelimitedTo(out);
      }
      parent.commitSection(summary,
          FSImageFormatProtobuf.SectionName.FILES_UNDERCONSTRUCTION);
    }

    private void save(OutputStream out, INode n) throws IOException {
      if (n.isDirectory()) {
        saveIntel(out, n.asDirectory()); // success
      } else if (n.isFile()) {
        saveIntel(out, n.asFile());  // success
      } else if (n.isSymlink()) {
        saveIntel(out, n.asSymlink()); // success
      }
    }

    public static String bytesToString(byte[] bytes) {
      String str = "";
      try {
        str = new String(bytes, "UTF-8");
      } catch (Exception e) {
        return null;
      }
      return str;
    }

    private void saveIntel(OutputStream out, INodeDirectory n) throws IOException {
      FlatBufferBuilder fbb = new FlatBufferBuilder();
      int ib = buildIntelINodeDirectory(n, parent.getSaverContext(), fbb);

      int inv = IntelINode.createIntelINode(fbb, IntelTypee.DIRECTORY, n.getId(),
          fbb.createString(bytesToString(n.getLocalNameBytes())), 0, ib, 0);

      IntelINode.finishIntelINodeBuffer(fbb, inv);
      byte[] bytes = fbb.sizedByteArray();
      writeTo(bytes, bytes.length, out);
    }

    private void save(OutputStream out, INodeDirectory n) throws IOException {
      INodeSection.INodeDirectory.Builder b = buildINodeDirectory(n,
          parent.getSaverContext());
      INodeSection.INode r = buildINodeCommon(n)
          .setType(INodeSection.INode.Type.DIRECTORY).setDirectory(b).build();
      r.writeDelimitedTo(out);
    }

    // should have a try
    private void saveIntel(OutputStream out, INodeFile n) throws IOException {

      FlatBufferBuilder fbb = new FlatBufferBuilder();
      final SaverContext state = parent.getSaverContext();

      int acl = 0;
      int xAttrs = 0;
      AclFeature f = n.getAclFeature();
      if (f != null) {
        acl = buildIntelAclEntries(f, state.getStringMap(), fbb);
      }
      XAttrFeature xAttrFeature = n.getXAttrFeature();
      if (xAttrFeature != null) {
        xAttrs = buildIntelXAttrs(xAttrFeature, state.getStringMap(), fbb);
      }
      int blocks = 0;
      int fileUC = 0;
      ArrayList<Integer> list = new ArrayList<>();

      if (n.getBlocks() != null) {
        for (Block block : n.getBlocks()) {
          list.add(PBHelper.convertIntel(block, fbb));
        }
      }

      FileUnderConstructionFeature uc = n.getFileUnderConstructionFeature();
      if (uc != null) {
        fileUC = IntelFileUnderConstructionFeature.
            createIntelFileUnderConstructionFeature(fbb,
                fbb.createString(uc.getClientName()), fbb.createString(uc.getClientMachine()));
      }

      blocks = IntelINodeFile.createBlocksVector(fbb, ArrayUtils.toPrimitive(list.toArray(new Integer[list.size()])));

      int ib = IntelINodeFile.createIntelINodeFile(fbb, n.getFileReplication(),
          n.getModificationTime(), n.getAccessTime(),
          n.getPreferredBlockSize(), buildPermissionStatus(n, state.getStringMap()),
          blocks, fileUC, acl, xAttrs, n.getLocalStoragePolicyID());


      int end = IntelINode.createIntelINode(fbb, IntelTypee.FILE, n.getId(),
          fbb.createString(bytesToString(n.getLocalNameBytes())), ib, 0, 0);
      IntelINode.finishIntelINodeBuffer(fbb, end);
      byte[] bytes = fbb.sizedByteArray();
      writeTo(bytes, bytes.length, out);
    }

    private void save(OutputStream out, INodeFile n) throws IOException {
      INodeSection.INodeFile.Builder b = buildINodeFile(n,
          parent.getSaverContext());

      if (n.getBlocks() != null) {
        for (Block block : n.getBlocks()) {
          b.addBlocks(PBHelper.convert(block));
        }
      }

      FileUnderConstructionFeature uc = n.getFileUnderConstructionFeature();
      if (uc != null) {
        INodeSection.FileUnderConstructionFeature f =
            INodeSection.FileUnderConstructionFeature
                .newBuilder().setClientName(uc.getClientName())
                .setClientMachine(uc.getClientMachine()).build();
        b.setFileUC(f);
      }

      INodeSection.INode r = buildINodeCommon(n)
          .setType(INodeSection.INode.Type.FILE).setFile(b).build();
      r.writeDelimitedTo(out);
    }


    private void saveIntel(OutputStream out, INodeSymlink n) throws IOException {
      SaverContext state = parent.getSaverContext();

      FlatBufferBuilder fbb = new FlatBufferBuilder();
      int ib = IntelINodeSymlink.createIntelINodeSymlink(fbb, buildPermissionStatus(n, state.getStringMap()),
          fbb.createString(bytesToString(n.getSymlink())), n.getModificationTime(), n.getAccessTime());

      int env = IntelINode.createIntelINode(fbb, IntelTypee.SYMLINK, n.getId(),
          fbb.createString(bytesToString(n.getLocalNameBytes())),
          0, 0, ib);
      IntelINode.finishIntelINodeBuffer(fbb, env);
      byte[] bytes = fbb.sizedByteArray();
      writeTo(bytes, bytes.length, out);
    }

    private void save(OutputStream out, INodeSymlink n) throws IOException {
      SaverContext state = parent.getSaverContext();
      INodeSection.INodeSymlink.Builder b = INodeSection.INodeSymlink
          .newBuilder()
          .setPermission(buildPermissionStatus(n, state.getStringMap()))
          .setTarget(ByteString.copyFrom(n.getSymlink()))
          .setModificationTime(n.getModificationTime())
          .setAccessTime(n.getAccessTime());

      INodeSection.INode r = buildINodeCommon(n)
          .setType(INodeSection.INode.Type.SYMLINK).setSymlink(b).build();
      r.writeDelimitedTo(out);
    }

    private final INodeSection.INode.Builder buildINodeCommon(INode n) {
      return INodeSection.INode.newBuilder()
          .setId(n.getId())
          .setName(ByteString.copyFrom(n.getLocalNameBytes()));
    }
  }

  private FSImageFormatPBINode() {
  }
}
