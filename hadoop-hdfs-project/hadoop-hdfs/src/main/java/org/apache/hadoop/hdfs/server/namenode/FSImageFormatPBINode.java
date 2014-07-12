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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.LoaderContext;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SaverContext;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeDirectorySection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.AclFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.XAttrCompactProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.XAttrFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
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
    
    public static ImmutableList<XAttr> loadXAttrs(
        XAttrFeatureProto proto, final String[] stringTable) {
      ImmutableList.Builder<XAttr> b = ImmutableList.builder();
      for (XAttrCompactProto xAttrCompactProto : proto.getXAttrsList()) {
        int v = xAttrCompactProto.getName();
        int nid = (v >> XATTR_NAME_OFFSET) & XATTR_NAME_MASK;
        int ns = (v >> XATTR_NAMESPACE_OFFSET) & XATTR_NAMESPACE_MASK;
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
        dir.addDirectoryWithQuotaFeature(nsQuota, dsQuota);
      }

      if (d.hasAcl()) {
        dir.addAclFeature(new AclFeature(loadAclEntries(d.getAcl(),
            state.getStringTable())));
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

    void loadINodeSection(InputStream in) throws IOException {
      INodeSection s = INodeSection.parseDelimitedFrom(in);
      fsn.resetLastInodeId(s.getLastInodeId());
      LOG.info("Loading " + s.getNumInodes() + " INodes.");
      for (int i = 0; i < s.getNumInodes(); ++i) {
        INodeSection.INode p = INodeSection.INode.parseDelimitedFrom(in);
        if (p.getId() == INodeId.ROOT_INODE_ID) {
          loadRootINode(p);
        } else {
          INode n = loadINode(p);
          dir.addToInodeMap(n);
        }
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
        fsn.leaseManager.addLease(uc.getClientName(), entry.getFullPath());
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

    private INodeFile loadINodeFile(INodeSection.INode n) {
      assert n.getType() == INodeSection.INode.Type.FILE;
      INodeSection.INodeFile f = n.getFile();
      List<BlockProto> bp = f.getBlocksList();
      short replication = (short) f.getReplication();
      LoaderContext state = parent.getLoaderContext();

      BlockInfo[] blocks = new BlockInfo[bp.size()];
      for (int i = 0, e = bp.size(); i < e; ++i) {
        blocks[i] = new BlockInfo(PBHelper.convert(bp.get(i)), replication);
      }
      final PermissionStatus permissions = loadPermission(f.getPermission(),
          parent.getLoaderContext().getStringTable());

      final INodeFile file = new INodeFile(n.getId(),
          n.getName().toByteArray(), permissions, f.getModificationTime(),
          f.getAccessTime(), blocks, replication, f.getPreferredBlockSize());

      if (f.hasAcl()) {
        file.addAclFeature(new AclFeature(loadAclEntries(f.getAcl(),
            state.getStringTable())));
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
          file.setBlock(file.numBlocks() - 1, new BlockInfoUnderConstruction(
              lastBlk, replication));
        }
      }
      return file;
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
      final Quota.Counts q = root.getQuotaCounts();
      final long nsQuota = q.get(Quota.NAMESPACE);
      final long dsQuota = q.get(Quota.DISKSPACE);
      if (nsQuota != -1 || dsQuota != -1) {
        dir.rootDir.getDirectoryWithQuotaFeature().setQuota(nsQuota, dsQuota);
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

    private static AclFeatureProto.Builder buildAclEntries(AclFeature f,
        final SaverContext.DeduplicationMap<String> map) {
      AclFeatureProto.Builder b = AclFeatureProto.newBuilder();
      for (AclEntry e : f.getEntries()) {
        int v = ((map.getId(e.getName()) & ACL_ENTRY_NAME_MASK) << ACL_ENTRY_NAME_OFFSET)
            | (e.getType().ordinal() << ACL_ENTRY_TYPE_OFFSET)
            | (e.getScope().ordinal() << ACL_ENTRY_SCOPE_OFFSET)
            | (e.getPermission().ordinal());
        b.addEntries(v);
      }
      return b;
    }
    
    private static XAttrFeatureProto.Builder buildXAttrs(XAttrFeature f,
        final SaverContext.DeduplicationMap<String> stringMap) {
      XAttrFeatureProto.Builder b = XAttrFeatureProto.newBuilder();
      for (XAttr a : f.getXAttrs()) {
        XAttrCompactProto.Builder xAttrCompactBuilder = XAttrCompactProto.
            newBuilder();
        int v = ((a.getNameSpace().ordinal() & XATTR_NAMESPACE_MASK) << 
            XATTR_NAMESPACE_OFFSET) 
            | ((stringMap.getId(a.getName()) & XATTR_NAME_MASK) << 
                XATTR_NAME_OFFSET);
        xAttrCompactBuilder.setName(v);
        if (a.getValue() != null) {
          xAttrCompactBuilder.setValue(PBHelper.getByteString(a.getValue()));
        }
        b.addXAttrs(xAttrCompactBuilder.build());
      }
      
      return b;
    }

    public static INodeSection.INodeFile.Builder buildINodeFile(
        INodeFileAttributes file, final SaverContext state) {
      INodeSection.INodeFile.Builder b = INodeSection.INodeFile.newBuilder()
          .setAccessTime(file.getAccessTime())
          .setModificationTime(file.getModificationTime())
          .setPermission(buildPermissionStatus(file, state.getStringMap()))
          .setPreferredBlockSize(file.getPreferredBlockSize())
          .setReplication(file.getFileReplication());

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

    public static INodeSection.INodeDirectory.Builder buildINodeDirectory(
        INodeDirectoryAttributes dir, final SaverContext state) {
      Quota.Counts quota = dir.getQuotaCounts();
      INodeSection.INodeDirectory.Builder b = INodeSection.INodeDirectory
          .newBuilder().setModificationTime(dir.getModificationTime())
          .setNsQuota(quota.get(Quota.NAMESPACE))
          .setDsQuota(quota.get(Quota.DISKSPACE))
          .setPermission(buildPermissionStatus(dir, state.getStringMap()));

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

    Saver(FSImageFormatProtobuf.Saver parent, FileSummary.Builder summary) {
      this.parent = parent;
      this.summary = summary;
      this.context = parent.getContext();
      this.fsn = context.getSourceNamesystem();
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

    void serializeINodeSection(OutputStream out) throws IOException {
      INodeMap inodesMap = fsn.dir.getINodeMap();

      INodeSection.Builder b = INodeSection.newBuilder()
          .setLastInodeId(fsn.getLastInodeId()).setNumInodes(inodesMap.size());
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

    void serializeFilesUCSection(OutputStream out) throws IOException {
      Map<String, INodeFile> ucMap = fsn.getFilesUnderConstruction();
      for (Map.Entry<String, INodeFile> entry : ucMap.entrySet()) {
        String path = entry.getKey();
        INodeFile file = entry.getValue();
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
        save(out, n.asDirectory());
      } else if (n.isFile()) {
        save(out, n.asFile());
      } else if (n.isSymlink()) {
        save(out, n.asSymlink());
      }
    }

    private void save(OutputStream out, INodeDirectory n) throws IOException {
      INodeSection.INodeDirectory.Builder b = buildINodeDirectory(n,
          parent.getSaverContext());
      INodeSection.INode r = buildINodeCommon(n)
          .setType(INodeSection.INode.Type.DIRECTORY).setDirectory(b).build();
      r.writeDelimitedTo(out);
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
