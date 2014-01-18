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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeDirectorySection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

final class FSImageFormatPBINode {
  private final static int USER_GROUP_STRID_MASK = (1 << 24) - 1;
  private final static int USER_STRID_OFFSET = 40;
  private final static int GROUP_STRID_OFFSET = 16;

  final static class Loader {
    private PermissionStatus loadPermission(long id) {
      short perm = (short) (id & ((1 << GROUP_STRID_OFFSET) - 1));
      int gsid = (int) ((id >> GROUP_STRID_OFFSET) & USER_GROUP_STRID_MASK);
      int usid = (int) ((id >> USER_STRID_OFFSET) & USER_GROUP_STRID_MASK);
      return new PermissionStatus(parent.stringTable[usid],
          parent.stringTable[gsid], new FsPermission(perm));
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
      }
    }

    void loadINodeSection(InputStream in) throws IOException {
      INodeSection s = INodeSection.parseDelimitedFrom(in);
      fsn.resetLastInodeId(s.getLastInodeId());
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
        INodeFile file = fsn.dir.getInode(entry.getInodeId()).asFile();
        FileUnderConstructionFeature uc = file.getFileUnderConstructionFeature();
        Preconditions.checkState(uc != null); // file must be under-construction
        fsn.leaseManager.addLease(uc.getClientName(), entry.getFullPath());
      }
    }

    private void addToParent(INodeDirectory parent, INode child) {
      FSDirectory fsDir = fsn.dir;
      if (parent == fsDir.rootDir && FSDirectory.isReservedName(child)) {
        throw new HadoopIllegalArgumentException("File name \""
            + child.getLocalName() + "\" is reserved. Please "
            + " change the name of the existing file or directory to another "
            + "name before upgrading to this release.");
      }
      // NOTE: This does not update space counts for parents
      if (!parent.addChild(child)) {
        return;
      }
      fsn.dir.cacheName(child);

      if (child.isFile()) {
        updateBlocksMap(child.asFile());
      }
    }

    private INode loadINode(INodeSection.INode n) {
      switch (n.getType()) {
      case FILE:
        return loadINodeFile(n);
      case DIRECTORY:
        return loadINodeDirectory(n);
      case SYMLINK:
        return loadINodeSymlink(n);
      default:
        break;
      }
      return null;
    }

    private INodeDirectory loadINodeDirectory(INodeSection.INode n) {
      assert n.getType() == INodeSection.INode.Type.DIRECTORY;
      INodeSection.INodeDirectory d = n.getDirectory();

      final PermissionStatus permissions = loadPermission(d.getPermission());
      final INodeDirectory dir = new INodeDirectory(n.getId(), n.getName()
          .toByteArray(), permissions, d.getModificationTime());

      final long nsQuota = d.getNsQuota(), dsQuota = d.getDsQuota();
      if (nsQuota >= 0 || dsQuota >= 0) {
        dir.addDirectoryWithQuotaFeature(nsQuota, dsQuota);
      }
      return dir;
    }

    private INodeFile loadINodeFile(INodeSection.INode n) {
      assert n.getType() == INodeSection.INode.Type.FILE;
      INodeSection.INodeFile f = n.getFile();
      List<BlockProto> bp = f.getBlocksList();
      short replication = (short) f.getReplication();

      BlockInfo[] blocks = new BlockInfo[bp.size()];
      for (int i = 0, e = bp.size(); i < e; ++i) {
        blocks[i] = new BlockInfo(PBHelper.convert(bp.get(i)), replication);
      }
      final PermissionStatus permissions = loadPermission(f.getPermission());

      final INodeFile file = new INodeFile(n.getId(),
          n.getName().toByteArray(), permissions, f.getModificationTime(),
          f.getAccessTime(), blocks, replication, f.getPreferredBlockSize());
      // under-construction information
      if (f.hasFileUC()) {
        INodeSection.FileUnderConstructionFeature uc = f.getFileUC();
        file.toUnderConstruction(uc.getClientName(), uc.getClientMachine(),
            null);
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
      final PermissionStatus permissions = loadPermission(s.getPermission());
      return new INodeSymlink(n.getId(), n.getName().toByteArray(), permissions,
          0, 0, s.getTarget().toStringUtf8());
    }

    private void loadRootINode(INodeSection.INode p) {
      INodeDirectory root = loadINodeDirectory(p);
      final Quota.Counts q = root.getQuotaCounts();
      final long nsQuota = q.get(Quota.NAMESPACE);
      final long dsQuota = q.get(Quota.DISKSPACE);
      if (nsQuota != -1 || dsQuota != -1) {
        dir.rootDir.getDirectoryWithQuotaFeature().setQuota(nsQuota, dsQuota);
      }
      dir.rootDir.cloneModificationTime(root);
      dir.rootDir.clonePermissionStatus(root);
    }

    private void updateBlocksMap(INodeFile file) {
      // Add file->block mapping
      final BlockInfo[] blocks = file.getBlocks();
      if (blocks != null) {
        final BlockManager bm = fsn.getBlockManager();
        for (int i = 0; i < blocks.length; i++) {
          file.setBlock(i, bm.addBlockCollection(blocks[i], file));
        }
      }
    }
  }

  final static class Saver {
    private final FSNamesystem fsn;
    private final FileSummary.Builder summary;
    private final FSImageFormatProtobuf.Saver parent;

    Saver(FSImageFormatProtobuf.Saver parent, FileSummary.Builder summary) {
      this.parent = parent;
      this.summary = summary;
      this.fsn = parent.context.getSourceNamesystem();
    }

    void serializeINodeDirectorySection(OutputStream out) throws IOException {
      for (INodeWithAdditionalFields n : fsn.dir.getINodeMap().getMap()) {
        if (!n.isDirectory())
          continue;

        ReadOnlyList<INode> children = n.asDirectory().getChildrenList(
            Snapshot.CURRENT_STATE_ID);
        if (children.size() > 0) {
          INodeDirectorySection.DirEntry.Builder b = INodeDirectorySection.
              DirEntry.newBuilder().setParent(n.getId());
          for (INode inode : children) {
            b.addChildren(inode.getId());
          }
          INodeDirectorySection.DirEntry e = b.build();
          e.writeDelimitedTo(out);
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

      for (INodeWithAdditionalFields n : inodesMap.getMap()) {
        save(out, n);
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

    private long buildPermissionStatus(INode n) {
      int userId = parent.getStringId(n.getUserName());
      int groupId = parent.getStringId(n.getGroupName());
      return ((userId & USER_GROUP_STRID_MASK) << USER_STRID_OFFSET)
          | ((groupId & USER_GROUP_STRID_MASK) << GROUP_STRID_OFFSET)
          | n.getFsPermissionShort();
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
      Quota.Counts quota = n.getQuotaCounts();
      INodeSection.INodeDirectory.Builder b = INodeSection.INodeDirectory
          .newBuilder().setModificationTime(n.getModificationTime())
          .setNsQuota(quota.get(Quota.NAMESPACE))
          .setDsQuota(quota.get(Quota.DISKSPACE))
          .setPermission(buildPermissionStatus(n));

      INodeSection.INode r = buildINodeCommon(n)
          .setType(INodeSection.INode.Type.DIRECTORY).setDirectory(b).build();
      r.writeDelimitedTo(out);
    }

    private void save(OutputStream out, INodeFile n) throws IOException {
      INodeSection.INodeFile.Builder b = INodeSection.INodeFile.newBuilder()
          .setAccessTime(n.getAccessTime())
          .setModificationTime(n.getModificationTime())
          .setPermission(buildPermissionStatus(n))
          .setPreferredBlockSize(n.getPreferredBlockSize())
          .setReplication(n.getFileReplication());

      for (Block block : n.getBlocks()) {
        b.addBlocks(PBHelper.convert(block));
      }

      FileUnderConstructionFeature uc = n.getFileUnderConstructionFeature();
      if (uc != null) {
        INodeSection.FileUnderConstructionFeature f = INodeSection.FileUnderConstructionFeature
            .newBuilder().setClientName(uc.getClientName())
            .setClientMachine(uc.getClientMachine()).build();
        b.setFileUC(f);
      }

      INodeSection.INode r = buildINodeCommon(n)
          .setType(INodeSection.INode.Type.FILE).setFile(b).build();
      r.writeDelimitedTo(out);
    }

    private void save(OutputStream out, INodeSymlink n) throws IOException {
      INodeSection.INodeSymlink.Builder b = INodeSection.INodeSymlink
          .newBuilder().setPermission(buildPermissionStatus(n))
          .setTarget(ByteString.copyFrom(n.getSymlink()));
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
