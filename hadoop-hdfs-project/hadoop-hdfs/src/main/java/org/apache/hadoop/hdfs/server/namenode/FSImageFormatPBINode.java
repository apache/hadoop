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

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeDirectorySection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.Permission;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

final class FSImageFormatPBINode {
  final static class Loader {
    private static PermissionStatus loadPermission(Permission p) {
      return new PermissionStatus(p.getUser(), p.getGroup(), new FsPermission(
          (short) p.getPermission()));
    }

    private final FSDirectory dir;
    private final FSNamesystem fsn;

    Loader(FSNamesystem fsn) {
      this.fsn = fsn;
      this.dir = fsn.dir;
    }

    void loadINodeDirectorySection(InputStream in) throws IOException {
      final INodeMap inodeMap = dir.getINodeMap();
      while (true) {
        INodeDirectorySection.DirEntry e = INodeDirectorySection.DirEntry
            .parseDelimitedFrom(in);

        if (e == null)
          break;

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
      return file;
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
    private final FileSummary.Builder headers;
    private final OutputStream out;
    private final FSImageFormatProtobuf.Saver parent;

    Saver(FSImageFormatProtobuf.Saver parent, OutputStream out,
        FileSummary.Builder headers) {
      this.parent = parent;
      this.out = out;
      this.headers = headers;
      this.fsn = parent.context.getSourceNamesystem();
    }

    void serializeINodeDirectorySection() throws IOException {
      for (INodeWithAdditionalFields n : fsn.dir.getINodeMap().getMap()) {
        if (!n.isDirectory())
          continue;

        INodeDirectory d = n.asDirectory();

        INodeDirectorySection.DirEntry.Builder b = INodeDirectorySection.DirEntry
            .newBuilder().setParent(n.getId());

        for (INode inode : d.getChildrenList(Snapshot.CURRENT_STATE_ID))
          b.addChildren(inode.getId());

        if (b.getChildrenCount() != 0) {
          INodeDirectorySection.DirEntry e = b.build();
          e.writeDelimitedTo(out);
        }
      }
      parent.commitSection(headers,
          FSImageFormatProtobuf.SectionName.INODE_DIR);
    }

    void serializeINodeSection() throws IOException {
      final INodeDirectory rootDir = fsn.dir.rootDir;
      final long numINodes = rootDir.getDirectoryWithQuotaFeature()
          .getSpaceConsumed().get(Quota.NAMESPACE);
      INodeSection.Builder b = INodeSection.newBuilder()
          .setLastInodeId(fsn.getLastInodeId()).setNumInodes(numINodes);
      INodeSection s = b.build();
      s.writeDelimitedTo(out);

      long i = 0;
      for (INodeWithAdditionalFields n : fsn.dir.getINodeMap().getMap()) {
        save(n);
        ++i;
      }
      Preconditions.checkState(numINodes == i);
      parent.commitSection(headers, FSImageFormatProtobuf.SectionName.INODE);
    }

    private INodeSection.Permission.Builder buildPermissionStatus(INode n) {
      return INodeSection.Permission.newBuilder().setUser(n.getUserName())
          .setGroup(n.getGroupName()).setPermission(n.getFsPermissionShort());
    }

    private void save(INode n) throws IOException {
      if (n.isDirectory()) {
        save(n.asDirectory());
      } else if (n.isFile()) {
        save(n.asFile());
      }
    }

    private void save(INodeDirectory n) throws IOException {
      Quota.Counts quota = n.getQuotaCounts();
      INodeSection.INodeDirectory.Builder b = INodeSection.INodeDirectory
          .newBuilder().setModificationTime(n.getModificationTime())
          .setNsQuota(quota.get(Quota.NAMESPACE))
          .setDsQuota(quota.get(Quota.DISKSPACE))
          .setPermission(buildPermissionStatus(n));

      INodeSection.INode r = INodeSection.INode.newBuilder()
          .setType(INodeSection.INode.Type.DIRECTORY).setId(n.getId())
          .setName(ByteString.copyFrom(n.getLocalNameBytes())).setDirectory(b).build();
      r.writeDelimitedTo(out);
    }

    private void save(INodeFile n) throws IOException {
      INodeSection.INodeFile.Builder b = INodeSection.INodeFile.newBuilder()
          .setAccessTime(n.getAccessTime())
          .setModificationTime(n.getModificationTime())
          .setPermission(buildPermissionStatus(n))
          .setPreferredBlockSize(n.getPreferredBlockSize())
          .setReplication(n.getFileReplication());

      for (Block block : n.getBlocks())
        b.addBlocks(PBHelper.convert(block));

      INodeSection.INode r = INodeSection.INode.newBuilder()
          .setType(INodeSection.INode.Type.FILE).setId(n.getId())
          .setName(ByteString.copyFrom(n.getLocalNameBytes())).setFile(b).build();
      r.writeDelimitedTo(out);
    }
  }

  private FSImageFormatPBINode() {
  }
}
