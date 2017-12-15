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

import com.google.protobuf.ByteString;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeFile;

import static org.apache.hadoop.hdfs.DFSUtil.LOG;
import static org.apache.hadoop.hdfs.DFSUtil.string2Bytes;
import static org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature.DEFAULT_NAMESPACE_QUOTA;
import static org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature.DEFAULT_STORAGE_SPACE_QUOTA;

/**
 * Traversal cursor in external filesystem.
 * TODO: generalize, move FS/FileRegion to FSTreePath
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class TreePath {
  private long id = -1;
  private final long parentId;
  private final FileStatus stat;
  private final TreeWalk.TreeIterator i;
  private final FileSystem fs;

  protected TreePath(FileStatus stat, long parentId, TreeWalk.TreeIterator i,
      FileSystem fs) {
    this.i = i;
    this.stat = stat;
    this.parentId = parentId;
    this.fs = fs;
  }

  public FileStatus getFileStatus() {
    return stat;
  }

  public long getParentId() {
    return parentId;
  }

  public long getId() {
    if (id < 0) {
      throw new IllegalStateException();
    }
    return id;
  }

  void accept(long id) {
    this.id = id;
    i.onAccept(this, id);
  }

  public INode toINode(UGIResolver ugi, BlockResolver blk,
      BlockAliasMap.Writer<FileRegion> out) throws IOException {
    if (stat.isFile()) {
      return toFile(ugi, blk, out);
    } else if (stat.isDirectory()) {
      return toDirectory(ugi);
    } else if (stat.isSymlink()) {
      throw new UnsupportedOperationException("symlinks not supported");
    } else {
      throw new UnsupportedOperationException("Unknown type: " + stat);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TreePath)) {
      return false;
    }
    TreePath o = (TreePath) other;
    return getParentId() == o.getParentId()
      && getFileStatus().equals(o.getFileStatus());
  }

  @Override
  public int hashCode() {
    long pId = getParentId() * getFileStatus().hashCode();
    return (int)(pId ^ (pId >>> 32));
  }

  void writeBlock(long blockId, long offset, long length, long genStamp,
      PathHandle pathHandle, BlockAliasMap.Writer<FileRegion> out)
      throws IOException {
    FileStatus s = getFileStatus();
    out.store(new FileRegion(blockId, s.getPath(), offset, length, genStamp,
        (pathHandle != null ? pathHandle.toByteArray() : new byte[0])));
  }

  INode toFile(UGIResolver ugi, BlockResolver blk,
      BlockAliasMap.Writer<FileRegion> out) throws IOException {
    final FileStatus s = getFileStatus();
    ugi.addUser(s.getOwner());
    ugi.addGroup(s.getGroup());
    INodeFile.Builder b = INodeFile.newBuilder()
        .setReplication(blk.getReplication(s))
        .setModificationTime(s.getModificationTime())
        .setAccessTime(s.getAccessTime())
        .setPreferredBlockSize(blk.preferredBlockSize(s))
        .setPermission(ugi.resolve(s))
        .setStoragePolicyID(HdfsConstants.PROVIDED_STORAGE_POLICY_ID);

    // pathhandle allows match as long as the file matches exactly.
    PathHandle pathHandle = null;
    if (fs != null) {
      try {
        pathHandle = fs.getPathHandle(s, Options.HandleOpt.exact());
      } catch (UnsupportedOperationException e) {
        LOG.warn(
            "Exact path handle not supported by filesystem " + fs.toString());
      }
    }
    // TODO: storage policy should be configurable per path; use BlockResolver
    long off = 0L;
    for (BlockProto block : blk.resolve(s)) {
      b.addBlocks(block);
      writeBlock(block.getBlockId(), off, block.getNumBytes(),
          block.getGenStamp(), pathHandle, out);
      off += block.getNumBytes();
    }
    INode.Builder ib = INode.newBuilder()
        .setType(INode.Type.FILE)
        .setId(id)
        .setName(ByteString.copyFrom(string2Bytes(s.getPath().getName())))
        .setFile(b);
    return ib.build();
  }

  INode toDirectory(UGIResolver ugi) {
    final FileStatus s = getFileStatus();
    ugi.addUser(s.getOwner());
    ugi.addGroup(s.getGroup());
    INodeDirectory.Builder b = INodeDirectory.newBuilder()
        .setModificationTime(s.getModificationTime())
        .setNsQuota(DEFAULT_NAMESPACE_QUOTA)
        .setDsQuota(DEFAULT_STORAGE_SPACE_QUOTA)
        .setPermission(ugi.resolve(s));
    INode.Builder ib = INode.newBuilder()
        .setType(INode.Type.DIRECTORY)
        .setId(id)
        .setName(ByteString.copyFrom(string2Bytes(s.getPath().getName())))
        .setDirectory(b);
    return ib.build();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ stat=\"").append(getFileStatus()).append("\"");
    sb.append(", id=").append(getId());
    sb.append(", parentId=").append(getParentId());
    sb.append(", iterObjId=").append(System.identityHashCode(i));
    sb.append(" }");
    return sb.toString();
  }
}
