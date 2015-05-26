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

import com.google.protobuf.ByteString;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import static org.apache.hadoop.hdfs.server.namenode.INodeId.INVALID_INODE_ID;

class RWTransaction extends Transaction {
  private static final long DELETED_INODE_ID = INVALID_INODE_ID;
  private static final ByteString DELETED_INODE = ByteString.EMPTY;

  private HashMap<Long, DB.INodeContainer> inodeMap = new HashMap<>();

  RWTransaction(FSDirectory fsd) {
    super(fsd);
  }

  RWTransaction begin() {
    fsd.writeLock();
    return this;
  }

  @Override
  FlatINode getINode(long id) {
    DB.INodeContainer c = inodeMap.get(id);
    if (c == null) {
      return getINodeFromDB(id);
    }
    return c.inode() == DELETED_INODE ? null : FlatINode.wrap(c.inode());
  }

  @Override
  long getChild(long parentId, ByteBuffer localName) {
    DB.INodeContainer c = inodeMap.get(parentId);
    if (c == null || c.children() == null) {
      return getChildFromDB(parentId, localName);
    }
    Long id = c.children().get(localName);
    return id == null || id == DELETED_INODE_ID ? INVALID_INODE_ID : id;
  }

  @Override
  NavigableMap<ByteBuffer, Long> childrenView(long parent) {
    // TODO: This function only provides a read-only view for the content in
    // the DB. It needs to consider the modification in this transaction to
    // implement transactional semantic.
    DB.INodeContainer c = fsd.db().getINode(parent);
    return c.readOnlyChildren();
  }

  @Override
  public void close() throws IOException {
    fsd.writeUnlock();
  }

  void putINode(long id, ByteString inode) {
    DB.INodeContainer c = ensureContainer(id);
    c.inode(inode);
  }

  void putChild(long parentId, ByteBuffer localName, long id) {
    DB.INodeContainer c = ensureContainer(parentId);
    ByteString s = ByteString.copyFrom(localName);
    c.ensureChildrenList().put(s.asReadOnlyByteBuffer(), id);
  }

  void deleteINode(long inodeId) {
    putINode(inodeId, DELETED_INODE);
  }

  void deleteChild(long parentId, ByteBuffer localName) {
    putChild(parentId, localName, DELETED_INODE_ID);
  }

  long allocateNewInodeId() {
    return fsd.allocateNewInodeId();
  }

  int getStringId(String str) {
    return fsd.ugid().getId(str);
  }

  private DB.INodeContainer ensureContainer(long inodeId) {
    DB.INodeContainer c = inodeMap.get(inodeId);
    if (c == null) {
      c = new DB.INodeContainer();
      inodeMap.put(inodeId, c);
    }
    return c;
  }

  void commit() {
    for (Map.Entry<Long, DB.INodeContainer> e : inodeMap.entrySet()) {
      long id = e.getKey();
      DB.INodeContainer c = e.getValue();
      if (c.inode() == DELETED_INODE) {
        fsd.db().inodeMap().remove(id);
        continue;
      }

      DB.INodeContainer dbContainer = fsd.db().ensureContainer(id);
      if (c.inode() != null) {
        dbContainer.inode(c.inode());
      }

      if (c.children() == null) {
        continue;
      }

      for (Map.Entry<ByteBuffer, Long> e1 : c.children().entrySet()) {
        ByteBuffer childName = e1.getKey();
        long childId = e1.getValue();
        if (e1.getValue() == INVALID_INODE_ID) {
          if (dbContainer.children() != null) {
            dbContainer.children().remove(childName);
          }
        } else {
          dbContainer.ensureChildrenList().put(childName, childId);
        }
      }
    }
    inodeMap.clear();
  }

  public void logMkDir(FlatINodesInPath iip) {
    fsd.getEditLog().logMkDir(iip, fsd.ugid());
  }

  public void logRename(
      String src, String dst, long mtime, boolean logRetryCache,
      Options.Rename[] options) {
    fsd.getEditLog().logRename(src, dst, mtime, logRetryCache, options);
  }

  public void logOpenFile(StringMap ugid, String src, FlatINode inode,
      boolean overwrite, boolean logRetryCache) {
    fsd.getEditLog().logOpenFile(ugid, src, inode, overwrite, logRetryCache);
  }

  public void logAddBlock(String src, FlatINodeFileFeature file) {
    fsd.getEditLog().logAddBlock(src, file);
  }

  public void logCloseFile(String path, FlatINode inode) {
    fsd.getEditLog().logCloseFile(fsd.ugid(), path, inode);
  }

  public void logSetPermissions(String src, FsPermission permission) {
    fsd.getEditLog().logSetPermissions(src, permission);
  }

  public void logTimes(String src, long mtime, long atime) {
    fsd.getEditLog().logTimes(src, mtime, atime);
  }

  public void logUpdateBlocks(String path, FlatINodeFileFeature file) {
    Block[] blocks = new Block[file.numBlocks()];
    int i = 0;
    for (Block b : file.blocks()) {
      blocks[i++] = b;
    }
    fsd.getEditLog().logUpdateBlocks(path, blocks, false);
  }
}
