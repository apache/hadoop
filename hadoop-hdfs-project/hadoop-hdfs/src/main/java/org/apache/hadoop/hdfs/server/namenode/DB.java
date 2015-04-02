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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.hdfs.server.namenode.INodeId.INVALID_INODE_ID;
import static org.apache.hadoop.hdfs.server.namenode.INodeId.ROOT_INODE_ID;

class DB {
  void addRoot(ByteString root) {
    INodeContainer c = new INodeContainer();
    c.inode(root);
    inodeMap.put(ROOT_INODE_ID, c);
  }

  static class INodeContainer {
    private static final NavigableMap<ByteBuffer, Long> EMPTY_MAP =
        Collections.unmodifiableNavigableMap(new TreeMap<ByteBuffer, Long>());
    private ByteString inode;
    /**
     * NOTE: The caller is responsible to ensure that the container owns the
     * ByteBuffer.
     */
    private TreeMap<ByteBuffer, Long> children;
    ByteString inode() {
      return inode;
    }

    void inode(ByteString inode) {
      this.inode = inode;
    }

    long getChild(ByteBuffer childName) {
      return children == null ? INVALID_INODE_ID : children
          .getOrDefault(childName, INVALID_INODE_ID);
    }

    Map<ByteBuffer, Long> ensureChildrenList() {
      if (children == null) {
        children = new TreeMap<>();
      }
      return children;
    }

    Map<ByteBuffer, Long> children() {
      return children;
    }

    NavigableMap<ByteBuffer, Long> readOnlyChildren() {
      return children == null ? EMPTY_MAP : Collections
          .unmodifiableNavigableMap(children);
    }
  }

  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private HashMap<Long, INodeContainer> inodeMap = new
      HashMap<>();

  HashMap<Long, INodeContainer> inodeMap() {
    return inodeMap;
  }

  DB.INodeContainer ensureContainer(long inodeId) {
    DB.INodeContainer c = inodeMap.get(inodeId);
    if (c == null) {
      c = new DB.INodeContainer();
      inodeMap.put(inodeId, c);
    }
    return c;
  }

  INodeContainer getINode(long id) {
    return inodeMap.get(id);
  }

  ReentrantReadWriteLock lock() {
    return lock;
  }
}
