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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.hdfsdb.*;
import org.apache.hadoop.hdfs.hdfsdb.DB;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.hadoop.hdfs.server.namenode.INodeId.INVALID_INODE_ID;

class LevelDBROTransaction extends ROTransaction {
  private final org.apache.hadoop.hdfs.hdfsdb.DB hdfsdb;

  private Snapshot snapshot;
  private final ReadOptions options = new ReadOptions();
  public static final ReadOptions OPTIONS = new ReadOptions();

  LevelDBROTransaction(FSDirectory fsd, org.apache.hadoop.hdfs.hdfsdb.DB db) {
    super(fsd);
    this.hdfsdb = db;
  }

  LevelDBROTransaction begin() {
    snapshot = hdfsdb.snapshot();
    options.snapshot(snapshot);
    return this;
  }

  @Override
  FlatINode getINode(long id) {
    return getFlatINode(id, hdfsdb, options);
  }

  @Override
  long getChild(long parentId, ByteBuffer localName) {
    return getChild(parentId, localName, hdfsdb, options);
  }

  @Override
  DBChildrenView childrenView(long parent) {
    return getChildrenView(parent, hdfsdb, options);
  }

  static FlatINode getFlatINode(
      long id, DB hdfsdb, ReadOptions options) {
    byte[] key = inodeKey(id);
    try {
      byte[] bytes = options == OPTIONS ? hdfsdb.get(options, key) : hdfsdb
          .snapshotGet(options, key);
      if (bytes == null) {
        return null;
      }
      return FlatINode.wrap(bytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static byte[] inodeKey(long id) {
    return new byte[]{'I',
          (byte) ((id >> 56) & 0xff),
          (byte) ((id >> 48) & 0xff),
          (byte) ((id >> 40) & 0xff),
          (byte) ((id >> 32) & 0xff),
          (byte) ((id >> 24) & 0xff),
          (byte) ((id >> 16) & 0xff),
          (byte) ((id >> 8) & 0xff),
          (byte) (id & 0xff),
          0
      };
  }

  static long getChild(
      long parentId, ByteBuffer localName, DB hdfsdb, ReadOptions options) {
    Preconditions.checkArgument(localName.hasRemaining());
    byte[] key = inodeChildKey(parentId, localName);
    try {
      byte[] bytes = options == OPTIONS ? hdfsdb.get(options, key) : hdfsdb
          .snapshotGet(options, key);
      if (bytes == null) {
        return INVALID_INODE_ID;
      }
      return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
          .asLongBuffer().get();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static byte[] inodeChildKey(long parentId, ByteBuffer localName) {
    byte[] key = new byte[10 + localName.remaining()];
    key[0] = 'I';
    for (int i = 0; i < 8; ++i) {
      key[1 + i] = (byte) ((parentId >> ((7 - i) * 8)) & 0xff);
    }
    key[9] = 1;
    ByteBuffer.wrap(key, 10, localName.remaining()).put(localName.duplicate());
    return key;
  }

  static DBChildrenView getChildrenView(
      long parent, DB hdfsdb, ReadOptions options) {
    byte[] key = new byte[]{'I',
        (byte) ((parent >> 56) & 0xff),
        (byte) ((parent >> 48) & 0xff),
        (byte) ((parent >> 40) & 0xff),
        (byte) ((parent >> 32) & 0xff),
        (byte) ((parent >> 24) & 0xff),
        (byte) ((parent >> 16) & 0xff),
        (byte) ((parent >> 8) & 0xff),
        (byte) (parent & 0xff),
        1
    };
    Iterator it = hdfsdb.iterator(options);
    it.seek(key);
    return new LevelDBChildrenView(parent, it);
  }

  @Override
  public void close() throws IOException {
    try {
      snapshot.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
