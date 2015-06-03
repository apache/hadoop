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
import org.apache.hadoop.hdfs.hdfsdb.WriteBatch;
import org.apache.hadoop.hdfs.hdfsdb.WriteOptions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class LevelDBRWTransaction extends RWTransaction {
  private static final WriteOptions WRITE_OPTIONS =
      new WriteOptions().skipWal(true);
  private final WriteBatch batch = new WriteBatch();
  private final org.apache.hadoop.hdfs.hdfsdb.DB hdfsdb;
  LevelDBRWTransaction(FSDirectory fsd) {
    super(fsd);
    this.hdfsdb = fsd.getLevelDb();
  }

  @Override
  FlatINode getINode(long id) {
    return LevelDBROTransaction.getFlatINode(id, hdfsdb,
                                             LevelDBROTransaction.OPTIONS);
  }

  @Override
  long getChild(long parentId, ByteBuffer localName) {
    return LevelDBROTransaction.getChild(parentId, localName, hdfsdb,
                                         LevelDBROTransaction.OPTIONS);
  }

  @Override
  DBChildrenView childrenView(long parent) {
    return LevelDBROTransaction.getChildrenView(parent, hdfsdb,
                                                LevelDBROTransaction.OPTIONS);
  }

  @Override
  public void close() throws IOException {
    super.close();
    batch.close();
  }

  void putINode(long id, ByteString inode) {
    batch.put(LevelDBROTransaction.inodeKey(id), inode.toByteArray());
  }

  void putChild(long parentId, ByteBuffer localName, long id) {
    byte[] v = new byte[8];
    ByteBuffer.wrap(v).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().put(id);
    batch.put(LevelDBROTransaction.inodeChildKey(parentId, localName), v);
  }

  void deleteINode(long inodeId) {
    batch.delete(LevelDBROTransaction.inodeKey(inodeId));
  }

  void deleteChild(long parentId, ByteBuffer localName) {
    batch.delete(LevelDBROTransaction.inodeChildKey(parentId, localName));
  }

  void commit() {
    try {
      hdfsdb.write(WRITE_OPTIONS, batch);
      fsd.clearCurrentLevelDBSnapshot();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}