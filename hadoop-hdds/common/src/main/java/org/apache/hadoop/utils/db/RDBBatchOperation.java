/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.utils.db;

import java.io.IOException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Batch operation implementation for rocks db.
 */
public class RDBBatchOperation implements BatchOperation {

  private final WriteBatch writeBatch;

  public RDBBatchOperation() {
    writeBatch = new WriteBatch();
  }

  public void commit(RocksDB db, WriteOptions writeOptions) throws IOException {
    try {
      db.write(writeOptions, writeBatch);
    } catch (RocksDBException e) {
      throw new IOException("Unable to write the batch.", e);
    }
  }

  @Override
  public void close() {
    writeBatch.close();
  }

  public void delete(ColumnFamilyHandle handle, byte[] key) throws IOException {
    try {
      writeBatch.delete(handle, key);
    } catch (RocksDBException e) {
      throw new IOException("Can't record batch delete operation.", e);
    }
  }

  public void put(ColumnFamilyHandle handle, byte[] key, byte[] value)
      throws IOException {
    try {
      writeBatch.put(handle, key, value);
    } catch (RocksDBException e) {
      throw new IOException("Can't record batch put operation.", e);
    }
  }
}
