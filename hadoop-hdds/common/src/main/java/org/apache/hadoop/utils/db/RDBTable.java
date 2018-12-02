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
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hdfs.DFSUtil;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB implementation of ozone metadata store.
 */
public class RDBTable implements Table<byte[], byte[]> {


  private static final Logger LOG =
      LoggerFactory.getLogger(RDBTable.class);

  private final RocksDB db;
  private final ColumnFamilyHandle handle;
  private final WriteOptions writeOptions;

  /**
   * Constructs a TableStore.
   *
   * @param db - DBstore that we are using.
   * @param handle - ColumnFamily Handle.
   * @param writeOptions - RocksDB write Options.
   */
  public RDBTable(RocksDB db, ColumnFamilyHandle handle,
      WriteOptions writeOptions) {
    this.db = db;
    this.handle = handle;
    this.writeOptions = writeOptions;
  }

  /**
   * Converts RocksDB exception to IOE.
   * @param msg  - Message to add to exception.
   * @param e - Original Exception.
   * @return  IOE.
   */
  public static IOException toIOException(String msg, RocksDBException e) {
    String statusCode = e.getStatus() == null ? "N/A" :
        e.getStatus().getCodeString();
    String errMessage = e.getMessage() == null ? "Unknown error" :
        e.getMessage();
    String output = msg + "; status : " + statusCode
        + "; message : " + errMessage;
    return new IOException(output, e);
  }

  /**
   * Returns the Column family Handle.
   *
   * @return ColumnFamilyHandle.
   */
  public ColumnFamilyHandle getHandle() {
    return handle;
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    try {
      db.put(handle, writeOptions, key, value);
    } catch (RocksDBException e) {
      LOG.error("Failed to write to DB. Key: {}", new String(key,
          StandardCharsets.UTF_8));
      throw toIOException("Failed to put key-value to metadata "
          + "store", e);
    }
  }

  @Override
  public void putWithBatch(BatchOperation batch, byte[] key, byte[] value)
      throws IOException {
    if (batch instanceof RDBBatchOperation) {
      ((RDBBatchOperation) batch).put(getHandle(), key, value);
    } else {
      throw new IllegalArgumentException("batch should be RDBBatchOperation");
    }
  }


  @Override
  public boolean isEmpty() throws IOException {
    try (TableIterator<byte[], ByteArrayKeyValue> keyIter = iterator()) {
      keyIter.seekToFirst();
      return !keyIter.hasNext();
    }
  }

  @Override
  public byte[] get(byte[] key) throws IOException {
    try {
      return db.get(handle, key);
    } catch (RocksDBException e) {
      throw toIOException(
          "Failed to get the value for the given key", e);
    }
  }

  @Override
  public void delete(byte[] key) throws IOException {
    try {
      db.delete(handle, key);
    } catch (RocksDBException e) {
      throw toIOException("Failed to delete the given key", e);
    }
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, byte[] key)
      throws IOException {
    if (batch instanceof RDBBatchOperation) {
      ((RDBBatchOperation) batch).delete(getHandle(), key);
    } else {
      throw new IllegalArgumentException("batch should be RDBBatchOperation");
    }

  }

  @Override
  public TableIterator<byte[], ByteArrayKeyValue> iterator() {
    ReadOptions readOptions = new ReadOptions();
    return new RDBStoreIterator(db.newIterator(handle, readOptions));
  }

  @Override
  public String getName() throws IOException {
    try {
      return DFSUtil.bytes2String(this.getHandle().getName());
    } catch (RocksDBException rdbEx) {
      throw toIOException("Unable to get the table name.", rdbEx);
    }
  }

  @Override
  public void close() throws Exception {
    // Nothing do for a Column Family.
  }
}
