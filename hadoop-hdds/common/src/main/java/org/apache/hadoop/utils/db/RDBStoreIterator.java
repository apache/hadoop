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
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import org.rocksdb.RocksIterator;

/**
 * RocksDB store iterator.
 */
public class RDBStoreIterator
    implements TableIterator<byte[], ByteArrayKeyValue> {

  private RocksIterator rocksDBIterator;

  public RDBStoreIterator(RocksIterator iterator) {
    this.rocksDBIterator = iterator;
    rocksDBIterator.seekToFirst();
  }

  @Override
  public void forEachRemaining(
      Consumer<? super ByteArrayKeyValue> action) {
    while (hasNext()) {
      action.accept(next());
    }
  }

  @Override
  public boolean hasNext() {
    return rocksDBIterator.isValid();
  }

  @Override
  public ByteArrayKeyValue next() {
    if (rocksDBIterator.isValid()) {
      ByteArrayKeyValue value =
          ByteArrayKeyValue.create(rocksDBIterator.key(), rocksDBIterator
              .value());
      rocksDBIterator.next();
      return value;
    }
    throw new NoSuchElementException("RocksDB Store has no more elements");
  }

  @Override
  public void seekToFirst() {
    rocksDBIterator.seekToFirst();
  }

  @Override
  public void seekToLast() {
    rocksDBIterator.seekToLast();
  }

  @Override
  public ByteArrayKeyValue seek(byte[] key) {
    rocksDBIterator.seek(key);
    if (rocksDBIterator.isValid()) {
      return ByteArrayKeyValue.create(rocksDBIterator.key(),
          rocksDBIterator.value());
    }
    return null;
  }

  @Override
  public byte[] key() {
    if (rocksDBIterator.isValid()) {
      return rocksDBIterator.key();
    }
    return null;
  }

  @Override
  public ByteArrayKeyValue value() {
    if (rocksDBIterator.isValid()) {
      return ByteArrayKeyValue.create(rocksDBIterator.key(),
          rocksDBIterator.value());
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    rocksDBIterator.close();
  }
}
