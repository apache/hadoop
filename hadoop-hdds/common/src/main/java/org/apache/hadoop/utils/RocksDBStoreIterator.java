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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.apache.hadoop.utils;

import org.rocksdb.RocksIterator;

import java.util.NoSuchElementException;

import org.apache.hadoop.utils.MetadataStore.KeyValue;

/**
 * RocksDB store iterator.
 */
public class RocksDBStoreIterator implements MetaStoreIterator<KeyValue> {

  private RocksIterator rocksDBIterator;

  public RocksDBStoreIterator(RocksIterator iterator) {
    this.rocksDBIterator = iterator;
    rocksDBIterator.seekToFirst();
  }

  @Override
  public boolean hasNext() {
    return rocksDBIterator.isValid();
  }

  @Override
  public KeyValue next() {
    if (rocksDBIterator.isValid()) {
      KeyValue value = KeyValue.create(rocksDBIterator.key(), rocksDBIterator
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

}
