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

import org.iq80.leveldb.DBIterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.hadoop.utils.MetadataStore.KeyValue;


/**
 * LevelDB store iterator.
 */
public class LevelDBStoreIterator implements MetaStoreIterator<KeyValue> {


  private DBIterator levelDBIterator;

  public LevelDBStoreIterator(DBIterator iterator) {
    this.levelDBIterator = iterator;
    levelDBIterator.seekToFirst();
  }

  @Override
  public boolean hasNext() {
    return levelDBIterator.hasNext();
  }

  @Override
  public KeyValue next() {
    if(levelDBIterator.hasNext()) {
      Map.Entry<byte[], byte[]> entry = levelDBIterator.next();
      return KeyValue.create(entry.getKey(), entry.getValue());
    }
    throw new NoSuchElementException("LevelDB Store has no more elements");
  }

  @Override
  public void seekToFirst() {
    levelDBIterator.seekToFirst();
  }

  @Override
  public void seekToLast() {
    levelDBIterator.seekToLast();
  }

}
