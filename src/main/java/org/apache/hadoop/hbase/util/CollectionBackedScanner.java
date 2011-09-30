/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.AbstractKeyValueScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;

/**
 * Utility scanner that wraps a sortable collection and serves
 * as a KeyValueScanner.
 */
public class CollectionBackedScanner extends AbstractKeyValueScanner {
  final private Iterable<KeyValue> data;
  final KeyValue.KVComparator comparator;
  private Iterator<KeyValue> iter;
  private KeyValue current;

  public CollectionBackedScanner(SortedSet<KeyValue> set) {
    this(set, KeyValue.COMPARATOR);
  }

  public CollectionBackedScanner(SortedSet<KeyValue> set,
      KeyValue.KVComparator comparator) {
    this.comparator = comparator;
    data = set;
    init();
  }

  public CollectionBackedScanner(List<KeyValue> list) {
    this(list, KeyValue.COMPARATOR);
  }

  public CollectionBackedScanner(List<KeyValue> list,
      KeyValue.KVComparator comparator) {
    Collections.sort(list, comparator);
    this.comparator = comparator;
    data = list;
    init();
  }

  public CollectionBackedScanner(KeyValue.KVComparator comparator,
      KeyValue... array) {
    this.comparator = comparator;

    List<KeyValue> tmp = new ArrayList<KeyValue>(array.length);
    for( int i = 0; i < array.length ; ++i) {
      tmp.add(array[i]);
    }
    Collections.sort(tmp, comparator);
    data = tmp;
    init();
  }

  private void init() {
    iter = data.iterator();
    if(iter.hasNext()){
      current = iter.next();
    }
  }

  @Override
  public KeyValue peek() {
    return current;
  }

  @Override
  public KeyValue next() {
    KeyValue oldCurrent = current;
    if(iter.hasNext()){
      current = iter.next();
    } else {
      current = null;
    }
    return oldCurrent;
  }

  @Override
  public boolean seek(KeyValue seekKv) {
    // restart iterator
    iter = data.iterator();
    return reseek(seekKv);
  }

  @Override
  public boolean reseek(KeyValue seekKv) {
    while(iter.hasNext()){
      KeyValue next = iter.next();
      int ret = comparator.compare(next, seekKv);
      if(ret >= 0){
        current = next;
        return true;
      }
    }
    return false;
  }

  @Override
  public long getSequenceID() {
    return 0;
  }

  @Override
  public void close() {
    // do nothing
  }
}
