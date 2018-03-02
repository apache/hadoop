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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Resizable-array implementation of the DiffList interface.
 * @param <T> Type of the object in the list
 */
public class DiffListByArrayList<T extends Comparable<Integer>>
    implements DiffList<T> {
  private final List<T> list;

  DiffListByArrayList(List<T> list) {
    this.list = list;
  }

  public DiffListByArrayList(int initialCapacity) {
    this(new ArrayList<>(initialCapacity));
  }

  @Override
  public T get(int i) {
    return list.get(i);
  }

  @Override
  public boolean isEmpty() {
    return list.isEmpty();
  }

  @Override
  public int size() {
    return list.size();
  }

  @Override
  public T remove(int i) {
    return list.remove(i);
  }

  @Override
  public boolean addLast(T t) {
    return list.add(t);
  }

  @Override
  public void addFirst(T t) {
    list.add(0, t);
  }

  @Override
  public int binarySearch(int i) {
    return Collections.binarySearch(list, i);
  }

  @Override
  public Iterator<T> iterator() {
    return list.iterator();
  }

  @Override
  public List<T> getMinListForRange(int startIndex, int endIndex,
      INodeDirectory dir) {
    return list.subList(startIndex, endIndex);
  }
}
