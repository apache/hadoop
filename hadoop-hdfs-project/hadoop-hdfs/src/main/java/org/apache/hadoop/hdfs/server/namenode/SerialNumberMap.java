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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Map object to serial number.
 * 
 * <p>It allows to get the serial number of an object, if the object doesn't
 * exist in the map, a new serial number increased by 1 is generated to
 * map to the object. The mapped object can also be got through the serial
 * number.
 * 
 * <p>The map is thread-safe.
 */
@InterfaceAudience.Private
public class SerialNumberMap<T> {
  private final AtomicInteger max = new AtomicInteger(1);
  private final ConcurrentMap<T, Integer> t2i =
      new ConcurrentHashMap<T, Integer>();
  private final ConcurrentMap<Integer, T> i2t =
      new ConcurrentHashMap<Integer, T>();

  public int get(T t) {
    if (t == null) {
      return 0;
    }
    Integer sn = t2i.get(t);
    if (sn == null) {
      sn = max.getAndIncrement();
      if (sn < 0) {
        throw new IllegalStateException("Too many elements!");
      }
      Integer old = t2i.putIfAbsent(t, sn);
      if (old != null) {
        return old;
      }
      i2t.put(sn, t);
    }
    return sn;
  }

  public T get(int i) {
    if (i == 0) {
      return null;
    }
    T t = i2t.get(i);
    if (t == null) {
      throw new IllegalStateException("!i2t.containsKey(" + i
          + "), this=" + this);
    }
    return t;
  }

  @Override
  public String toString() {
    return "max=" + max + ",\n  t2i=" + t2i + ",\n  i2t=" + i2t;
  }
}