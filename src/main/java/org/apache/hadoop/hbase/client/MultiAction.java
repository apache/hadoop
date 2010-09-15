/*
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HServerAddress;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeMap;

/**
 * Container for Actions (i.e. Get, Delete, or Put), which are grouped by
 * regionName. Intended to be used with HConnectionManager.processBatch()
 */
public final class MultiAction implements Writable {

  // map of regions to lists of puts/gets/deletes for that region.
  public Map<byte[], List<Action>> actions = new TreeMap<byte[], List<Action>>(
      Bytes.BYTES_COMPARATOR);

  public MultiAction() {
  }

  /**
   * Get the total number of Actions
   *
   * @return total number of Actions for all groups in this container.
   */
  public int size() {
    int size = 0;
    for (List l : actions.values()) {
      size += l.size();
    }
    return size;
  }

  /**
   * Add an Action to this container based on it's regionName. If the regionName
   * is wrong, the initial execution will fail, but will be automatically
   * retried after looking up the correct region.
   *
   * @param regionName
   * @param a
   */
  public void add(byte[] regionName, Action a) {
    List<Action> rsActions = actions.get(regionName);
    if (rsActions == null) {
      rsActions = new ArrayList<Action>();
      actions.put(regionName, rsActions);
    }
    rsActions.add(a);
  }

  public Set<byte[]> getRegions() {
    return actions.keySet();
  }

  /**
   * @return All actions from all regions in this container
   */
  public List<Action> allActions() {
    List<Action> res = new ArrayList<Action>();
    for (List<Action> lst : actions.values()) {
      res.addAll(lst);
    }
    return res;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(actions.size());
    for (Map.Entry<byte[], List<Action>> e : actions.entrySet()) {
      Bytes.writeByteArray(out, e.getKey());
      List<Action> lst = e.getValue();
      out.writeInt(lst.size());
      for (Action a : lst) {
        HbaseObjectWritable.writeObject(out, a, Action.class, null);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    actions.clear();
    int mapSize = in.readInt();
    for (int i = 0; i < mapSize; i++) {
      byte[] key = Bytes.readByteArray(in);
      int listSize = in.readInt();
      List<Action> lst = new ArrayList<Action>(listSize);
      for (int j = 0; j < listSize; j++) {
        lst.add((Action) HbaseObjectWritable.readObject(in, null));
      }
      actions.put(key, lst);
    }
  }

}
