/*
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Data type class for putting multiple regions worth of puts in one RPC.
 */
public class MultiPut implements Writable {
  public HServerAddress address; // client code ONLY

  // map of regions to lists of puts for that region.
  public Map<byte[], List<Put> > puts = new TreeMap<byte[], List<Put>>(Bytes.BYTES_COMPARATOR);

  /**
   * Writable constructor only.
   */
  public MultiPut() {}

  /**
   * MultiPut for putting multiple regions worth of puts in one RPC.
   * @param a address
   */
  public MultiPut(HServerAddress a) {
    address = a;
  }

  public int size() {
    int size = 0;
    for( List<Put> l : puts.values()) {
      size += l.size();
    }
    return size;
  }

  public void add(byte[] regionName, Put aPut) {
    List<Put> rsput = puts.get(regionName);
    if (rsput == null) {
      rsput = new ArrayList<Put>();
      puts.put(regionName, rsput);
    }
    rsput.add(aPut);
  }

  public Collection<Put> allPuts() {
    List<Put> res = new ArrayList<Put>();
    for ( List<Put> pp : puts.values() ) {
      res.addAll(pp);
    }
    return res;
  }


  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(puts.size());
    for( Map.Entry<byte[],List<Put>> e : puts.entrySet()) {
      Bytes.writeByteArray(out, e.getKey());

      List<Put> ps = e.getValue();
      out.writeInt(ps.size());
      for( Put p : ps ) {
        p.write(out);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    puts.clear();

    int mapSize = in.readInt();

    for (int i = 0 ; i < mapSize; i++) {
      byte[] key = Bytes.readByteArray(in);

      int listSize = in.readInt();
      List<Put> ps = new ArrayList<Put>(listSize);
      for ( int j = 0 ; j < listSize; j++ ) {
        Put put = new Put();
        put.readFields(in);
        ps.add(put);
      }
      puts.put(key, ps);
    }
  }
}
