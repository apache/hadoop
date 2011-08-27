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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * @deprecated Use MultiAction instead
 * Data type class for putting multiple regions worth of puts in one RPC.
 */
public class MultiPut extends Operation implements Writable {
  public HServerAddress address; // client code ONLY

  // TODO make this configurable
  public static final int DEFAULT_MAX_PUT_OUTPUT = 10;

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

  /**
   * Compile the table and column family (i.e. schema) information 
   * into a String. Useful for parsing and aggregation by debugging,
   * logging, and administration tools.
   * @return Map
   */
  @Override
  public Map<String, Object> getFingerprint() {
    Map<String, Object> map = new HashMap<String, Object>();
    // for extensibility, we have a map of table information that we will
    // populate with only family information for each table
    Map<String, Map> tableInfo = 
      new HashMap<String, Map>();
    map.put("tables", tableInfo);
    for (Map.Entry<byte[], List<Put>> entry : puts.entrySet()) {
      // our fingerprint only concerns itself with which families are touched,
      // not how many Puts touch them, so we use this Set to do just that.
      Set<String> familySet;
      try {
        // since the puts are stored by region, we may have already
        // recorded families for this region. if that is the case,
        // we want to add to the existing Set. if not, we make a new Set.
        String tableName = Bytes.toStringBinary(
            HRegionInfo.parseRegionName(entry.getKey())[0]);
        if (tableInfo.get(tableName) == null) {
          Map<String, Object> table = new HashMap<String, Object>();
          familySet = new TreeSet<String>();
          table.put("families", familySet);
          tableInfo.put(tableName, table);
        } else {
          familySet = (Set<String>) tableInfo.get(tableName).get("families");
        }
      } catch (IOException ioe) {
        // in the case of parse error, default to labeling by region
        Map<String, Object> table = new HashMap<String, Object>();
        familySet = new TreeSet<String>();
        table.put("families", familySet);
        tableInfo.put(Bytes.toStringBinary(entry.getKey()), table);
      }   
      // we now iterate through each Put and keep track of which families 
      // are affected in this table.
      for (Put p : entry.getValue()) {
        for (byte[] fam : p.getFamilyMap().keySet()) {
          familySet.add(Bytes.toStringBinary(fam));
        }
      }
    }
    return map;
  }

  /**
   * Compile the details beyond the scope of getFingerprint (mostly 
   * toMap from the Puts) into a Map along with the fingerprinted 
   * information. Useful for debugging, logging, and administration tools.
   * @param maxCols a limit on the number of columns output prior to truncation
   * @return Map
   */
  @Override
  public Map<String, Object> toMap(int maxCols) {
    Map<String, Object> map = getFingerprint();
    Map<String, Object> tableInfo = (Map<String, Object>) map.get("tables");
    int putCount = 0;
    for (Map.Entry<byte[], List<Put>> entry : puts.entrySet()) {
      // If the limit has been hit for put output, just adjust our counter
      if (putCount >= DEFAULT_MAX_PUT_OUTPUT) {
        putCount += entry.getValue().size();
        continue;
      }
      List<Put> regionPuts = entry.getValue();
      List<Map<String, Object>> putSummaries =
        new ArrayList<Map<String, Object>>();
      // find out how many of this region's puts we can add without busting
      // the maximum
      int regionPutsToAdd = regionPuts.size();
      putCount += regionPutsToAdd;
      if (putCount > DEFAULT_MAX_PUT_OUTPUT) {
        regionPutsToAdd -= putCount - DEFAULT_MAX_PUT_OUTPUT;
      }
      for (Iterator<Put> iter = regionPuts.iterator(); regionPutsToAdd-- > 0;) {
        putSummaries.add(iter.next().toMap(maxCols));
      }
      // attempt to extract the table name from the region name
      String tableName = "";
      try {
        tableName = Bytes.toStringBinary(
            HRegionInfo.parseRegionName(entry.getKey())[0]);
      } catch (IOException ioe) {
        // in the case of parse error, default to labeling by region
        tableName = Bytes.toStringBinary(entry.getKey());
      }
      // since the puts are stored by region, we may have already 
      // recorded puts for this table. if that is the case, 
      // we want to add to the existing List. if not, we place a new list 
      // in the map
      Map<String, Object> table =
        (Map<String, Object>) tableInfo.get(tableName);
      if (table == null) {
        // in case the Put has changed since getFingerprint's map was built
        table = new HashMap<String, Object>();
        tableInfo.put(tableName, table);
        table.put("puts", putSummaries);
      } else if (table.get("puts") == null) {
        table.put("puts", putSummaries);
      } else {
        ((List<Map<String, Object>>) table.get("puts")).addAll(putSummaries);
      }
    }
    map.put("totalPuts", putCount);
    return map;
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
