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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.SplitKeyValue;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.io.Writable;

/**
 * Single row result of a {@link Get} or {@link Scan} query.<p>
 * 
 * Convenience methods are available that return various {@link Map}
 * structures and values directly.<p>
 * 
 * To get a complete mapping of all cells in the Result, which can include 
 * multiple families and multiple versions, use {@link #getMap()}.<p>
 * 
 * To get a mapping of each family to its columns (qualifiers and values), 
 * including only the latest version of each, use {@link #getNoVersionMap()}.
 * 
 * To get a mapping of qualifiers to latest values for an individual family use 
 * {@link #getFamilyMap(byte[])}.<p>
 * 
 * To get the latest value for a specific family and qualifier use {@link #getValue(byte[], byte[])}.
 *
 * A Result is backed by an array of {@link KeyValue} objects, each representing
 * an HBase cell defined by the row, family, qualifier, timestamp, and value.<p>
 * 
 * The underlying {@link KeyValue} objects can be accessed through the methods
 * {@link #sorted()} and {@link #list()}.  Each KeyValue can then be accessed
 * through {@link KeyValue#getRow()}, {@link KeyValue#getFamily()}, {@link KeyValue#getQualifier()}, 
 * {@link KeyValue#getTimestamp()}, and {@link KeyValue#getValue()}.
 */
public class Result implements Writable {
  private KeyValue [] kvs = null;
  private NavigableMap<byte[], 
     NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = null;
  // We're not using java serialization.  Transient here is just a marker to say
  // that this is where we cache row if we're ever asked for it.
  private transient byte [] row = null;

  /**
   * Constructor used for Writable.
   */
  public Result() {}

  /**
   * Instantiate a Result with the specified array of KeyValues.
   * @param kvs array of KeyValues
   */
  public Result(KeyValue [] kvs) {
    if(kvs != null && kvs.length > 0) {
      this.kvs = kvs;
    }
  }

  /**
   * Instantiate a Result with the specified List of KeyValues.
   * @param kvs List of KeyValues
   */
  public Result(List<KeyValue> kvs) {
    this(kvs.toArray(new KeyValue[0]));
  }

  /**
   * Method for retrieving the row that this result is for
   * @return row
   */
  public synchronized byte [] getRow() {
    if (this.row == null) {
      this.row =
        this.kvs == null || this.kvs.length == 0? null: this.kvs[0].getRow();
    }
    return this.row;
  }

  /**
   * Return the unsorted array of KeyValues backing this Result instance.
   * @return unsorted array of KeyValues
   */
  public KeyValue[] raw() {
    return kvs;
  }

  /**
   * Create a sorted list of the KeyValue's in this result.
   * 
   * @return The sorted list of KeyValue's.
   */
  public List<KeyValue> list() {
    return Arrays.asList(sorted());
  }

  /**
   * Returns a sorted array of KeyValues in this Result.
   * <p>
   * Note: Sorting is done in place, so the backing array will be sorted
   * after calling this method.
   * @return sorted array of KeyValues
   */
  public KeyValue[] sorted() {
    if (isEmpty()) {
      return null;
    }
    Arrays.sort(kvs, (Comparator<KeyValue>)KeyValue.COMPARATOR);
    return kvs;
  }

  /**
   * Map of families to all versions of its qualifiers and values.
   * <p>
   * Returns a three level Map of the form: 
   * <code>Map<family,Map<qualifier,Map<timestamp,value>>></code>
   * <p>
   * Note: All other map returning methods make use of this map internally. 
   * @return map from families to qualifiers to versions
   */
  public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getMap() {
    if(this.familyMap != null) {
      return this.familyMap;
    }
    if(isEmpty()) {
      return null;
    }
    this.familyMap =
      new TreeMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
      (Bytes.BYTES_COMPARATOR);
    for(KeyValue kv : this.kvs) {
      SplitKeyValue splitKV = kv.split();
      byte [] family = splitKV.getFamily();
      NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = 
        familyMap.get(family);
      if(columnMap == null) {
        columnMap = new TreeMap<byte[], NavigableMap<Long, byte[]>>
          (Bytes.BYTES_COMPARATOR);
        familyMap.put(family, columnMap);
      }
      byte [] qualifier = splitKV.getQualifier();
      NavigableMap<Long, byte[]> versionMap = columnMap.get(qualifier);
      if(versionMap == null) {
        versionMap = new TreeMap<Long, byte[]>(new Comparator<Long>() {
          public int compare(Long l1, Long l2) {
            return l2.compareTo(l1);
          }
        });
        columnMap.put(qualifier, versionMap);
      }
      Long timestamp = Bytes.toLong(splitKV.getTimestamp());
      byte [] value = splitKV.getValue();
      versionMap.put(timestamp, value);
    }
    return this.familyMap;
  }

  /**
   * Map of families to their most recent qualifiers and values.
   * <p>
   * Returns a two level Map of the form: <code>Map<family,Map<qualifier,value>></code>
   * <p>
   * The most recent version of each qualifier will be used.
   * @return map from families to qualifiers and value
   */
  public NavigableMap<byte[], NavigableMap<byte[], byte[]>> getNoVersionMap() {
    if(this.familyMap == null) {
      getMap();
    }
    if(isEmpty()) {
      return null;
    }
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> returnMap = 
      new TreeMap<byte[], NavigableMap<byte[], byte[]>>(Bytes.BYTES_COMPARATOR);
    for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> 
      familyEntry : familyMap.entrySet()) {
      NavigableMap<byte[], byte[]> qualifierMap = 
        new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      for(Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifierEntry :
        familyEntry.getValue().entrySet()) {
        byte [] value = 
          qualifierEntry.getValue().get(qualifierEntry.getValue().firstKey());
        qualifierMap.put(qualifierEntry.getKey(), value);
      }
      returnMap.put(familyEntry.getKey(), qualifierMap);
    }
    return returnMap;
  }

  /**
   * Map of qualifiers to values.
   * <p>
   * Returns a Map of the form: <code>Map<qualifier,value></code>
   * @return map of qualifiers to values
   */
  public NavigableMap<byte[], byte[]> getFamilyMap(byte [] family) {
    if(this.familyMap == null) {
      getMap();
    }
    if(isEmpty()) {
      return null;
    }
    NavigableMap<byte[], byte[]> returnMap = 
      new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifierMap = 
      familyMap.get(family);
    if(qualifierMap == null) {
      return returnMap;
    }
    for(Map.Entry<byte[], NavigableMap<Long, byte[]>> entry : 
      qualifierMap.entrySet()) {
      byte [] value = 
        entry.getValue().get(entry.getValue().firstKey());
      returnMap.put(entry.getKey(), value);
    }
    return returnMap;
  }

  /**
   * Get the latest version of the specified column.
   * @param family family name
   * @param qualifier column qualifier
   * @return value of latest version of column, null if none found
   */
  public byte [] getValue(byte [] family, byte [] qualifier) {
    Map.Entry<Long,byte[]> entry = getKeyValue(family, qualifier);
    return entry == null? null: entry.getValue();
  }

  public Cell getCellValue(byte[] family, byte[] qualifier) {
    Map.Entry<Long,byte[]> val = getKeyValue(family, qualifier);
    if (val == null)
      return null;
    return new Cell(val.getValue(), val.getKey());
  }

  /**
   * @return First KeyValue in this Result as a Cell or null if empty.
   */
  public Cell getCellValue() {
    return isEmpty()? null: new Cell(kvs[0].getValue(), kvs[0].getTimestamp());
  }

  /**
   * @return This Result as array of Cells or null if empty.
   */
  public Cell [] getCellValues() {
    if (isEmpty()) return null;
    Cell [] results = new Cell[kvs.length];
    for (int i = 0; i < kvs.length; i++) {
      results[i] = new Cell(kvs[i].getValue(), kvs[i].getTimestamp());
    }
    return results;
  }

  private Map.Entry<Long,byte[]> getKeyValue(byte[] family, byte[] qualifier) {
    if(this.familyMap == null) {
      getMap();
    }
    if(isEmpty()) {
      return null;
    }
    NavigableMap<byte [], NavigableMap<Long, byte[]>> qualifierMap =
      familyMap.get(family);
    if(qualifierMap == null) {
      return null;
    }
    NavigableMap<Long, byte[]> versionMap =
      getVersionMap(qualifierMap, qualifier);
    if(versionMap == null) {
      return null;
    }
    return versionMap.firstEntry();
  }
  
  private NavigableMap<Long, byte[]> getVersionMap(
      NavigableMap<byte [], NavigableMap<Long, byte[]>> qualifierMap, byte [] qualifier) {
    return qualifier != null?
      qualifierMap.get(qualifier): qualifierMap.get(new byte[0]);
  }
  
  /**
   * Get the latest version of the specified column,
   * using <pre>family:qualifier</pre> notation.
   * @param column column in family:qualifier notation
   * @return value of latest version of column, null if none found
   */
  public byte [] getValue(byte [] column) {
    try {
      byte [][] split = KeyValue.parseColumn(column);
      return getValue(split[0], split[1]);
    } catch(Exception e) {
      return null;
    }
  }
  
  /**
   * Checks for existence of the specified column.
   * @param family family name
   * @param qualifier column qualifier
   * @return true if at least one value exists in the result, false if not
   */
  public boolean containsColumn(byte [] family, byte [] qualifier) {
    if(this.familyMap == null) {
      getMap();
    }
    if(isEmpty()) {
      return false;
    }
    NavigableMap<byte [], NavigableMap<Long, byte[]>> qualifierMap =
      familyMap.get(family);
    if(qualifierMap == null) {
      return false;
    }
    NavigableMap<Long, byte[]> versionMap = getVersionMap(qualifierMap, qualifier);
    if(versionMap == null) {
      return false;
    }
    return true;
  }
  
  /**
   * Returns this Result in the old return format, {@link RowResult}.
   * @return a RowResult
   */
  public RowResult getRowResult() {
    return RowResult.createRowResult(Arrays.asList(kvs));
  }
  
  /**
   * Returns the value of the first column in the Result.
   * @return value of the first column
   */
  public byte [] value() {
    if (isEmpty()) {
      return null;
    }
    return kvs[0].getValue();
  }
  
  /**
   * Check if the underlying KeyValue [] is empty or not
   * @return true if empty
   */
  public boolean isEmpty() {
    return this.kvs == null || this.kvs.length == 0;
  }
  
  /**
   * @return the size of the underlying KeyValue []
   */
  public int size() {
    return this.kvs == null? 0: this.kvs.length;
  }
  
  /**
   * @return String
   */
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("keyvalues=");
    if(isEmpty()) {
      sb.append("NONE");
      return sb.toString();
    }
    sb.append("{");
    boolean moreThanOne = false;
    for(KeyValue kv : this.kvs) {
      if(moreThanOne) {
        sb.append(", ");
      } else {
        moreThanOne = true;
      }
      sb.append(kv.toString());
    }
    sb.append("}");
    return sb.toString();
  }
  
  //Writable
  public void readFields(final DataInput in)
  throws IOException {
    familyMap = null;
    row = null;
    int numKeys = in.readInt();
    this.kvs = new KeyValue[numKeys];
    if(numKeys == 0) {
      return;
    }
    int totalBuffer = in.readInt();
    byte [] buf = new byte[totalBuffer];
    int offset = 0;
    for(int i=0; i<numKeys; i++) {
      int keyLength = in.readInt();
      in.readFully(buf, offset, keyLength);
      kvs[i] = new KeyValue(buf, offset, keyLength);
      offset += keyLength;
    }
  }
  
  public void write(final DataOutput out)
  throws IOException {
    if(isEmpty()) {
      out.writeInt(0);
    } else {
      int len = this.kvs.length;
      out.writeInt(len);
      int totalLen = 0;
      for(KeyValue kv : kvs) {
        totalLen += kv.getLength();
      }
      out.writeInt(totalLen);
      for(KeyValue kv : kvs) {
        out.writeInt(kv.getLength());
        out.write(kv.getBuffer(), kv.getOffset(), kv.getLength());
      }
    }
  }
  
  public static void writeArray(final DataOutput out, Result [] results)
  throws IOException {
    if(results == null || results.length == 0) {
      out.writeInt(0);
      return;
    }
    out.writeInt(results.length);
    int bufLen = 0;
    for(Result result : results) {
      if(result == null || result.isEmpty()) {
        continue;
      }
      for(KeyValue key : result.raw()) {
        bufLen += key.getLength();
      }
    }
    out.writeInt(bufLen);
    for(Result result : results) {
      if(result == null || result.isEmpty()) {
        out.writeInt(0);
        continue;
      }
      out.writeInt(result.size());
      for(KeyValue kv : result.raw()) {
        out.writeInt(kv.getLength());
        out.write(kv.getBuffer(), kv.getOffset(), kv.getLength());
      }
    }
  }
  
  public static Result [] readArray(final DataInput in)
  throws IOException {
    int numResults = in.readInt();
    if(numResults == 0) {
      return new Result[0];
    }
    Result [] results = new Result[numResults];
    int bufSize = in.readInt();
    byte [] buf = new byte[bufSize];
    int offset = 0;
    for(int i=0;i<numResults;i++) {
      int numKeys = in.readInt();
      KeyValue [] keys = new KeyValue[numKeys];
      for(int j=0;j<numKeys;j++) {
        int keyLen = in.readInt();
        in.readFully(buf, offset, keyLen);
        keys[j] = new KeyValue(buf, offset, keyLen);
        offset += keyLen;
      }
      results[i] = new Result(keys);
    }
    return results;
  }
}
