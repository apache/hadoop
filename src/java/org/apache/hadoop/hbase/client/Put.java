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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;


/** 
 * Used to perform Put operations for a single row.
 * <p>
 * To perform a Put, instantiate a Put object with the row to insert to and
 * for each column to be inserted, execute {@link #add(byte[], byte[], byte[]) add} or
 * {@link #add(byte[], byte[], long, byte[]) add} if setting the timestamp.
 */
public class Put implements HeapSize, Writable, Comparable<Put> {
  private byte [] row = null;
  private long timestamp = HConstants.LATEST_TIMESTAMP;
  private long lockId = -1L;
  private Map<byte [], List<KeyValue>> familyMap =
    new TreeMap<byte [], List<KeyValue>>(Bytes.BYTES_COMPARATOR);
  
  /** Constructor for Writable.  DO NOT USE */
  public Put() {}
  
  /**
   * Create a Put operation for the specified row.
   * @param row row key
   */
  public Put(byte [] row) {
    this(row, null);
  }
  
  /**
   * Create a Put operation for the specified row, using an existing row lock.
   * @param row row key
   * @param rowLock previously acquired row lock, or null
   */
  public Put(byte [] row, RowLock rowLock) {
    if(row == null || row.length > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("Row key is invalid");
    }
    this.row = row;
    if(rowLock != null) {
      this.lockId = rowLock.getLockId();
    }
  }
  
  /**
   * Copy constructor.  Creates a Put operation cloned from the specified Put.
   * @param putToCopy put to copy
   */
  public Put(Put putToCopy) {
    this(putToCopy.getRow(), putToCopy.getRowLock());
    this.familyMap = 
      new TreeMap<byte [], List<KeyValue>>(Bytes.BYTES_COMPARATOR);
    for(Map.Entry<byte [], List<KeyValue>> entry :
      putToCopy.getFamilyMap().entrySet()) {
      this.familyMap.put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Add the specified column and value to this Put operation.
   * @param family family name
   * @param qualifier column qualifier
   * @param value column value
   */
  public void add(byte [] family, byte [] qualifier, byte [] value) {
    add(family, qualifier, this.timestamp, value);
  }

  /**
   * Add the specified column and value, with the specified timestamp as 
   * its version to this Put operation.
   * @param column Old style column name with family and qualifier put together
   * with a colon.
   * @param timestamp version timestamp
   * @param value column value
   */
  public void add(byte [] column, long timestamp, byte [] value) {
    byte [][] parts = KeyValue.parseColumn(column);
    add(parts[0], parts[1], timestamp, value);
  }

  /**
   * Add the specified column and value, with the specified timestamp as 
   * its version to this Put operation.
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param value column value
   */
  public void add(byte [] family, byte [] qualifier, long timestamp, byte [] value) {
    List<KeyValue> list = familyMap.get(family);
    if(list == null) {
      list = new ArrayList<KeyValue>();
    }
    KeyValue kv = new KeyValue(this.row, family, qualifier, timestamp, 
      KeyValue.Type.Put, value); 
    list.add(kv);
    familyMap.put(family, list);
  }
  
  /**
   * Add the specified KeyValue to this Put operation.
   * @param kv
   */
  public void add(KeyValue kv) {
    byte [] family = kv.getFamily();
    List<KeyValue> list = familyMap.get(family);
    if(list == null) {
      list = new ArrayList<KeyValue>();
    }
    list.add(kv);
    familyMap.put(family, list);
  }
  
  
  /**
   * Method for retrieving the put's familyMap
   * @return familyMap
   */
  public Map<byte [], List<KeyValue>> getFamilyMap() {
    return this.familyMap;
  }
  
  /**
   * Method for retrieving the put's row
   * @return row 
   */
  public byte [] getRow() {
    return this.row;
  }
  
  /**
   * Method for retrieving the put's RowLock
   * @return RowLock
   */
  public RowLock getRowLock() {
    return new RowLock(this.row, this.lockId);
  }
  
  /**
   * Method for retrieving the put's lockId
   * @return lockId
   */
  public long getLockId() {
  	return this.lockId;
  }
  
  /**
   * Method to check if the familyMap is empty
   * @return true if empty, false otherwise
   */
  public boolean isEmpty() {
    return familyMap.isEmpty();
  }
  
  /**
   * Method for setting the timestamp
   * @param timestamp
   */
  public void setTimeStamp(long timestamp) {
    this.timestamp = timestamp;
  }
  
  public int numFamilies() {
    return familyMap.size();
  }
  
  public int size() {
    int size = 0;
    for(List<KeyValue> kvList : this.familyMap.values()) {
      size += kvList.size();
    }
    return size;
  }
  
  /**
   * @return String 
   */
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("row=");
    sb.append(Bytes.toString(this.row));
    sb.append(", families={");
    boolean moreThanOne = false;
    for(Map.Entry<byte [], List<KeyValue>> entry : this.familyMap.entrySet()) {
      if(moreThanOne) {
        sb.append(", ");
      } else {
        moreThanOne = true;
      }
      sb.append("(family=");
      sb.append(Bytes.toString(entry.getKey()));
      sb.append(", keyvalues=(");
      boolean moreThanOneB = false;
      for(KeyValue kv : entry.getValue()) {
        if(moreThanOneB) {
          sb.append(", ");
        } else {
          moreThanOneB = true;
        }
        sb.append(kv.toString());
      }
      sb.append(")");
    }
    sb.append("}");
    return sb.toString();
  }
  
  public int compareTo(Put p) {
    return Bytes.compareTo(this.getRow(), p.getRow());
  }
  
  //HeapSize
  public long heapSize() {
  	long totalSize = 0;
  	for(Map.Entry<byte [], List<KeyValue>> entry : this.familyMap.entrySet()) {
  	  for(KeyValue kv : entry.getValue()) {
  		totalSize += kv.heapSize();
  	  }
  	}
    return totalSize;
  }
  
  //Writable
  public void readFields(final DataInput in)
  throws IOException {
    this.row = Bytes.readByteArray(in);
    this.timestamp = in.readLong();
    this.lockId = in.readLong();
    int numFamilies = in.readInt();
    this.familyMap = 
      new TreeMap<byte [],List<KeyValue>>(Bytes.BYTES_COMPARATOR);
    for(int i=0;i<numFamilies;i++) {
      byte [] family = Bytes.readByteArray(in);
      int numKeys = in.readInt();
      List<KeyValue> keys = new ArrayList<KeyValue>(numKeys);
      int totalLen = in.readInt();
      byte [] buf = new byte[totalLen];
      int offset = 0;
      for(int j=0;j<numKeys;j++) {
        int keyLength = in.readInt();
        in.readFully(buf, offset, keyLength);
        keys.add(new KeyValue(buf, offset, keyLength));
        offset += keyLength;
      }
      this.familyMap.put(family, keys);
    }
  }
  
  public void write(final DataOutput out)
  throws IOException {
    Bytes.writeByteArray(out, this.row);
    out.writeLong(this.timestamp);
    out.writeLong(this.lockId);
    out.writeInt(familyMap.size());
    for(Map.Entry<byte [], List<KeyValue>> entry : familyMap.entrySet()) {
      Bytes.writeByteArray(out, entry.getKey());
      List<KeyValue> keys = entry.getValue();
      out.writeInt(keys.size());
      int totalLen = 0;
      for(KeyValue kv : keys) {
        totalLen += kv.getLength();
      }
      out.writeInt(totalLen);
      for(KeyValue kv : keys) {
        out.writeInt(kv.getLength());
        out.write(kv.getBuffer(), kv.getOffset(), kv.getLength());
      }
    }
  }
}
