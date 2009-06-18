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
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Used to perform Delete operations on a single row.
 * <p>
 * To delete an entire row, instantiate a Delete object with the row 
 * to delete.  To further define the scope of what to delete, perform
 * additional methods as outlined below.
 * <p>
 * To delete specific families, execute {@link #deleteFamily(byte []) deleteFamily}
 * for each family to delete.
 * <p>
 * To delete multiple versions of specific columns, execute
 * {@link #deleteColumns(byte [],byte []) deleteColumns}
 * for each column to delete.  
 * <p>
 * To delete specific versions of specific columns, execute
 * {@link #deleteColumn(byte [],byte [],long) deleteColumn}
 * for each column version to delete.
 * <p>
 * Specifying timestamps calling constructor, deleteFamily, and deleteColumns
 * will delete all versions with a timestamp less than or equal to that
 * specified.  Specifying a timestamp to deleteColumn will delete versions
 * only with a timestamp equal to that specified.
 * <p>The timestamp passed to the constructor is only used ONLY for delete of
 * rows.  For anything less -- a deleteColumn, deleteColumns or
 * deleteFamily -- then you need to use the method overrides that take a
 * timestamp.  The constructor timestamp is not referenced.
 */
public class Delete implements Writable {
  private byte [] row = null;
  // This ts is only used when doing a deleteRow.  Anything less, 
  private long ts;
  private long lockId = -1L;
  private final Map<byte [], List<KeyValue>> familyMap = 
    new TreeMap<byte [], List<KeyValue>>(Bytes.BYTES_COMPARATOR);

  /** Constructor for Writable.  DO NOT USE */
  public Delete() {
    this(null);
  }

  /**
   * Create a Delete operation for the specified row.
   * <p>
   * If no further operations are done, this will delete everything
   * associated with the specified row (all versions of all columns in all
   * families).
   * @param row row key
   */
  public Delete(byte [] row) {
    this(row, HConstants.LATEST_TIMESTAMP, null);
  }

  /**
   * Create a Delete operation for the specified row and timestamp, using
   * an optional row lock.
   * <p>
   * If no further operations are done, this will delete all columns in all
   * families of the specified row with a timestamp less than or equal to the 
   * specified timestamp.
   * @param row row key
   * @param timestamp maximum version timestamp
   * @param rowLock previously acquired row lock, or null
   */
  public Delete(byte [] row, long timestamp, RowLock rowLock) {
    this.row = row;
    this.ts = timestamp;
    if (rowLock != null) {
    	this.lockId = rowLock.getLockId();
    }
  }

  /**
   * Method to check if the familyMap is empty
   * @return true if empty, false otherwise
   */
  public boolean isEmpty() {
    return familyMap.isEmpty();
  }

  /**
   * Delete all versions of all columns of the specified family.
   * <p>
   * Overrides previous calls to deleteColumn and deleteColumns for the
   * specified family.
   * @param family family name
   */
  public void deleteFamily(byte [] family) {
	this.deleteFamily(family, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Delete all columns of the specified family with a timestamp less than
   * or equal to the specified timestamp.
   * <p>
   * Overrides previous calls to deleteColumn and deleteColumns for the
   * specified family.
   * @param family family name
   * @param timestamp maximum version timestamp
   */
  public void deleteFamily(byte [] family, long timestamp) {
    List<KeyValue> list = familyMap.get(family);
    if(list == null) {
      list = new ArrayList<KeyValue>();
    } else if(!list.isEmpty()) {
      list.clear();
    }
    list.add(new KeyValue(row, family, null, timestamp, KeyValue.Type.DeleteFamily));
    familyMap.put(family, list);
  }
  
  /**
   * Delete all versions of the specified column.
   * @param family family name
   * @param qualifier column qualifier
   */
  public void deleteColumns(byte [] family, byte [] qualifier) {
    this.deleteColumns(family, qualifier, HConstants.LATEST_TIMESTAMP);
  }
  
  /**
   * Delete all versions of the specified column with a timestamp less than
   * or equal to the specified timestamp.
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp maximum version timestamp
   */
  public void deleteColumns(byte [] family, byte [] qualifier, long timestamp) {
    List<KeyValue> list = familyMap.get(family);
    if (list == null) {
      list = new ArrayList<KeyValue>();
    }
    list.add(new KeyValue(this.row, family, qualifier, timestamp,
      KeyValue.Type.DeleteColumn));
    familyMap.put(family, list);
  }
  
  /**
   * Delete the latest version of the specified column.
   * This is an expensive call in that on the server-side, it first does a
   * get to find the latest versions timestamp.  Then it adds a delete using
   * the fetched cells timestamp.
   * @param family family name
   * @param qualifier column qualifier
   */
  public void deleteColumn(byte [] family, byte [] qualifier) {
    this.deleteColumn(family, qualifier, HConstants.LATEST_TIMESTAMP);
  }
  
  /**
   * Delete the specified version of the specified column.
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   */
  public void deleteColumn(byte [] family, byte [] qualifier, long timestamp) {
    List<KeyValue> list = familyMap.get(family);
    if(list == null) {
      list = new ArrayList<KeyValue>();
    }
    list.add(new KeyValue(
        this.row, family, qualifier, timestamp, KeyValue.Type.Delete));
    familyMap.put(family, list);
  }
  
  /**
   * Delete the latest version of the specified column, given in
   * <code>family:qualifier</code> notation.
   * @param column colon-delimited family and qualifier 
   */
  public void deleteColumn(byte [] column) {
    byte [][] parts = KeyValue.parseColumn(column);
    this.deleteColumn(parts[0], parts[1], HConstants.LATEST_TIMESTAMP);
  }
  
  /**
   * Method for retrieving the delete's familyMap 
   * @return familyMap
   */
  public Map<byte [], List<KeyValue>> getFamilyMap() {
    return this.familyMap;
  }
  
  /**
   *  Method for retrieving the delete's row
   * @return row
   */
  public byte [] getRow() {
    return this.row;
  }
  
  /**
   * Method for retrieving the delete's RowLock
   * @return RowLock
   */
  public RowLock getRowLock() {
    return new RowLock(this.row, this.lockId);
  }
  
  /**
   * Method for retrieving the delete's lock ID.
   * 
   * @return The lock ID.
   */
  public long getLockId() {
	return this.lockId;
  }
  
  /**
   * Method for retrieving the delete's timestamp
   * @return timestamp
   */
  public long getTimeStamp() {
    return this.ts;
  }
  
  /**
   * @return string
   */
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("row=");
    sb.append(Bytes.toString(this.row));
    sb.append(", ts=");
    sb.append(this.ts);
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
  
  //Writable
  public void readFields(final DataInput in) throws IOException {
    this.row = Bytes.readByteArray(in);
    this.ts = in.readLong();
    this.lockId = in.readLong();
    this.familyMap.clear();
    int numFamilies = in.readInt();
    for(int i=0;i<numFamilies;i++) {
      byte [] family = Bytes.readByteArray(in);
      int numColumns = in.readInt();
      List<KeyValue> list = new ArrayList<KeyValue>(numColumns);
      for(int j=0;j<numColumns;j++) {
    	KeyValue kv = new KeyValue();
    	kv.readFields(in);
    	list.add(kv);
      }
      this.familyMap.put(family, list);
    }
  }  
  
  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.row);
    out.writeLong(this.ts);
    out.writeLong(this.lockId);
    out.writeInt(familyMap.size());
    for(Map.Entry<byte [], List<KeyValue>> entry : familyMap.entrySet()) {
      Bytes.writeByteArray(out, entry.getKey());
      List<KeyValue> list = entry.getValue();
      out.writeInt(list.size());
      for(KeyValue kv : list) {
        kv.write(out);
      }
    }
  }
}