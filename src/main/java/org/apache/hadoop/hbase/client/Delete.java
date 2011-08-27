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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Used to perform Delete operations on a single row.
 * <p>
 * To delete an entire row, instantiate a Delete object with the row
 * to delete.  To further define the scope of what to delete, perform
 * additional methods as outlined below.
 * <p>
 * To delete specific families, execute {@link #deleteFamily(byte[]) deleteFamily}
 * for each family to delete.
 * <p>
 * To delete multiple versions of specific columns, execute
 * {@link #deleteColumns(byte[], byte[]) deleteColumns}
 * for each column to delete.
 * <p>
 * To delete specific versions of specific columns, execute
 * {@link #deleteColumn(byte[], byte[], long) deleteColumn}
 * for each column version to delete.
 * <p>
 * Specifying timestamps, deleteFamily and deleteColumns will delete all
 * versions with a timestamp less than or equal to that passed.  If no
 * timestamp is specified, an entry is added with a timestamp of 'now'
 * where 'now' is the servers's System.currentTimeMillis().
 * Specifying a timestamp to the deleteColumn method will
 * delete versions only with a timestamp equal to that specified.
 * If no timestamp is passed to deleteColumn, internally, it figures the
 * most recent cell's timestamp and adds a delete at that timestamp; i.e.
 * it deletes the most recently added cell.
 * <p>The timestamp passed to the constructor is used ONLY for delete of
 * rows.  For anything less -- a deleteColumn, deleteColumns or
 * deleteFamily -- then you need to use the method overrides that take a
 * timestamp.  The constructor timestamp is not referenced.
 */
public class Delete extends Operation
  implements Writable, Row, Comparable<Row> {
  private static final byte DELETE_VERSION = (byte)3;

  private byte [] row = null;
  // This ts is only used when doing a deleteRow.  Anything less,
  private long ts;
  private long lockId = -1L;
  private boolean writeToWAL = true;
  private final Map<byte [], List<KeyValue>> familyMap =
    new TreeMap<byte [], List<KeyValue>>(Bytes.BYTES_COMPARATOR);

  // a opaque blob that can be passed into a Delete. 
  private Map<String, byte[]> attributes;

  /** Constructor for Writable.  DO NOT USE */
  public Delete() {
    this((byte [])null);
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
   * an optional row lock.<p>
   *
   * If no further operations are done, this will delete all columns in all
   * families of the specified row with a timestamp less than or equal to the
   * specified timestamp.<p>
   *
   * This timestamp is ONLY used for a delete row operation.  If specifying
   * families or columns, you must specify each timestamp individually.
   * @param row row key
   * @param timestamp maximum version timestamp (only for delete row)
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
   * @param d Delete to clone.
   */
  public Delete(final Delete d) {
    this.row = d.getRow();
    this.ts = d.getTimeStamp();
    this.lockId = d.getLockId();
    this.familyMap.putAll(d.getFamilyMap());
    this.writeToWAL = d.writeToWAL;
  }

  public int compareTo(final Row d) {
    return Bytes.compareTo(this.getRow(), d.getRow());
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
   * @return this for invocation chaining
   */
  public Delete deleteFamily(byte [] family) {
    this.deleteFamily(family, HConstants.LATEST_TIMESTAMP);
    return this;
  }

  /**
   * Delete all columns of the specified family with a timestamp less than
   * or equal to the specified timestamp.
   * <p>
   * Overrides previous calls to deleteColumn and deleteColumns for the
   * specified family.
   * @param family family name
   * @param timestamp maximum version timestamp
   * @return this for invocation chaining
   */
  public Delete deleteFamily(byte [] family, long timestamp) {
    List<KeyValue> list = familyMap.get(family);
    if(list == null) {
      list = new ArrayList<KeyValue>();
    } else if(!list.isEmpty()) {
      list.clear();
    }
    list.add(new KeyValue(row, family, null, timestamp, KeyValue.Type.DeleteFamily));
    familyMap.put(family, list);
    return this;
  }

  /**
   * Delete all versions of the specified column.
   * @param family family name
   * @param qualifier column qualifier
   * @return this for invocation chaining
   */
  public Delete deleteColumns(byte [] family, byte [] qualifier) {
    this.deleteColumns(family, qualifier, HConstants.LATEST_TIMESTAMP);
    return this;
  }

  /**
   * Delete all versions of the specified column with a timestamp less than
   * or equal to the specified timestamp.
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp maximum version timestamp
   * @return this for invocation chaining
   */
  public Delete deleteColumns(byte [] family, byte [] qualifier, long timestamp) {
    List<KeyValue> list = familyMap.get(family);
    if (list == null) {
      list = new ArrayList<KeyValue>();
    }
    list.add(new KeyValue(this.row, family, qualifier, timestamp,
      KeyValue.Type.DeleteColumn));
    familyMap.put(family, list);
    return this;
  }

  /**
   * Delete the latest version of the specified column.
   * This is an expensive call in that on the server-side, it first does a
   * get to find the latest versions timestamp.  Then it adds a delete using
   * the fetched cells timestamp.
   * @param family family name
   * @param qualifier column qualifier
   * @return this for invocation chaining
   */
  public Delete deleteColumn(byte [] family, byte [] qualifier) {
    this.deleteColumn(family, qualifier, HConstants.LATEST_TIMESTAMP);
    return this;
  }

  /**
   * Delete the specified version of the specified column.
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @return this for invocation chaining
   */
  public Delete deleteColumn(byte [] family, byte [] qualifier, long timestamp) {
    List<KeyValue> list = familyMap.get(family);
    if(list == null) {
      list = new ArrayList<KeyValue>();
    }
    list.add(new KeyValue(
        this.row, family, qualifier, timestamp, KeyValue.Type.Delete));
    familyMap.put(family, list);
    return this;
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
   * Set the timestamp of the delete.
   * 
   * @param timestamp
   */
  public void setTimestamp(long timestamp) {
    this.ts = timestamp;
  }

  /**
   * Sets arbitrary delete's attribute.
   * In case value = null attribute is removed from the attributes map.
   * @param name attribute name
   * @param value attribute value
   */
  public void setAttribute(String name, byte[] value) {
    if (attributes == null && value == null) {
      return;
    }

    if (attributes == null) {
      attributes = new HashMap<String, byte[]>();
    }

    if (value == null) {
      attributes.remove(name);
      if (attributes.isEmpty()) {
        this.attributes = null;
      }
    } else {
      attributes.put(name, value);
    }
  }

  /**
   * Gets put's attribute
   * @param name attribute name
   * @return attribute value if attribute is set, <tt>null</tt> otherwise
   */
  public byte[] getAttribute(String name) {
    if (attributes == null) {
      return null;
    }
    return attributes.get(name);
  }

  /**
   * Gets all scan's attributes
   * @return unmodifiable map of all attributes
   */
  public Map<String, byte[]> getAttributesMap() {
    if (attributes == null) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(attributes);
  }

  /**
   * Compile the column family (i.e. schema) information
   * into a Map. Useful for parsing and aggregation by debugging,
   * logging, and administration tools.
   * @return Map
   */
  @Override
  public Map<String, Object> getFingerprint() {
    Map<String, Object> map = new HashMap<String, Object>();
    List<String> families = new ArrayList<String>();
    // ideally, we would also include table information, but that information
    // is not stored in each Operation instance.
    map.put("families", families);
    for (Map.Entry<byte [], List<KeyValue>> entry : this.familyMap.entrySet()) {
      families.add(Bytes.toStringBinary(entry.getKey()));
    }
    return map;
  }

  /**
   * Compile the details beyond the scope of getFingerprint (row, columns,
   * timestamps, etc.) into a Map along with the fingerprinted information.
   * Useful for debugging, logging, and administration tools.
   * @param maxCols a limit on the number of columns output prior to truncation
   * @return Map
   */
  @Override
  public Map<String, Object> toMap(int maxCols) {
    // we start with the fingerprint map and build on top of it.
    Map<String, Object> map = getFingerprint();
    // replace the fingerprint's simple list of families with a 
    // map from column families to lists of qualifiers and kv details
    Map<String, List<Map<String, Object>>> columns =
      new HashMap<String, List<Map<String, Object>>>();
    map.put("families", columns);
    map.put("row", Bytes.toStringBinary(this.row));
    map.put("ts", this.ts);
    int colCount = 0;
    // iterate through all column families affected by this Delete
    for (Map.Entry<byte [], List<KeyValue>> entry : this.familyMap.entrySet()) {
      // map from this family to details for each kv affected within the family
      List<Map<String, Object>> qualifierDetails =
        new ArrayList<Map<String, Object>>();
      columns.put(Bytes.toStringBinary(entry.getKey()), qualifierDetails);
      colCount += entry.getValue().size();
      if (maxCols <= 0) {
        continue;
      }
      // add details for each kv
      for (KeyValue kv : entry.getValue()) {
        if (--maxCols <= 0 ) {
          continue;
        }
        Map<String, Object> kvMap = kv.toStringMap();
        // row and family information are already available in the bigger map
        kvMap.remove("row");
        kvMap.remove("family");
        qualifierDetails.add(kvMap);
      }
    }
    map.put("totalColumns", colCount);
    return map;
  }

  //Writable
  public void readFields(final DataInput in) throws IOException {
    int version = in.readByte();
    if (version > DELETE_VERSION) {
      throw new IOException("version not supported");
    }
    this.row = Bytes.readByteArray(in);
    this.ts = in.readLong();
    this.lockId = in.readLong();
    if (version > 2) {
      this.writeToWAL = in.readBoolean();
    }
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
    if (version > 1) {
      int numAttributes = in.readInt();
      if (numAttributes > 0) {
        this.attributes = new HashMap<String, byte[]>();
        for(int i=0; i<numAttributes; i++) {
          String name = WritableUtils.readString(in);
          byte[] value = Bytes.readByteArray(in);
          this.attributes.put(name, value);
        }
      }
    }
  }

  public void write(final DataOutput out) throws IOException {
    out.writeByte(DELETE_VERSION);
    Bytes.writeByteArray(out, this.row);
    out.writeLong(this.ts);
    out.writeLong(this.lockId);
    out.writeBoolean(this.writeToWAL);
    out.writeInt(familyMap.size());
    for(Map.Entry<byte [], List<KeyValue>> entry : familyMap.entrySet()) {
      Bytes.writeByteArray(out, entry.getKey());
      List<KeyValue> list = entry.getValue();
      out.writeInt(list.size());
      for(KeyValue kv : list) {
        kv.write(out);
      }
    }
    if (this.attributes == null) {
      out.writeInt(0);
    } else {
      out.writeInt(this.attributes.size());
      for (Map.Entry<String, byte[]> attr : this.attributes.entrySet()) {
        WritableUtils.writeString(out, attr.getKey());
        Bytes.writeByteArray(out, attr.getValue());
      }
    }
  }

  /**
   * Delete all versions of the specified column, given in
   * <code>family:qualifier</code> notation, and with a timestamp less than
   * or equal to the specified timestamp.
   * @param column colon-delimited family and qualifier
   * @param timestamp maximum version timestamp
   * @deprecated use {@link #deleteColumn(byte[], byte[], long)} instead
   * @return this for invocation chaining
   */
  public Delete deleteColumns(byte [] column, long timestamp) {
    byte [][] parts = KeyValue.parseColumn(column);
    this.deleteColumns(parts[0], parts[1], timestamp);
    return this;
  }

  /**
   * Delete the latest version of the specified column, given in
   * <code>family:qualifier</code> notation.
   * @param column colon-delimited family and qualifier
   * @deprecated use {@link #deleteColumn(byte[], byte[])} instead
   * @return this for invocation chaining
   */
  public Delete deleteColumn(byte [] column) {
    byte [][] parts = KeyValue.parseColumn(column);
    this.deleteColumn(parts[0], parts[1], HConstants.LATEST_TIMESTAMP);
    return this;
  }

  /**
   * @return true if edits should be applied to WAL, false if not
   */
  public boolean getWriteToWAL() {
    return this.writeToWAL;
  }

  /**
   * Set whether this Delete should be written to the WAL or not.
   * Not writing the WAL means you may lose edits on server crash.
   * @param write true if edits should be written to WAL, false if not
   */
  public void setWriteToWAL(boolean write) {
    this.writeToWAL = write;
  }
}
