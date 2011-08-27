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
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;


/**
 * Used to perform Put operations for a single row.
 * <p>
 * To perform a Put, instantiate a Put object with the row to insert to and
 * for each column to be inserted, execute {@link #add(byte[], byte[], byte[]) add} or
 * {@link #add(byte[], byte[], long, byte[]) add} if setting the timestamp.
 */
public class Put extends Operation
  implements HeapSize, Writable, Row, Comparable<Row> {
  private static final byte PUT_VERSION = (byte)2;

  private byte [] row = null;
  private long timestamp = HConstants.LATEST_TIMESTAMP;
  private long lockId = -1L;
  private boolean writeToWAL = true;

  private Map<byte [], List<KeyValue>> familyMap =
    new TreeMap<byte [], List<KeyValue>>(Bytes.BYTES_COMPARATOR);

  // a opaque blob that can be passed into a Put. 
  private Map<String, byte[]> attributes;

  private static final long OVERHEAD = ClassSize.align(
      ClassSize.OBJECT + 2 * ClassSize.REFERENCE +
      2 * Bytes.SIZEOF_LONG + Bytes.SIZEOF_BOOLEAN +
      ClassSize.REFERENCE + ClassSize.TREEMAP);

  /** Constructor for Writable. DO NOT USE */
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
      this(row, HConstants.LATEST_TIMESTAMP, rowLock);
  }

  /**
   * Create a Put operation for the specified row, using a given timestamp.
   *
   * @param row row key
   * @param ts timestamp
   */
  public Put(byte[] row, long ts) {
    this(row, ts, null);
  }

  /**
   * Create a Put operation for the specified row, using a given timestamp, and an existing row lock.
   * @param row row key
   * @param ts timestamp
   * @param rowLock previously acquired row lock, or null
   */
  public Put(byte [] row, long ts, RowLock rowLock) {
    if(row == null || row.length > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("Row key is invalid");
    }
    this.row = Arrays.copyOf(row, row.length);
    this.timestamp = ts;
    if(rowLock != null) {
      this.lockId = rowLock.getLockId();
    }
  }

  /**
   * Copy constructor.  Creates a Put operation cloned from the specified Put.
   * @param putToCopy put to copy
   */
  public Put(Put putToCopy) {
    this(putToCopy.getRow(), putToCopy.timestamp, putToCopy.getRowLock());
    this.familyMap =
      new TreeMap<byte [], List<KeyValue>>(Bytes.BYTES_COMPARATOR);
    for(Map.Entry<byte [], List<KeyValue>> entry :
      putToCopy.getFamilyMap().entrySet()) {
      this.familyMap.put(entry.getKey(), entry.getValue());
    }
    this.writeToWAL = putToCopy.writeToWAL;
  }

  /**
   * Add the specified column and value to this Put operation.
   * @param family family name
   * @param qualifier column qualifier
   * @param value column value
   * @return this
   */
  public Put add(byte [] family, byte [] qualifier, byte [] value) {
    return add(family, qualifier, this.timestamp, value);
  }

  /**
   * Add the specified column and value, with the specified timestamp as
   * its version to this Put operation.
   * @param family family name
   * @param qualifier column qualifier
   * @param ts version timestamp
   * @param value column value
   * @return this
   */
  public Put add(byte [] family, byte [] qualifier, long ts, byte [] value) {
    List<KeyValue> list = getKeyValueList(family);
    KeyValue kv = createPutKeyValue(family, qualifier, ts, value);
    list.add(kv);
    familyMap.put(kv.getFamily(), list);
    return this;
  }

  /**
   * Add the specified KeyValue to this Put operation.  Operation assumes that
   * the passed KeyValue is immutable and its backing array will not be modified
   * for the duration of this Put.
   * @param kv individual KeyValue
   * @return this
   * @throws java.io.IOException e
   */
  public Put add(KeyValue kv) throws IOException{
    byte [] family = kv.getFamily();
    List<KeyValue> list = getKeyValueList(family);
    //Checking that the row of the kv is the same as the put
    int res = Bytes.compareTo(this.row, 0, row.length,
        kv.getBuffer(), kv.getRowOffset(), kv.getRowLength());
    if(res != 0) {
      throw new IOException("The row in the recently added KeyValue " +
          Bytes.toStringBinary(kv.getBuffer(), kv.getRowOffset(),
        kv.getRowLength()) + " doesn't match the original one " +
        Bytes.toStringBinary(this.row));
    }
    list.add(kv);
    familyMap.put(family, list);
    return this;
  }

  /*
   * Create a KeyValue with this objects row key and the Put identifier.
   *
   * @return a KeyValue with this objects row key and the Put identifier.
   */
  private KeyValue createPutKeyValue(byte[] family, byte[] qualifier, long ts,
      byte[] value) {
  return  new KeyValue(this.row, family, qualifier, ts, KeyValue.Type.Put,
      value);
  }

  /**
   * A convenience method to determine if this object's familyMap contains
   * a value assigned to the given family & qualifier.
   * Both given arguments must match the KeyValue object to return true.
   *
   * @param family column family
   * @param qualifier column qualifier
   * @return returns true if the given family and qualifier already has an
   * existing KeyValue object in the family map.
   */
  public boolean has(byte [] family, byte [] qualifier) {
  return has(family, qualifier, this.timestamp, new byte[0], true, true);
  }

  /**
   * A convenience method to determine if this object's familyMap contains
   * a value assigned to the given family, qualifier and timestamp.
   * All 3 given arguments must match the KeyValue object to return true.
   *
   * @param family column family
   * @param qualifier column qualifier
   * @param ts timestamp
   * @return returns true if the given family, qualifier and timestamp already has an
   * existing KeyValue object in the family map.
   */
  public boolean has(byte [] family, byte [] qualifier, long ts) {
  return has(family, qualifier, ts, new byte[0], false, true);
  }

  /**
   * A convenience method to determine if this object's familyMap contains
   * a value assigned to the given family, qualifier and timestamp.
   * All 3 given arguments must match the KeyValue object to return true.
   *
   * @param family column family
   * @param qualifier column qualifier
   * @param value value to check
   * @return returns true if the given family, qualifier and value already has an
   * existing KeyValue object in the family map.
   */
  public boolean has(byte [] family, byte [] qualifier, byte [] value) {
    return has(family, qualifier, this.timestamp, value, true, false);
  }

  /**
   * A convenience method to determine if this object's familyMap contains
   * the given value assigned to the given family, qualifier and timestamp.
   * All 4 given arguments must match the KeyValue object to return true.
   *
   * @param family column family
   * @param qualifier column qualifier
   * @param ts timestamp
   * @param value value to check
   * @return returns true if the given family, qualifier timestamp and value
   * already has an existing KeyValue object in the family map.
   */
  public boolean has(byte [] family, byte [] qualifier, long ts, byte [] value) {
      return has(family, qualifier, ts, value, false, false);
  }

  /*
   * Private method to determine if this object's familyMap contains
   * the given value assigned to the given family, qualifier and timestamp
   * respecting the 2 boolean arguments
   *
   * @param family
   * @param qualifier
   * @param ts
   * @param value
   * @param ignoreTS
   * @param ignoreValue
   * @return returns true if the given family, qualifier timestamp and value
   * already has an existing KeyValue object in the family map.
   */
  private boolean has(byte [] family, byte [] qualifier, long ts, byte [] value,
      boolean ignoreTS, boolean ignoreValue) {
    List<KeyValue> list = getKeyValueList(family);
    if (list.size() == 0) {
      return false;
    }
    // Boolean analysis of ignoreTS/ignoreValue.
    // T T => 2
    // T F => 3 (first is always true)
    // F T => 2
    // F F => 1
    if (!ignoreTS && !ignoreValue) {
      KeyValue kv = createPutKeyValue(family, qualifier, ts, value);
      return (list.contains(kv));
    } else if (ignoreValue) {
      for (KeyValue kv: list) {
        if (Arrays.equals(kv.getFamily(), family) && Arrays.equals(kv.getQualifier(), qualifier)
            && kv.getTimestamp() == ts) {
          return true;
        }
      }
    } else {
      // ignoreTS is always true
      for (KeyValue kv: list) {
      if (Arrays.equals(kv.getFamily(), family) && Arrays.equals(kv.getQualifier(), qualifier)
              && Arrays.equals(kv.getValue(), value)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns a list of all KeyValue objects with matching column family and qualifier.
   *
   * @param family column family
   * @param qualifier column qualifier
   * @return a list of KeyValue objects with the matching family and qualifier,
   * returns an empty list if one doesnt exist for the given family.
   */
  public List<KeyValue> get(byte[] family, byte[] qualifier) {
    List<KeyValue> filteredList = new ArrayList<KeyValue>();
    for (KeyValue kv: getKeyValueList(family)) {
      if (Arrays.equals(kv.getQualifier(), qualifier)) {
        filteredList.add(kv);
      }
    }
    return filteredList;
  }

  /**
   * Creates an empty list if one doesnt exist for the given column family
   * or else it returns the associated list of KeyValue objects.
   *
   * @param family column family
   * @return a list of KeyValue objects, returns an empty list if one doesnt exist.
   */
  private List<KeyValue> getKeyValueList(byte[] family) {
    List<KeyValue> list = familyMap.get(family);
    if(list == null) {
      list = new ArrayList<KeyValue>(0);
    }
    return list;
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
   * @return Timestamp
   */
  public long getTimeStamp() {
    return this.timestamp;
  }

  /**
   * @return the number of different families included in this put
   */
  public int numFamilies() {
    return familyMap.size();
  }

  /**
   * @return the total number of KeyValues that will be added with this put
   */
  public int size() {
    int size = 0;
    for(List<KeyValue> kvList : this.familyMap.values()) {
      size += kvList.size();
    }
    return size;
  }

  /**
   * @return true if edits should be applied to WAL, false if not
   */
  public boolean getWriteToWAL() {
    return this.writeToWAL;
  }

  /**
   * Set whether this Put should be written to the WAL or not.
   * Not writing the WAL means you may lose edits on server crash.
   * @param write true if edits should be written to WAL, false if not
   */
  public void setWriteToWAL(boolean write) {
    this.writeToWAL = write;
  }

  /**
   * Sets arbitrary put's attribute.
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
    int colCount = 0;
    // iterate through all column families affected by this Put
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

  public int compareTo(Row p) {
    return Bytes.compareTo(this.getRow(), p.getRow());
  }

  //HeapSize
  public long heapSize() {
    long heapsize = OVERHEAD;
    //Adding row
    heapsize += ClassSize.align(ClassSize.ARRAY + this.row.length);

    //Adding map overhead
    heapsize +=
      ClassSize.align(this.familyMap.size() * ClassSize.MAP_ENTRY);
    for(Map.Entry<byte [], List<KeyValue>> entry : this.familyMap.entrySet()) {
      //Adding key overhead
      heapsize +=
        ClassSize.align(ClassSize.ARRAY + entry.getKey().length);

      //This part is kinds tricky since the JVM can reuse references if you
      //store the same value, but have a good match with SizeOf at the moment
      //Adding value overhead
      heapsize += ClassSize.align(ClassSize.ARRAYLIST);
      int size = entry.getValue().size();
      heapsize += ClassSize.align(ClassSize.ARRAY +
          size * ClassSize.REFERENCE);

      for(KeyValue kv : entry.getValue()) {
        heapsize += kv.heapSize();
      }
    }
    if (attributes != null) {
      heapsize += ClassSize.align(this.attributes.size() * ClassSize.MAP_ENTRY);
      for(Map.Entry<String, byte[]> entry : this.attributes.entrySet()) {
        heapsize += ClassSize.align(ClassSize.STRING + entry.getKey().length());
        heapsize += ClassSize.align(ClassSize.ARRAY + entry.getValue().length);
      }
    }
    return ClassSize.align((int)heapsize);
  }

  //Writable
  public void readFields(final DataInput in)
  throws IOException {
    int version = in.readByte();
    if (version > PUT_VERSION) {
      throw new IOException("version not supported");
    }
    this.row = Bytes.readByteArray(in);
    this.timestamp = in.readLong();
    this.lockId = in.readLong();
    this.writeToWAL = in.readBoolean();
    int numFamilies = in.readInt();
    if (!this.familyMap.isEmpty()) this.familyMap.clear();
    for(int i=0;i<numFamilies;i++) {
      byte [] family = Bytes.readByteArray(in);
      int numKeys = in.readInt();
      List<KeyValue> keys = new ArrayList<KeyValue>(numKeys);
      int totalLen = in.readInt();
      byte [] buf = new byte[totalLen];
      int offset = 0;
      for (int j = 0; j < numKeys; j++) {
        int keyLength = in.readInt();
        in.readFully(buf, offset, keyLength);
        keys.add(new KeyValue(buf, offset, keyLength));
        offset += keyLength;
      }
      this.familyMap.put(family, keys);
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

  public void write(final DataOutput out)
  throws IOException {
    out.writeByte(PUT_VERSION);
    Bytes.writeByteArray(out, this.row);
    out.writeLong(this.timestamp);
    out.writeLong(this.lockId);
    out.writeBoolean(this.writeToWAL);
    out.writeInt(familyMap.size());
    for (Map.Entry<byte [], List<KeyValue>> entry : familyMap.entrySet()) {
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
   * Add the specified column and value, with the specified timestamp as
   * its version to this Put operation.
   * @param column Old style column name with family and qualifier put together
   * with a colon.
   * @param ts version timestamp
   * @param value column value
   * @deprecated use {@link #add(byte[], byte[], long, byte[])} instead
   * @return true
   */
  public Put add(byte [] column, long ts, byte [] value) {
    byte [][] parts = KeyValue.parseColumn(column);
    return add(parts[0], parts[1], ts, value);
  }
}
