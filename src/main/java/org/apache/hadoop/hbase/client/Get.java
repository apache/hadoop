/**
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Used to perform Get operations on a single row.
 * <p>
 * To get everything for a row, instantiate a Get object with the row to get.
 * To further define the scope of what to get, perform additional methods as
 * outlined below.
 * <p>
 * To get all columns from specific families, execute {@link #addFamily(byte[]) addFamily}
 * for each family to retrieve.
 * <p>
 * To get specific columns, execute {@link #addColumn(byte[], byte[]) addColumn}
 * for each column to retrieve.
 * <p>
 * To only retrieve columns within a specific range of version timestamps,
 * execute {@link #setTimeRange(long, long) setTimeRange}.
 * <p>
 * To only retrieve columns with a specific timestamp, execute
 * {@link #setTimeStamp(long) setTimestamp}.
 * <p>
 * To limit the number of versions of each column to be returned, execute
 * {@link #setMaxVersions(int) setMaxVersions}.
 * <p>
 * To add a filter, execute {@link #setFilter(Filter) setFilter}.
 */
public class Get extends OperationWithAttributes
  implements Writable, Row, Comparable<Row> {
  private static final byte GET_VERSION = (byte)2;

  private byte [] row = null;
  private long lockId = -1L;
  private int maxVersions = 1;
  private boolean cacheBlocks = true;
  private Filter filter = null;
  private TimeRange tr = new TimeRange();
  private Map<byte [], NavigableSet<byte []>> familyMap =
    new TreeMap<byte [], NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);

  /** Constructor for Writable.  DO NOT USE */
  public Get() {}

  /**
   * Create a Get operation for the specified row.
   * <p>
   * If no further operations are done, this will get the latest version of
   * all columns in all families of the specified row.
   * @param row row key
   */
  public Get(byte [] row) {
    this(row, null);
  }

  /**
   * Create a Get operation for the specified row, using an existing row lock.
   * <p>
   * If no further operations are done, this will get the latest version of
   * all columns in all families of the specified row.
   * @param row row key
   * @param rowLock previously acquired row lock, or null
   */
  public Get(byte [] row, RowLock rowLock) {
    this.row = row;
    if(rowLock != null) {
      this.lockId = rowLock.getLockId();
    }
  }

  /**
   * Get all columns from the specified family.
   * <p>
   * Overrides previous calls to addColumn for this family.
   * @param family family name
   * @return the Get object
   */
  public Get addFamily(byte [] family) {
    familyMap.remove(family);
    familyMap.put(family, null);
    return this;
  }

  /**
   * Get the column from the specific family with the specified qualifier.
   * <p>
   * Overrides previous calls to addFamily for this family.
   * @param family family name
   * @param qualifier column qualifier
   * @return the Get objec
   */
  public Get addColumn(byte [] family, byte [] qualifier) {
    NavigableSet<byte []> set = familyMap.get(family);
    if(set == null) {
      set = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
    }
    set.add(qualifier);
    familyMap.put(family, set);
    return this;
  }

  /**
   * Get versions of columns only within the specified timestamp range,
   * [minStamp, maxStamp).
   * @param minStamp minimum timestamp value, inclusive
   * @param maxStamp maximum timestamp value, exclusive
   * @throws IOException if invalid time range
   * @return this for invocation chaining
   */
  public Get setTimeRange(long minStamp, long maxStamp)
  throws IOException {
    tr = new TimeRange(minStamp, maxStamp);
    return this;
  }

  /**
   * Get versions of columns with the specified timestamp.
   * @param timestamp version timestamp
   * @return this for invocation chaining
   */
  public Get setTimeStamp(long timestamp) {
    try {
      tr = new TimeRange(timestamp, timestamp+1);
    } catch(IOException e) {
      // Will never happen
    }
    return this;
  }

  /**
   * Get all available versions.
   * @return this for invocation chaining
   */
  public Get setMaxVersions() {
    this.maxVersions = Integer.MAX_VALUE;
    return this;
  }

  /**
   * Get up to the specified number of versions of each column.
   * @param maxVersions maximum versions for each column
   * @throws IOException if invalid number of versions
   * @return this for invocation chaining
   */
  public Get setMaxVersions(int maxVersions) throws IOException {
    if(maxVersions <= 0) {
      throw new IOException("maxVersions must be positive");
    }
    this.maxVersions = maxVersions;
    return this;
  }

  /**
   * Apply the specified server-side filter when performing the Get.
   * Only {@link Filter#filterKeyValue(KeyValue)} is called AFTER all tests
   * for ttl, column match, deletes and max versions have been run.
   * @param filter filter to run on the server
   * @return this for invocation chaining
   */
  public Get setFilter(Filter filter) {
    this.filter = filter;
    return this;
  }

  /* Accessors */

  /**
   * @return Filter
   */
  public Filter getFilter() {
    return this.filter;
  }

  /**
   * Set whether blocks should be cached for this Get.
   * <p>
   * This is true by default.  When true, default settings of the table and
   * family are used (this will never override caching blocks if the block
   * cache is disabled for that family or entirely).
   *
   * @param cacheBlocks if false, default settings are overridden and blocks
   * will not be cached
   */
  public void setCacheBlocks(boolean cacheBlocks) {
    this.cacheBlocks = cacheBlocks;
  }

  /**
   * Get whether blocks should be cached for this Get.
   * @return true if default caching should be used, false if blocks should not
   * be cached
   */
  public boolean getCacheBlocks() {
    return cacheBlocks;
  }

  /**
   * Method for retrieving the get's row
   * @return row
   */
  public byte [] getRow() {
    return this.row;
  }

  /**
   * Method for retrieving the get's RowLock
   * @return RowLock
   */
  public RowLock getRowLock() {
    return new RowLock(this.row, this.lockId);
  }

  /**
   * Method for retrieving the get's lockId
   * @return lockId
   */
  public long getLockId() {
    return this.lockId;
  }

  /**
   * Method for retrieving the get's maximum number of version
   * @return the maximum number of version to fetch for this get
   */
  public int getMaxVersions() {
    return this.maxVersions;
  }

  /**
   * Method for retrieving the get's TimeRange
   * @return timeRange
   */
  public TimeRange getTimeRange() {
    return this.tr;
  }

  /**
   * Method for retrieving the keys in the familyMap
   * @return keys in the current familyMap
   */
  public Set<byte[]> familySet() {
    return this.familyMap.keySet();
  }

  /**
   * Method for retrieving the number of families to get from
   * @return number of families
   */
  public int numFamilies() {
    return this.familyMap.size();
  }

  /**
   * Method for checking if any families have been inserted into this Get
   * @return true if familyMap is non empty false otherwise
   */
  public boolean hasFamilies() {
    return !this.familyMap.isEmpty();
  }

  /**
   * Method for retrieving the get's familyMap
   * @return familyMap
   */
  public Map<byte[],NavigableSet<byte[]>> getFamilyMap() {
    return this.familyMap;
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
    List<String> families = new ArrayList<String>();
    map.put("families", families);
    for (Map.Entry<byte [], NavigableSet<byte[]>> entry :
      this.familyMap.entrySet()) {
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
    Map<String, List<String>> columns = new HashMap<String, List<String>>();
    map.put("families", columns);
    // add scalar information first
    map.put("row", Bytes.toStringBinary(this.row));
    map.put("maxVersions", this.maxVersions);
    map.put("cacheBlocks", this.cacheBlocks);
    List<Long> timeRange = new ArrayList<Long>();
    timeRange.add(this.tr.getMin());
    timeRange.add(this.tr.getMax());
    map.put("timeRange", timeRange);
    int colCount = 0;
    // iterate through affected families and add details
    for (Map.Entry<byte [], NavigableSet<byte[]>> entry :
      this.familyMap.entrySet()) {
      List<String> familyList = new ArrayList<String>();
      columns.put(Bytes.toStringBinary(entry.getKey()), familyList);
      if(entry.getValue() == null) {
        colCount++;
        --maxCols;
        familyList.add("ALL");
      } else {
        colCount += entry.getValue().size();
        if (maxCols <= 0) {
          continue;
        }
        for (byte [] column : entry.getValue()) {
          if (--maxCols <= 0) {
            continue;
          }
          familyList.add(Bytes.toStringBinary(column));
        }
      }   
    }   
    map.put("totalColumns", colCount);
    return map;
  }

  //Row
  public int compareTo(Row other) {
    return Bytes.compareTo(this.getRow(), other.getRow());
  }
  
  //Writable
  public void readFields(final DataInput in)
  throws IOException {
    int version = in.readByte();
    if (version > GET_VERSION) {
      throw new IOException("unsupported version");
    }
    this.row = Bytes.readByteArray(in);
    this.lockId = in.readLong();
    this.maxVersions = in.readInt();
    boolean hasFilter = in.readBoolean();
    if (hasFilter) {
      this.filter = (Filter)createForName(Bytes.toString(Bytes.readByteArray(in)));
      this.filter.readFields(in);
    }
    this.cacheBlocks = in.readBoolean();
    this.tr = new TimeRange();
    tr.readFields(in);
    int numFamilies = in.readInt();
    this.familyMap =
      new TreeMap<byte [],NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);
    for(int i=0; i<numFamilies; i++) {
      byte [] family = Bytes.readByteArray(in);
      boolean hasColumns = in.readBoolean();
      NavigableSet<byte []> set = null;
      if(hasColumns) {
        int numColumns = in.readInt();
        set = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
        for(int j=0; j<numColumns; j++) {
          byte [] qualifier = Bytes.readByteArray(in);
          set.add(qualifier);
        }
      }
      this.familyMap.put(family, set);
    }
    readAttributes(in);
  }

  public void write(final DataOutput out)
  throws IOException {
    out.writeByte(GET_VERSION);
    Bytes.writeByteArray(out, this.row);
    out.writeLong(this.lockId);
    out.writeInt(this.maxVersions);
    if(this.filter == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      Bytes.writeByteArray(out, Bytes.toBytes(filter.getClass().getName()));
      filter.write(out);
    }
    out.writeBoolean(this.cacheBlocks);
    tr.write(out);
    out.writeInt(familyMap.size());
    for(Map.Entry<byte [], NavigableSet<byte []>> entry :
      familyMap.entrySet()) {
      Bytes.writeByteArray(out, entry.getKey());
      NavigableSet<byte []> columnSet = entry.getValue();
      if(columnSet == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        out.writeInt(columnSet.size());
        for(byte [] qualifier : columnSet) {
          Bytes.writeByteArray(out, qualifier);
        }
      }
    }
    writeAttributes(out);
  }

  @SuppressWarnings("unchecked")
  private Writable createForName(String className) {
    try {
      Class<? extends Writable> clazz =
        (Class<? extends Writable>) Class.forName(className);
      return WritableFactories.newInstance(clazz, new Configuration());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Can't find class " + className);
    }
  }

  /**
   * Adds an array of columns specified the old format, family:qualifier.
   * <p>
   * Overrides previous calls to addFamily for any families in the input.
   * @param columns array of columns, formatted as <pre>family:qualifier</pre>
   * @deprecated issue multiple {@link #addColumn(byte[], byte[])} instead
   * @return this for invocation chaining
   */
  @SuppressWarnings({"deprecation"})
  public Get addColumns(byte [][] columns) {
    if (columns == null) return this;
    for (byte[] column : columns) {
      try {
        addColumn(column);
      } catch (Exception ignored) {
      }
    }
    return this;
  }

  /**
   *
   * @param column Old format column.
   * @return This.
   * @deprecated use {@link #addColumn(byte[], byte[])} instead
   */
  public Get addColumn(final byte [] column) {
    if (column == null) return this;
    byte [][] split = KeyValue.parseColumn(column);
    if (split.length > 1 && split[1] != null && split[1].length > 0) {
      addColumn(split[0], split[1]);
    } else {
      addFamily(split[0]);
    }
    return this;
  }
}
