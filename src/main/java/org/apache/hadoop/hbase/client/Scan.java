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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.IncompatibleFilterException;
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
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Used to perform Scan operations.
 * <p>
 * All operations are identical to {@link Get} with the exception of
 * instantiation.  Rather than specifying a single row, an optional startRow
 * and stopRow may be defined.  If rows are not specified, the Scanner will
 * iterate over all rows.
 * <p>
 * To scan everything for each row, instantiate a Scan object.
 * <p>
 * To modify scanner caching for just this scan, use {@link #setCaching(int) setCaching}.
 * If caching is NOT set, we will use the caching value of the hosting
 * {@link HTable}.  See {@link HTable#setScannerCaching(int)}.
 * <p>
 * To further define the scope of what to get when scanning, perform additional
 * methods as outlined below.
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
 * To limit the maximum number of values returned for each call to next(),
 * execute {@link #setBatch(int) setBatch}.
 * <p>
 * To add a filter, execute {@link #setFilter(org.apache.hadoop.hbase.filter.Filter) setFilter}.
 * <p>
 * Expert: To explicitly disable server-side block caching for this scan,
 * execute {@link #setCacheBlocks(boolean)}.
 */
public class Scan extends OperationWithAttributes implements Writable {
  private static final byte SCAN_VERSION = (byte)2;
  private byte [] startRow = HConstants.EMPTY_START_ROW;
  private byte [] stopRow  = HConstants.EMPTY_END_ROW;
  private int maxVersions = 1;
  private int batch = -1;

  /*
   * -1 means no caching
   */
  private int caching = -1;
  private boolean cacheBlocks = true;
  private Filter filter = null;
  private TimeRange tr = new TimeRange();
  private Map<byte [], NavigableSet<byte []>> familyMap =
    new TreeMap<byte [], NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);

  /**
   * Create a Scan operation across all rows.
   */
  public Scan() {}

  public Scan(byte [] startRow, Filter filter) {
    this(startRow);
    this.filter = filter;
  }

  /**
   * Create a Scan operation starting at the specified row.
   * <p>
   * If the specified row does not exist, the Scanner will start from the
   * next closest row after the specified row.
   * @param startRow row to start scanner at or after
   */
  public Scan(byte [] startRow) {
    this.startRow = startRow;
  }

  /**
   * Create a Scan operation for the range of rows specified.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  public Scan(byte [] startRow, byte [] stopRow) {
    this.startRow = startRow;
    this.stopRow = stopRow;
  }

  /**
   * Creates a new instance of this class while copying all values.
   *
   * @param scan  The scan instance to copy from.
   * @throws IOException When copying the values fails.
   */
  public Scan(Scan scan) throws IOException {
    startRow = scan.getStartRow();
    stopRow  = scan.getStopRow();
    maxVersions = scan.getMaxVersions();
    batch = scan.getBatch();
    caching = scan.getCaching();
    cacheBlocks = scan.getCacheBlocks();
    filter = scan.getFilter(); // clone?
    TimeRange ctr = scan.getTimeRange();
    tr = new TimeRange(ctr.getMin(), ctr.getMax());
    Map<byte[], NavigableSet<byte[]>> fams = scan.getFamilyMap();
    for (Map.Entry<byte[],NavigableSet<byte[]>> entry : fams.entrySet()) {
      byte [] fam = entry.getKey();
      NavigableSet<byte[]> cols = entry.getValue();
      if (cols != null && cols.size() > 0) {
        for (byte[] col : cols) {
          addColumn(fam, col);
        }
      } else {
        addFamily(fam);
      }
    }
  }

  /**
   * Builds a scan object with the same specs as get.
   * @param get get to model scan after
   */
  public Scan(Get get) {
    this.startRow = get.getRow();
    this.stopRow = get.getRow();
    this.filter = get.getFilter();
    this.cacheBlocks = get.getCacheBlocks();
    this.maxVersions = get.getMaxVersions();
    this.tr = get.getTimeRange();
    this.familyMap = get.getFamilyMap();
  }

  public boolean isGetScan() {
    return this.startRow != null && this.startRow.length > 0 &&
      Bytes.equals(this.startRow, this.stopRow);
  }

  /**
   * Get all columns from the specified family.
   * <p>
   * Overrides previous calls to addColumn for this family.
   * @param family family name
   * @return this
   */
  public Scan addFamily(byte [] family) {
    familyMap.remove(family);
    familyMap.put(family, null);
    return this;
  }

  /**
   * Get the column from the specified family with the specified qualifier.
   * <p>
   * Overrides previous calls to addFamily for this family.
   * @param family family name
   * @param qualifier column qualifier
   * @return this
   */
  public Scan addColumn(byte [] family, byte [] qualifier) {
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
   * [minStamp, maxStamp).  Note, default maximum versions to return is 1.  If
   * your time range spans more than one version and you want all versions
   * returned, up the number of versions beyond the defaut.
   * @param minStamp minimum timestamp value, inclusive
   * @param maxStamp maximum timestamp value, exclusive
   * @throws IOException if invalid time range
   * @see #setMaxVersions()
   * @see #setMaxVersions(int)
   * @return this
   */
  public Scan setTimeRange(long minStamp, long maxStamp)
  throws IOException {
    tr = new TimeRange(minStamp, maxStamp);
    return this;
  }

  /**
   * Get versions of columns with the specified timestamp. Note, default maximum
   * versions to return is 1.  If your time range spans more than one version
   * and you want all versions returned, up the number of versions beyond the
   * defaut.
   * @param timestamp version timestamp
   * @see #setMaxVersions()
   * @see #setMaxVersions(int)
   * @return this
   */
  public Scan setTimeStamp(long timestamp) {
    try {
      tr = new TimeRange(timestamp, timestamp+1);
    } catch(IOException e) {
      // Will never happen
    }
    return this;
  }

  /**
   * Set the start row of the scan.
   * @param startRow row to start scan on, inclusive
   * @return this
   */
  public Scan setStartRow(byte [] startRow) {
    this.startRow = startRow;
    return this;
  }

  /**
   * Set the stop row.
   * @param stopRow row to end at (exclusive)
   * @return this
   */
  public Scan setStopRow(byte [] stopRow) {
    this.stopRow = stopRow;
    return this;
  }

  /**
   * Get all available versions.
   * @return this
   */
  public Scan setMaxVersions() {
    this.maxVersions = Integer.MAX_VALUE;
    return this;
  }

  /**
   * Get up to the specified number of versions of each column.
   * @param maxVersions maximum versions for each column
   * @return this
   */
  public Scan setMaxVersions(int maxVersions) {
    this.maxVersions = maxVersions;
    return this;
  }

  /**
   * Set the maximum number of values to return for each call to next()
   * @param batch the maximum number of values
   */
  public void setBatch(int batch) {
	if(this.hasFilter() && this.filter.hasFilterRow()) {
	  throw new IncompatibleFilterException(
        "Cannot set batch on a scan using a filter" +
        " that returns true for filter.hasFilterRow");
	}
    this.batch = batch;
  }

  /**
   * Set the number of rows for caching that will be passed to scanners.
   * If not set, the default setting from {@link HTable#getScannerCaching()} will apply.
   * Higher caching values will enable faster scanners but will use more memory.
   * @param caching the number of rows for caching
   */
  public void setCaching(int caching) {
    this.caching = caching;
  }

  /**
   * Apply the specified server-side filter when performing the Scan.
   * @param filter filter to run on the server
   * @return this
   */
  public Scan setFilter(Filter filter) {
    this.filter = filter;
    return this;
  }

  /**
   * Setting the familyMap
   * @param familyMap map of family to qualifier
   * @return this
   */
  public Scan setFamilyMap(Map<byte [], NavigableSet<byte []>> familyMap) {
    this.familyMap = familyMap;
    return this;
  }

  /**
   * Getting the familyMap
   * @return familyMap
   */
  public Map<byte [], NavigableSet<byte []>> getFamilyMap() {
    return this.familyMap;
  }

  /**
   * @return the number of families in familyMap
   */
  public int numFamilies() {
    if(hasFamilies()) {
      return this.familyMap.size();
    }
    return 0;
  }

  /**
   * @return true if familyMap is non empty, false otherwise
   */
  public boolean hasFamilies() {
    return !this.familyMap.isEmpty();
  }

  /**
   * @return the keys of the familyMap
   */
  public byte[][] getFamilies() {
    if(hasFamilies()) {
      return this.familyMap.keySet().toArray(new byte[0][0]);
    }
    return null;
  }

  /**
   * @return the startrow
   */
  public byte [] getStartRow() {
    return this.startRow;
  }

  /**
   * @return the stoprow
   */
  public byte [] getStopRow() {
    return this.stopRow;
  }

  /**
   * @return the max number of versions to fetch
   */
  public int getMaxVersions() {
    return this.maxVersions;
  }

  /**
   * @return maximum number of values to return for a single call to next()
   */
  public int getBatch() {
    return this.batch;
  }

  /**
   * @return caching the number of rows fetched when calling next on a scanner
   */
  public int getCaching() {
    return this.caching;
  }

  /**
   * @return TimeRange
   */
  public TimeRange getTimeRange() {
    return this.tr;
  }

  /**
   * @return RowFilter
   */
  public Filter getFilter() {
    return filter;
  }

  /**
   * @return true is a filter has been specified, false if not
   */
  public boolean hasFilter() {
    return filter != null;
  }

  /**
   * Set whether blocks should be cached for this Scan.
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
   * Get whether blocks should be cached for this Scan.
   * @return true if default caching should be used, false if blocks should not
   * be cached
   */
  public boolean getCacheBlocks() {
    return cacheBlocks;
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
    if(this.familyMap.size() == 0) {
      map.put("families", "ALL");
      return map;
    } else {
      map.put("families", families);
    }
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
    // start with the fingerpring map and build on top of it
    Map<String, Object> map = getFingerprint();
    // map from families to column list replaces fingerprint's list of families
    Map<String, List<String>> familyColumns =
      new HashMap<String, List<String>>();
    map.put("families", familyColumns);
    // add scalar information first
    map.put("startRow", Bytes.toStringBinary(this.startRow));
    map.put("stopRow", Bytes.toStringBinary(this.stopRow));
    map.put("maxVersions", this.maxVersions);
    map.put("batch", this.batch);
    map.put("caching", this.caching);
    map.put("cacheBlocks", this.cacheBlocks);
    List<Long> timeRange = new ArrayList<Long>();
    timeRange.add(this.tr.getMin());
    timeRange.add(this.tr.getMax());
    map.put("timeRange", timeRange);
    int colCount = 0;
    // iterate through affected families and list out up to maxCols columns
    for (Map.Entry<byte [], NavigableSet<byte[]>> entry :
      this.familyMap.entrySet()) {
      List<String> columns = new ArrayList<String>();
      familyColumns.put(Bytes.toStringBinary(entry.getKey()), columns);
      if(entry.getValue() == null) {
        colCount++;
        --maxCols;
        columns.add("ALL");
      } else {
        colCount += entry.getValue().size();
        if (maxCols <= 0) {
          continue;
        } 
        for (byte [] column : entry.getValue()) {
          if (--maxCols <= 0) {
            continue;
          }
          columns.add(Bytes.toStringBinary(column));
        }
      } 
    }       
    map.put("totalColumns", colCount);
    return map;
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

  //Writable
  public void readFields(final DataInput in)
  throws IOException {
    int version = in.readByte();
    if (version > (int)SCAN_VERSION) {
      throw new IOException("version not supported");
    }
    this.startRow = Bytes.readByteArray(in);
    this.stopRow = Bytes.readByteArray(in);
    this.maxVersions = in.readInt();
    this.batch = in.readInt();
    this.caching = in.readInt();
    this.cacheBlocks = in.readBoolean();
    if(in.readBoolean()) {
      this.filter = (Filter)createForName(Bytes.toString(Bytes.readByteArray(in)));
      this.filter.readFields(in);
    }
    this.tr = new TimeRange();
    tr.readFields(in);
    int numFamilies = in.readInt();
    this.familyMap =
      new TreeMap<byte [], NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);
    for(int i=0; i<numFamilies; i++) {
      byte [] family = Bytes.readByteArray(in);
      int numColumns = in.readInt();
      TreeSet<byte []> set = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
      for(int j=0; j<numColumns; j++) {
        byte [] qualifier = Bytes.readByteArray(in);
        set.add(qualifier);
      }
      this.familyMap.put(family, set);
    }

    if (version > 1) {
      readAttributes(in);
    }
  }

  public void write(final DataOutput out)
  throws IOException {
    out.writeByte(SCAN_VERSION);
    Bytes.writeByteArray(out, this.startRow);
    Bytes.writeByteArray(out, this.stopRow);
    out.writeInt(this.maxVersions);
    out.writeInt(this.batch);
    out.writeInt(this.caching);
    out.writeBoolean(this.cacheBlocks);
    if(this.filter == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      Bytes.writeByteArray(out, Bytes.toBytes(filter.getClass().getName()));
      filter.write(out);
    }
    tr.write(out);
    out.writeInt(familyMap.size());
    for(Map.Entry<byte [], NavigableSet<byte []>> entry : familyMap.entrySet()) {
      Bytes.writeByteArray(out, entry.getKey());
      NavigableSet<byte []> columnSet = entry.getValue();
      if(columnSet != null){
        out.writeInt(columnSet.size());
        for(byte [] qualifier : columnSet) {
          Bytes.writeByteArray(out, qualifier);
        }
      } else {
        out.writeInt(0);
      }
    }
    writeAttributes(out);
  }

   /**
   * Parses a combined family and qualifier and adds either both or just the
   * family in case there is not qualifier. This assumes the older colon
   * divided notation, e.g. "data:contents" or "meta:".
   * <p>
   * Note: It will through an error when the colon is missing.
   *
   * @param familyAndQualifier family and qualifier
   * @return A reference to this instance.
   * @throws IllegalArgumentException When the colon is missing.
   * @deprecated use {@link #addColumn(byte[], byte[])} instead
   */
  public Scan addColumn(byte[] familyAndQualifier) {
    byte [][] fq = KeyValue.parseColumn(familyAndQualifier);
    if (fq.length > 1 && fq[1] != null && fq[1].length > 0) {
      addColumn(fq[0], fq[1]);
    } else {
      addFamily(fq[0]);
    }
    return this;
  }

  /**
   * Adds an array of columns specified using old format, family:qualifier.
   * <p>
   * Overrides previous calls to addFamily for any families in the input.
   *
   * @param columns array of columns, formatted as <pre>family:qualifier</pre>
   * @deprecated issue multiple {@link #addColumn(byte[], byte[])} instead
   * @return this
   */
  public Scan addColumns(byte [][] columns) {
    for (byte[] column : columns) {
      addColumn(column);
    }
    return this;
  }

  /**
   * Convenience method to help parse old style (or rather user entry on the
   * command line) column definitions, e.g. "data:contents mime:". The columns
   * must be space delimited and always have a colon (":") to denote family
   * and qualifier.
   *
   * @param columns  The columns to parse.
   * @return A reference to this instance.
   * @deprecated use {@link #addColumn(byte[], byte[])} instead
   */
  public Scan addColumns(String columns) {
    String[] cols = columns.split(" ");
    for (String col : cols) {
      addColumn(Bytes.toBytes(col));
    }
    return this;
  }

  /**
   * Helps to convert the binary column families and qualifiers to a text
   * representation, e.g. "data:mimetype data:contents meta:". Binary values
   * are properly encoded using {@link Bytes#toBytesBinary(String)}.
   *
   * @return The columns in an old style string format.
   * @deprecated
   */
  public String getInputColumns() {
    StringBuilder cols = new StringBuilder("");
    for (Map.Entry<byte[], NavigableSet<byte[]>> e :
      familyMap.entrySet()) {
      byte[] fam = e.getKey();
      if (cols.length() > 0) cols.append(" ");
      NavigableSet<byte[]> quals = e.getValue();
      // check if this family has qualifiers
      if (quals != null && quals.size() > 0) {
        StringBuilder cs = new StringBuilder("");
        for (byte[] qual : quals) {
          if (cs.length() > 0) cs.append(" ");
          // encode values to make parsing easier later
          cs.append(Bytes.toStringBinary(fam)).append(":").append(Bytes.toStringBinary(qual));
        }
        cols.append(cs);
      } else {
        // only add the family but with old style delimiter
        cols.append(Bytes.toStringBinary(fam)).append(":");
      }
    }
    return cols.toString();
  }
}
