/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A Key for a stored row.
 */
public class HStoreKey implements WritableComparable<HStoreKey>, HeapSize {
  /**
   * Colon character in UTF-8
   */
  public static final char COLUMN_FAMILY_DELIMITER = ':';
  
  private byte [] row = HConstants.EMPTY_BYTE_ARRAY;
  private byte [] column = HConstants.EMPTY_BYTE_ARRAY;
  private long timestamp = Long.MAX_VALUE;

  /*
   * regionInfo is only used as a hack to compare HSKs.
   * It is not serialized.  See https://issues.apache.org/jira/browse/HBASE-832
   */
  private HRegionInfo regionInfo = null;
  
  /**
   * Estimated size tax paid for each instance of HSK.  Estimate based on
   * study of jhat and jprofiler numbers.
   */
  // In jprofiler, says shallow size is 48 bytes.  Add to it cost of two
  // byte arrays and then something for the HRI hosting.
  public static final int ESTIMATED_HEAP_TAX = 48;

  /** Default constructor used in conjunction with Writable interface */
  public HStoreKey() {
    super();
  }
  
  /**
   * Create an HStoreKey specifying only the row
   * The column defaults to the empty string, the time stamp defaults to
   * Long.MAX_VALUE and the table defaults to empty string
   * 
   * @param row - row key
   */
  public HStoreKey(final byte [] row) {
    this(row, Long.MAX_VALUE);
  }

  /**
   * Create an HStoreKey specifying only the row
   * The column defaults to the empty string, the time stamp defaults to
   * Long.MAX_VALUE and the table defaults to empty string
   * 
   * @param row - row key
   */
  public HStoreKey(final String row) {
    this(row, Long.MAX_VALUE);
  }

  /**
   * Create an HStoreKey specifying the row and timestamp
   * The column and table names default to the empty string
   * 
   * @param row row key
   * @param hri
   */
  public HStoreKey(final byte [] row, final HRegionInfo hri) {
    this(row, HConstants.EMPTY_BYTE_ARRAY, hri);
  }
 
  /**
   * Create an HStoreKey specifying the row and timestamp
   * The column and table names default to the empty string
   * 
   * @param row row key
   * @param timestamp timestamp value
   * @param hri HRegionInfo
   */
  public HStoreKey(final byte [] row, long timestamp, final HRegionInfo hri) {
    this(row, HConstants.EMPTY_BYTE_ARRAY, timestamp, hri);
  }

  /**
   * Create an HStoreKey specifying the row and timestamp
   * The column and table names default to the empty string
   * 
   * @param row row key
   * @param timestamp timestamp value
   */
  public HStoreKey(final byte [] row, long timestamp) {
    this(row, HConstants.EMPTY_BYTE_ARRAY, timestamp);
  }

  /**
   * Create an HStoreKey specifying the row and timestamp
   * The column and table names default to the empty string
   * 
   * @param row row key
   * @param timestamp timestamp value
   */
  public HStoreKey(final String row, long timestamp) {
    this (row, "", timestamp, new HRegionInfo());
  }

  /**
   * Create an HStoreKey specifying the row and column names
   * The timestamp defaults to LATEST_TIMESTAMP
   * and table name defaults to the empty string
   * 
   * @param row row key
   * @param column column key
   */
  public HStoreKey(final String row, final String column) {
    this(row, column, HConstants.LATEST_TIMESTAMP, new HRegionInfo());
  }

  /**
   * Create an HStoreKey specifying the row and column names
   * The timestamp defaults to LATEST_TIMESTAMP
   * and table name defaults to the empty string
   * 
   * @param row row key
   * @param column column key
   */
  public HStoreKey(final byte [] row, final byte [] column) {
    this(row, column, HConstants.LATEST_TIMESTAMP);
  }
  
  /**
   * Create an HStoreKey specifying the row, column names and table name
   * The timestamp defaults to LATEST_TIMESTAMP
   * 
   * @param row row key
   * @param column column key
   * @param regionInfo region info
   */
  public HStoreKey(final byte [] row, 
      final byte [] column, final HRegionInfo regionInfo) {
    this(row, column, HConstants.LATEST_TIMESTAMP, regionInfo);
  }

  /**
   * Create an HStoreKey specifying all the fields
   * Does not make copies of the passed byte arrays. Presumes the passed 
   * arrays immutable.
   * @param row row key
   * @param column column key
   * @param timestamp timestamp value
   * @param regionInfo region info
   */
  public HStoreKey(final String row, 
      final String column, long timestamp, final HRegionInfo regionInfo) {
    this (Bytes.toBytes(row), Bytes.toBytes(column), 
        timestamp, regionInfo);
  }

  /**
   * Create an HStoreKey specifying all the fields with unspecified table
   * Does not make copies of the passed byte arrays. Presumes the passed 
   * arrays immutable.
   * @param row row key
   * @param column column key
   * @param timestamp timestamp value
   */
  public HStoreKey(final byte [] row, final byte [] column, long timestamp) {
    this(row, column, timestamp, null);
  }
  
  /**
   * Create an HStoreKey specifying all the fields with specified table
   * Does not make copies of the passed byte arrays. Presumes the passed 
   * arrays immutable.
   * @param row row key
   * @param column column key
   * @param timestamp timestamp value
   * @param regionInfo region info
   */
  public HStoreKey(final byte [] row, 
      final byte [] column, long timestamp, final HRegionInfo regionInfo) {
    // Make copies
    this.row = row;
    this.column = column;
    this.timestamp = timestamp;
    this.regionInfo = regionInfo;
  }

  /**
   * Constructs a new HStoreKey from another
   * 
   * @param other the source key
   */
  public HStoreKey(HStoreKey other) {
    this(other.getRow(), other.getColumn(), other.getTimestamp(),
      other.getHRegionInfo());
  }
  
  /**
   * Change the value of the row key
   * 
   * @param newrow new row key value
   */
  public void setRow(byte [] newrow) {
    this.row = newrow;
  }
  
  /**
   * Change the value of the column in this key
   * 
   * @param c new column family value
   */
  public void setColumn(byte [] c) {
    this.column = c;
  }

  /**
   * Change the value of the timestamp field
   * 
   * @param timestamp new timestamp value
   */
  public void setVersion(long timestamp) {
    this.timestamp = timestamp;
  }
  
  /**
   * Set the value of this HStoreKey from the supplied key
   * 
   * @param k key value to copy
   */
  public void set(HStoreKey k) {
    this.row = k.getRow();
    this.column = k.getColumn();
    this.timestamp = k.getTimestamp();
  }
  
  /** @return value of row key */
  public byte [] getRow() {
    return row;
  }
  
  /** @return value of column */
  public byte [] getColumn() {
    return this.column;
  }

  /** @return value of timestamp */
  public long getTimestamp() {
    return this.timestamp;
  }
  
  /** @return value of regioninfo */
  public HRegionInfo getHRegionInfo() {
    return this.regionInfo;
  }
  
  /**
   * @param hri
   */
  public void setHRegionInfo(final HRegionInfo hri) {
    this.regionInfo = hri;
  }
  
  /**
   * Compares the row and column of two keys
   * @param other Key to compare against. Compares row and column.
   * @return True if same row and column.
   * @see #matchesWithoutColumn(HStoreKey)
   * @see #matchesRowFamily(HStoreKey)
   */ 
  public boolean matchesRowCol(HStoreKey other) {
    return HStoreKey.equalsTwoRowKeys(getHRegionInfo(), getRow(), other.getRow()) &&
      Bytes.equals(getColumn(), other.getColumn());
  }
  
  /**
   * Compares the row and timestamp of two keys
   * 
   * @param other Key to copmare against. Compares row and timestamp.
   * 
   * @return True if same row and timestamp is greater than <code>other</code>
   * @see #matchesRowCol(HStoreKey)
   * @see #matchesRowFamily(HStoreKey)
   */
  public boolean matchesWithoutColumn(HStoreKey other) {
    return equalsTwoRowKeys(getHRegionInfo(), getRow(), other.getRow()) &&
      getTimestamp() >= other.getTimestamp();
  }
  
  /**
   * Compares the row and column family of two keys
   * 
   * @param that Key to compare against. Compares row and column family
   * 
   * @return true if same row and column family
   * @see #matchesRowCol(HStoreKey)
   * @see #matchesWithoutColumn(HStoreKey)
   */
  public boolean matchesRowFamily(HStoreKey that) {
    int delimiterIndex = getFamilyDelimiterIndex(getColumn());
    return equalsTwoRowKeys(getHRegionInfo(), getRow(), that.getRow()) &&
      Bytes.compareTo(getColumn(), 0, delimiterIndex, that.getColumn(), 0,
        delimiterIndex) == 0;
  }
  
  @Override
  public String toString() {
    return Bytes.toString(this.row) + "/" + Bytes.toString(this.column) + "/" +
      timestamp;
  }
  
  @Override
  public boolean equals(Object obj) {
    HStoreKey other = (HStoreKey)obj;
    // Do a quick check.
    if (this.row.length != other.row.length ||
        this.column.length != other.column.length ||
        this.timestamp != other.timestamp) {
      return false;
    }
    return compareTo(other) == 0;
  }
  
  @Override
  public int hashCode() {
    int result = Bytes.hashCode(getRow());
    result ^= Bytes.hashCode(getColumn());
    result ^= getTimestamp();
    return result;
  }

  // Comparable

  public int compareTo(final HStoreKey o) {
    return compareTo(this.regionInfo, this, o);
  }
  
  static int compareTo(final HRegionInfo hri, final HStoreKey left,
      final HStoreKey right) {
    // We can be passed null
    if (left == null && right == null) return 0;
    if (left == null) return -1;
    if (right == null) return 1;
    
    int result = compareTwoRowKeys(hri, left.getRow(), right.getRow());
    if (result != 0) {
      return result;
    }
    result = left.getColumn() == null && right.getColumn() == null? 0:
      left.getColumn() == null && right.getColumn() != null? -1:
        left.getColumn() != null && right.getColumn() == null? 1:
      Bytes.compareTo(left.getColumn(), right.getColumn());
    if (result != 0) {
      return result;
    }
    // The below older timestamps sorting ahead of newer timestamps looks
    // wrong but it is intentional. This way, newer timestamps are first
    // found when we iterate over a memcache and newer versions are the
    // first we trip over when reading from a store file.
    if (left.getTimestamp() < right.getTimestamp()) {
      result = 1;
    } else if (left.getTimestamp() > right.getTimestamp()) {
      result = -1;
    }
    // Because of HBASE-877, our BeforeThisStoreKey trick no longer works in
    // mapfiles and so instead we need to do this weird check here below.
    return result == 0 && left instanceof BeforeThisStoreKey? -1:
      result == 0 && right instanceof BeforeThisStoreKey? 1:
      result;
  }

  /**
   * @param column
   * @return New byte array that holds <code>column</code> family prefix only
   * (Does not include the colon DELIMITER).
   * @throws ColumnNameParseException 
   * @see #parseColumn(byte[])
   */
  public static byte [] getFamily(final byte [] column)
  throws ColumnNameParseException {
    int index = getFamilyDelimiterIndex(column);
    if (index <= 0) {
      throw new ColumnNameParseException("Missing ':' delimiter between " +
        "column family and qualifier in the passed column name <" +
        Bytes.toString(column) + ">");
    }
    byte [] result = new byte[index];
    System.arraycopy(column, 0, result, 0, index);
    return result;
  }
  
  /**
   * @param column
   * @return Return hash of family portion of passed column.
   */
  public static Integer getFamilyMapKey(final byte [] column) {
    int index = getFamilyDelimiterIndex(column);
    // If index < -1, presume passed column is a family name absent colon
    // delimiter
    return Bytes.mapKey(column, index > 0? index: column.length);
  }
  
  /**
   * @param family
   * @param column
   * @return True if <code>column</code> has a family of <code>family</code>.
   */
  public static boolean matchingFamily(final byte [] family,
      final byte [] column) {
    // Make sure index of the ':' is at same offset.
    int index = getFamilyDelimiterIndex(column);
    if (index != family.length) {
      return false;
    }
    return Bytes.compareTo(family, 0, index, column, 0, index) == 0;
  }
  
  /**
   * @param family
   * @return Return <code>family</code> plus the family delimiter.
   */
  public static byte [] addDelimiter(final byte [] family) {
    // Manufacture key by adding delimiter to the passed in colFamily.
    byte [] familyPlusDelimiter = new byte [family.length + 1];
    System.arraycopy(family, 0, familyPlusDelimiter, 0, family.length);
    familyPlusDelimiter[family.length] = HStoreKey.COLUMN_FAMILY_DELIMITER;
    return familyPlusDelimiter;
  }

  /**
   * @param column
   * @return New byte array that holds <code>column</code> qualifier suffix.
   * @see #parseColumn(byte[])
   */
  public static byte [] getQualifier(final byte [] column) {
    int index = getFamilyDelimiterIndex(column);
    int len = column.length - (index + 1);
    byte [] result = new byte[len];
    System.arraycopy(column, index + 1, result, 0, len);
    return result;
  }

  /**
   * @param c Column name
   * @return Return array of size two whose first element has the family
   * prefix of passed column <code>c</code> and whose second element is the
   * column qualifier.
   * @throws ColumnNameParseException 
   */
  public static byte [][] parseColumn(final byte [] c)
  throws ColumnNameParseException {
    byte [][] result = new byte [2][];
    int index = getFamilyDelimiterIndex(c);
    if (index == -1) {
      throw new ColumnNameParseException("Impossible column name: " + c);
    }
    result[0] = new byte [index];
    System.arraycopy(c, 0, result[0], 0, index);
    int len = c.length - (index + 1);
    result[1] = new byte[len];
    System.arraycopy(c, index + 1 /*Skip delimiter*/, result[1], 0,
      len);
    return result;
  }
  
  /**
   * @param b
   * @return Index of the family-qualifier colon delimiter character in passed
   * buffer.
   */
  public static int getFamilyDelimiterIndex(final byte [] b) {
    if (b == null) {
      throw new NullPointerException();
    }
    int result = -1;
    for (int i = 0; i < b.length; i++) {
      if (b[i] == COLUMN_FAMILY_DELIMITER) {
        result = i;
        break;
      }
    }
    return result;
  }

  /**
   * Returns row and column bytes out of an HStoreKey.
   * @param hsk Store key.
   * @return byte array encoding of HStoreKey
   */
  public static byte[] getBytes(final HStoreKey hsk) {
    return Bytes.add(hsk.getRow(), hsk.getColumn());
  }
  
  /**
   * Utility method to compare two row keys.
   * This is required because of the meta delimiters.
   * This is a hack.
   * @param regionInfo
   * @param rowA
   * @param rowB
   * @return value of the comparison
   */
  public static int compareTwoRowKeys(HRegionInfo regionInfo, 
      byte[] rowA, byte[] rowB) {
    if (regionInfo != null && regionInfo.isMetaRegion()) {
      byte[][] keysA = stripStartKeyMeta(rowA);
      byte[][] KeysB = stripStartKeyMeta(rowB);
      int rowCompare = Bytes.compareTo(keysA[0], KeysB[0]);
      if(rowCompare == 0)
        rowCompare = Bytes.compareTo(keysA[1], KeysB[1]);
      return rowCompare;
    }
    return Bytes.compareTo(rowA, rowB);
  }
  
  /**
   * Utility method to check if two row keys are equal.
   * This is required because of the meta delimiters
   * This is a hack
   * @param regionInfo
   * @param rowA
   * @param rowB
   * @return if it's equal
   */
  public static boolean equalsTwoRowKeys(HRegionInfo regionInfo, 
      byte[] rowA, byte[] rowB) {
    return ((rowA == null) && (rowB == null)) ? true:
      (rowA == null) || (rowB == null) || (rowA.length != rowB.length) ? false:
        compareTwoRowKeys(regionInfo,rowA,rowB) == 0;
  }
  
  private static byte[][] stripStartKeyMeta(byte[] rowKey) {
    int offset = -1;
    for (int i = rowKey.length - 1; i > 0; i--) {
      if (rowKey[i] == HConstants.META_ROW_DELIMITER) {
        offset = i;
        break;
      }
    }
    byte [] row = rowKey;
    byte [] timestamp = HConstants.EMPTY_BYTE_ARRAY;
    if (offset != -1) {
      row = new byte[offset];
      System.arraycopy(rowKey, 0, row, 0, offset);
      timestamp = new byte[rowKey.length - offset - 1];
      System.arraycopy(rowKey, offset+1, timestamp, 0,rowKey.length - offset - 1);
    }
    byte[][] elements = new byte[2][];
    elements[0] = row;
    elements[1] = timestamp;
    return elements;
  }
  
  // Writable

  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.row);
    Bytes.writeByteArray(out, this.column);
    out.writeLong(timestamp);
  }

  public void readFields(DataInput in) throws IOException {
    this.row = Bytes.readByteArray(in);
    this.column = Bytes.readByteArray(in);
    this.timestamp = in.readLong();
  }

  public long heapSize() {
    return getRow().length + Bytes.ESTIMATED_HEAP_TAX +
      getColumn().length + Bytes.ESTIMATED_HEAP_TAX +
      ESTIMATED_HEAP_TAX;
  }

  /**
   * Passed as comparator for memcache and for store files.  See HBASE-868.
   */
  public static class HStoreKeyWritableComparator extends WritableComparator {
    private final HRegionInfo hri;
    
    /** @param hri */
    public HStoreKeyWritableComparator(final HRegionInfo hri) {
      super(HStoreKey.class);
      this.hri = hri;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public int compare(final WritableComparable left,
        final WritableComparable right) {
      return compareTo(this.hri, (HStoreKey)left, (HStoreKey)right);
    }
  }
  
  /**
   * Pass this class into {@link org.apache.hadoop.io.MapFile}.getClosest when
   * searching for the key that comes BEFORE this one but NOT this one.  This
   * class will return > 0 when asked to compare against itself rather than 0.
   * This is a hack for case where getClosest returns a deleted key and we want
   * to get the previous.  Can't unless use use this class; it'll just keep
   * returning us the deleted key (getClosest gets exact or nearest before when
   * you pass true argument).  TODO: Throw this class away when MapFile has
   * a real 'previous' method.  See HBASE-751.
   */
  public static class BeforeThisStoreKey extends HStoreKey {
    private final HStoreKey beforeThisKey;

    /**
     * @param beforeThisKey 
     */
    public BeforeThisStoreKey(final HStoreKey beforeThisKey) {
      super();
      this.beforeThisKey = beforeThisKey;
    }
    
    @Override
    public int compareTo(final HStoreKey o) {
      int result = this.beforeThisKey.compareTo(o);
      return result == 0? -1: result;
    }
    
    @Override
    public boolean equals(Object obj) {
      return false;
    }

    @Override
    public byte[] getColumn() {
      return this.beforeThisKey.getColumn();
    }

    @Override
    public byte[] getRow() {
      return this.beforeThisKey.getRow();
    }

    @Override
    public long heapSize() {
      return this.beforeThisKey.heapSize();
    }

    @Override
    public long getTimestamp() {
      return this.beforeThisKey.getTimestamp();
    }

    @Override
    public int hashCode() {
      return this.beforeThisKey.hashCode();
    }

    @Override
    public boolean matchesRowCol(HStoreKey other) {
      return this.beforeThisKey.matchesRowCol(other);
    }

    @Override
    public boolean matchesRowFamily(HStoreKey that) {
      return this.beforeThisKey.matchesRowFamily(that);
    }

    @Override
    public boolean matchesWithoutColumn(HStoreKey other) {
      return this.beforeThisKey.matchesWithoutColumn(other);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.beforeThisKey.readFields(in);
    }

    @Override
    public void set(HStoreKey k) {
      this.beforeThisKey.set(k);
    }

    @Override
    public void setColumn(byte[] c) {
      this.beforeThisKey.setColumn(c);
    }

    @Override
    public void setRow(byte[] newrow) {
      this.beforeThisKey.setRow(newrow);
    }

    @Override
    public void setVersion(long timestamp) {
      this.beforeThisKey.setVersion(timestamp);
    }

    @Override
    public String toString() {
      return this.beforeThisKey.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      this.beforeThisKey.write(out);
    }
    
    @Override
    public HRegionInfo getHRegionInfo() {
      return this.beforeThisKey.getHRegionInfo();
    }
    
    @Override
    public void setHRegionInfo(final HRegionInfo hri) {
      this.beforeThisKey.setHRegionInfo(hri);
    }
  }
}