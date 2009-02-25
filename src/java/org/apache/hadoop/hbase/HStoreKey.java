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


import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

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

  /**
   * Estimated size tax paid for each instance of HSK.  Estimate based on
   * study of jhat and jprofiler numbers.
   */
  // In jprofiler, says shallow size is 48 bytes.  Add to it cost of two
  // byte arrays and then something for the HRI hosting.
  public static final int ESTIMATED_HEAP_TAX = 48;
  
  public static final StoreKeyByteComparator BYTECOMPARATOR =
    new StoreKeyByteComparator();

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
    this(Bytes.toBytes(row), Long.MAX_VALUE);
  }
 
  /**
   * Create an HStoreKey specifying the row and timestamp
   * The column and table names default to the empty string
   * 
   * @param row row key
   * @param timestamp timestamp value
   * @param hri HRegionInfo
   */
  public HStoreKey(final byte [] row, final long timestamp) {
    this(row, HConstants.EMPTY_BYTE_ARRAY, timestamp);
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
    this(row, column, HConstants.LATEST_TIMESTAMP);
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
   * Create an HStoreKey specifying all the fields
   * Does not make copies of the passed byte arrays. Presumes the passed 
   * arrays immutable.
   * @param row row key
   * @param column column key
   * @param timestamp timestamp value
   * @param regionInfo region info
   */
  public HStoreKey(final String row, final String column, final long timestamp) {
    this (Bytes.toBytes(row), Bytes.toBytes(column), timestamp);
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
  public HStoreKey(final byte [] row, final byte [] column, final long timestamp) {
    // Make copies
    this.row = row;
    this.column = column;
    this.timestamp = timestamp;
  }

  /**
   * Constructs a new HStoreKey from another
   * 
   * @param other the source key
   */
  public HStoreKey(final HStoreKey other) {
    this(other.getRow(), other.getColumn(), other.getTimestamp());
  }

  public HStoreKey(final ByteBuffer bb) {
    this(getRow(bb), getColumn(bb), getTimestamp(bb));
  }

  /**
   * Change the value of the row key
   * 
   * @param newrow new row key value
   */
  public void setRow(final byte [] newrow) {
    this.row = newrow;
  }
  
  /**
   * Change the value of the column in this key
   * 
   * @param c new column family value
   */
  public void setColumn(final byte [] c) {
    this.column = c;
  }

  /**
   * Change the value of the timestamp field
   * 
   * @param timestamp new timestamp value
   */
  public void setVersion(final long timestamp) {
    this.timestamp = timestamp;
  }
  
  /**
   * Set the value of this HStoreKey from the supplied key
   * 
   * @param k key value to copy
   */
  public void set(final HStoreKey k) {
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

  /**
   * Compares the row and column of two keys
   * @param other Key to compare against. Compares row and column.
   * @return True if same row and column.
   * @see #matchesWithoutColumn(HStoreKey)
   * @see #matchesRowFamily(HStoreKey)
   */ 
  public boolean matchesRowCol(final HStoreKey other) {
    return HStoreKey.equalsTwoRowKeys(getRow(), other.getRow()) &&
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
  public boolean matchesWithoutColumn(final HStoreKey other) {
    return equalsTwoRowKeys(getRow(), other.getRow()) &&
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
  public boolean matchesRowFamily(final HStoreKey that) {
    final int delimiterIndex = getFamilyDelimiterIndex(getColumn());
    return equalsTwoRowKeys(getRow(), that.getRow()) &&
      Bytes.compareTo(getColumn(), 0, delimiterIndex, that.getColumn(), 0,
        delimiterIndex) == 0;
  }
  
  @Override
  public String toString() {
    return Bytes.toString(this.row) + "/" + Bytes.toString(this.column) + "/" +
      timestamp;
  }
  
  @Override
  public boolean equals(final Object obj) {
    final HStoreKey other = (HStoreKey)obj;
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
    return compareTo(this, o);
  }
  static int compareTo(final HStoreKey left, final HStoreKey right) {
    // We can be passed null
    if (left == null && right == null) return 0;
    if (left == null) return -1;
    if (right == null) return 1;
    
    int result = compareTwoRowKeys(left.getRow(), right.getRow());
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
    return result;
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
    final int index = getFamilyDelimiterIndex(column);
    if (index <= 0) {
      throw new ColumnNameParseException("Missing ':' delimiter between " +
        "column family and qualifier in the passed column name <" +
        Bytes.toString(column) + ">");
    }
    final byte [] result = new byte[index];
    System.arraycopy(column, 0, result, 0, index);
    return result;
  }
  
  /**
   * @param column
   * @return Return hash of family portion of passed column.
   */
  public static Integer getFamilyMapKey(final byte [] column) {
    final int index = getFamilyDelimiterIndex(column);
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
    final int index = getFamilyDelimiterIndex(column);
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
    final byte [] familyPlusDelimiter = new byte [family.length + 1];
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
    final int index = getFamilyDelimiterIndex(column);
    final int len = column.length - (index + 1);
    final byte [] result = new byte[len];
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
    final byte [][] result = new byte [2][];
    final int index = getFamilyDelimiterIndex(c);
    if (index == -1) {
      throw new ColumnNameParseException("Impossible column name: " + c);
    }
    result[0] = new byte [index];
    System.arraycopy(c, 0, result[0], 0, index);
    final int len = c.length - (index + 1);
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
   * Utility method to compare two row keys.
   * This is required because of the meta delimiters.
   * This is a hack.
   * @param regionInfo
   * @param rowA
   * @param rowB
   * @return value of the comparison
   */
  public static int compareTwoRowKeys(final byte[] rowA, final byte[] rowB) {
    return Bytes.compareTo(rowA, rowB);
  }
  
  /**
   * Utility method to check if two row keys are equal.
   * This is required because of the meta delimiters
   * This is a hack
   * @param rowA
   * @param rowB
   * @return if it's equal
   */
  public static boolean equalsTwoRowKeys(final byte[] rowA, final byte[] rowB) {
    return ((rowA == null) && (rowB == null)) ? true:
      (rowA == null) || (rowB == null) || (rowA.length != rowB.length) ? false:
        compareTwoRowKeys(rowA,rowB) == 0;
  }

  // Writable

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.row);
    Bytes.writeByteArray(out, this.column);
    out.writeLong(timestamp);
  }

  public void readFields(final DataInput in) throws IOException {
    this.row = Bytes.readByteArray(in);
    this.column = Bytes.readByteArray(in);
    this.timestamp = in.readLong();
  }

  /**
   * @param hsk
   * @return Size of this key in serialized bytes.
   */
  public static int getSerializedSize(final HStoreKey hsk) {
    return getSerializedSize(hsk.getRow()) +
      getSerializedSize(hsk.getColumn()) +
      Bytes.SIZEOF_LONG;
  }

  /**
   * @param b
   * @return Length of buffer when its been serialized.
   */
  private static int getSerializedSize(final byte [] b) {
    return b == null? 1: b.length + WritableUtils.getVIntSize(b.length);
  }

  public long heapSize() {
    return getRow().length + Bytes.ESTIMATED_HEAP_TAX +
      getColumn().length + Bytes.ESTIMATED_HEAP_TAX +
      ESTIMATED_HEAP_TAX;
  }

  /**
   * @return The bytes of <code>hsk</code> gotten by running its 
   * {@link Writable#write(java.io.DataOutput)} method.
   * @throws IOException
   */
  public byte [] getBytes() throws IOException {
    return getBytes(this);
  }

  /**
   * Return serialize <code>hsk</code> bytes.
   * Note, this method's implementation has changed.  Used to just return
   * row and column.  This is a customized version of
   * {@link Writables#getBytes(Writable)}
   * @param hsk Instance
   * @return The bytes of <code>hsk</code> gotten by running its 
   * {@link Writable#write(java.io.DataOutput)} method.
   * @throws IOException
   */
  public static byte [] getBytes(final HStoreKey hsk) throws IOException {
    // TODO: Redo with system.arraycopy instead of DOS.
    if (hsk == null) {
      throw new IllegalArgumentException("Writable cannot be null");
    }
    final int serializedSize = getSerializedSize(hsk);
    final ByteArrayOutputStream byteStream = new ByteArrayOutputStream(serializedSize);
    DataOutputStream out = new DataOutputStream(byteStream);
    try {
      hsk.write(out);
      out.close();
      out = null;
      final byte [] serializedKey = byteStream.toByteArray();
      if (serializedKey.length != serializedSize) {
        // REMOVE THIS AFTER CONFIDENCE THAT OUR SIZING IS BEING DONE PROPERLY
        throw new AssertionError("Sizes do not agree " + serializedKey.length +
          ", " + serializedSize);
      }
      return serializedKey;
    } finally {
      if (out != null) {
        out.close();
      }
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
   * @deprecated
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
      final int result = this.beforeThisKey.compareTo(o);
      return result == 0? -1: result;
    }
    
    @Override
    public boolean equals(final Object obj) {
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
    public boolean matchesRowCol(final HStoreKey other) {
      return this.beforeThisKey.matchesRowCol(other);
    }

    @Override
    public boolean matchesRowFamily(final HStoreKey that) {
      return this.beforeThisKey.matchesRowFamily(that);
    }

    @Override
    public boolean matchesWithoutColumn(final HStoreKey other) {
      return this.beforeThisKey.matchesWithoutColumn(other);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
      this.beforeThisKey.readFields(in);
    }

    @Override
    public void set(final HStoreKey k) {
      this.beforeThisKey.set(k);
    }

    @Override
    public void setColumn(final byte[] c) {
      this.beforeThisKey.setColumn(c);
    }

    @Override
    public void setRow(final byte[] newrow) {
      this.beforeThisKey.setRow(newrow);
    }

    @Override
    public void setVersion(final long timestamp) {
      this.beforeThisKey.setVersion(timestamp);
    }

    @Override
    public String toString() {
      return this.beforeThisKey.toString();
    }

    @Override
    public void write(final DataOutput out) throws IOException {
      this.beforeThisKey.write(out);
    }
  }

  /**
   * Passed as comparator for memcache and for store files.  See HBASE-868.
   */
  public static class HStoreKeyWritableComparator extends WritableComparator {
    public HStoreKeyWritableComparator() {
      super(HStoreKey.class);
    }
    
    @SuppressWarnings("unchecked")
    public int compare(final WritableComparable left,
        final WritableComparable right) {
      return compareTo((HStoreKey)left, (HStoreKey)right);
    }
  }

  /**
   * @param bb ByteBuffer that contains serialized HStoreKey
   * @return Row
   */
  public static byte [] getRow(final ByteBuffer bb) {
    byte firstByte = bb.get(0);
    int vint = firstByte;
    int vintWidth = WritableUtils.decodeVIntSize(firstByte);
    if (vintWidth != 1) {
      vint = getBigVint(vintWidth, firstByte, bb.array(), bb.arrayOffset());
    }
    byte [] b = new byte [vint];
    System.arraycopy(bb.array(), bb.arrayOffset() + vintWidth, b, 0, vint);
    return b;
  }

  /**
   * @param bb ByteBuffer that contains serialized HStoreKey
   * @return Column
   */
  public static byte [] getColumn(final ByteBuffer bb) {
    byte firstByte = bb.get(0);
    int vint = firstByte;
    int vintWidth = WritableUtils.decodeVIntSize(firstByte);
    if (vintWidth != 1) {
      vint = getBigVint(vintWidth, firstByte, bb.array(), bb.arrayOffset());
    }
    // Skip over row.
    int offset = vint + vintWidth;
    firstByte = bb.get(offset);
    vint = firstByte;
    vintWidth = WritableUtils.decodeVIntSize(firstByte);
    if (vintWidth != 1) {
      vint = getBigVint(vintWidth, firstByte, bb.array(),
        bb.arrayOffset() + offset);
    }
    byte [] b = new byte [vint];
    System.arraycopy(bb.array(), bb.arrayOffset() + offset + vintWidth, b, 0,
      vint);
    return b;
  }

  /**
   * @param bb ByteBuffer that contains serialized HStoreKey
   * @return Timestamp
   */
  public static long getTimestamp(final ByteBuffer bb) {
    byte firstByte = bb.get(0);
    int vint = firstByte;
    int vintWidth = WritableUtils.decodeVIntSize(firstByte);
    if (vintWidth != 1) {
      vint = getBigVint(vintWidth, firstByte, bb.array(), bb.arrayOffset());
    }
    // Skip over row.
    int offset = vint + vintWidth;
    firstByte = bb.get(offset);
    vint = firstByte;
    vintWidth = WritableUtils.decodeVIntSize(firstByte);
    if (vintWidth != 1) {
      vint = getBigVint(vintWidth, firstByte, bb.array(),
        bb.arrayOffset() + offset);
    }
    // Skip over column
    offset += (vint + vintWidth);
    return bb.getLong(offset);
  }

  /**
   * RawComparator for plain -- i.e. non-catalog table keys such as 
   * -ROOT- and .META. -- HStoreKeys.  Compares at byte level.
   */
  public static class StoreKeyByteComparator implements RawComparator<byte []> {
    public StoreKeyByteComparator() {
      super();
    }

    public int compare(final byte[] b1, final byte[] b2) {
      return compare(b1, 0, b1.length, b2, 0, b2.length);
    }

    public int compare(final byte [] b1, int o1, int l1,
        final byte [] b2, int o2, int l2) {
      // Below is byte compare without creating new objects.  Its awkward but
      // seems no way around getting vint width, value, and compare result any
      // other way. The passed byte arrays, b1 and b2, have a vint, row, vint,
      // column, timestamp in them.  The byte array was written by the
      // #write(DataOutputStream) method above. See it to better understand the
      // below.

      // Calculate vint and vint width for rows in b1 and b2.
      byte firstByte1 = b1[o1];
      int vint1 = firstByte1;
      int vintWidth1 = WritableUtils.decodeVIntSize(firstByte1);
      if (vintWidth1 != 1) {
        vint1 = getBigVint(vintWidth1, firstByte1, b1, o1);
      }
      byte firstByte2 = b2[o2];
      int vint2 = firstByte2;
      int vintWidth2 = WritableUtils.decodeVIntSize(firstByte2);
      if (vintWidth2 != 1) {
        vint2 = getBigVint(vintWidth2, firstByte2, b2, o2);
      }
      // Compare the rows.
      int result = WritableComparator.compareBytes(b1, o1 + vintWidth1, vint1,
          b2, o2 + vintWidth2, vint2);
      if (result != 0) {
        return result;
      }

      // Update offsets and lengths so we are aligned on columns.
      int diff1 = vintWidth1 + vint1;
      o1 += diff1;
      l1 -= diff1;
      int diff2 = vintWidth2 + vint2;
      o2 += diff2;
      l2 -= diff2;
      // Calculate vint and vint width for columns in b1 and b2.
      firstByte1 = b1[o1];
      vint1 = firstByte1;
      vintWidth1 = WritableUtils.decodeVIntSize(firstByte1);
      if (vintWidth1 != 1) {
        vint1 = getBigVint(vintWidth1, firstByte1, b1, o1);
      }
      firstByte2 = b2[o2];
      vint2 = firstByte2;
      vintWidth2 = WritableUtils.decodeVIntSize(firstByte2);
      if (vintWidth2 != 1) {
        vint2 = getBigVint(vintWidth2, firstByte2, b2, o2);
      }
      // Compare columns.
      result = WritableComparator.compareBytes(b1, o1 + vintWidth1, vint1,
          b2, o2 + vintWidth2, vint2);
      if (result != 0) {
        return result;
      }

      // Update offsets and lengths.
      diff1 = vintWidth1 + vint1;
      o1 += diff1;
      l1 -= diff1;
      diff2 = vintWidth2 + vint2;
      o2 += diff2;
      l2 -= diff2;
      // The below older timestamps sorting ahead of newer timestamps looks
      // wrong but it is intentional. This way, newer timestamps are first
      // found when we iterate over a memcache and newer versions are the
      // first we trip over when reading from a store file.
      for (int i = 0; i < l1; i++) {
        int leftb = b1[o1 + i] & 0xff;
        int rightb = b2[o2 + i] & 0xff;
        if (leftb < rightb) {
          return 1;
        } else if (leftb > rightb) {
          return -1;
        }
      }
      return 0;
    }
  }

  /*
   * Vint is wider than one byte.  Find out how much bigger it is.
   * @param vintWidth
   * @param firstByte
   * @param buffer
   * @param offset
   * @return
   */
  static int getBigVint(final int vintWidth, final byte firstByte,
      final byte [] buffer, final int offset) {
    long i = 0;
    for (int idx = 0; idx < vintWidth - 1; idx++) {
      final byte b = buffer[offset + 1 + idx];
      i = i << 8;
      i = i | (b & 0xFF);
    }
    i = (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
    if (i > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Calculated vint too large");
    }
    return (int)i;
  }

  /**
   * Create a store key.
   * @param bb
   * @return HStoreKey instance made of the passed <code>b</code>.
   * @throws IOException
   */
  public static HStoreKey create(final ByteBuffer bb)
  throws IOException {
    byte firstByte = bb.get(0);
    int vint = firstByte;
    int vintWidth = WritableUtils.decodeVIntSize(firstByte);
    if (vintWidth != 1) {
      vint = getBigVint(vintWidth, firstByte, bb.array(), bb.arrayOffset());
    }
    byte [] row = new byte [vint];
    System.arraycopy(bb.array(), bb.arrayOffset() + vintWidth,
      row, 0, row.length);
    // Skip over row.
    int offset = vint + vintWidth;
    firstByte = bb.get(offset);
    vint = firstByte;
    vintWidth = WritableUtils.decodeVIntSize(firstByte);
    if (vintWidth != 1) {
      vint = getBigVint(vintWidth, firstByte, bb.array(),
        bb.arrayOffset() + offset);
    }
    byte [] column = new byte [vint];
    System.arraycopy(bb.array(), bb.arrayOffset() + offset + vintWidth,
      column, 0, column.length);
    // Skip over column
    offset += (vint + vintWidth);
    long ts = bb.getLong(offset);
    return new HStoreKey(row, column, ts);
  }

  /**
   * Create a store key.
   * @param b Serialized HStoreKey; a byte array with a row only in it won't do.
   * It must have all the vints denoting r/c/ts lengths.
   * @return HStoreKey instance made of the passed <code>b</code>.
   * @throws IOException
   */
  public static HStoreKey create(final byte [] b) throws IOException {
    return create(b, 0, b.length);
  }

  /**
   * Create a store key.
   * @param b Serialized HStoreKey
   * @param offset
   * @param length
   * @return HStoreKey instance made of the passed <code>b</code>.
   * @throws IOException
   */
  public static HStoreKey create(final byte [] b, final int offset,
    final int length)
  throws IOException {
    return (HStoreKey)Writables.getWritable(b, offset, length, new HStoreKey());
  }
}