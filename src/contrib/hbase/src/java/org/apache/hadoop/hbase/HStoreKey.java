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

import org.apache.hadoop.io.*;

import java.io.*;

/**
 * A Key for a stored row
 */
public class HStoreKey implements WritableComparable {
  // TODO: Move these utility methods elsewhere (To a Column class?).
  /**
   * Extracts the column family name from a column
   * For example, returns 'info' if the specified column was 'info:server'
   * @param col name of column
   * @return column family name
   * @throws InvalidColumnNameException 
   */
  public static Text extractFamily(final Text col)
  throws InvalidColumnNameException {
    return extractFamily(col, false);
  }
  
  /**
   * Extracts the column family name from a column
   * For example, returns 'info' if the specified column was 'info:server'
   * @param col name of column
   * @param withColon if returned family name should include the ':' suffix.
   * @return column family name
   * @throws InvalidColumnNameException 
   */
  public static Text extractFamily(final Text col, final boolean withColon)
  throws InvalidColumnNameException {
    int offset = getColonOffset(col);
    // Include ':' in copy?
    offset += (withColon)? 1: 0;
    if (offset == col.getLength()) {
      return col;
    }
    byte [] buffer = new byte[offset];
    System.arraycopy(col.getBytes(), 0, buffer, 0, offset);
    return new Text(buffer);
  }
  
  /**
   * Extracts the column qualifier, the portion that follows the colon (':')
   * family/qualifier separator.
   * For example, returns 'server' if the specified column was 'info:server'
   * @param col name of column
   * @return column qualifier or null if there is no qualifier.
   * @throws InvalidColumnNameException 
   */
  public static Text extractQualifier(final Text col)
  throws InvalidColumnNameException {
    int offset = getColonOffset(col);
    if (offset + 1 == col.getLength()) {
      return null;
    }
    int bufferLength = col.getLength() - (offset + 1);
    byte [] buffer = new byte[bufferLength];
    System.arraycopy(col.getBytes(), offset + 1, buffer, 0, bufferLength);
    return new Text(buffer);
  }
  
  private static int getColonOffset(final Text col)
  throws InvalidColumnNameException {
    int offset = col.find(":");
    if(offset < 0) {
      throw new InvalidColumnNameException(col + " is missing the colon " +
        "family/qualifier separator");
    }
    return offset;
  }

  Text row;
  Text column;
  long timestamp;


  /** Default constructor used in conjunction with Writable interface */
  public HStoreKey() {
    this(new Text());
  }
  
  /**
   * Create an HStoreKey specifying only the row
   * The column defaults to the empty string and the time stamp defaults to
   * Long.MAX_VALUE
   * 
   * @param row - row key
   */
  public HStoreKey(Text row) {
    this(row, Long.MAX_VALUE);
  }
  
  /**
   * Create an HStoreKey specifying the row and timestamp
   * The column name defaults to the empty string
   * 
   * @param row row key
   * @param timestamp timestamp value
   */
  public HStoreKey(Text row, long timestamp) {
    this(row, new Text(), timestamp);
  }
  
  /**
   * Create an HStoreKey specifying the row and column names
   * The timestamp defaults to Long.MAX_VALUE
   * 
   * @param row row key
   * @param column column key
   */
  public HStoreKey(Text row, Text column) {
    this(row, column, Long.MAX_VALUE);
  }
  
  /**
   * Create an HStoreKey specifying all the fields
   * 
   * @param row row key
   * @param column column key
   * @param timestamp timestamp value
   */
  public HStoreKey(Text row, Text column, long timestamp) {
    this.row = new Text(row);
    this.column = new Text(column);
    this.timestamp = timestamp;
  }
  
  /** @return Approximate size in bytes of this key. */
  public long getSize() {
    return this.row.getLength() + this.column.getLength() +
      8 /* There is no sizeof in java. Presume long is 8 (64bit machine)*/;
  }
  
  /**
   * Constructs a new HStoreKey from another
   * 
   * @param other the source key
   */
  public HStoreKey(HStoreKey other) {
    this(other.row, other.column, other.timestamp);
  }
  
  /**
   * Change the value of the row key
   * 
   * @param newrow new row key value
   */
  public void setRow(Text newrow) {
    this.row.set(newrow);
  }
  
  /**
   * Change the value of the column key
   * 
   * @param newcol new column key value
   */
  public void setColumn(Text newcol) {
    this.column.set(newcol);
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
  public Text getRow() {
    return row;
  }
  
  /** @return value of column key */
  public Text getColumn() {
    return column;
  }
  
  /** @return value of timestamp */
  public long getTimestamp() {
    return timestamp;
  }
  
  /**
   * Compares the row and column of two keys
   * @param other Key to compare against. Compares row and column.
   * @return True if same row and column.
   * @see #matchesWithoutColumn(HStoreKey)
   * @see #matchesRowFamily(HStoreKey)
   */ 
  public boolean matchesRowCol(HStoreKey other) {
    return this.row.compareTo(other.row) == 0
      && this.column.compareTo(other.column) == 0;
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
    return this.row.compareTo(other.row) == 0
      && this.timestamp >= other.getTimestamp();
  }
  
  /**
   * Compares the row and column family of two keys
   * 
   * @param that Key to compare against. Compares row and column family
   * 
   * @return true if same row and column family
   * @throws InvalidColumnNameException 
   * @see #matchesRowCol(HStoreKey)
   * @see #matchesWithoutColumn(HStoreKey)
   */
  public boolean matchesRowFamily(HStoreKey that)
  throws InvalidColumnNameException {
    return this.row.compareTo(that.row) == 0 &&
      extractFamily(this.column).
        compareTo(extractFamily(that.getColumn())) == 0;
  }
  
  /** {@inheritDoc} */
  @Override
  public String toString() {
    return row.toString() + "/" + column.toString() + "/" + timestamp;
  }
  
  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    return compareTo(obj) == 0;
  }
  
  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    int result = this.row.hashCode();
    result ^= this.column.hashCode();
    result ^= Long.valueOf(this.timestamp).hashCode();
    return result;
  }

  // Comparable

  /** {@inheritDoc} */
  public int compareTo(Object o) {
    HStoreKey other = (HStoreKey) o;
    int result = this.row.compareTo(other.row);
    if(result == 0) {
      result = this.column.compareTo(other.column);
      if(result == 0) {
        if(this.timestamp < other.timestamp) {
          result = 1;
        } else if(this.timestamp > other.timestamp) {
          result = -1;
        }
      }
    }
    return result;
  }

  // Writable

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    row.write(out);
    column.write(out);
    out.writeLong(timestamp);
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    row.readFields(in);
    column.readFields(in);
    timestamp = in.readLong();
  }
}

