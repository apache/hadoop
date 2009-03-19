/**
 * Copyright 2008 The Apache Software Foundation
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
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import junit.framework.TestCase;

/**
 * Test that individual classes can migrate themselves.
 */
public class TestClassMigration extends TestCase {
  
  /**
   * Test we can migrate a 0.1 version of HSK.
   * @throws Exception
   */
  public void testMigrateHStoreKey() throws Exception {
    long now = System.currentTimeMillis();
    byte [] nameBytes = Bytes.toBytes(getName());
    Text nameText = new Text(nameBytes);
    HStoreKey01Branch  hsk = new HStoreKey01Branch(nameText, nameText, now);
    byte [] b = Writables.getBytes(hsk);
    HStoreKey deserializedHsk =
      (HStoreKey)Writables.getWritable(b, new HStoreKey());
    assertEquals(deserializedHsk.getTimestamp(), hsk.getTimestamp());
    assertTrue(Bytes.equals(nameBytes, deserializedHsk.getColumn()));
    assertTrue(Bytes.equals(nameBytes, deserializedHsk.getRow()));
  }
  
  /**
   * HBase 0.1 branch HStoreKey.  Same in all regards except the utility
   * methods have been removed.
   * Used in test of HSK migration test.
   */
  private static class HStoreKey01Branch implements WritableComparable {
    /**
     * Colon character in UTF-8
     */
    public static final char COLUMN_FAMILY_DELIMITER = ':';
    
    private Text row;
    private Text column;
    private long timestamp;


    /** Default constructor used in conjunction with Writable interface */
    public HStoreKey01Branch() {
      this(new Text());
    }
    
    /**
     * Create an HStoreKey specifying only the row
     * The column defaults to the empty string and the time stamp defaults to
     * Long.MAX_VALUE
     * 
     * @param row - row key
     */
    public HStoreKey01Branch(Text row) {
      this(row, Long.MAX_VALUE);
    }
    
    /**
     * Create an HStoreKey specifying the row and timestamp
     * The column name defaults to the empty string
     * 
     * @param row row key
     * @param timestamp timestamp value
     */
    public HStoreKey01Branch(Text row, long timestamp) {
      this(row, new Text(), timestamp);
    }
    
    /**
     * Create an HStoreKey specifying the row and column names
     * The timestamp defaults to LATEST_TIMESTAMP
     * 
     * @param row row key
     * @param column column key
     */
    public HStoreKey01Branch(Text row, Text column) {
      this(row, column, HConstants.LATEST_TIMESTAMP);
    }
    
    /**
     * Create an HStoreKey specifying all the fields
     * 
     * @param row row key
     * @param column column key
     * @param timestamp timestamp value
     */
    public HStoreKey01Branch(Text row, Text column, long timestamp) {
      // Make copies by doing 'new Text(arg)'.
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
    public HStoreKey01Branch(HStoreKey01Branch other) {
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
    public void set(HStoreKey01Branch k) {
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

    @Override
    public String toString() {
      return row.toString() + "/" + column.toString() + "/" + timestamp;
    }
    
    @Override
    public boolean equals(Object obj) {
      return compareTo(obj) == 0;
    }
    
    @Override
    public int hashCode() {
      int result = this.row.hashCode();
      result ^= this.column.hashCode();
      result ^= this.timestamp;
      return result;
    }

    // Comparable

    public int compareTo(Object o) {
      HStoreKey01Branch other = (HStoreKey01Branch)o;
      int result = this.row.compareTo(other.row);
      if (result != 0) {
        return result;
      }
      result = this.column.compareTo(other.column);
      if (result != 0) {
        return result;
      }
      // The below older timestamps sorting ahead of newer timestamps looks
      // wrong but it is intentional. This way, newer timestamps are first
      // found when we iterate over a memcache and newer versions are the
      // first we trip over when reading from a store file.
      if (this.timestamp < other.timestamp) {
        result = 1;
      } else if (this.timestamp > other.timestamp) {
        result = -1;
      }
      return result;
    }

    // Writable

    public void write(DataOutput out) throws IOException {
      row.write(out);
      column.write(out);
      out.writeLong(timestamp);
    }

    public void readFields(DataInput in) throws IOException {
      row.readFields(in);
      column.readFields(in);
      timestamp = in.readLong();
    }

    /**
     * Returns row and column bytes out of an HStoreKey.
     * @param hsk Store key.
     * @return byte array encoding of HStoreKey
     * @throws UnsupportedEncodingException
     */
    public static byte[] getBytes(final HStoreKey hsk)
    throws UnsupportedEncodingException {
      StringBuilder s = new StringBuilder(Bytes.toString(hsk.getRow()));
      s.append(Bytes.toString(hsk.getColumn()));
      return s.toString().getBytes(HConstants.UTF8_ENCODING);
    }
  }
}