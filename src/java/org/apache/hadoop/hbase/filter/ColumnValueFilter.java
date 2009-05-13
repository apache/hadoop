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
package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.io.ObjectWritable;

/**
 * This filter is used to filter based on the value of a given column. It takes
 * an operator (equal, greater, not equal, etc) and either a byte [] value or a
 * byte [] comparator. If we have a byte [] value then we just do a
 * lexicographic compare. If this is not sufficient (eg you want to deserialize
 * a long and then compare it to a fixed long value), then you can pass in your
 * own comparator instead.
 */
public class ColumnValueFilter implements RowFilterInterface {
  /** Comparison operators. */
  public enum CompareOp {
    /** less than */
    LESS,
    /** less than or equal to */
    LESS_OR_EQUAL,
    /** equals */
    EQUAL,
    /** not equal */
    NOT_EQUAL,
    /** greater than or equal to */
    GREATER_OR_EQUAL,
    /** greater than */
    GREATER;
  }

  private byte[] columnName;
  private CompareOp compareOp;
  private byte[] value;
  private WritableByteArrayComparable comparator;
  private boolean filterIfColumnMissing;

  ColumnValueFilter() {
    // for Writable
  }

  /**
   * Constructor.
   * 
   * @param columnName name of column
   * @param compareOp operator
   * @param value value to compare column values against
   */
  public ColumnValueFilter(final byte[] columnName, final CompareOp compareOp,
      final byte[] value) {
    this(columnName, compareOp, value, true);
  }
  
  /**
   * Constructor.
   * 
   * @param columnName name of column
   * @param compareOp operator
   * @param value value to compare column values against
   * @param filterIfColumnMissing if true then we will filter rows that don't have the column. 
   */
  public ColumnValueFilter(final byte[] columnName, final CompareOp compareOp,
      final byte[] value, boolean filterIfColumnMissing) {
    this.columnName = columnName;
    this.compareOp = compareOp;
    this.value = value;
    this.filterIfColumnMissing = filterIfColumnMissing;
  }

  /**
   * Constructor.
   * 
   * @param columnName name of column
   * @param compareOp operator
   * @param comparator Comparator to use.
   */
  public ColumnValueFilter(final byte[] columnName, final CompareOp compareOp,
      final WritableByteArrayComparable comparator) {
    this(columnName, compareOp, comparator, true);
  }
  
  /**
  * Constructor.
  * 
  * @param columnName name of column
  * @param compareOp operator
  * @param comparator Comparator to use.
  * @param filterIfColumnMissing if true then we will filter rows that don't have the column. 
  */
 public ColumnValueFilter(final byte[] columnName, final CompareOp compareOp,
     final WritableByteArrayComparable comparator, boolean filterIfColumnMissing) {
   this.columnName = columnName;
   this.compareOp = compareOp;
   this.comparator = comparator;
   this.filterIfColumnMissing = filterIfColumnMissing;
 }

  public boolean filterRowKey(final byte[] rowKey) {
    return filterRowKey(rowKey, 0, rowKey.length);
  }

  public boolean filterRowKey(byte[] rowKey, int offset, int length) {
    return false;
  }

  
  public boolean filterColumn(final byte[] rowKey,
      final byte[] colKey, final byte[] data) {
    if (!filterIfColumnMissing) {
      return false; // Must filter on the whole row
    }
    if (!Arrays.equals(colKey, columnName)) {
      return false;
    }
    return filterColumnValue(data, 0, data.length); 
  }


  public boolean filterColumn(byte[] rowKey, int roffset, int rlength,
      byte[] cn, int coffset, int clength, byte[] columnValue,
      int voffset, int vlength) {
    if (!filterIfColumnMissing) {
      return false; // Must filter on the whole row
    }
    if (Bytes.compareTo(cn, coffset, clength,
        this.columnName, 0, this.columnName.length) != 0) {
      return false;
    }
    return filterColumnValue(columnValue, voffset, vlength);
  }

  private boolean filterColumnValue(final byte [] data, final int offset,
      final int length) {
    int compareResult;
    if (comparator != null) {
      compareResult = comparator.compareTo(data);
    } else {
      compareResult = compare(value, data);
    }

    switch (compareOp) {
    case LESS:
      return compareResult <= 0;
    case LESS_OR_EQUAL:
      return compareResult < 0;
    case EQUAL:
      return compareResult != 0;
    case NOT_EQUAL:
      return compareResult == 0;
    case GREATER_OR_EQUAL:
      return compareResult > 0;
    case GREATER:
      return compareResult >= 0;
    default:
      throw new RuntimeException("Unknown Compare op " + compareOp.name());
    }
  }
  
  public boolean filterAllRemaining() {
    return false;
  }

  public boolean filterRow(final SortedMap<byte[], Cell> columns) {
    if (columns == null)
      return false;
    if (filterIfColumnMissing) {
      return !columns.containsKey(columnName);
    } 
    // Otherwise we must do the filter here
    Cell colCell = columns.get(columnName);
      if (colCell == null) {
        return false;
      }
      byte [] v = colCell.getValue();
      return this.filterColumnValue(v, 0, v.length);
  }

  public boolean filterRow(List<KeyValue> results) {
    if (results == null) return false;
    KeyValue found = null;
    if (filterIfColumnMissing) {
      boolean doesntHaveIt = true;
      for (KeyValue kv: results) {
        if (kv.matchingColumn(columnName)) {
          doesntHaveIt = false;
          found = kv;
          break;
        }
      }
      if (doesntHaveIt) return doesntHaveIt;
    }
    if (found == null) {
      for (KeyValue kv: results) {
        if (kv.matchingColumn(columnName)) {
          found = kv;
          break;
        }
      }
    }
    if (found == null) {
      return false;
    }
    return this.filterColumnValue(found.getValue(), found.getValueOffset(),
      found.getValueLength());
  }

  private int compare(final byte[] b1, final byte[] b2) {
    int len = Math.min(b1.length, b2.length);

    for (int i = 0; i < len; i++) {
      if (b1[i] != b2[i]) {
        return b1[i] - b2[i];
      }
    }
    return b1.length - b2.length;
  }

  public boolean processAlways() {
    return false;
  }

  public void reset() {
    // Nothing.
  }

  public void rowProcessed(final boolean filtered,
      final byte[] key) {
    // Nothing
  }


  public void rowProcessed(boolean filtered, byte[] key, int offset, int length) {
    // Nothing
  }

  public void validate(final byte[][] columns) {
    // Nothing
  }

  public void readFields(final DataInput in) throws IOException {
    int valueLen = in.readInt();
    if (valueLen > 0) {
      value = new byte[valueLen];
      in.readFully(value);
    }
    columnName = Bytes.readByteArray(in);
    compareOp = CompareOp.valueOf(in.readUTF());
    comparator = (WritableByteArrayComparable) ObjectWritable.readObject(in,
        new HBaseConfiguration());
    filterIfColumnMissing = in.readBoolean();
  }

  public void write(final DataOutput out) throws IOException {
    if (value == null) {
      out.writeInt(0);
    } else {
      out.writeInt(value.length);
      out.write(value);
    }
    Bytes.writeByteArray(out, columnName);
    out.writeUTF(compareOp.name());
    ObjectWritable.writeObject(out, comparator,
        WritableByteArrayComparable.class, new HBaseConfiguration());
    out.writeBoolean(filterIfColumnMissing);
  }
}