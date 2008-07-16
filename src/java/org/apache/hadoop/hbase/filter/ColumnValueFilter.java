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
import java.util.SortedMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.io.ObjectWritable;

/**
 * This filter is used to filter based on the value of a given column. It takes
 * an operator (equal, greater, not equal, etc) and either a byte [] value or a
 * byte [] comparator. If we have a byte [] value then we just do a
 * lexicographic compare. If this is not sufficient (eg you want to deserialize
 * a long and then compare it to a fixed long value, then you can pass in your
 * own comparator instead.
 */
public class ColumnValueFilter implements RowFilterInterface {

  /** Comparison operator. */
  public enum CompareOp {
    LESS, LESS_OR_EQUAL, EQUAL, NOT_EQUAL, GREATER_OR_EQUAL, GREATER;
  }

  private byte[] columnName;
  private CompareOp compareOp;
  private byte[] value;
  private WritableByteArrayComparable comparator;

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
    this.columnName = columnName;
    this.compareOp = compareOp;
    this.value = value;
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
    this.columnName = columnName;
    this.compareOp = compareOp;
    this.comparator = comparator;
  }

  /** {@inheritDoc} */
  public boolean filterRowKey(@SuppressWarnings("unused") final byte[] rowKey) {
    return false;
  }

  /** {@inheritDoc} */
  public boolean filterColumn(@SuppressWarnings("unused") final byte[] rowKey,
      final byte[] colKey, final byte[] data) {
    if (!Arrays.equals(colKey, columnName)) {
      return false;
    }

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

  /** {@inheritDoc} */
  public boolean filterAllRemaining() {
    return false;
  }

  /** {@inheritDoc} */
  public boolean filterRow(final SortedMap<byte[], Cell> columns) {
    // Don't let rows through if they don't have the column we are checking
    return !columns.containsKey(columnName);
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

  /** {@inheritDoc} */
  public boolean processAlways() {
    return false;
  }

  /** {@inheritDoc} */
  public void reset() {
    // Nothing.
  }

  /** {@inheritDoc} */
  public void rowProcessed(@SuppressWarnings("unused") final boolean filtered,
      @SuppressWarnings("unused") final byte[] key) {
    // Nothing
  }

  /** {@inheritDoc} */
  public void validate(@SuppressWarnings("unused") final byte[][] columns) {
    // Nothing
  }

  /** {@inheritDoc} */
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
  }

  /** {@inheritDoc} */
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
  }

}
