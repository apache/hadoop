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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ObjectWritable;

/**
 * This filter is used to filter based on the value of a given column. It takes
 * an operator (equal, greater, not equal, etc) and either a byte [] value or a
 * byte [] comparator. If we have a byte [] value then we just do a
 * lexicographic compare. For example, if passed value is 'b' and cell has 'a'
 * and the compare operator is LESS, then we will filter out this cell (return
 * true).  If this is not sufficient (eg you want to deserialize
 * a long and then compare it to a fixed long value), then you can pass in your
 * own comparator instead.
 * */
public class ValueFilter implements Filter {
  static final Log LOG = LogFactory.getLog(ValueFilter.class);

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

  private byte [] columnFamily;
  private byte [] columnQualifier; 
  private CompareOp compareOp;
  private byte [] value;
  private WritableByteArrayComparable comparator;
  private boolean filterIfColumnMissing;

  private boolean filterThisRow = false;
  private boolean foundColValue = false;

  ValueFilter() {
    // for Writable
  }

  /**
   * Constructor.
   * 
   * @param family name of column family
   * @param qualifier name of column qualifier
   * @param compareOp operator
   * @param value value to compare column values against
   */
  public ValueFilter(final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final byte[] value) {
    this(family, qualifier, compareOp, value, true);
  }

  /**
   * Constructor.
   * 
   * @param family name of column family
   * @param qualifier name of column qualifier
   * @param compareOp operator
   * @param value value to compare column values against
   * @param filterIfColumnMissing if true then we will filter rows that don't
   * have the column.
   */
  public ValueFilter(final byte [] family, final byte [] qualifier,
      final CompareOp compareOp,
      final byte[] value, boolean filterIfColumnMissing) {
    this.columnFamily = family;
    this.columnQualifier = qualifier;
    this.compareOp = compareOp;
    this.value = value;
    this.filterIfColumnMissing = filterIfColumnMissing;
  }

  /**
   * Constructor.
   * 
   * @param family name of column family
   * @param qualifier name of column qualifier
   * @param compareOp operator
   * @param comparator Comparator to use.
   */
  public ValueFilter(final byte [] family, final byte [] qualifier,
      final CompareOp compareOp,
      final WritableByteArrayComparable comparator) {
    this(family, qualifier, compareOp, comparator, true);
  }

  /**
   * Constructor.
   * 
   * @param family name of column family
   * @param qualifier name of column qualifier
   * @param compareOp operator
   * @param comparator Comparator to use.
   * @param filterIfColumnMissing if true then we will filter rows that don't
   * have the column.
   */
  public ValueFilter(final byte [] family, final byte [] qualifier,
      final CompareOp compareOp,
      final WritableByteArrayComparable comparator,
      boolean filterIfColumnMissing) {
    this.columnFamily = family;
    this.columnQualifier = qualifier;
    this.compareOp = compareOp;
    this.comparator = comparator;
    this.filterIfColumnMissing = filterIfColumnMissing;
  }

  public boolean filterRowKey(byte[] rowKey, int offset, int length) {
    return false;
  }

  public ReturnCode filterKeyValue(KeyValue keyValue) {
    if (!keyValue.matchingColumn(this.columnFamily, this.columnQualifier)) {
      return ReturnCode.INCLUDE;
    }
    this.foundColValue = true;
    boolean filtered = filterColumnValue(keyValue.getBuffer(),
      keyValue.getValueOffset(), keyValue.getValueLength());
    if (filtered) {
      this.filterThisRow = true;
      return ReturnCode.NEXT_ROW;
    }
    return ReturnCode.INCLUDE;
  }

  private boolean filterColumnValue(final byte[] data, final int offset,
      final int length) {
    int compareResult;
    if (comparator != null) {
      compareResult = comparator.compareTo(Arrays.copyOfRange(data, offset,
          offset + length));
    } else {
      compareResult = Bytes.compareTo(value, 0, value.length, data, offset,
          length);
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

  public boolean filterRow() {
    boolean result = filterThisRow || (filterIfColumnMissing && !foundColValue);
    filterThisRow = false;
    foundColValue = false;
    return result;
  }

  public void reset() {
    // Nothing.
  }

  public void readFields(final DataInput in) throws IOException {
    int valueLen = in.readInt();
    if (valueLen > 0) {
      value = new byte[valueLen];
      in.readFully(value);
    }
    this.columnFamily = Bytes.readByteArray(in);
    this.columnQualifier = Bytes.readByteArray(in);
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
    Bytes.writeByteArray(out, this.columnFamily);
    Bytes.writeByteArray(out, this.columnQualifier);
    out.writeUTF(compareOp.name());
    ObjectWritable.writeObject(out, comparator,
        WritableByteArrayComparable.class, new HBaseConfiguration());
    out.writeBoolean(filterIfColumnMissing);
  }
}