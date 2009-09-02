/**
 * Copyright 2009 The Apache Software Foundation
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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This filter is used to filter cells based on value. It takes a {@link #Filter.CompareOp} 
 * operator (equal, greater, not equal, etc), and either a byte [] value or 
 * a {@link #WritableByteArrayComparable}.
 * <p>
 * If we have a byte [] value then we just do a lexicographic compare. For 
 * example, if passed value is 'b' and cell has 'a' and the compare operator 
 * is LESS, then we will filter out this cell (return true).  If this is not 
 * sufficient (eg you want to deserialize a long and then compare it to a fixed 
 * long value), then you can pass in your own comparator instead.
 * <p>
 * You must also specify a family and qualifier.  Only the value of this column 
 * will be tested.  All other 
 * <p>
 * To prevent the entire row from being emitted if this filter determines the
 * column does not pass (it should be filtered), wrap this filter with a
 * {@link SkipFilter}.
 * <p>
 * To filter based on the value of all scanned columns, use {@link ValueFilter}.
 */
public class SingleColumnValueFilter implements Filter {
  static final Log LOG = LogFactory.getLog(SingleColumnValueFilter.class);

  private byte [] columnFamily;
  private byte [] columnQualifier; 
  private CompareOp compareOp;
  private WritableByteArrayComparable comparator;

  /**
   * Writable constructor, do not use.
   */
  public SingleColumnValueFilter() {
  }
  
  /**
   * Constructor for binary compare of the value of a single column.  If the
   * column is found and the condition passes, all columns of the row will be
   * emitted.  If the column is not found or the condition fails, the row will
   * not be emitted.
   * 
   * @param family name of column family
   * @param qualifier name of column qualifier
   * @param compareOp operator
   * @param value value to compare column values against
   */
  public SingleColumnValueFilter(final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final byte[] value) {
    this(family, qualifier, compareOp, new BinaryComparator(value));
  }

  /**
   * Constructor for binary compare of the value of a single column.  If the
   * column is found and the condition passes, all columns of the row will be
   * emitted.  If the condition fails, the row will not be emitted.
   * <p>
   * Use the filterIfColumnMissing flag to set whether the rest of the columns
   * in a row will be emitted if the specified column to check is not found in
   * the row.
   * 
   * @param family name of column family
   * @param qualifier name of column qualifier
   * @param compareOp operator
   * @param comparator Comparator to use.
   */
  public SingleColumnValueFilter(final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final WritableByteArrayComparable comparator) {
    this.columnFamily = family;
    this.columnQualifier = qualifier;
    this.compareOp = compareOp;
    this.comparator = comparator;
  }
  
  public boolean filterRowKey(byte[] rowKey, int offset, int length) {
    return false;
  }

  public ReturnCode filterKeyValue(KeyValue keyValue) {
    if (!keyValue.matchingColumn(this.columnFamily, this.columnQualifier)) {
      return ReturnCode.INCLUDE;
    }
    if (filterColumnValue(keyValue.getBuffer(),
        keyValue.getValueOffset(), keyValue.getValueLength())) {
      return ReturnCode.NEXT_ROW;
    }
    return ReturnCode.INCLUDE;
  }

  private boolean filterColumnValue(final byte[] data, final int offset,
      final int length) {
    int compareResult = comparator.compareTo(Arrays.copyOfRange(data, offset,
        offset + length));

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
    return false;
  }

  public void reset() {
  }

  public void readFields(final DataInput in) throws IOException {
    this.columnFamily = Bytes.readByteArray(in);
    if(this.columnFamily.length == 0) {
      this.columnFamily = null;
    }
    this.columnQualifier = Bytes.readByteArray(in);
    if(this.columnQualifier.length == 0) {
      this.columnQualifier = null;
    }
    compareOp = CompareOp.valueOf(in.readUTF());
    comparator = (WritableByteArrayComparable) HbaseObjectWritable.readObject(in,
        null);
  }

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.columnFamily);
    Bytes.writeByteArray(out, this.columnQualifier);
    out.writeUTF(compareOp.name());
    HbaseObjectWritable.writeObject(out, comparator,
        WritableByteArrayComparable.class, null);
  }
}