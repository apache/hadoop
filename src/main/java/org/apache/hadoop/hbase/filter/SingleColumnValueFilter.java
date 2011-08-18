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

package org.apache.hadoop.hbase.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import com.google.common.base.Preconditions;

/**
 * This filter is used to filter cells based on value. It takes a {@link CompareFilter.CompareOp}
 * operator (equal, greater, not equal, etc), and either a byte [] value or
 * a WritableByteArrayComparable.
 * <p>
 * If we have a byte [] value then we just do a lexicographic compare. For
 * example, if passed value is 'b' and cell has 'a' and the compare operator
 * is LESS, then we will filter out this cell (return true).  If this is not
 * sufficient (eg you want to deserialize a long and then compare it to a fixed
 * long value), then you can pass in your own comparator instead.
 * <p>
 * You must also specify a family and qualifier.  Only the value of this column
 * will be tested. When using this filter on a {@link Scan} with specified
 * inputs, the column to be tested should also be added as input (otherwise
 * the filter will regard the column as missing).
 * <p>
 * To prevent the entire row from being emitted if the column is not found
 * on a row, use {@link #setFilterIfMissing}.
 * Otherwise, if the column is found, the entire row will be emitted only if
 * the value passes.  If the value fails, the row will be filtered out.
 * <p>
 * In order to test values of previous versions (timestamps), set
 * {@link #setLatestVersionOnly} to false. The default is true, meaning that
 * only the latest version's value is tested and all previous versions are ignored.
 * <p>
 * To filter based on the value of all scanned columns, use {@link ValueFilter}.
 */
public class SingleColumnValueFilter extends FilterBase {
  static final Log LOG = LogFactory.getLog(SingleColumnValueFilter.class);

  protected byte [] columnFamily;
  protected byte [] columnQualifier;
  private CompareOp compareOp;
  private WritableByteArrayComparable comparator;
  private boolean foundColumn = false;
  private boolean matchedColumn = false;
  private boolean filterIfMissing = false;
  private boolean latestVersionOnly = true;

  /**
   * Writable constructor, do not use.
   */
  public SingleColumnValueFilter() {
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

  /**
   * @return operator
   */
  public CompareOp getOperator() {
    return compareOp;
  }

  /**
   * @return the comparator
   */
  public WritableByteArrayComparable getComparator() {
    return comparator;
  }

  /**
   * @return the family
   */
  public byte[] getFamily() {
    return columnFamily;
  }

  /**
   * @return the qualifier
   */
  public byte[] getQualifier() {
    return columnQualifier;
  }

  public ReturnCode filterKeyValue(KeyValue keyValue) {
    // System.out.println("REMOVE KEY=" + keyValue.toString() + ", value=" + Bytes.toString(keyValue.getValue()));
    if (this.matchedColumn) {
      // We already found and matched the single column, all keys now pass
      return ReturnCode.INCLUDE;
    } else if (this.latestVersionOnly && this.foundColumn) {
      // We found but did not match the single column, skip to next row
      return ReturnCode.NEXT_ROW;
    }
    if (!keyValue.matchingColumn(this.columnFamily, this.columnQualifier)) {
      return ReturnCode.INCLUDE;
    }
    foundColumn = true;
    if (filterColumnValue(keyValue.getBuffer(),
        keyValue.getValueOffset(), keyValue.getValueLength())) {
      return this.latestVersionOnly? ReturnCode.NEXT_ROW: ReturnCode.INCLUDE;
    }
    this.matchedColumn = true;
    return ReturnCode.INCLUDE;
  }

  private boolean filterColumnValue(final byte [] data, final int offset,
      final int length) {
    // TODO: Can this filter take a rawcomparator so don't have to make this
    // byte array copy?
    int compareResult =
      this.comparator.compareTo(Arrays.copyOfRange(data, offset, offset + length));
    switch (this.compareOp) {
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

  public boolean filterRow() {
    // If column was found, return false if it was matched, true if it was not
    // If column not found, return true if we filter if missing, false if not
    return this.foundColumn? !this.matchedColumn: this.filterIfMissing;
  }

  public void reset() {
    foundColumn = false;
    matchedColumn = false;
  }

  /**
   * Get whether entire row should be filtered if column is not found.
   * @return true if row should be skipped if column not found, false if row
   * should be let through anyways
   */
  public boolean getFilterIfMissing() {
    return filterIfMissing;
  }

  /**
   * Set whether entire row should be filtered if column is not found.
   * <p>
   * If true, the entire row will be skipped if the column is not found.
   * <p>
   * If false, the row will pass if the column is not found.  This is default.
   * @param filterIfMissing flag
   */
  public void setFilterIfMissing(boolean filterIfMissing) {
    this.filterIfMissing = filterIfMissing;
  }

  /**
   * Get whether only the latest version of the column value should be compared.
   * If true, the row will be returned if only the latest version of the column
   * value matches. If false, the row will be returned if any version of the
   * column value matches. The default is true.
   * @return return value
   */
  public boolean getLatestVersionOnly() {
    return latestVersionOnly;
  }

  /**
   * Set whether only the latest version of the column value should be compared.
   * If true, the row will be returned if only the latest version of the column
   * value matches. If false, the row will be returned if any version of the
   * column value matches. The default is true.
   * @param latestVersionOnly flag
   */
  public void setLatestVersionOnly(boolean latestVersionOnly) {
    this.latestVersionOnly = latestVersionOnly;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 4 || filterArguments.size() == 6,
                                "Expected 4 or 6 but got: %s", filterArguments.size());
    byte [] family = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
    byte [] qualifier = ParseFilter.removeQuotesFromByteArray(filterArguments.get(1));
    CompareOp compareOp = ParseFilter.createCompareOp(filterArguments.get(2));
    WritableByteArrayComparable comparator = ParseFilter.createComparator(
      ParseFilter.removeQuotesFromByteArray(filterArguments.get(3)));

    if (comparator instanceof RegexStringComparator ||
        comparator instanceof SubstringComparator) {
      if (compareOp != CompareOp.EQUAL &&
          compareOp != CompareOp.NOT_EQUAL) {
        throw new IllegalArgumentException ("A regexstring comparator and substring comparator " +
                                            "can only be used with EQUAL and NOT_EQUAL");
      }
    }

    SingleColumnValueFilter filter = new SingleColumnValueFilter(family, qualifier,
                                                                 compareOp, comparator);

    if (filterArguments.size() == 6) {
      boolean filterIfMissing = ParseFilter.convertByteArrayToBoolean(filterArguments.get(4));
      boolean latestVersionOnly = ParseFilter.convertByteArrayToBoolean(filterArguments.get(5));
      filter.setFilterIfMissing(filterIfMissing);
      filter.setLatestVersionOnly(latestVersionOnly);
    }
    return filter;
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
    this.compareOp = CompareOp.valueOf(in.readUTF());
    this.comparator =
      (WritableByteArrayComparable)HbaseObjectWritable.readObject(in, null);
    this.foundColumn = in.readBoolean();
    this.matchedColumn = in.readBoolean();
    this.filterIfMissing = in.readBoolean();
    this.latestVersionOnly = in.readBoolean();
  }

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.columnFamily);
    Bytes.writeByteArray(out, this.columnQualifier);
    out.writeUTF(compareOp.name());
    HbaseObjectWritable.writeObject(out, comparator,
        WritableByteArrayComparable.class, null);
    out.writeBoolean(foundColumn);
    out.writeBoolean(matchedColumn);
    out.writeBoolean(filterIfMissing);
    out.writeBoolean(latestVersionOnly);
  }
}
