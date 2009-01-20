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
package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.regionserver.HLogEdit;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Implementation of RowFilterInterface that can filter by rowkey regular
 * expression and/or individual column values (equals comparison only). Multiple
 * column filters imply an implicit conjunction of filter criteria.
 * 
 * Note that column value filtering in this interface has been replaced by
 * {@link ColumnValueFilter}.
 */
public class RegExpRowFilter implements RowFilterInterface {

  private Pattern rowKeyPattern = null;
  private String rowKeyRegExp = null;

  @Deprecated
  private Map<byte [], byte[]> equalsMap =
    new TreeMap<byte [], byte[]>(Bytes.BYTES_COMPARATOR);
  @Deprecated
  private Set<byte []> nullColumns =
    new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);

  /**
   * Default constructor, filters nothing. Required though for RPC
   * deserialization.
   */
  public RegExpRowFilter() {
    super();
  }

  /**
   * Constructor that takes a row key regular expression to filter on.
   * 
   * @param rowKeyRegExp
   */
  public RegExpRowFilter(final String rowKeyRegExp) {
    this.rowKeyRegExp = rowKeyRegExp;
  }

  /**
   * @deprecated Column filtering has been replaced by {@link ColumnValueFilter}
   * Constructor that takes a row key regular expression to filter on.
   * 
   * @param rowKeyRegExp
   * @param columnFilter
   */
  @Deprecated
  public RegExpRowFilter(final String rowKeyRegExp,
      final Map<byte [], Cell> columnFilter) {
    this.rowKeyRegExp = rowKeyRegExp;
    this.setColumnFilters(columnFilter);
  }
  
  public void rowProcessed(boolean filtered, byte [] rowKey) {
    //doesn't care
  }

  public boolean processAlways() {
    return false;
  }
  
  /**
   * @deprecated Column filtering has been replaced by {@link ColumnValueFilter}
   * Specify a value that must be matched for the given column.
   * 
   * @param colKey
   *          the column to match on
   * @param value
   *          the value that must equal the stored value.
   */
  @Deprecated
  public void setColumnFilter(final byte [] colKey, final byte[] value) {
    if (value == null) {
      nullColumns.add(colKey);
    } else {
      equalsMap.put(colKey, value);
    }
  }

  /**
   * @deprecated Column filtering has been replaced by {@link ColumnValueFilter}
   * Set column filters for a number of columns.
   * 
   * @param columnFilter
   *          Map of columns with value criteria.
   */
  @Deprecated
  public void setColumnFilters(final Map<byte [], Cell> columnFilter) {
    if (null == columnFilter) {
      nullColumns.clear();
      equalsMap.clear();
    } else {
      for (Entry<byte [], Cell> entry : columnFilter.entrySet()) {
        setColumnFilter(entry.getKey(), entry.getValue().getValue());
      }
    }
  }

  public void reset() {
    // Nothing to reset
  }

  public boolean filterAllRemaining() {
    return false;
  }

  public boolean filterRowKey(final byte [] rowKey) {
    return (filtersByRowKey() && rowKey != null)?
      !getRowKeyPattern().matcher(Bytes.toString(rowKey)).matches():
      false;
  }

  public boolean filterColumn(final byte [] rowKey, final byte [] colKey,
      final byte[] data) {
    if (filterRowKey(rowKey)) {
      return true;
    }
    if (filtersByColumnValue()) {
      byte[] filterValue = equalsMap.get(colKey);
      if (null != filterValue) {
        return !Arrays.equals(filterValue, data);
      }
    }
    if (nullColumns.contains(colKey)) {
      if (data != null && !HLogEdit.isDeleted(data)) {
        return true;
      }
    }
    return false;
  }

  public boolean filterRow(final SortedMap<byte [], Cell> columns) {
    for (Entry<byte [], Cell> col : columns.entrySet()) {
      if (nullColumns.contains(col.getKey())
          && !HLogEdit.isDeleted(col.getValue().getValue())) {
        return true;
      }
    }
    for (byte [] col : equalsMap.keySet()) {
      if (!columns.containsKey(col)) {
        return true;
      }
    }
    return false;
  }

  @Deprecated
  private boolean filtersByColumnValue() {
    return equalsMap != null && equalsMap.size() > 0;
  }

  private boolean filtersByRowKey() {
    return null != rowKeyPattern || null != rowKeyRegExp;
  }

  private String getRowKeyRegExp() {
    if (null == rowKeyRegExp && rowKeyPattern != null) {
      rowKeyRegExp = rowKeyPattern.toString();
    }
    return rowKeyRegExp;
  }

  private Pattern getRowKeyPattern() {
    if (rowKeyPattern == null && rowKeyRegExp != null) {
      rowKeyPattern = Pattern.compile(rowKeyRegExp);
    }
    return rowKeyPattern;
  }

  public void readFields(final DataInput in) throws IOException {
    boolean hasRowKeyPattern = in.readBoolean();
    if (hasRowKeyPattern) {
      rowKeyRegExp = in.readUTF();
    }
    // equals map
    equalsMap.clear();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      byte [] key = Bytes.readByteArray(in);
      int len = in.readInt();
      byte[] value = null;
      if (len >= 0) {
        value = new byte[len];
        in.readFully(value);
      }
      setColumnFilter(key, value);
    }
    // nullColumns
    nullColumns.clear();
    size = in.readInt();
    for (int i = 0; i < size; i++) {
      setColumnFilter(Bytes.readByteArray(in), null);
    }
  }

  public void validate(final byte [][] columns) {
    Set<byte []> invalids = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
    for (byte [] colKey : getFilterColumns()) {
      boolean found = false;
      for (byte [] col : columns) {
        if (Bytes.equals(col, colKey)) {
          found = true;
          break;
        }
      }
      if (!found) {
        invalids.add(colKey);
      }
    }

    if (invalids.size() > 0) {
      throw new InvalidRowFilterException(String.format(
          "RowFilter contains criteria on columns %s not in %s", invalids,
          Arrays.toString(columns)));
    }
  }

  @Deprecated
  private Set<byte []> getFilterColumns() {
    Set<byte []> cols = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
    cols.addAll(equalsMap.keySet());
    cols.addAll(nullColumns);
    return cols;
  }

  public void write(final DataOutput out) throws IOException {
    if (!filtersByRowKey()) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeUTF(getRowKeyRegExp());
    }

    // equalsMap
    out.writeInt(equalsMap.size());
    for (Entry<byte [], byte[]> entry : equalsMap.entrySet()) {
      Bytes.writeByteArray(out, entry.getKey());
      byte[] value = entry.getValue();
      out.writeInt(value.length);
      out.write(value);
    }

    // null columns
    out.writeInt(nullColumns.size());
    for (byte [] col : nullColumns) {
      Bytes.writeByteArray(out, col);
    }
  }
}