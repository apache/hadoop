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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.hbase.HLogEdit;

/**
 * Implementation of RowFilterInterface that can filter by rowkey regular
 * expression and/or individual column values (equals comparison only).
 * Multiple column filters imply an implicit conjunction of filter criteria.
 */
public class RegExpRowFilter implements RowFilterInterface {

  private Pattern rowKeyPattern = null;
  private String rowKeyRegExp = null;

  private Map<Text, byte[]> equalsMap = new HashMap<Text, byte[]>();
  private Set<Text> nullColumns = new HashSet<Text>();

  static final Log LOG = LogFactory.getLog(RegExpRowFilter.class);
  
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
   * Constructor that takes a row key regular expression to filter on.
   * 
   * @param rowKeyRegExp
   * @param columnFilter
   */
  public RegExpRowFilter(final String rowKeyRegExp,
      final Map<Text, byte[]> columnFilter) {
    this.rowKeyRegExp = rowKeyRegExp;
    this.setColumnFilters(columnFilter);
  }
  
  /** {@inheritDoc} */
  @SuppressWarnings("unused")
  public void rowProcessed(boolean filtered, Text rowKey) {
    //doesn't care
  }

  /** {@inheritDoc} */
  public boolean processAlways() {
    return false;
  }
  
  /**
   * Specify a value that must be matched for the given column.
   * 
   * @param colKey
   *          the column to match on
   * @param value
   *          the value that must equal the stored value.
   */
  public void setColumnFilter(final Text colKey, final byte[] value) {
    if (value == null) {
      nullColumns.add(colKey);
    } else {
      equalsMap.put(colKey, value);
    }
  }

  /**
   * Set column filters for a number of columns.
   * 
   * @param columnFilter
   *          Map of columns with value criteria.
   */
  public void setColumnFilters(final Map<Text, byte[]> columnFilter) {
    if (null == columnFilter) {
      nullColumns.clear();
      equalsMap.clear();
    } else {
      for (Entry<Text, byte[]> entry : columnFilter.entrySet()) {
        setColumnFilter(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * 
   * {@inheritDoc}
   */
  public void reset() {
    // Nothing to reset
  }

  /**
   * 
   * {@inheritDoc}
   */
  public boolean filterAllRemaining() {
    return false;
  }

  /**
   * 
   * {@inheritDoc}
   */
  public boolean filter(final Text rowKey) {
    if (filtersByRowKey() && rowKey != null) {
      boolean result = !getRowKeyPattern().matcher(rowKey.toString()).matches();
      if (LOG.isDebugEnabled()) {
        LOG.debug("filter returning " + result + " for rowKey: " + rowKey);
      }
      return result;
    }
    return false;
  }

  /**
   * 
   * {@inheritDoc}
   */
  public boolean filter(final Text rowKey, final Text colKey,
      final byte[] data) {
    if (filter(rowKey)) {
      return true;
    }
    if (filtersByColumnValue()) {
      byte[] filterValue = equalsMap.get(colKey);
      if (null != filterValue) {
        boolean result = !Arrays.equals(filterValue, data);
        if (LOG.isDebugEnabled()) {
          LOG.debug("filter returning " + result + " for rowKey: " + rowKey + 
            " colKey: " + colKey);
        }
        return result;
      }
    }
    if (nullColumns.contains(colKey)) {
      if (data != null && !HLogEdit.isDeleted(data)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("filter returning true for rowKey: " + rowKey + 
            " colKey: " + colKey);
        }
        return true;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("filter returning false for rowKey: " + rowKey + " colKey: " + 
        colKey);
    }
    return false;
  }

  /**
   * 
   * {@inheritDoc}
   */
  public boolean filterNotNull(final TreeMap<Text, byte[]> columns) {
    for (Entry<Text, byte[]> col : columns.entrySet()) {
      if (nullColumns.contains(col.getKey())
          && !HLogEdit.isDeleted(col.getValue())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("filterNotNull returning true for colKey: " + col.getKey()
            + ", column should be null.");
        }
        return true;
      }
    }
    for (Text col : equalsMap.keySet()) {
      if (!columns.containsKey(col)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("filterNotNull returning true for colKey: " + col + 
            ", column not found in given TreeMap<Text, byte[]>.");
        }
        return true;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("filterNotNull returning false.");
    }
    return false;
  }

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

  /**
   * 
   * {@inheritDoc}
   */
  public void readFields(final DataInput in) throws IOException {
    boolean hasRowKeyPattern = in.readBoolean();
    if (hasRowKeyPattern) {
      rowKeyRegExp = in.readUTF();
    }
    // equals map
    equalsMap.clear();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      Text key = new Text();
      key.readFields(in);
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
      Text key = new Text();
      key.readFields(in);
      setColumnFilter(key, null);
    }
  }

  /**
   * 
   * {@inheritDoc}
   */
  public void validate(final Text[] columns) {
    Set<Text> invalids = new HashSet<Text>();
    for (Text colKey : getFilterColumns()) {
      boolean found = false;
      for (Text col : columns) {
        if (col.equals(colKey)) {
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

  private Set<Text> getFilterColumns() {
    Set<Text> cols = new HashSet<Text>();
    cols.addAll(equalsMap.keySet());
    cols.addAll(nullColumns);
    return cols;
  }

  /**
   * 
   * {@inheritDoc}
   */
  public void write(final DataOutput out) throws IOException {
    if (!filtersByRowKey()) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeUTF(getRowKeyRegExp());
    }

    // equalsMap
    out.writeInt(equalsMap.size());
    for (Entry<Text, byte[]> entry : equalsMap.entrySet()) {
      entry.getKey().write(out);
      byte[] value = entry.getValue();
      out.writeInt(value.length);
      out.write(value);
    }

    // null columns
    out.writeInt(nullColumns.size());
    for (Text col : nullColumns) {
      col.write(out);
    }
  }
}