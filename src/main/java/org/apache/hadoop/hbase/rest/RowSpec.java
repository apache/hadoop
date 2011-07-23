/*
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

package org.apache.hadoop.hbase.rest;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Parses a path based row/column/timestamp specification into its component
 * elements.
 * <p>
 *  
 */
public class RowSpec {
  public static final long DEFAULT_START_TIMESTAMP = 0;
  public static final long DEFAULT_END_TIMESTAMP = Long.MAX_VALUE;
  
  private byte[] row = HConstants.EMPTY_START_ROW;
  private byte[] endRow = null;
  private TreeSet<byte[]> columns =
    new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
  private long startTime = DEFAULT_START_TIMESTAMP;
  private long endTime = DEFAULT_END_TIMESTAMP;
  private int maxVersions = HColumnDescriptor.DEFAULT_VERSIONS;
  private int maxValues = Integer.MAX_VALUE;

  public RowSpec(String path) throws IllegalArgumentException {
    int i = 0;
    while (path.charAt(i) == '/') {
      i++;
    }
    i = parseRowKeys(path, i);
    i = parseColumns(path, i);
    i = parseTimestamp(path, i);
    i = parseQueryParams(path, i);
  }

  private int parseRowKeys(final String path, int i)
      throws IllegalArgumentException {
    String startRow = null, endRow = null;
    try {
      StringBuilder sb = new StringBuilder();
      char c;
      while (i < path.length() && (c = path.charAt(i)) != '/') {
        sb.append(c);
        i++;
      }
      i++;
      String row = startRow = sb.toString();
      int idx = startRow.indexOf(',');
      if (idx != -1) {
        startRow = URLDecoder.decode(row.substring(0, idx),
          HConstants.UTF8_ENCODING);
        endRow = URLDecoder.decode(row.substring(idx + 1),
          HConstants.UTF8_ENCODING);
      } else {
        startRow = URLDecoder.decode(row, HConstants.UTF8_ENCODING);
      }
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException(e);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    // HBase does not support wildcards on row keys so we will emulate a
    // suffix glob by synthesizing appropriate start and end row keys for
    // table scanning
    if (startRow.charAt(startRow.length() - 1) == '*') {
      if (endRow != null)
        throw new IllegalArgumentException("invalid path: start row "+
          "specified with wildcard");
      this.row = Bytes.toBytes(startRow.substring(0, 
        startRow.lastIndexOf("*")));
      this.endRow = new byte[this.row.length + 1];
      System.arraycopy(this.row, 0, this.endRow, 0, this.row.length);
      this.endRow[this.row.length] = (byte)255;
    } else {
      this.row = Bytes.toBytes(startRow.toString());
      if (endRow != null) {
        this.endRow = Bytes.toBytes(endRow.toString());
      }
    }
    return i;
  }

  private int parseColumns(final String path, int i)
      throws IllegalArgumentException {
    if (i >= path.length()) {
      return i;
    }
    try {
      char c;
      StringBuilder column = new StringBuilder();
      while (i < path.length() && (c = path.charAt(i)) != '/') {
        if (c == ',') {
          if (column.length() < 1) {
            throw new IllegalArgumentException("invalid path");
          }
          String s = URLDecoder.decode(column.toString(),
            HConstants.UTF8_ENCODING);
          if (!s.contains(":")) {
            this.columns.add(Bytes.toBytes(s + ":"));
          } else {
            this.columns.add(Bytes.toBytes(s));
          }
          column.setLength(0);
          i++;
          continue;
        }
        column.append(c);
        i++;
      }
      i++;
      // trailing list entry
      if (column.length() > 1) {
        String s = URLDecoder.decode(column.toString(),
          HConstants.UTF8_ENCODING);
        if (!s.contains(":")) {
          this.columns.add(Bytes.toBytes(s + ":"));
        } else {
          this.columns.add(Bytes.toBytes(s));
        }
      }
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException(e);
    } catch (UnsupportedEncodingException e) {
      // shouldn't happen
      throw new RuntimeException(e);
    }
    return i;
  }

  private int parseTimestamp(final String path, int i)
      throws IllegalArgumentException {
    if (i >= path.length()) {
      return i;
    }
    long time0 = 0, time1 = 0;
    try {
      char c = 0;
      StringBuilder stamp = new StringBuilder();
      while (i < path.length()) {
        c = path.charAt(i);
        if (c == '/' || c == ',') {
          break;
        }
        stamp.append(c);
        i++;
      }
      try {
        time0 = Long.valueOf(URLDecoder.decode(stamp.toString(),
          HConstants.UTF8_ENCODING));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(e);
      }
      if (c == ',') {
        stamp = new StringBuilder();
        i++;
        while (i < path.length() && ((c = path.charAt(i)) != '/')) {
          stamp.append(c);
          i++;
        }
        try {
          time1 = Long.valueOf(URLDecoder.decode(stamp.toString(),
            HConstants.UTF8_ENCODING));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(e);
        }
      }
      if (c == '/') {
        i++;
      }
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException(e);
    } catch (UnsupportedEncodingException e) {
      // shouldn't happen
      throw new RuntimeException(e);
    }
    if (time1 != 0) {
      startTime = time0;
      endTime = time1;
    } else {
      endTime = time0;
    }
    return i;
  }

  private int parseQueryParams(final String path, int i) {
    if (i >= path.length()) {
      return i;
    }
    StringBuilder query = new StringBuilder();
    try {
      query.append(URLDecoder.decode(path.substring(i), 
        HConstants.UTF8_ENCODING));
    } catch (UnsupportedEncodingException e) {
      // should not happen
      throw new RuntimeException(e);
    }
    i += query.length();
    int j = 0;
    while (j < query.length()) {
      char c = query.charAt(j);
      if (c != '?' && c != '&') {
        break;
      }
      if (++j > query.length()) {
        throw new IllegalArgumentException("malformed query parameter");
      }
      char what = query.charAt(j);
      if (++j > query.length()) {
        break;
      }
      c = query.charAt(j);
      if (c != '=') {
        throw new IllegalArgumentException("malformed query parameter");
      }
      if (++j > query.length()) {
        break;
      }
      switch (what) {
      case 'm': {
        StringBuilder sb = new StringBuilder();
        while (j <= query.length()) {
          c = query.charAt(i);
          if (c < '0' || c > '9') {
            j--;
            break;
          }
          sb.append(c);
        }
        maxVersions = Integer.valueOf(sb.toString());
      } break;
      case 'n': {
        StringBuilder sb = new StringBuilder();
        while (j <= query.length()) {
          c = query.charAt(i);
          if (c < '0' || c > '9') {
            j--;
            break;
          }
          sb.append(c);
        }
        maxValues = Integer.valueOf(sb.toString());
      } break;
      default:
        throw new IllegalArgumentException("unknown parameter '" + c + "'");
      }
    }
    return i;
  }

  public RowSpec(byte[] startRow, byte[] endRow, byte[][] columns,
      long startTime, long endTime, int maxVersions) {
    this.row = startRow;
    this.endRow = endRow;
    if (columns != null) {
      for (byte[] col: columns) {
        this.columns.add(col);
      }
    }
    this.startTime = startTime;
    this.endTime = endTime;
    this.maxVersions = maxVersions;
  }

  public RowSpec(byte[] startRow, byte[] endRow, Collection<byte[]> columns,
      long startTime, long endTime, int maxVersions) {
    this.row = startRow;
    this.endRow = endRow;
    if (columns != null) {
      this.columns.addAll(columns);
    }
    this.startTime = startTime;
    this.endTime = endTime;
    this.maxVersions = maxVersions;
  }

  public boolean isSingleRow() {
    return endRow == null;
  }

  public int getMaxVersions() {
    return maxVersions;
  }

  public void setMaxVersions(final int maxVersions) {
    this.maxVersions = maxVersions;
  }

  public int getMaxValues() {
    return maxValues;
  }

  public void setMaxValues(final int maxValues) {
    this.maxValues = maxValues;
  }

  public boolean hasColumns() {
    return !columns.isEmpty();
  }

  public byte[] getRow() {
    return row;
  }

  public byte[] getStartRow() {
    return row;
  }

  public boolean hasEndRow() {
    return endRow != null;
  }

  public byte[] getEndRow() {
    return endRow;
  }

  public void addColumn(final byte[] column) {
    columns.add(column);
  }

  public byte[][] getColumns() {
    return columns.toArray(new byte[columns.size()][]);
  }

  public boolean hasTimestamp() {
    return (startTime == 0) && (endTime != Long.MAX_VALUE);
  }

  public long getTimestamp() {
    return endTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(final long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("{startRow => '");
    if (row != null) {
      result.append(Bytes.toString(row));
    }
    result.append("', endRow => '");
    if (endRow != null)  {
      result.append(Bytes.toString(endRow));
    }
    result.append("', columns => [");
    for (byte[] col: columns) {
      result.append(" '");
      result.append(Bytes.toString(col));
      result.append("'");
    }
    result.append(" ], startTime => ");
    result.append(Long.toString(startTime));
    result.append(", endTime => ");
    result.append(Long.toString(endTime));
    result.append(", maxVersions => ");
    result.append(Integer.toString(maxVersions));
    result.append(", maxValues => ");
    result.append(Integer.toString(maxValues));
    result.append("}");
    return result.toString();
  }
}
