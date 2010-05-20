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
package org.apache.hadoop.hbase.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.InputSplit;

/**
 * A table split corresponds to a key range [low, high)
 */
@Deprecated
public class TableSplit implements InputSplit, Comparable<TableSplit> {
  private byte [] m_tableName;
  private byte [] m_startRow;
  private byte [] m_endRow;
  private String m_regionLocation;

  /** default constructor */
  public TableSplit() {
    this(HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
      HConstants.EMPTY_BYTE_ARRAY, "");
  }

  /**
   * Constructor
   * @param tableName
   * @param startRow
   * @param endRow
   * @param location
   */
  public TableSplit(byte [] tableName, byte [] startRow, byte [] endRow,
      final String location) {
    this.m_tableName = tableName;
    this.m_startRow = startRow;
    this.m_endRow = endRow;
    this.m_regionLocation = location;
  }

  /** @return table name */
  public byte [] getTableName() {
    return this.m_tableName;
  }

  /** @return starting row key */
  public byte [] getStartRow() {
    return this.m_startRow;
  }

  /** @return end row key */
  public byte [] getEndRow() {
    return this.m_endRow;
  }

  /** @return the region's hostname */
  public String getRegionLocation() {
    return this.m_regionLocation;
  }

  public String[] getLocations() {
    return new String[] {this.m_regionLocation};
  }

  public long getLength() {
    // Not clear how to obtain this... seems to be used only for sorting splits
    return 0;
  }

  public void readFields(DataInput in) throws IOException {
    this.m_tableName = Bytes.readByteArray(in);
    this.m_startRow = Bytes.readByteArray(in);
    this.m_endRow = Bytes.readByteArray(in);
    this.m_regionLocation = Bytes.toString(Bytes.readByteArray(in));
  }

  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.m_tableName);
    Bytes.writeByteArray(out, this.m_startRow);
    Bytes.writeByteArray(out, this.m_endRow);
    Bytes.writeByteArray(out, Bytes.toBytes(this.m_regionLocation));
  }

  @Override
  public String toString() {
    return m_regionLocation + ":" +
      Bytes.toStringBinary(m_startRow) + "," + Bytes.toStringBinary(m_endRow);
  }

  public int compareTo(TableSplit o) {
    return Bytes.compareTo(getStartRow(), o.getStartRow());
  }
}