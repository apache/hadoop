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
public class TableSplit implements InputSplit {
  private byte [] m_tableName;
  private byte [] m_startRow;
  private byte [] m_endRow;

  /** default constructor */
  public TableSplit() {
    this(HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
      HConstants.EMPTY_BYTE_ARRAY);
  }

  /**
   * Constructor
   * @param tableName
   * @param startRow
   * @param endRow
   */
  public TableSplit(byte [] tableName, byte [] startRow, byte [] endRow) {
    m_tableName = tableName;
    m_startRow = startRow;
    m_endRow = endRow;
  }

  /** @return table name */
  public byte [] getTableName() {
    return m_tableName;
  }

  /** @return starting row key */
  public byte [] getStartRow() {
    return m_startRow;
  }

  /** @return end row key */
  public byte [] getEndRow() {
    return m_endRow;
  }

  public long getLength() {
    // Not clear how to obtain this... seems to be used only for sorting splits
    return 0;
  }

  public String[] getLocations() {
    // Return a random node from the cluster for now
    return new String[] { };
  }

  public void readFields(DataInput in) throws IOException {
    this.m_tableName = Bytes.readByteArray(in);
    this.m_startRow = Bytes.readByteArray(in);
    this.m_endRow = Bytes.readByteArray(in);
  }

  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.m_tableName);
    Bytes.writeByteArray(out, this.m_startRow);
    Bytes.writeByteArray(out, this.m_endRow);
  }

  @Override
  public String toString() {
    return Bytes.toString(m_tableName) +"," + Bytes.toString(m_startRow) +
      "," + Bytes.toString(m_endRow);
  }
}