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
package org.apache.hadoop.hbase.rest.descriptors;

/**
 * 
 */
public class ScannerDescriptor {
  byte[][] columns;
  long timestamp;
  byte[] startRow;
  byte[] stopRow;
  String filters;

  /**
   * @param columns
   * @param timestamp
   * @param startRow
   * @param stopRow
   * @param filters
   */
  public ScannerDescriptor(byte[][] columns, long timestamp, byte[] startRow,
      byte[] stopRow, String filters) {
    super();
    this.columns = columns;
    this.timestamp = timestamp;
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.filters = filters;
    
    if(this.startRow == null) {
      this.startRow = new byte[0];
    }
    if(this.stopRow == null) {
      this.stopRow = new byte[0];
    }
  }

  /**
   * @return the columns
   */
  public byte[][] getColumns() {
    return columns;
  }

  /**
   * @param columns
   *          the columns to set
   */
  public void setColumns(byte[][] columns) {
    this.columns = columns;
  }

  /**
   * @return the timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @param timestamp
   *          the timestamp to set
   */
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * @return the startRow
   */
  public byte[] getStartRow() {
    return startRow;
  }

  /**
   * @param startRow
   *          the startRow to set
   */
  public void setStartRow(byte[] startRow) {
    this.startRow = startRow;
  }

  /**
   * @return the stopRow
   */
  public byte[] getStopRow() {
    return stopRow;
  }

  /**
   * @param stopRow
   *          the stopRow to set
   */
  public void setStopRow(byte[] stopRow) {
    this.stopRow = stopRow;
  }

  /**
   * @return the filters
   */
  public String getFilters() {
    return filters;
  }

  /**
   * @param filters
   *          the filters to set
   */
  public void setFilters(String filters) {
    this.filters = filters;
  }
}