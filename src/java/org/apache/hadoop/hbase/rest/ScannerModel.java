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
package org.apache.hadoop.hbase.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.rest.descriptors.ScannerIdentifier;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;

/**
 * 
 */
public class ScannerModel extends AbstractModel {

  public ScannerModel(HBaseConfiguration config, HBaseAdmin admin) {
    super.initialize(config, admin);
  }
  
  //
  // Normal Scanner
  //
  protected static class ScannerMaster {

    protected static final Map<Integer, Scanner> scannerMap = new ConcurrentHashMap<Integer, Scanner>();
    protected static final AtomicInteger nextScannerId = new AtomicInteger(1);

    public Integer addScanner(Scanner scanner) {
      Integer i = Integer.valueOf(nextScannerId.getAndIncrement());
      scannerMap.put(i, scanner);
      return i;
    }

    public Scanner getScanner(Integer id) {
      return scannerMap.get(id);
    }

    public Scanner removeScanner(Integer id) {
      return scannerMap.remove(id);
    }

    /**
     * @param id
     *          id of scanner to close
     */
    public void scannerClose(Integer id) {
      Scanner s = scannerMap.remove(id);
      s.close();
    }
  }

  protected static final ScannerMaster scannerMaster = new ScannerMaster();

  /**
   * returns the next numResults RowResults from the Scaner mapped to Integer
   * id. If the end of the table is reached, the scanner is closed and all
   * succesfully retrieved rows are returned.
   * 
   * @param id
   *          id target scanner is mapped to.
   * @param numRows
   *          number of results to return.
   * @return all successfully retrieved rows.
   * @throws org.apache.hadoop.hbase.rest.exception.HBaseRestException
   */
  public RowResult[] scannerGet(Integer id, Long numRows)
      throws HBaseRestException {
    try {
      ArrayList<RowResult> a;
      Scanner s;
      RowResult r;

      a = new ArrayList<RowResult>();
      s = scannerMaster.getScanner(id);

      if (s == null) {
        throw new HBaseRestException("ScannerId: " + id
            + " is unavailable.  Please create a new scanner");
      }

      for (int i = 0; i < numRows; i++) {
        if ((r = s.next()) != null) {
          a.add(r);
        } else {
          scannerMaster.scannerClose(id);
          break;
        }
      }

      return a.toArray(new RowResult[0]);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  /**
   * Returns all rows inbetween the scanners current position and the end of the
   * table.
   * 
   * @param id
   *          id of scanner to use
   * @return all rows till end of table
   * @throws org.apache.hadoop.hbase.rest.exception.HBaseRestException
   */
  public RowResult[] scannerGet(Integer id) throws HBaseRestException {
    try {
      ArrayList<RowResult> a;
      Scanner s;
      RowResult r;

      a = new ArrayList<RowResult>();
      s = scannerMaster.getScanner(id);

      while ((r = s.next()) != null) {
        a.add(r);
      }

      scannerMaster.scannerClose(id);

      return a.toArray(new RowResult[0]);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public boolean scannerClose(Integer id) throws HBaseRestException {
    Scanner s = scannerMaster.removeScanner(id);

    if (s == null) {
      throw new HBaseRestException("Scanner id: " + id + " does not exist");
    }
    return true;
  }

  // Scanner Open Methods
  // No Columns
  public ScannerIdentifier scannerOpen(byte[] tableName)
      throws HBaseRestException {
    return scannerOpen(tableName, getColumns(tableName));
  }

  public ScannerIdentifier scannerOpen(byte[] tableName, long timestamp)
      throws HBaseRestException {
    return scannerOpen(tableName, getColumns(tableName), timestamp);
  }

  public ScannerIdentifier scannerOpen(byte[] tableName, byte[] startRow)
      throws HBaseRestException {
    return scannerOpen(tableName, getColumns(tableName), startRow);
  }

  public ScannerIdentifier scannerOpen(byte[] tableName, byte[] startRow,
      long timestamp) throws HBaseRestException {
    return scannerOpen(tableName, getColumns(tableName), startRow, timestamp);
  }

  public ScannerIdentifier scannerOpen(byte[] tableName,
      RowFilterInterface filter) throws HBaseRestException {
    return scannerOpen(tableName, getColumns(tableName), filter);
  }

  public ScannerIdentifier scannerOpen(byte[] tableName, long timestamp,
      RowFilterInterface filter) throws HBaseRestException {
    return scannerOpen(tableName, getColumns(tableName), timestamp, filter);
  }

  public ScannerIdentifier scannerOpen(byte[] tableName, byte[] startRow,
      RowFilterInterface filter) throws HBaseRestException {
    return scannerOpen(tableName, getColumns(tableName), startRow, filter);
  }

  public ScannerIdentifier scannerOpen(byte[] tableName, byte[] startRow,
      long timestamp, RowFilterInterface filter) throws HBaseRestException {
    return scannerOpen(tableName, getColumns(tableName), startRow, timestamp,
        filter);
  }

  // With Columns
  public ScannerIdentifier scannerOpen(byte[] tableName, byte[][] columns,
      long timestamp) throws HBaseRestException {
    try {
      HTable table;
      table = new HTable(tableName);
      return new ScannerIdentifier(scannerMaster.addScanner(table.getScanner(
          columns, HConstants.EMPTY_START_ROW, timestamp)));
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public ScannerIdentifier scannerOpen(byte[] tableName, byte[][] columns)
      throws HBaseRestException {
    return scannerOpen(tableName, columns, HConstants.LATEST_TIMESTAMP);
  }

  public ScannerIdentifier scannerOpen(byte[] tableName, byte[][] columns,
      byte[] startRow, long timestamp) throws HBaseRestException {
    try {
      HTable table;
      table = new HTable(tableName);
      return new ScannerIdentifier(scannerMaster.addScanner(table.getScanner(
          columns, startRow, timestamp)));
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public ScannerIdentifier scannerOpen(byte[] tableName, byte[][] columns,
      byte[] startRow) throws HBaseRestException {
    return scannerOpen(tableName, columns, startRow,
        HConstants.LATEST_TIMESTAMP);
  }

  public ScannerIdentifier scannerOpen(byte[] tableName, byte[][] columns,
      long timestamp, RowFilterInterface filter) throws HBaseRestException {
    try {
      HTable table;
      table = new HTable(tableName);
      return new ScannerIdentifier(scannerMaster.addScanner(table.getScanner(
          columns, HConstants.EMPTY_START_ROW, timestamp, filter)));
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public ScannerIdentifier scannerOpen(byte[] tableName, byte[][] columns,
      RowFilterInterface filter) throws HBaseRestException {
    return scannerOpen(tableName, columns, HConstants.LATEST_TIMESTAMP, filter);
  }

  public ScannerIdentifier scannerOpen(byte[] tableName, byte[][] columns,
      byte[] startRow, long timestamp, RowFilterInterface filter)
      throws HBaseRestException {
    try {
      HTable table;
      table = new HTable(tableName);
      return new ScannerIdentifier(scannerMaster.addScanner(table.getScanner(
          columns, startRow, timestamp, filter)));
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public ScannerIdentifier scannerOpen(byte[] tableName, byte[][] columns,
      byte[] startRow, RowFilterInterface filter) throws HBaseRestException {
    return scannerOpen(tableName, columns, startRow,
        HConstants.LATEST_TIMESTAMP, filter);
  }
  
}
