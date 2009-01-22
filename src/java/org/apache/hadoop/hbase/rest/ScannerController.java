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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.filter.RowFilterSet;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.hbase.rest.descriptors.ScannerDescriptor;
import org.apache.hadoop.hbase.rest.descriptors.ScannerIdentifier;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.rest.filter.FilterFactory;
import org.apache.hadoop.hbase.rest.parser.IHBaseRestParser;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 */
public class ScannerController extends AbstractController {

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.AbstractController#delete(org.apache.hadoop
   * .hbase.rest.Status, byte[][], java.util.Map)
   */
  @Override
  public void delete(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap) throws HBaseRestException {
    ScannerModel innerModel = this.getModel();
    if (pathSegments.length == 3
        && Bytes.toString(pathSegments[1]).toLowerCase().equals(
            RESTConstants.SCANNER)) {
      // get the scannerId
      Integer scannerId = null;
      String scannerIdString = new String(pathSegments[2]);
      if (!Pattern.matches("^\\d+$", scannerIdString)) {
        throw new HBaseRestException(
            "the scannerid in the path and must be an integer");
      }
      scannerId = Integer.parseInt(scannerIdString);

      try {
        innerModel.scannerClose(scannerId);
        s.setOK();
      } catch (HBaseRestException e) {
        s.setNotFound();
      }
    } else {
      s.setBadRequest("invalid query");
    }
    s.respond();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.AbstractController#generateModel(org.apache
   * .hadoop.hbase.HBaseConfiguration,
   * org.apache.hadoop.hbase.client.HBaseAdmin)
   */
  @Override
  protected AbstractModel generateModel(HBaseConfiguration conf, HBaseAdmin a) {
    return new ScannerModel(conf, a);
  }

  protected ScannerModel getModel() {
    return (ScannerModel) model;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.AbstractController#get(org.apache.hadoop.hbase
   * .rest.Status, byte[][], java.util.Map)
   */
  @Override
  public void get(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap) throws HBaseRestException {

    s.setBadRequest("invalid query");
    s.respond();

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.AbstractController#post(org.apache.hadoop.
   * hbase.rest.Status, byte[][], java.util.Map, byte[],
   * org.apache.hadoop.hbase.rest.parser.IHBaseRestParser)
   */
  @Override
  public void post(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap, byte[] input, IHBaseRestParser parser)
      throws HBaseRestException {
    ScannerModel innerModel = this.getModel();
    byte[] tableName;
    tableName = pathSegments[0];

    // Otherwise we interpret this request as a scanner request.
    if (pathSegments.length == 2
        && Bytes.toString(pathSegments[1]).toLowerCase().equals(
            RESTConstants.SCANNER)) { // new scanner request
      ScannerDescriptor sd = this.getScannerDescriptor(queryMap);
      s.setScannerCreated(createScanner(innerModel, tableName, sd));
    } else if (pathSegments.length == 3
        && Bytes.toString(pathSegments[1]).toLowerCase().equals(
            RESTConstants.SCANNER)) { // open scanner request
      // first see if the limit variable is present
      Long numRows = 1L;
      String[] numRowsString = queryMap.get(RESTConstants.LIMIT);
      if (numRowsString != null && Pattern.matches("^\\d+$", numRowsString[0])) {
        numRows = Long.parseLong(numRowsString[0]);
      }
      // get the scannerId
      Integer scannerId = null;
      String scannerIdString = new String(pathSegments[2]);
      if (!Pattern.matches("^\\d+$", scannerIdString)) {
        throw new HBaseRestException(
            "the scannerid in the path and must be an integer");
      }
      scannerId = Integer.parseInt(scannerIdString);

      try {
        s.setOK(innerModel.scannerGet(scannerId, numRows));
      } catch (HBaseRestException e) {
        s.setNotFound();
      }
    } else {
      s.setBadRequest("Unknown Query.");
    }
    s.respond();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.AbstractController#put(org.apache.hadoop.hbase
   * .rest.Status, byte[][], java.util.Map, byte[],
   * org.apache.hadoop.hbase.rest.parser.IHBaseRestParser)
   */
  @Override
  public void put(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap, byte[] input, IHBaseRestParser parser)
      throws HBaseRestException {

    s.setBadRequest("invalid query");
    s.respond();

  }

  private ScannerDescriptor getScannerDescriptor(Map<String, String[]> queryMap) {
    long timestamp = 0L;
    byte[] startRow = null;
    byte[] stopRow = null;
    String filters = null;

    String[] timeStampString = queryMap.get(RESTConstants.SCANNER_TIMESTAMP);
    if (timeStampString != null && timeStampString.length == 1) {
      timestamp = Long.parseLong(timeStampString[0]);
    }

    String[] startRowString = queryMap.get(RESTConstants.SCANNER_START_ROW);
    if (startRowString != null && startRowString.length == 1) {
      startRow = Bytes.toBytes(startRowString[0]);
    }

    String[] stopRowString = queryMap.get(RESTConstants.SCANNER_STOP_ROW);
    if (stopRowString != null && stopRowString.length == 1) {
      stopRow = Bytes.toBytes(stopRowString[0]);
    }

    String[] filtersStrings = queryMap.get(RESTConstants.SCANNER_FILTER);
    if (filtersStrings != null && filtersStrings.length > 0) {
      filters = "";
      for (@SuppressWarnings("unused")
      String filter : filtersStrings) {
        // TODO filters are not hooked up yet... And the String should probably
        // be changed to a set
      }
    }
    return new ScannerDescriptor(this.getColumnsFromQueryMap(queryMap),
        timestamp, startRow, stopRow, filters);
  }

  protected ScannerIdentifier createScanner(ScannerModel innerModel,
      byte[] tableName, ScannerDescriptor scannerDescriptor)
      throws HBaseRestException {

    RowFilterInterface filterSet = null;

    // Might want to change this. I am doing this so that I can use
    // a switch statement that is more efficient.
    int switchInt = 0;
    if (scannerDescriptor.getColumns() != null
        && scannerDescriptor.getColumns().length > 0) {
      switchInt += 1;
    }
    switchInt += (scannerDescriptor.getTimestamp() != 0L) ? (1 << 1) : 0;
    switchInt += (scannerDescriptor.getStartRow().length > 0) ? (1 << 2) : 0;
    switchInt += (scannerDescriptor.getStopRow().length > 0) ? (1 << 3) : 0;
    if (scannerDescriptor.getFilters() != null
        && !scannerDescriptor.getFilters().equals("")) {
      switchInt += (scannerDescriptor.getFilters() != null) ? (1 << 4) : 0;
      filterSet = unionFilters(scannerDescriptor.getFilters());
    }

    return scannerSwitch(switchInt, innerModel, tableName, scannerDescriptor
        .getColumns(), scannerDescriptor.getTimestamp(), scannerDescriptor
        .getStartRow(), scannerDescriptor.getStopRow(), filterSet);
  }

  public ScannerIdentifier scannerSwitch(int switchInt,
      ScannerModel innerModel, byte[] tableName, byte[][] columns,
      long timestamp, byte[] startRow, byte[] stopRow,
      RowFilterInterface filterSet) throws HBaseRestException {
    switch (switchInt) {
    case 0:
      return innerModel.scannerOpen(tableName);
    case 1:
      return innerModel.scannerOpen(tableName, columns);
    case 2:
      return innerModel.scannerOpen(tableName, timestamp);
    case 3:
      return innerModel.scannerOpen(tableName, columns, timestamp);
    case 4:
      return innerModel.scannerOpen(tableName, startRow);
    case 5:
      return innerModel.scannerOpen(tableName, columns, startRow);
    case 6:
      return innerModel.scannerOpen(tableName, startRow, timestamp);
    case 7:
      return innerModel.scannerOpen(tableName, columns, startRow, timestamp);
    case 8:
      return innerModel.scannerOpen(tableName, getStopRow(stopRow));
    case 9:
      return innerModel.scannerOpen(tableName, columns, getStopRow(stopRow));
    case 10:
      return innerModel.scannerOpen(tableName, timestamp, getStopRow(stopRow));
    case 11:
      return innerModel.scannerOpen(tableName, columns, timestamp,
          getStopRow(stopRow));
    case 12:
      return innerModel.scannerOpen(tableName, startRow, getStopRow(stopRow));
    case 13:
      return innerModel.scannerOpen(tableName, columns, startRow,
          getStopRow(stopRow));
    case 14:
      return innerModel.scannerOpen(tableName, startRow, timestamp,
          getStopRow(stopRow));
    case 15:
      return innerModel.scannerOpen(tableName, columns, startRow, timestamp,
          getStopRow(stopRow));
    case 16:
      return innerModel.scannerOpen(tableName, filterSet);
    case 17:
      return innerModel.scannerOpen(tableName, columns, filterSet);
    case 18:
      return innerModel.scannerOpen(tableName, timestamp, filterSet);
    case 19:
      return innerModel.scannerOpen(tableName, columns, timestamp, filterSet);
    case 20:
      return innerModel.scannerOpen(tableName, startRow, filterSet);
    case 21:
      return innerModel.scannerOpen(tableName, columns, startRow, filterSet);
    case 22:
      return innerModel.scannerOpen(tableName, startRow, timestamp, filterSet);
    case 23:
      return innerModel.scannerOpen(tableName, columns, startRow, timestamp,
          filterSet);
    case 24:
      return innerModel.scannerOpen(tableName, getStopRowUnionFilter(stopRow,
          filterSet));
    case 25:
      return innerModel.scannerOpen(tableName, columns, getStopRowUnionFilter(
          stopRow, filterSet));
    case 26:
      return innerModel.scannerOpen(tableName, timestamp,
          getStopRowUnionFilter(stopRow, filterSet));
    case 27:
      return innerModel.scannerOpen(tableName, columns, timestamp,
          getStopRowUnionFilter(stopRow, filterSet));
    case 28:
      return innerModel.scannerOpen(tableName, startRow, getStopRowUnionFilter(
          stopRow, filterSet));
    case 29:
      return innerModel.scannerOpen(tableName, columns, startRow,
          getStopRowUnionFilter(stopRow, filterSet));
    case 30:
      return innerModel.scannerOpen(tableName, startRow, timestamp,
          getStopRowUnionFilter(stopRow, filterSet));
    case 31:
      return innerModel.scannerOpen(tableName, columns, startRow, timestamp,
          getStopRowUnionFilter(stopRow, filterSet));
    default:
      return null;
    }
  }

  protected RowFilterInterface getStopRow(byte[] stopRow) {
    return new WhileMatchRowFilter(new StopRowFilter(stopRow));
  }

  protected RowFilterInterface getStopRowUnionFilter(byte[] stopRow,
      RowFilterInterface filter) {
    Set<RowFilterInterface> filterSet = new HashSet<RowFilterInterface>();
    filterSet.add(getStopRow(stopRow));
    filterSet.add(filter);
    return new RowFilterSet(filterSet);
  }

  /**
   * Given a list of filters in JSON string form, returns a RowSetFilter that
   * returns true if all input filters return true on a Row (aka an AND
   * statement).
   * 
   * @param filters
   *          array of input filters in a JSON String
   * @return RowSetFilter with all input filters in an AND Statement
   * @throws org.apache.hadoop.hbase.rest.exception.HBaseRestException
   */
  protected RowFilterInterface unionFilters(String filters)
      throws HBaseRestException {
    FilterFactory f = RESTConstants.filterFactories.get("RowFilterSet");
    return f.getFilterFromJSON(filters);
  }

}
