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
package org.apache.hadoop.hbase.rest.parser;

import java.util.ArrayList;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.rest.RESTConstants;
import org.apache.hadoop.hbase.rest.descriptors.RowUpdateDescriptor;
import org.apache.hadoop.hbase.rest.descriptors.ScannerDescriptor;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * 
 */
public class JsonRestParser implements IHBaseRestParser {

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.parser.IHBaseRestParser#getTableDescriptor
   * (byte[])
   */
  public HTableDescriptor getTableDescriptor(byte[] input)
      throws HBaseRestException {
    try {
      JSONObject o;
      HTableDescriptor h;
      JSONArray columnDescriptorArray;
      o = new JSONObject(new String(input));
      columnDescriptorArray = o.getJSONArray("column_families");
      h = new HTableDescriptor(o.getString("name"));

      for (int i = 0; i < columnDescriptorArray.length(); i++) {
        JSONObject json_columnDescriptor = columnDescriptorArray
            .getJSONObject(i);
        h.addFamily(this.getColumnDescriptor(json_columnDescriptor));
      }
      return h;
    } catch (Exception e) {
      throw new HBaseRestException(e);
    }
  }

  private HColumnDescriptor getColumnDescriptor(JSONObject jsonObject)
      throws JSONException {
    String strTemp;
    strTemp = jsonObject.getString("name");
    if (strTemp.charAt(strTemp.length() - 1) != ':') {
      strTemp += ":";
    }

    byte[] name = Bytes.toBytes(strTemp);

    int maxVersions;
    HColumnDescriptor.CompressionType cType;
    boolean inMemory;
    boolean blockCacheEnabled;
    int maxValueLength;
    int timeToLive;
    boolean bloomfilter;

    try {
      bloomfilter = jsonObject.getBoolean("bloomfilter");
    } catch (JSONException e) {
      bloomfilter = false;
    }

    try {
      maxVersions = jsonObject.getInt("max_versions");
    } catch (JSONException e) {
      maxVersions = 3;
    }

    try {
      cType = HColumnDescriptor.CompressionType.valueOf(jsonObject
          .getString("compression_type"));
    } catch (JSONException e) {
      cType = HColumnDescriptor.CompressionType.NONE;
    }

    try {
      inMemory = jsonObject.getBoolean("in_memory");
    } catch (JSONException e) {
      inMemory = false;
    }

    try {
      blockCacheEnabled = jsonObject.getBoolean("block_cache_enabled");
    } catch (JSONException e) {
      blockCacheEnabled = false;
    }

    try {
      maxValueLength = jsonObject.getInt("max_value_length");
    } catch (JSONException e) {
      maxValueLength = 2147483647;
    }

    try {
      timeToLive = jsonObject.getInt("time_to_live");
    } catch (JSONException e) {
      timeToLive = Integer.MAX_VALUE;
    }

    return new HColumnDescriptor(name, maxVersions, cType, inMemory,
        blockCacheEnabled, maxValueLength, timeToLive, bloomfilter);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.parser.IHBaseRestParser#getColumnDescriptors
   * (byte[])
   */
  public ArrayList<HColumnDescriptor> getColumnDescriptors(byte[] input)
      throws HBaseRestException {
    ArrayList<HColumnDescriptor> columns = new ArrayList<HColumnDescriptor>();
    try {
      JSONObject o;
      JSONArray columnDescriptorArray;
      o = new JSONObject(new String(input));
      columnDescriptorArray = o.getJSONArray("column_families");

      for (int i = 0; i < columnDescriptorArray.length(); i++) {
        JSONObject json_columnDescriptor = columnDescriptorArray
            .getJSONObject(i);
        columns.add(this.getColumnDescriptor(json_columnDescriptor));
      }
    } catch (JSONException e) {
      throw new HBaseRestException("Error Parsing json input", e);
    }

    return columns;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.parser.IHBaseRestParser#getScannerDescriptor
   * (byte[])
   */
  public ScannerDescriptor getScannerDescriptor(byte[] input)
      throws HBaseRestException {
    JSONObject scannerDescriptor;
    JSONArray columnArray;

    byte[][] columns = null;
    long timestamp;
    byte[] startRow;
    byte[] stopRow;
    String filters;

    try {
      scannerDescriptor = new JSONObject(new String(input));

      columnArray = scannerDescriptor.optJSONArray(RESTConstants.COLUMNS);
      timestamp = scannerDescriptor.optLong(RESTConstants.SCANNER_TIMESTAMP);
      startRow = Bytes.toBytes(scannerDescriptor.optString(
          RESTConstants.SCANNER_START_ROW, ""));
      stopRow = Bytes.toBytes(scannerDescriptor.optString(
          RESTConstants.SCANNER_STOP_ROW, ""));
      filters = scannerDescriptor.optString(RESTConstants.SCANNER_FILTER);

      if (columnArray != null) {
        columns = new byte[columnArray.length()][];
        for (int i = 0; i < columnArray.length(); i++) {
          columns[i] = Bytes.toBytes(columnArray.optString(i));
        }
      }

      return new ScannerDescriptor(columns, timestamp, startRow, stopRow,
          filters);
    } catch (JSONException e) {
      throw new HBaseRestException("error parsing json string", e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.parser.IHBaseRestParser#getRowUpdateDescriptor
   * (byte[], byte[][])
   */
  public RowUpdateDescriptor getRowUpdateDescriptor(byte[] input,
      byte[][] pathSegments) throws HBaseRestException {

    RowUpdateDescriptor rud = new RowUpdateDescriptor();
    JSONArray a;

    rud.setTableName(Bytes.toString(pathSegments[0]));
    rud.setRowName(Bytes.toString(pathSegments[2]));

    try {
      JSONObject updateObject = new JSONObject(new String(input));
      a = updateObject.getJSONArray(RESTConstants.COLUMNS);
      for (int i = 0; i < a.length(); i++) {
        rud.getColVals().put(
            Bytes.toBytes(a.getJSONObject(i).getString(RESTConstants.NAME)),
            org.apache.hadoop.hbase.util.Base64.decode(a.getJSONObject(i)
                .getString(RESTConstants.VALUE)));
      }
    } catch (JSONException e) {
      throw new HBaseRestException("Error parsing row update json", e);
    }
    return rud;
  }

}
