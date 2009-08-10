/**
 * Copyright 2008 The Apache Software Foundation
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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;

public class TimestampModel extends AbstractModel {

  @SuppressWarnings("unused")
  private Log LOG = LogFactory.getLog(TimestampModel.class);

  public TimestampModel(HBaseConfiguration conf, HBaseAdmin admin) {
    super.initialize(conf, admin);
  }

  public void delete(byte [] tableName, Delete delete)
  throws HBaseRestException {
    try {
      HTable table = new HTable(this.conf, tableName);
      table.delete(delete);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }
  
  @Deprecated
  public void delete(byte[] tableName, byte[] rowName, long timestamp)
      throws HBaseRestException {
    Delete delete = new Delete(rowName, timestamp, null);
    delete(tableName, delete);
  }
  
  @Deprecated
  public void delete(byte[] tableName, byte[] rowName, byte[][] columns,
      long timestamp) throws HBaseRestException {
    Delete delete  = new Delete(rowName, timestamp, null);
    for(byte [] column : columns) {
      byte [][] famAndQf = KeyValue.parseColumn(column);
      delete.deleteColumn(famAndQf[0], famAndQf[1]);
    }
    delete(tableName, delete);
  }

  public Result get(final byte [] tableName, final Get get)
  throws HBaseRestException {
    try {
      HTable table = new HTable(this.conf, tableName);
      return table.get(get);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }
  
  @Deprecated
  public Cell get(byte[] tableName, byte[] rowName, byte[] columnName,
      long timestamp) throws HBaseRestException {
    Get get = new Get(rowName);
    byte [][] famAndQf = KeyValue.parseColumn(columnName); 
    get.addColumn(famAndQf[0], famAndQf[1]);
    get.setTimeStamp(timestamp);
    return get(tableName, get).getCellValue(famAndQf[0], famAndQf[1]);
  }

  @Deprecated
  public Cell[] get(byte[] tableName, byte[] rowName, byte[] columnName,
      long timestamp, int numVersions) throws IOException, HBaseRestException {
    Get get = new Get(rowName);
    byte [][] famAndQf = KeyValue.parseColumn(columnName); 
    get.addColumn(famAndQf[0], famAndQf[1]);
    get.setTimeStamp(timestamp);
    get.setMaxVersions(numVersions);
    Result result = get(tableName, get);
    List<Cell> cells = new ArrayList<Cell>();
    for(KeyValue kv : result.sorted()) {
      cells.add(new Cell(kv.getValue(), kv.getTimestamp()));
    }
    return cells.toArray(new Cell [0]);
  }

  @Deprecated
  public RowResult get(byte[] tableName, byte[] rowName, byte[][] columns,
      long timestamp) throws HBaseRestException {
    Get get = new Get(rowName);
    for(byte [] column : columns) {
      byte [][] famAndQf = KeyValue.parseColumn(column);
      get.addColumn(famAndQf[0], famAndQf[1]);
    }
    get.setTimeStamp(timestamp);
    return get(tableName, get).getRowResult();
  }

  /**
   * @param tableName
   * @param rowName
   * @param timestamp
   * @return RowResult
   * @throws HBaseRestException
   */
  public RowResult get(byte[] tableName, byte[] rowName, long timestamp)
      throws HBaseRestException {
    Get get = new Get(rowName);
    get.setTimeStamp(timestamp);
    return get(tableName, get).getRowResult();
  }

  public void post(byte[] tableName, byte[] rowName, byte[] columnName,
      long timestamp, byte[] value) throws HBaseRestException {
    try {
      HTable table = new HTable(this.conf, tableName);
      Put put = new Put(rowName);
      put.setTimeStamp(timestamp);
      byte [][] famAndQf = KeyValue.parseColumn(columnName);
      put.add(famAndQf[0], famAndQf[1], value);
      table.put(put);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }
}
