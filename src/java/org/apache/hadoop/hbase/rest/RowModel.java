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
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.rest.descriptors.TimestampsDescriptor;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;

public class RowModel extends AbstractModel {

  @SuppressWarnings("unused")
  private Log LOG = LogFactory.getLog(RowModel.class);

  public RowModel(HBaseConfiguration conf, HBaseAdmin admin) {
    super.initialize(conf, admin);
  }

  @Deprecated
  public RowResult get(byte[] tableName, byte[] rowName)
      throws HBaseRestException {
    return get(tableName, new Get(rowName)).getRowResult();
  }

  public Result get(byte[] tableName, Get get)
  throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      return table.get(get);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }
  
  @Deprecated
  public RowResult get(byte[] tableName, byte[] rowName, byte[][] columns)
      throws HBaseRestException {
    Get get = new Get(rowName);
    for(byte [] column : columns) {
      byte [][] famAndQf = KeyValue.parseColumn(column);
      get.addColumn(famAndQf[0], famAndQf[1]);
    }
    return get(tableName, get).getRowResult();
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
  
  @Deprecated
  public RowResult get(byte[] tableName, byte[] rowName, long timestamp)
      throws HBaseRestException {
    Get get = new Get(rowName);
    get.setTimeStamp(timestamp);
    return get(tableName, get).getRowResult();
  }

  public TimestampsDescriptor getTimestamps(
      @SuppressWarnings("unused") byte[] tableName,
      @SuppressWarnings("unused") byte[] rowName) throws HBaseRestException {
    // try {
    // TimestampsDescriptor tsd = new TimestampsDescriptor();
    // HTable table = new HTable(tableName);
    // RowResult row = table.getRow(rowName);

    throw new HBaseRestException("operation currently unsupported");

    // } catch (IOException e) {
    // throw new HBaseRestException("Error finding timestamps for row: "
    // + Bytes.toString(rowName), e);
    // }

  }

  public void post(byte[] tableName, Put put) throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      table.put(put);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public void post(byte[] tableName, List<Put> puts)
      throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      table.put(puts);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }
  
  @Deprecated
  public void delete(byte[] tableName, byte[] rowName)
      throws HBaseRestException {
    Delete delete = new Delete(rowName);
    delete(tableName, delete);
  }

  @Deprecated
  public void delete(byte[] tableName, byte[] rowName, byte[][] columns)
  throws HBaseRestException {
    Delete delete = new Delete(rowName);
    for(byte [] column : columns) {
      byte [][] famAndQf = KeyValue.parseColumn(column);
      delete.deleteColumn(famAndQf[0], famAndQf[1]);
    }
    delete(tableName, delete);
  }
  
  public void delete(byte[] tableName, Delete delete)
  throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      table.delete(delete);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }
}
