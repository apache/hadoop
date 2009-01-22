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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
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

  public RowResult get(byte[] tableName, byte[] rowName)
      throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      return table.getRow(rowName);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public RowResult get(byte[] tableName, byte[] rowName, byte[][] columns)
      throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      return table.getRow(rowName, columns);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public RowResult get(byte[] tableName, byte[] rowName, byte[][] columns,
      long timestamp) throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      return table.getRow(rowName, columns, timestamp);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public RowResult get(byte[] tableName, byte[] rowName, long timestamp)
      throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      return table.getRow(rowName, timestamp);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
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

  public void post(byte[] tableName, BatchUpdate b) throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      table.commit(b);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public void post(byte[] tableName, List<BatchUpdate> b)
      throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      table.commit(b);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public void delete(byte[] tableName, byte[] rowName)
      throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      table.deleteAll(rowName);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public void delete(byte[] tableName, byte[] rowName, byte[][] columns) throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      for (byte[] column : columns) {
        table.deleteAll(rowName, column);
      }
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }
}
