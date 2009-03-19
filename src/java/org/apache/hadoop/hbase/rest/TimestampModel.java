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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
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

  public void delete(byte[] tableName, byte[] rowName, long timestamp)
      throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      table.deleteAll(rowName, timestamp);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public void delete(byte[] tableName, byte[] rowName, byte[][] columns,
      long timestamp) throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      for (byte[] column : columns) {
        table.deleteAll(rowName, column, timestamp);
      }
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public Cell get(byte[] tableName, byte[] rowName, byte[] columnName,
      long timestamp) throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      return table.get(rowName, columnName, timestamp, 1)[0];
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public Cell[] get(byte[] tableName, byte[] rowName, byte[] columnName,
      long timestamp, int numVersions) throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      return table.get(rowName, columnName, timestamp, numVersions);
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

  /**
   * @param tableName
   * @param rowName
   * @param timestamp
   * @return RowResult
   * @throws HBaseRestException
   */
  public RowResult get(byte[] tableName, byte[] rowName, long timestamp)
      throws HBaseRestException {
    try {
      HTable table = new HTable(tableName);
      return table.getRow(rowName, timestamp);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  public void post(byte[] tableName, byte[] rowName, byte[] columnName,
      long timestamp, byte[] value) throws HBaseRestException {
    try {
      HTable table;
      BatchUpdate b;

      table = new HTable(tableName);
      b = new BatchUpdate(rowName, timestamp);

      b.put(columnName, value);
      table.commit(b);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }
}
