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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.rest.serializer.IRestSerializer;
import org.apache.hadoop.hbase.rest.serializer.ISerializable;
import org.apache.hadoop.hbase.util.Bytes;

import agilejson.TOJSON;

public class TableModel extends AbstractModel {

  @SuppressWarnings("unused")
  private Log LOG = LogFactory.getLog(TableModel.class);

  public TableModel(HBaseConfiguration config, HBaseAdmin admin) {
    super.initialize(config, admin);
  }

  // Get Methods
  public RowResult[] get(byte[] tableName) throws HBaseRestException {
    return get(tableName, getColumns(tableName));
  }

  /**
   * Returns all cells from all rows from the given table in the given columns.
   * The output is in the order that the columns are given.
   * 
   * @param tableName
   *          table name
   * @param columnNames
   *          column names
   * @return resultant rows
   * @throws org.apache.hadoop.hbase.rest.exception.HBaseRestException
   */
  public RowResult[] get(byte[] tableName, byte[][] columnNames)
      throws HBaseRestException {
    try {
      ArrayList<RowResult> a = new ArrayList<RowResult>();
      HTable table = new HTable(tableName);

      Scanner s = table.getScanner(columnNames);
      RowResult r;

      while ((r = s.next()) != null) {
        a.add(r);
      }

      return a.toArray(new RowResult[0]);
    } catch (Exception e) {
      throw new HBaseRestException(e);
    }
  }

  protected boolean doesTableExist(byte[] tableName) throws HBaseRestException {
    try {
      return this.admin.tableExists(tableName);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }
  
  protected void disableTable(byte[] tableName) throws HBaseRestException {
    try {
      this.admin.disableTable(tableName);
    } catch (IOException e) {
      throw new HBaseRestException("IOException disabling table", e);
    }
  }
  
  protected void enableTable(byte[] tableName) throws HBaseRestException {
    try {
      this.admin.enableTable(tableName);
    } catch (IOException e) {
      throw new HBaseRestException("IOException enabiling table", e);
    }
  }

  public boolean updateTable(String tableName,
      ArrayList<HColumnDescriptor> columns) throws HBaseRestException {
    HTableDescriptor htc = null;
    try {
      htc = this.admin.getTableDescriptor(tableName);
    } catch (IOException e) {
      throw new HBaseRestException("Table does not exist");
    }

    for (HColumnDescriptor column : columns) {
      if (htc.hasFamily(Bytes.toBytes(column.getNameAsString()))) {
        try {
          this.admin.disableTable(tableName);
          this.admin.modifyColumn(tableName, column.getNameAsString(), column);
          this.admin.enableTable(tableName);
        } catch (IOException e) {
          throw new HBaseRestException("unable to modify column "
              + column.getNameAsString(), e);
        }
      } else {
        try {
          this.admin.disableTable(tableName);
          this.admin.addColumn(tableName, column);
          this.admin.enableTable(tableName);
        } catch (IOException e) {
          throw new HBaseRestException("unable to add column "
              + column.getNameAsString(), e);
        }
      }
    }

    return true;

  }

  /**
   * Get table metadata.
   * 
   * @param request
   * @param response
   * @param tableName
   * @throws IOException
   */
  public HTableDescriptor getTableMetadata(final String tableName)
      throws HBaseRestException {
    HTableDescriptor descriptor = null;
    try {
      HTableDescriptor[] tables = this.admin.listTables();
      for (int i = 0; i < tables.length; i++) {
        if (Bytes.toString(tables[i].getName()).equals(tableName)) {
          descriptor = tables[i];
          break;
        }
      }
      if (descriptor == null) {

      } else {
        return descriptor;
      }
    } catch (IOException e) {
      throw new HBaseRestException("error processing request.");
    }
    return descriptor;
  }

  /**
   * Return region offsets.
   * 
   * @param request
   * @param response
   */
  public Regions getTableRegions(final String tableName)
      throws HBaseRestException {
    try {
      HTable table = new HTable(this.conf, tableName);
      // Presumption is that this.table has already been focused on target
      // table.
      Regions regions = new Regions(table.getStartKeys());
      // Presumption is that this.table has already been set against target
      // table
      return regions;
    } catch (IOException e) {
      throw new HBaseRestException("Unable to get regions from table");
    }
  }

  // Post Methods
  /**
   * Creates table tableName described by the json in input.
   * 
   * @param tableName
   *          table name
   * @param htd
   *          HBaseTableDescriptor for the table to be created
   * 
   * @return true if operation does not fail due to a table with the given
   *         tableName not existing.
   * @throws org.apache.hadoop.hbase.rest.exception.HBaseRestException
   */
  public boolean post(byte[] tableName, HTableDescriptor htd)
      throws HBaseRestException {
    try {
      if (!this.admin.tableExists(tableName)) {
        this.admin.createTable(htd);
        return true;
      }
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
    return false;
  }

  /**
   * Deletes table tableName
   * 
   * @param tableName
   *          name of the table.
   * @return true if table exists and deleted, false if table does not exist.
   * @throws org.apache.hadoop.hbase.rest.exception.HBaseRestException
   */
  public boolean delete(byte[] tableName) throws HBaseRestException {
    try {
      if (this.admin.tableExists(tableName)) {
        this.admin.disableTable(tableName);
        this.admin.deleteTable(tableName);
        return true;
      }
      return false;
    } catch (Exception e) {
      throw new HBaseRestException(e);
    }
  }

  public class Regions implements ISerializable {
    byte[][] regionKey;

    public Regions(byte[][] bs) {
      super();
      this.regionKey = bs;
    }

    @SuppressWarnings("unused")
    private Regions() {
    }

    /**
     * @return the regionKey
     */
    @TOJSON(fieldName = "region")
    public byte[][] getRegionKey() {
      return regionKey;
    }

    /**
     * @param regionKey
     *          the regionKey to set
     */
    public void setRegionKey(byte[][] regionKey) {
      this.regionKey = regionKey;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.hbase.rest.xml.IOutputXML#toXML()
     */
    public void restSerialize(IRestSerializer serializer)
        throws HBaseRestException {
      serializer.serializeRegionData(this);
    }
  }
}
