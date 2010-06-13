/**
 * Copyright 2009 The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

import java.io.IOException;

/**
 * Scanner class that contains the <code>.META.</code> table scanning logic
 * and uses a Retryable scanner. Provided visitors will be called
 * for each row.
 */
class MetaScanner {

  /**
   * Scans the meta table and calls a visitor on each RowResult and uses a empty
   * start row value as table name.
   *
   * @param configuration conf
   * @param visitor A custom visitor
   * @throws IOException e
   */
  public static void metaScan(Configuration configuration,
      MetaScannerVisitor visitor)
  throws IOException {
    metaScan(configuration, visitor, HConstants.EMPTY_START_ROW);
  }

  /**
   * Scans the meta table and calls a visitor on each RowResult. Uses a table
   * name to locate meta regions.
   *
   * @param configuration config
   * @param visitor visitor object
   * @param tableName table name
   * @throws IOException e
   */
  public static void metaScan(Configuration configuration,
      MetaScannerVisitor visitor, byte[] tableName)
  throws IOException {
    metaScan(configuration, visitor, tableName, null, Integer.MAX_VALUE);
  }

  /**
   * Scans the meta table and calls a visitor on each RowResult. Uses a table
   * name and a row name to locate meta regions. And it only scans at most
   * <code>rowLimit</code> of rows.
   *
   * @param configuration HBase configuration.
   * @param visitor Visitor object.
   * @param tableName User table name.
   * @param row Name of the row at the user table. The scan will start from
   * the region row where the row resides.
   * @param rowLimit Max of processed rows. If it is less than 0, it
   * will be set to default value <code>Integer.MAX_VALUE</code>.
   * @throws IOException e
   */
  public static void metaScan(Configuration configuration,
      MetaScannerVisitor visitor, byte[] tableName, byte[] row,
      int rowLimit)
  throws IOException {
    int rowUpperLimit = rowLimit > 0 ? rowLimit: Integer.MAX_VALUE;

    HConnection connection = HConnectionManager.getConnection(configuration);
    byte [] startRow = tableName == null || tableName.length == 0 ?
        HConstants.EMPTY_START_ROW :
          HRegionInfo.createRegionName(tableName, row, HConstants.ZEROES,
              false);

    // if row is not null, we want to use the startKey of the row's region as
    // the startRow for the meta scan.
    if (row != null) {
      HTable metaTable = new HTable(HConstants.META_TABLE_NAME);
      Result startRowResult = metaTable.getRowOrBefore(startRow,
          HConstants.CATALOG_FAMILY);
      if (startRowResult == null) {
        throw new TableNotFoundException("Cannot find row in .META. for table: "
            + Bytes.toString(tableName) + ", row=" + Bytes.toString(startRow));
      }
      byte[] value = startRowResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.REGIONINFO_QUALIFIER);
      if (value == null || value.length == 0) {
        throw new IOException("HRegionInfo was null or empty in Meta for " +
          Bytes.toString(tableName) + ", row=" + Bytes.toString(startRow));
      }
      HRegionInfo regionInfo = Writables.getHRegionInfo(value);

      byte[] rowBefore = regionInfo.getStartKey();
      startRow = HRegionInfo.createRegionName(tableName, rowBefore,
          HConstants.ZEROES, false);
    }

    // Scan over each meta region
    ScannerCallable callable;
    int rows = Math.min(rowLimit,
        configuration.getInt("hbase.meta.scanner.caching", 100));
    do {
      final Scan scan = new Scan(startRow).addFamily(HConstants.CATALOG_FAMILY);
      callable = new ScannerCallable(connection, HConstants.META_TABLE_NAME,
          scan);
      // Open scanner
      connection.getRegionServerWithRetries(callable);

      int processedRows = 0;
      try {
        callable.setCaching(rows);
        done: do {
          if (processedRows >= rowUpperLimit) {
            break;
          }
          //we have all the rows here
          Result [] rrs = connection.getRegionServerWithRetries(callable);
          if (rrs == null || rrs.length == 0 || rrs[0].size() == 0) {
            break; //exit completely
          }
          for (Result rr : rrs) {
            if (processedRows >= rowUpperLimit) {
              break done;
            }
            if (!visitor.processRow(rr))
              break done; //exit completely
            processedRows++;
          }
          //here, we didn't break anywhere. Check if we have more rows
        } while(true);
        // Advance the startRow to the end key of the current region
        startRow = callable.getHRegionInfo().getEndKey();
      } finally {
        // Close scanner
        callable.setClose();
        connection.getRegionServerWithRetries(callable);
      }
    } while (Bytes.compareTo(startRow, HConstants.LAST_ROW) != 0);
  }

  /**
   * Visitor class called to process each row of the .META. table
   */
  interface MetaScannerVisitor {
    /**
     * Visitor method that accepts a RowResult and the meta region location.
     * Implementations can return false to stop the region's loop if it becomes
     * unnecessary for some reason.
     *
     * @param rowResult result
     * @return A boolean to know if it should continue to loop in the region
     * @throws IOException e
     */
    public boolean processRow(Result rowResult) throws IOException;
  }
}
