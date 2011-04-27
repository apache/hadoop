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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Scanner class that contains the <code>.META.</code> table scanning logic
 * and uses a Retryable scanner. Provided visitors will be called
 * for each row.
 * 
 * Although public visibility, this is not a public-facing API and may evolve in
 * minor releases.
 */
public class MetaScanner {
  private static final Log LOG = LogFactory.getLog(MetaScanner.class);
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
    metaScan(configuration, visitor, null);
  }

  /**
   * Scans the meta table and calls a visitor on each RowResult. Uses a table
   * name to locate meta regions.
   *
   * @param configuration config
   * @param visitor visitor object
   * @param userTableName User table name in meta table to start scan at.  Pass
   * null if not interested in a particular table.
   * @throws IOException e
   */
  public static void metaScan(Configuration configuration,
      MetaScannerVisitor visitor, byte [] userTableName)
  throws IOException {
    metaScan(configuration, visitor, userTableName, null, Integer.MAX_VALUE);
  }

  /**
   * Scans the meta table and calls a visitor on each RowResult. Uses a table
   * name and a row name to locate meta regions. And it only scans at most
   * <code>rowLimit</code> of rows.
   *
   * @param configuration HBase configuration.
   * @param visitor Visitor object.
   * @param userTableName User table name in meta table to start scan at.  Pass
   * null if not interested in a particular table.
   * @param row Name of the row at the user table. The scan will start from
   * the region row where the row resides.
   * @param rowLimit Max of processed rows. If it is less than 0, it
   * will be set to default value <code>Integer.MAX_VALUE</code>.
   * @throws IOException e
   */
  public static void metaScan(Configuration configuration,
      MetaScannerVisitor visitor, byte [] userTableName, byte[] row,
      int rowLimit)
  throws IOException {
    metaScan(configuration, visitor, userTableName, row, rowLimit,
      HConstants.META_TABLE_NAME);
  }

  /**
   * Scans the meta table and calls a visitor on each RowResult. Uses a table
   * name and a row name to locate meta regions. And it only scans at most
   * <code>rowLimit</code> of rows.
   *
   * @param configuration HBase configuration.
   * @param visitor Visitor object.
   * @param tableName User table name in meta table to start scan at.  Pass
   * null if not interested in a particular table.
   * @param row Name of the row at the user table. The scan will start from
   * the region row where the row resides.
   * @param rowLimit Max of processed rows. If it is less than 0, it
   * will be set to default value <code>Integer.MAX_VALUE</code>.
   * @param metaTableName Meta table to scan, root or meta.
   * @throws IOException e
   */
  public static void metaScan(Configuration configuration,
      MetaScannerVisitor visitor, byte [] tableName, byte[] row,
      int rowLimit, final byte [] metaTableName)
  throws IOException {
    int rowUpperLimit = rowLimit > 0 ? rowLimit: Integer.MAX_VALUE;

    HConnection connection = HConnectionManager.getConnection(configuration);
    // if row is not null, we want to use the startKey of the row's region as
    // the startRow for the meta scan.
    byte[] startRow;
    if (row != null) {
      // Scan starting at a particular row in a particular table
      assert tableName != null;
      byte[] searchRow =
        HRegionInfo.createRegionName(tableName, row, HConstants.NINES,
          false);

      HTable metaTable = new HTable(configuration, HConstants.META_TABLE_NAME);
      Result startRowResult = metaTable.getRowOrBefore(searchRow,
          HConstants.CATALOG_FAMILY);
      if (startRowResult == null) {
        throw new TableNotFoundException("Cannot find row in .META. for table: "
            + Bytes.toString(tableName) + ", row=" + Bytes.toString(searchRow));
      }
      byte[] value = startRowResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.REGIONINFO_QUALIFIER);
      if (value == null || value.length == 0) {
        throw new IOException("HRegionInfo was null or empty in Meta for " +
          Bytes.toString(tableName) + ", row=" + Bytes.toString(searchRow));
      }
      HRegionInfo regionInfo = Writables.getHRegionInfo(value);

      byte[] rowBefore = regionInfo.getStartKey();
      startRow = HRegionInfo.createRegionName(tableName, rowBefore,
          HConstants.ZEROES, false);
    } else if (tableName == null || tableName.length == 0) {
      // Full META scan
      startRow = HConstants.EMPTY_START_ROW;
    } else {
      // Scan META for an entire table
      startRow = HRegionInfo.createRegionName(
          tableName, HConstants.EMPTY_START_ROW, HConstants.ZEROES, false);
    }

    // Scan over each meta region
    ScannerCallable callable;
    int rows = Math.min(rowLimit,
        configuration.getInt("hbase.meta.scanner.caching", 100));
    do {
      final Scan scan = new Scan(startRow).addFamily(HConstants.CATALOG_FAMILY);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Scanning " + Bytes.toString(metaTableName) +
          " starting at row=" + Bytes.toString(startRow) + " for max=" +
          rowUpperLimit + " rows");
      }
      callable = new ScannerCallable(connection, metaTableName, scan);
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
   * Lists all of the regions currently in META.
   * @param conf
   * @return List of all user-space regions.
   * @throws IOException
   */
  public static List<HRegionInfo> listAllRegions(Configuration conf)
  throws IOException {
    return listAllRegions(conf, true);
  }

  /**
   * Lists all of the regions currently in META.
   * @param conf
   * @param offlined True if we are to include offlined regions, false and we'll
   * leave out offlined regions from returned list.
   * @return List of all user-space regions.
   * @throws IOException
   */
  public static List<HRegionInfo> listAllRegions(Configuration conf, final boolean offlined)
  throws IOException {
    final List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
        @Override
        public boolean processRow(Result result) throws IOException {
          if (result == null || result.isEmpty()) {
            return true;
          }
          byte [] bytes = result.getValue(HConstants.CATALOG_FAMILY,
            HConstants.REGIONINFO_QUALIFIER);
          if (bytes == null) {
            LOG.warn("Null REGIONINFO_QUALIFIER: " + result);
            return true;
          }
          HRegionInfo regionInfo = Writables.getHRegionInfo(bytes);
          // If region offline AND we are not to include offlined regions, return.
          if (regionInfo.isOffline() && !offlined) return true;
          regions.add(regionInfo);
          return true;
        }
    };
    metaScan(conf, visitor);
    return regions;
  }

  /**
   * Lists all of the table regions currently in META.
   * @param conf
   * @param offlined True if we are to include offlined regions, false and we'll
   * leave out offlined regions from returned list.
   * @return Map of all user-space regions to servers
   * @throws IOException
   */
  public static NavigableMap<HRegionInfo, ServerName> allTableRegions(Configuration conf, final byte [] tablename, final boolean offlined)
  throws IOException {
    final NavigableMap<HRegionInfo, ServerName> regions =
      new TreeMap<HRegionInfo, ServerName>();
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      @Override
      public boolean processRow(Result rowResult) throws IOException {
        HRegionInfo info = Writables.getHRegionInfo(
            rowResult.getValue(HConstants.CATALOG_FAMILY,
                HConstants.REGIONINFO_QUALIFIER));
        if (!(Bytes.equals(info.getTableDesc().getName(), tablename))) {
          return false;
        }
        byte [] value = rowResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.SERVER_QUALIFIER);
        String hostAndPort = null;
        if (value != null && value.length > 0) {
          hostAndPort = Bytes.toString(value);
        }
        value = rowResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.STARTCODE_QUALIFIER);
        long startcode = -1L;
        if (value != null && value.length > 0) startcode = Bytes.toLong(value);
        if (!(info.isOffline() || info.isSplit())) {
          ServerName sn = null;
          if (hostAndPort != null && hostAndPort.length() > 0) {
            sn = new ServerName(hostAndPort, startcode);
          }
          regions.put(new UnmodifyableHRegionInfo(info), sn);
        }
        return true;
      }
    };
    metaScan(conf, visitor);
    return regions;
  }

  /**
   * Visitor class called to process each row of the .META. table
   */
  public interface MetaScannerVisitor {
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