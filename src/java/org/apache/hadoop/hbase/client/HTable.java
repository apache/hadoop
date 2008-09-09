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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Used to communicate with a single HBase table
 */
public class HTable {
  private final HConnection connection;
  private final byte [] tableName;
  private HBaseConfiguration configuration;

  /**
   * Creates an object to access a HBase table
   *
   * @param tableName name of the table
   * @throws IOException
   */
  public HTable(final String tableName)
  throws IOException {
    this(new HBaseConfiguration(), Bytes.toBytes(tableName));
  }

  /**
   * Creates an object to access a HBase table
   *
   * @param tableName name of the table
   * @throws IOException
   */
  public HTable(final byte [] tableName)
  throws IOException {
    this(new HBaseConfiguration(), tableName);
  }

  /**
   * Creates an object to access a HBase table
   * 
   * @param conf configuration object
   * @param tableName name of the table
   * @throws IOException
   */
  public HTable(HBaseConfiguration conf, final String tableName)
  throws IOException {
    this(conf, Bytes.toBytes(tableName));
  }

  /**
   * Creates an object to access a HBase table
   * 
   * @param conf configuration object
   * @param tableName name of the table
   * @throws IOException
   */
  public HTable(HBaseConfiguration conf, final byte [] tableName)
  throws IOException {
    this.connection = HConnectionManager.getConnection(conf);
    this.configuration = conf;
    this.tableName = tableName;
    this.connection.locateRegion(tableName, HConstants.EMPTY_START_ROW);
  }

  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException
   */
  public static boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(Bytes.toBytes(tableName));
  }
  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException
   */
  public static boolean isTableEnabled(byte[] tableName) throws IOException {
    return isTableEnabled(new HBaseConfiguration(), tableName);
  }
  
  /**
   * @param conf HBaseConfiguration object
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException
   */
  public static boolean isTableEnabled(HBaseConfiguration conf, String tableName)
  throws IOException {
    return isTableEnabled(conf, Bytes.toBytes(tableName));
  }

  /**
   * @param conf HBaseConfiguration object
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException
   */
  public static boolean isTableEnabled(HBaseConfiguration conf, byte[] tableName)
  throws IOException {
    return HConnectionManager.getConnection(conf).isTableEnabled(tableName);
  }

  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return Location of row.
   * @throws IOException
   */
  public HRegionLocation getRegionLocation(final String row)
  throws IOException {
    return connection.getRegionLocation(tableName, Bytes.toBytes(row), false);
  }

  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return Location of row.
   * @throws IOException
   */
  public HRegionLocation getRegionLocation(final byte [] row)
  throws IOException {
    return connection.getRegionLocation(tableName, row, false);
  }

  /** @return the table name */
  public byte [] getTableName() {
    return this.tableName;
  }

  /**
   * Used by unit tests and tools to do low-level manipulations.  Not for
   * general use.
   * @return An HConnection instance.
   */
  public HConnection getConnection() {
    return this.connection;
  }

  /**
   * @return table metadata 
   * @throws IOException
   */
  public HTableDescriptor getTableDescriptor() throws IOException {
    return new UnmodifyableHTableDescriptor(
      this.connection.getHTableDescriptor(this.tableName));
  }

  /**
   * Gets the starting row key for every region in the currently open table
   * 
   * @return Array of region starting row keys
   * @throws IOException
   */
  @SuppressWarnings("null")
  public byte[][] getStartKeys() throws IOException {
    final List<byte[]> keyList = new ArrayList<byte[]>();

    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      public boolean processRow(RowResult rowResult) throws IOException {
        HRegionInfo info = Writables.getHRegionInfo(
            rowResult.get(HConstants.COL_REGIONINFO));
        if (Bytes.equals(info.getTableDesc().getName(), getTableName())) {
          if (!(info.isOffline() || info.isSplit())) {
            keyList.add(info.getStartKey());
          }
        }
        return true;
      }

    };
    MetaScanner.metaScan(configuration, visitor, this.tableName);
    return keyList.toArray(new byte[keyList.size()][]);
  }

  /**
   * Get all the regions and their address for this table
   * 
   * @return A map of HRegionInfo with it's server address
   * @throws IOException
   */
  public Map<HRegionInfo, HServerAddress> getRegionsInfo() throws IOException {
    final Map<HRegionInfo, HServerAddress> regionMap =
      new TreeMap<HRegionInfo, HServerAddress>();

    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      public boolean processRow(RowResult rowResult) throws IOException {
        HRegionInfo info = Writables.getHRegionInfo(
            rowResult.get(HConstants.COL_REGIONINFO));
        
        if (!(Bytes.equals(info.getTableDesc().getName(), getTableName()))) {
          return false;
        }

        HServerAddress server = new HServerAddress();
        Cell c = rowResult.get(HConstants.COL_SERVER);
        if (c != null && c.getValue() != null && c.getValue().length > 0) {
          String address = Bytes.toString(c.getValue());
          server = new HServerAddress(address);
        }
        
        if (!(info.isOffline() || info.isSplit())) {
          regionMap.put(new UnmodifyableHRegionInfo(info), server);
        }
        return true;
      }

    };
    MetaScanner.metaScan(configuration, visitor, tableName);
    return regionMap;
  }

  /**
   * Get a single value for the specified row and column
   * 
   * @param row row key
   * @param column column name
   * @return value for specified row/column
   * @throws IOException
   */
  public Cell get(final String row, final String column)
  throws IOException {
    return get(Bytes.toBytes(row), Bytes.toBytes(column));
  }

  /** 
   * Get a single value for the specified row and column
   *
   * @param row row key
   * @param column column name
   * @param numVersions - number of versions to retrieve
   * @return value for specified row/column
   * @throws IOException
   */
  public Cell[] get(final String row, final String column, int numVersions)
  throws IOException {
    return get(Bytes.toBytes(row), Bytes.toBytes(column), numVersions);
  }

  /** 
   * Get a single value for the specified row and column
   *
   * @param row row key
   * @param column column name
   * @return value for specified row/column
   * @throws IOException
   */
  public Cell get(final byte [] row, final byte [] column)
  throws IOException {
    return connection.getRegionServerWithRetries(
        new ServerCallable<Cell>(connection, tableName, row) {
          public Cell call() throws IOException {
            Cell[] result = server.get(location.getRegionInfo().getRegionName(), 
                row, column, -1, -1);
            return (result == null)? null : result[0];
          }
        }
    );
  }

  /** 
   * Get the specified number of versions of the specified row and column
   * @param row row key
   * @param column column name
   * @param numVersions number of versions to retrieve
   * @return Array of Cells.
   * @throws IOException
   */
  public Cell[] get(final byte [] row, final byte [] column,
    final int numVersions) 
  throws IOException {
    return connection.getRegionServerWithRetries(
        new ServerCallable<Cell[]>(connection, tableName, row) {
          public Cell[] call() throws IOException {
            return server.get(location.getRegionInfo().getRegionName(), row, 
                column, -1, numVersions);
          }
        }
    );
  }

  /** 
   * Get the specified number of versions of the specified row and column with
   * the specified timestamp.
   *
   * @param row         - row key
   * @param column      - column name
   * @param timestamp   - timestamp
   * @param numVersions - number of versions to retrieve
   * @return            - array of values that match the above criteria
   * @throws IOException
   */
  public Cell[] get(final String row, final String column,
    final long timestamp, final int numVersions)
  throws IOException {
    return get(Bytes.toBytes(row), Bytes.toBytes(column), timestamp, numVersions);
  }

  /** 
   * Get the specified number of versions of the specified row and column with
   * the specified timestamp.
   *
   * @param row         - row key
   * @param column      - column name
   * @param timestamp   - timestamp
   * @param numVersions - number of versions to retrieve
   * @return            - array of values that match the above criteria
   * @throws IOException
   */
  public Cell[] get(final byte [] row, final byte [] column,
    final long timestamp, final int numVersions)
  throws IOException {
    Cell[] values = null;
    values = connection.getRegionServerWithRetries(
        new ServerCallable<Cell[]>(connection, tableName, row) {
          public Cell[] call() throws IOException {
            return server.get(location.getRegionInfo().getRegionName(), row, 
                column, timestamp, numVersions);
          }
        }
    );

    if (values != null) {
      ArrayList<Cell> cellValues = new ArrayList<Cell>();
      for (int i = 0 ; i < values.length; i++) {
        cellValues.add(values[i]);
      }
      return cellValues.toArray(new Cell[values.length]);
    }
    return null;
  }

  /** 
   * Get all the data for the specified row at the latest timestamp
   * 
   * @param row row key
   * @return RowResult is empty if row does not exist.
   * @throws IOException
   */
  public RowResult getRow(final String row) throws IOException {
    return getRow(Bytes.toBytes(row));
  }

  /** 
   * Get all the data for the specified row at the latest timestamp
   * 
   * @param row row key
   * @return RowResult is empty if row does not exist.
   * @throws IOException
   */
  public RowResult getRow(final byte [] row) throws IOException {
    return getRow(row, HConstants.LATEST_TIMESTAMP);
  }

  /** 
   * Get all the data for the specified row at a specified timestamp
   * 
   * @param row row key
   * @param ts timestamp
   * @return RowResult is empty if row does not exist.
   * @throws IOException
   */
  public RowResult getRow(final String row, final long ts) 
  throws IOException {
    return getRow(Bytes.toBytes(row), ts);
  }

  /** 
   * Get all the data for the specified row at a specified timestamp
   * 
   * @param row row key
   * @param ts timestamp
   * @return RowResult is empty if row does not exist.
   * @throws IOException
   */
  public RowResult getRow(final byte [] row, final long ts) 
  throws IOException {
    return getRow(row,null,ts);
  }

  /** 
   * Get selected columns for the specified row at the latest timestamp
   * 
   * @param row row key
   * @param columns Array of column names and families you want to retrieve.
   * @return RowResult is empty if row does not exist.
   * @throws IOException
   */
  public RowResult getRow(final String row, final String [] columns) 
  throws IOException {
    return getRow(Bytes.toBytes(row), Bytes.toByteArrays(columns));
  }

  /** 
   * Get selected columns for the specified row at the latest timestamp
   * 
   * @param row row key
   * @param columns Array of column names and families you want to retrieve.
   * @return RowResult is empty if row does not exist.
   * @throws IOException
   */
  public RowResult getRow(final byte [] row, final byte [][] columns) 
  throws IOException {
    return getRow(row, columns, HConstants.LATEST_TIMESTAMP);
  }

  /** 
   * Get selected columns for the specified row at a specified timestamp
   * 
   * @param row row key
   * @param columns Array of column names and families you want to retrieve.
   * @param ts timestamp
   * @return RowResult is empty if row does not exist.
   * @throws IOException
   */
  public RowResult getRow(final String row, final String [] columns, 
    final long ts) 
  throws IOException {  
    return getRow(Bytes.toBytes(row), Bytes.toByteArrays(columns), ts);
  }

  /** 
   * Get selected columns for the specified row at a specified timestamp
   * 
   * @param row row key
   * @param columns Array of column names and families you want to retrieve.
   * @param ts timestamp
   * @return RowResult is empty if row does not exist.
   * @throws IOException
   */
  public RowResult getRow(final byte [] row, final byte [][] columns, 
    final long ts) 
  throws IOException {       
    return getRow(row,columns,ts,null);
  }

  /** 
   * Get selected columns for the specified row at a specified timestamp
   * using existing row lock.
   * 
   * @param row row key
   * @param columns Array of column names and families you want to retrieve.
   * @param ts timestamp
   * @param rl row lock
   * @return RowResult is empty if row does not exist.
   * @throws IOException
   */
  public RowResult getRow(final byte [] row, final byte [][] columns, 
    final long ts, final RowLock rl) 
  throws IOException {       
    return connection.getRegionServerWithRetries(
        new ServerCallable<RowResult>(connection, tableName, row) {
          public RowResult call() throws IOException {
            long lockId = -1L;
            if(rl != null) {
              lockId = rl.getLockId();
            }
            return server.getRow(location.getRegionInfo().getRegionName(), row, 
                columns, ts, lockId);
          }
        }
    );
  }

  /** 
   * Get a scanner on the current table starting at first row.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final String [] columns)
  throws IOException {
    return getScanner(Bytes.toByteArrays(columns), HConstants.EMPTY_START_ROW);
  }

  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final String [] columns, final String startRow)
  throws IOException {
    return getScanner(Bytes.toByteArrays(columns), Bytes.toBytes(startRow));
  }

  /** 
   * Get a scanner on the current table starting at first row.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final byte[][] columns)
  throws IOException {
    return getScanner(columns, HConstants.EMPTY_START_ROW,
      HConstants.LATEST_TIMESTAMP, null);
  }

  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final byte[][] columns, final byte [] startRow)
  throws IOException {
    return getScanner(columns, startRow, HConstants.LATEST_TIMESTAMP, null);
  }
  
  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @param timestamp only return results whose timestamp <= this value
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final byte[][] columns, final byte [] startRow,
    long timestamp)
  throws IOException {
    return getScanner(columns, startRow, timestamp, null);
  }
  
  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @param filter a row filter using row-key regexp and/or column data filter.
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final byte[][] columns, final byte [] startRow,
    RowFilterInterface filter)
  throws IOException { 
    return getScanner(columns, startRow, HConstants.LATEST_TIMESTAMP, filter);
  }
  
  /** 
   * Get a scanner on the current table starting at the specified row and
   * ending just before <code>stopRow<code>.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @param stopRow Row to stop scanning on. Once we hit this row we stop
   * returning values; i.e. we return the row before this one but not the
   * <code>stopRow</code> itself.
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final byte [][] columns,
    final byte [] startRow, final byte [] stopRow)
  throws IOException {
    return getScanner(columns, startRow, stopRow,
      HConstants.LATEST_TIMESTAMP);
  }

  /** 
   * Get a scanner on the current table starting at the specified row and
   * ending just before <code>stopRow<code>.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @param stopRow Row to stop scanning on. Once we hit this row we stop
   * returning values; i.e. we return the row before this one but not the
   * <code>stopRow</code> itself.
   * @param timestamp only return results whose timestamp <= this value
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final String [] columns,
    final String startRow, final String stopRow, final long timestamp)
  throws IOException {
    return getScanner(Bytes.toByteArrays(columns), Bytes.toBytes(startRow),
      Bytes.toBytes(stopRow), timestamp);
  }

  /** 
   * Get a scanner on the current table starting at the specified row and
   * ending just before <code>stopRow<code>.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @param stopRow Row to stop scanning on. Once we hit this row we stop
   * returning values; i.e. we return the row before this one but not the
   * <code>stopRow</code> itself.
   * @param timestamp only return results whose timestamp <= this value
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final byte [][] columns,
    final byte [] startRow, final byte [] stopRow, final long timestamp)
  throws IOException {
    return getScanner(columns, startRow, timestamp,
      new WhileMatchRowFilter(new StopRowFilter(stopRow)));
  }

  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @param timestamp only return results whose timestamp <= this value
   * @param filter a row filter using row-key regexp and/or column data filter.
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(String[] columns,
    String startRow, long timestamp, RowFilterInterface filter)
  throws IOException {
    return getScanner(Bytes.toByteArrays(columns), Bytes.toBytes(startRow),
      timestamp, filter);
  }

  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @param timestamp only return results whose timestamp <= this value
   * @param filter a row filter using row-key regexp and/or column data filter.
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final byte [][] columns,
    final byte [] startRow, long timestamp, RowFilterInterface filter)
  throws IOException {
    ClientScanner s = new ClientScanner(columns, startRow, timestamp, filter);
    s.initialize();
    return s;
  }
  
  /**
   * Completely delete the row's cells.
   *
   * @param row Key of the row you want to completely delete.
   * @throws IOException
   */
  public void deleteAll(final byte [] row) throws IOException {
    deleteAll(row, null);
  }

  /**
   * Completely delete the row's cells.
   *
   * @param row Key of the row you want to completely delete.
   * @throws IOException
   */
  public void deleteAll(final String row) throws IOException {
    deleteAll(row, null);
  }
  
  /**
   * Completely delete the row's cells.
   *
   * @param row Key of the row you want to completely delete.
   * @param column column to be deleted
   * @throws IOException
   */
  public void deleteAll(final byte [] row, final byte [] column)
  throws IOException {
    deleteAll(row, column, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Completely delete the row's cells.
   *
   * @param row Key of the row you want to completely delete.
   * @param ts Delete all cells of the same timestamp or older.
   * @throws IOException
   */
  public void deleteAll(final byte [] row, final long ts)
  throws IOException {
    deleteAll(row, null, ts);
  }

  /**
   * Completely delete the row's cells.
   *
   * @param row Key of the row you want to completely delete.
   * @param ts Delete all cells of the same timestamp or older.
   * @throws IOException
   */
  public void deleteAll(final String row, final long ts)
  throws IOException {
    deleteAll(row, null, ts);
  }

  /** 
   * Delete all cells that match the passed row and column.
   * @param row Row to update
   * @param column name of column whose value is to be deleted
   * @throws IOException 
   */
  public void deleteAll(final String row, final String column)
  throws IOException {
    deleteAll(row, column, HConstants.LATEST_TIMESTAMP);
  }

  /** 
   * Delete all cells that match the passed row and column and whose
   * timestamp is equal-to or older than the passed timestamp.
   * @param row Row to update
   * @param column name of column whose value is to be deleted
   * @param ts Delete all cells of the same timestamp or older.
   * @throws IOException 
   */
  public void deleteAll(final String row, final String column, final long ts)
  throws IOException {
    deleteAll(Bytes.toBytes(row),
      column != null? Bytes.toBytes(column): null, ts);
  }

  /** 
   * Delete all cells that match the passed row and column and whose
   * timestamp is equal-to or older than the passed timestamp.
   * @param row Row to update
   * @param column name of column whose value is to be deleted
   * @param ts Delete all cells of the same timestamp or older.
   * @throws IOException 
   */
  public void deleteAll(final byte [] row, final byte [] column, final long ts)
  throws IOException {
    deleteAll(row,column,ts,null);
  }

  /** 
   * Delete all cells that match the passed row and column and whose
   * timestamp is equal-to or older than the passed timestamp, using an
   * existing row lock.
   * @param row Row to update
   * @param column name of column whose value is to be deleted
   * @param ts Delete all cells of the same timestamp or older.
   * @param rl Existing row lock
   * @throws IOException 
   */
  public void deleteAll(final byte [] row, final byte [] column, final long ts,
      final RowLock rl)
  throws IOException {
    connection.getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, row) {
          public Boolean call() throws IOException {
            long lockId = -1L;
            if(rl != null) {
              lockId = rl.getLockId();
            }
            if (column != null) {
              this.server.deleteAll(location.getRegionInfo().getRegionName(),
                row, column, ts, lockId);
            } else {
              this.server.deleteAll(location.getRegionInfo().getRegionName(),
                  row, ts, lockId);
            }
            return null;
          }
        }
    );
  }

  /**
   * Delete all cells for a row with matching column family at all timestamps.
   *
   * @param row The row to operate on
   * @param family The column family to match
   * @throws IOException
   */
  public void deleteFamily(final String row, final String family) 
  throws IOException {
    deleteFamily(row, family, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Delete all cells for a row with matching column family at all timestamps.
   *
   * @param row The row to operate on
   * @param family The column family to match
   * @throws IOException
   */
  public void deleteFamily(final byte[] row, final byte[] family) 
  throws IOException {
    deleteFamily(row, family, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Delete all cells for a row with matching column family with timestamps
   * less than or equal to <i>timestamp</i>.
   *
   * @param row The row to operate on
   * @param family The column family to match
   * @param timestamp Timestamp to match
   * @throws IOException
   */  
  public void deleteFamily(final String row, final String family,
      final long timestamp)
  throws IOException{
    deleteFamily(Bytes.toBytes(row), Bytes.toBytes(family), timestamp);
  }

  /**
   * Delete all cells for a row with matching column family with timestamps
   * less than or equal to <i>timestamp</i>.
   *
   * @param row The row to operate on
   * @param family The column family to match
   * @param timestamp Timestamp to match
   * @throws IOException
   */
  public void deleteFamily(final byte [] row, final byte [] family, 
    final long timestamp)
  throws IOException {
    deleteFamily(row,family,timestamp,null);
  }

  /**
   * Delete all cells for a row with matching column family with timestamps
   * less than or equal to <i>timestamp</i>, using existing row lock.
   *
   * @param row The row to operate on
   * @param family The column family to match
   * @param timestamp Timestamp to match
   * @param rl Existing row lock
   * @throws IOException
   */
  public void deleteFamily(final byte [] row, final byte [] family, 
    final long timestamp, final RowLock rl)
  throws IOException {
    connection.getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, row) {
          public Boolean call() throws IOException {
            long lockId = -1L;
            if(rl != null) {
              lockId = rl.getLockId();
            }
            server.deleteFamily(location.getRegionInfo().getRegionName(), row, 
                family, timestamp, lockId);
            return null;
          }
        }
    );
  }

  /**
   * Commit a BatchUpdate to the table.
   * @param batchUpdate
   * @throws IOException
   */ 
  public synchronized void commit(final BatchUpdate batchUpdate) 
  throws IOException {
    commit(batchUpdate,null);
  }
  
  /**
   * Commit a BatchUpdate to the table using existing row lock.
   * @param batchUpdate
   * @param rl Existing row lock
   * @throws IOException
   */ 
  public synchronized void commit(final BatchUpdate batchUpdate,
      final RowLock rl) 
  throws IOException {
    if (batchUpdate.getRow() == null)
      throw new IllegalArgumentException("update has null row");
    connection.getRegionServerWithRetries(
      new ServerCallable<Boolean>(connection, tableName, batchUpdate.getRow()) {
        public Boolean call() throws IOException {
          long lockId = -1L;
          if(rl != null) {
            lockId = rl.getLockId();
          }
          server.batchUpdate(location.getRegionInfo().getRegionName(), 
            batchUpdate, lockId);
          return null;
        }
      }
    );  
  }
  
  /**
   * Commit a RowsBatchUpdate to the table.
   * @param batchUpdates
   * @throws IOException
   */ 
  public synchronized void commit(final List<BatchUpdate> batchUpdates) 
  throws IOException {
    for (BatchUpdate batchUpdate : batchUpdates) 
      commit(batchUpdate,null);
  }

  /**
   * Obtain a row lock
   * @param row The row to lock
   * @return rowLock RowLock containing row and lock id
   * @throws IOException
   */
  public RowLock lockRow(final byte [] row)
  throws IOException {
    return connection.getRegionServerWithRetries(
      new ServerCallable<RowLock>(connection, tableName, row) {
        public RowLock call() throws IOException {
          long lockId =
              server.lockRow(location.getRegionInfo().getRegionName(), row);
          RowLock rowLock = new RowLock(row,lockId);
          return rowLock;
        }
      }
    );
  }

  /**
   * Release a row lock
   * @param rl The row lock to release
   * @throws IOException
   */
  public void unlockRow(final RowLock rl)
  throws IOException {
    connection.getRegionServerWithRetries(
      new ServerCallable<Boolean>(connection, tableName, rl.getRow()) {
        public Boolean call() throws IOException {
          server.unlockRow(location.getRegionInfo().getRegionName(),
              rl.getLockId());
          return null;
        }
      }
    );
  }

  /**
   * Implements the scanner interface for the HBase client.
   * If there are multiple regions in a table, this scanner will iterate
   * through them all.
   */
  protected class ClientScanner implements Scanner {
    private final Log CLIENT_LOG = LogFactory.getLog(this.getClass());
    private byte[][] columns;
    private byte [] startRow;
    protected long scanTime;
    @SuppressWarnings("hiding")
    private boolean closed = false;
    private HRegionInfo currentRegion = null;
    private ScannerCallable callable = null;
    protected RowFilterInterface filter;

    protected ClientScanner(final byte[][] columns, final byte [] startRow,
        final long timestamp, final RowFilterInterface filter) {
      if (CLIENT_LOG.isDebugEnabled()) {
        CLIENT_LOG.debug("Creating scanner over " + Bytes.toString(getTableName()) +
          " starting at key '" + Bytes.toString(startRow) + "'");
      }
      // save off the simple parameters
      this.columns = columns;
      this.startRow = startRow;
      this.scanTime = timestamp;
      
      // save the filter, and make sure that the filter applies to the data
      // we're expecting to pull back
      this.filter = filter;
      if (filter != null) {
        filter.validate(columns);
      }
    }

    //TODO: change visibility to protected
    
    public void initialize() throws IOException {
      nextScanner();
    }
    
    protected byte[][] getColumns() {
      return columns;
    }
    
    protected long getTimestamp() {
      return scanTime;
    }
    
    protected RowFilterInterface getFilter() {
      return filter;
    }
        
    /*
     * Gets a scanner for the next region.
     * Returns false if there are no more scanners.
     */
    private boolean nextScanner() throws IOException {
      // Close the previous scanner if it's open
      if (this.callable != null) {
        this.callable.setClose();
        getConnection().getRegionServerWithRetries(callable);
        this.callable = null;
      }

      // if we're at the end of the table, then close and return false
      // to stop iterating
      if (currentRegion != null) {
        if (CLIENT_LOG.isDebugEnabled()) {
          CLIENT_LOG.debug("Advancing forward from region " + currentRegion);
        }

        byte [] endKey = currentRegion.getEndKey();
        if (endKey == null ||
            Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY) ||
            filterSaysStop(endKey)) {
          close();
          return false;
        }
      } 
      
      HRegionInfo oldRegion = this.currentRegion;
      byte [] localStartKey = oldRegion == null? startRow: oldRegion.getEndKey();

      if (CLIENT_LOG.isDebugEnabled()) {
        CLIENT_LOG.debug("Advancing internal scanner to startKey at '" +
          Bytes.toString(localStartKey) + "'");
      }
            
      try {
        callable = getScannerCallable(localStartKey);
        // open a scanner on the region server starting at the 
        // beginning of the region
        getConnection().getRegionServerWithRetries(callable);
        currentRegion = callable.getHRegionInfo();
      } catch (IOException e) {
        close();
        throw e;
      }
      return true;
    }
    
    protected ScannerCallable getScannerCallable(byte [] localStartKey) {
      return new ScannerCallable(getConnection(), getTableName(), columns, 
          localStartKey, scanTime, filter);
    }

    /**
     * @param endKey
     * @return Returns true if the passed region endkey is judged beyond
     * filter.
     */
    private boolean filterSaysStop(final byte [] endKey) {
      if (this.filter == null) {
        return false;
      }
      // Let the filter see current row.
      this.filter.filterRowKey(endKey);
      return this.filter.filterAllRemaining();
    }

    public RowResult next() throws IOException {
      if (this.closed) {
        return null;
      }
      RowResult values = null;
      do {
        values = getConnection().getRegionServerWithRetries(callable);
      } while ((values == null || values.size() == 0) && nextScanner());

      if (values != null && values.size() != 0) {
        return values;
      }
      return null;
    }

    public void close() {
      if (callable != null) {
        callable.setClose();
        try {
          getConnection().getRegionServerWithRetries(callable);
        } catch (IOException e) {
          // We used to catch this error, interpret, and rethrow. However, we
          // have since decided that it's not nice for a scanner's close to
          // throw exceptions. Chances are it was just an UnknownScanner
          // exception due to lease time out.
        }
        callable = null;
      }
      closed = true;
    }

    public Iterator<RowResult> iterator() {
      return new Iterator<RowResult>() {
        // The next RowResult, possibly pre-read
        RowResult next = null;
        
        // return true if there is another item pending, false if there isn't.
        // this method is where the actual advancing takes place, but you need
        // to call next() to consume it. hasNext() will only advance if there
        // isn't a pending next().
        public boolean hasNext() {
          if (next == null) {
            try {
              next = ClientScanner.this.next();
              return next != null;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }            
          }
          return true;
        }

        // get the pending next item and advance the iterator. returns null if
        // there is no next item.
        public RowResult next() {
          // since hasNext() does the real advancing, we call this to determine
          // if there is a next before proceeding.
          if (!hasNext()) {
            return null;
          }
          
          // if we get to here, then hasNext() has given us an item to return.
          // we want to return the item and then null out the next pointer, so
          // we use a temporary variable.
          RowResult temp = next;
          next = null;
          return temp;
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
