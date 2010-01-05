/*
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
package org.apache.hadoop.hbase.thrift.generated;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.*;

public class Hbase {

  public interface Iface {

    /**
     * Brings a table on-line (enables it)
     * @param tableName name of the table
     * 
     * @param tableName
     */
    public void enableTable(byte[] tableName) throws IOError, TException;

    /**
     * Disables a table (takes it off-line) If it is being served, the master
     * will tell the servers to stop serving it.
     * @param tableName name of the table
     * 
     * @param tableName
     */
    public void disableTable(byte[] tableName) throws IOError, TException;

    /**
     * @param tableName name of table to check
     * @return true if table is on-line
     * 
     * @param tableName
     */
    public boolean isTableEnabled(byte[] tableName) throws IOError, TException;

    public void compact(byte[] tableNameOrRegionName) throws IOError, TException;

    public void majorCompact(byte[] tableNameOrRegionName) throws IOError, TException;

    /**
     * List all the userspace tables.
     * @return - returns a list of names
     */
    public List<byte[]> getTableNames() throws IOError, TException;

    /**
     * List all the column families assoicated with a table.
     * @param tableName table name
     * @return list of column family descriptors
     * 
     * @param tableName
     */
    public Map<byte[],ColumnDescriptor> getColumnDescriptors(byte[] tableName) throws IOError, TException;

    /**
     * List the regions associated with a table.
     * @param tableName table name
     * @return list of region descriptors
     * 
     * @param tableName
     */
    public List<TRegionInfo> getTableRegions(byte[] tableName) throws IOError, TException;

    /**
     * Create a table with the specified column families.  The name
     * field for each ColumnDescriptor must be set and must end in a
     * colon (:).  All other fields are optional and will get default
     * values if not explicitly specified.
     * 
     * @param tableName name of table to create
     * @param columnFamilies list of column family descriptors
     * 
     * @throws IllegalArgument if an input parameter is invalid
     * @throws AlreadyExists if the table name already exists
     * 
     * @param tableName
     * @param columnFamilies
     */
    public void createTable(byte[] tableName, List<ColumnDescriptor> columnFamilies) throws IOError, IllegalArgument, AlreadyExists, TException;

    /**
     * Deletes a table
     * @param tableName name of table to delete
     * @throws IOError if table doesn't exist on server or there was some other
     * problem
     * 
     * @param tableName
     */
    public void deleteTable(byte[] tableName) throws IOError, TException;

    /**
     * Get a single TCell for the specified table, row, and column at the
     * latest timestamp. Returns an empty list if no such value exists.
     * 
     * @param tableName name of table
     * @param row row key
     * @param column column name
     * @return value for specified row/column
     * 
     * @param tableName
     * @param row
     * @param column
     */
    public List<TCell> get(byte[] tableName, byte[] row, byte[] column) throws IOError, TException;

    /**
     * Get the specified number of versions for the specified table,
     * row, and column.
     * 
     * @param tableName name of table
     * @param row row key
     * @param column column name
     * @param numVersions number of versions to retrieve
     * @return list of cells for specified row/column
     * 
     * @param tableName
     * @param row
     * @param column
     * @param numVersions
     */
    public List<TCell> getVer(byte[] tableName, byte[] row, byte[] column, int numVersions) throws IOError, TException;

    /**
     * Get the specified number of versions for the specified table,
     * row, and column.  Only versions less than or equal to the specified
     * timestamp will be returned.
     * 
     * @param tableName name of table
     * @param row row key
     * @param column column name
     * @param timestamp timestamp
     * @param numVersions number of versions to retrieve
     * @return list of cells for specified row/column
     * 
     * @param tableName
     * @param row
     * @param column
     * @param timestamp
     * @param numVersions
     */
    public List<TCell> getVerTs(byte[] tableName, byte[] row, byte[] column, long timestamp, int numVersions) throws IOError, TException;

    /**
     * Get all the data for the specified table and row at the latest
     * timestamp. Returns an empty list if the row does not exist.
     * 
     * @param tableName name of table
     * @param row row key
     * @return TRowResult containing the row and map of columns to TCells
     * 
     * @param tableName
     * @param row
     */
    public List<TRowResult> getRow(byte[] tableName, byte[] row) throws IOError, TException;

    /**
     * Get the specified columns for the specified table and row at the latest
     * timestamp. Returns an empty list if the row does not exist.
     * 
     * @param tableName name of table
     * @param row row key
     * @param columns List of columns to return, null for all columns
     * @return TRowResult containing the row and map of columns to TCells
     * 
     * @param tableName
     * @param row
     * @param columns
     */
    public List<TRowResult> getRowWithColumns(byte[] tableName, byte[] row, List<byte[]> columns) throws IOError, TException;

    /**
     * Get all the data for the specified table and row at the specified
     * timestamp. Returns an empty list if the row does not exist.
     * 
     * @param tableName of table
     * @param row row key
     * @param timestamp timestamp
     * @return TRowResult containing the row and map of columns to TCells
     * 
     * @param tableName
     * @param row
     * @param timestamp
     */
    public List<TRowResult> getRowTs(byte[] tableName, byte[] row, long timestamp) throws IOError, TException;

    /**
     * Get the specified columns for the specified table and row at the specified
     * timestamp. Returns an empty list if the row does not exist.
     * 
     * @param tableName name of table
     * @param row row key
     * @param columns List of columns to return, null for all columns
     * @return TRowResult containing the row and map of columns to TCells
     * 
     * @param tableName
     * @param row
     * @param columns
     * @param timestamp
     */
    public List<TRowResult> getRowWithColumnsTs(byte[] tableName, byte[] row, List<byte[]> columns, long timestamp) throws IOError, TException;

    /**
     * Apply a series of mutations (updates/deletes) to a row in a
     * single transaction.  If an exception is thrown, then the
     * transaction is aborted.  Default current timestamp is used, and
     * all entries will have an identical timestamp.
     * 
     * @param tableName name of table
     * @param row row key
     * @param mutations list of mutation commands
     * 
     * @param tableName
     * @param row
     * @param mutations
     */
    public void mutateRow(byte[] tableName, byte[] row, List<Mutation> mutations) throws IOError, IllegalArgument, TException;

    /**
     * Apply a series of mutations (updates/deletes) to a row in a
     * single transaction.  If an exception is thrown, then the
     * transaction is aborted.  The specified timestamp is used, and
     * all entries will have an identical timestamp.
     * 
     * @param tableName name of table
     * @param row row key
     * @param mutations list of mutation commands
     * @param timestamp timestamp
     * 
     * @param tableName
     * @param row
     * @param mutations
     * @param timestamp
     */
    public void mutateRowTs(byte[] tableName, byte[] row, List<Mutation> mutations, long timestamp) throws IOError, IllegalArgument, TException;

    /**
     * Apply a series of batches (each a series of mutations on a single row)
     * in a single transaction.  If an exception is thrown, then the
     * transaction is aborted.  Default current timestamp is used, and
     * all entries will have an identical timestamp.
     * 
     * @param tableName name of table
     * @param rowBatches list of row batches
     * 
     * @param tableName
     * @param rowBatches
     */
    public void mutateRows(byte[] tableName, List<BatchMutation> rowBatches) throws IOError, IllegalArgument, TException;

    /**
     * Apply a series of batches (each a series of mutations on a single row)
     * in a single transaction.  If an exception is thrown, then the
     * transaction is aborted.  The specified timestamp is used, and
     * all entries will have an identical timestamp.
     * 
     * @param tableName name of table
     * @param rowBatches list of row batches
     * @param timestamp timestamp
     * 
     * @param tableName
     * @param rowBatches
     * @param timestamp
     */
    public void mutateRowsTs(byte[] tableName, List<BatchMutation> rowBatches, long timestamp) throws IOError, IllegalArgument, TException;

    /**
     * Atomically increment the column value specified.  Returns the next value post increment.
     * @param tableName name of table
     * @param row row to increment
     * @param column name of column
     * @param value amount to increment by
     * 
     * @param tableName
     * @param row
     * @param column
     * @param value
     */
    public long atomicIncrement(byte[] tableName, byte[] row, byte[] column, long value) throws IOError, IllegalArgument, TException;

    /**
     * Delete all cells that match the passed row and column.
     * 
     * @param tableName name of table
     * @param row Row to update
     * @param column name of column whose value is to be deleted
     * 
     * @param tableName
     * @param row
     * @param column
     */
    public void deleteAll(byte[] tableName, byte[] row, byte[] column) throws IOError, TException;

    /**
     * Delete all cells that match the passed row and column and whose
     * timestamp is equal-to or older than the passed timestamp.
     * 
     * @param tableName name of table
     * @param row Row to update
     * @param column name of column whose value is to be deleted
     * @param timestamp timestamp
     * 
     * @param tableName
     * @param row
     * @param column
     * @param timestamp
     */
    public void deleteAllTs(byte[] tableName, byte[] row, byte[] column, long timestamp) throws IOError, TException;

    /**
     * Completely delete the row's cells.
     * 
     * @param tableName name of table
     * @param row key of the row to be completely deleted.
     * 
     * @param tableName
     * @param row
     */
    public void deleteAllRow(byte[] tableName, byte[] row) throws IOError, TException;

    /**
     * Completely delete the row's cells marked with a timestamp
     * equal-to or older than the passed timestamp.
     * 
     * @param tableName name of table
     * @param row key of the row to be completely deleted.
     * @param timestamp timestamp
     * 
     * @param tableName
     * @param row
     * @param timestamp
     */
    public void deleteAllRowTs(byte[] tableName, byte[] row, long timestamp) throws IOError, TException;

    /**
     * Get a scanner on the current table starting at the specified row and
     * ending at the last row in the table.  Return the specified columns.
     * 
     * @param columns columns to scan. If column name is a column family, all
     * columns of the specified column family are returned.  Its also possible
     * to pass a regex in the column qualifier.
     * @param tableName name of table
     * @param startRow starting row in table to scan.  send "" (empty string) to
     *                 start at the first row.
     * 
     * @return scanner id to be used with other scanner procedures
     * 
     * @param tableName
     * @param startRow
     * @param columns
     */
    public int scannerOpen(byte[] tableName, byte[] startRow, List<byte[]> columns) throws IOError, TException;

    /**
     * Get a scanner on the current table starting and stopping at the
     * specified rows.  ending at the last row in the table.  Return the
     * specified columns.
     * 
     * @param columns columns to scan. If column name is a column family, all
     * columns of the specified column family are returned.  Its also possible
     * to pass a regex in the column qualifier.
     * @param tableName name of table
     * @param startRow starting row in table to scan.  send "" (empty string) to
     *                 start at the first row.
     * @param stopRow row to stop scanning on.  This row is *not* included in the
     *                scanner's results
     * 
     * @return scanner id to be used with other scanner procedures
     * 
     * @param tableName
     * @param startRow
     * @param stopRow
     * @param columns
     */
    public int scannerOpenWithStop(byte[] tableName, byte[] startRow, byte[] stopRow, List<byte[]> columns) throws IOError, TException;

    /**
     * Open a scanner for a given prefix.  That is all rows will have the specified
     * prefix. No other rows will be returned.
     * 
     * @param tableName name of table
     * @param startAndPrefix the prefix (and thus start row) of the keys you want
     * @param columns the columns you want returned
     * @return scanner id to use with other scanner calls
     * 
     * @param tableName
     * @param startAndPrefix
     * @param columns
     */
    public int scannerOpenWithPrefix(byte[] tableName, byte[] startAndPrefix, List<byte[]> columns) throws IOError, TException;

    /**
     * Get a scanner on the current table starting at the specified row and
     * ending at the last row in the table.  Return the specified columns.
     * Only values with the specified timestamp are returned.
     * 
     * @param columns columns to scan. If column name is a column family, all
     * columns of the specified column family are returned.  Its also possible
     * to pass a regex in the column qualifier.
     * @param tableName name of table
     * @param startRow starting row in table to scan.  send "" (empty string) to
     *                 start at the first row.
     * @param timestamp timestamp
     * 
     * @return scanner id to be used with other scanner procedures
     * 
     * @param tableName
     * @param startRow
     * @param columns
     * @param timestamp
     */
    public int scannerOpenTs(byte[] tableName, byte[] startRow, List<byte[]> columns, long timestamp) throws IOError, TException;

    /**
     * Get a scanner on the current table starting and stopping at the
     * specified rows.  ending at the last row in the table.  Return the
     * specified columns.  Only values with the specified timestamp are
     * returned.
     * 
     * @param columns columns to scan. If column name is a column family, all
     * columns of the specified column family are returned.  Its also possible
     * to pass a regex in the column qualifier.
     * @param tableName name of table
     * @param startRow starting row in table to scan.  send "" (empty string) to
     *                 start at the first row.
     * @param stopRow row to stop scanning on.  This row is *not* included
     *                in the scanner's results
     * @param timestamp timestamp
     * 
     * @return scanner id to be used with other scanner procedures
     * 
     * @param tableName
     * @param startRow
     * @param stopRow
     * @param columns
     * @param timestamp
     */
    public int scannerOpenWithStopTs(byte[] tableName, byte[] startRow, byte[] stopRow, List<byte[]> columns, long timestamp) throws IOError, TException;

    /**
     * Returns the scanner's current row value and advances to the next
     * row in the table.  When there are no more rows in the table, or a key
     * greater-than-or-equal-to the scanner's specified stopRow is reached,
     * an empty list is returned.
     * 
     * @param id id of a scanner returned by scannerOpen
     * @return a TRowResult containing the current row and a map of the columns to TCells.
     * @throws IllegalArgument if ScannerID is invalid
     * @throws NotFound when the scanner reaches the end
     * 
     * @param id
     */
    public List<TRowResult> scannerGet(int id) throws IOError, IllegalArgument, TException;

    /**
     * Returns, starting at the scanner's current row value nbRows worth of
     * rows and advances to the next row in the table.  When there are no more
     * rows in the table, or a key greater-than-or-equal-to the scanner's
     * specified stopRow is reached,  an empty list is returned.
     * 
     * @param id id of a scanner returned by scannerOpen
     * @param nbRows number of results to regturn
     * @return a TRowResult containing the current row and a map of the columns to TCells.
     * @throws IllegalArgument if ScannerID is invalid
     * @throws NotFound when the scanner reaches the end
     * 
     * @param id
     * @param nbRows
     */
    public List<TRowResult> scannerGetList(int id, int nbRows) throws IOError, IllegalArgument, TException;

    /**
     * Closes the server-state associated with an open scanner.
     * 
     * @param id id of a scanner returned by scannerOpen
     * @throws IllegalArgument if ScannerID is invalid
     * 
     * @param id
     */
    public void scannerClose(int id) throws IOError, IllegalArgument, TException;

  }

  public static class Client implements Iface {
    public Client(TProtocol prot)
    {
      this(prot, prot);
    }

    public Client(TProtocol iprot, TProtocol oprot)
    {
      iprot_ = iprot;
      oprot_ = oprot;
    }

    protected TProtocol iprot_;
    protected TProtocol oprot_;

    protected int seqid_;

    public TProtocol getInputProtocol()
    {
      return this.iprot_;
    }

    public TProtocol getOutputProtocol()
    {
      return this.oprot_;
    }

    public void enableTable(byte[] tableName) throws IOError, TException
    {
      send_enableTable(tableName);
      recv_enableTable();
    }

    public void send_enableTable(byte[] tableName) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("enableTable", TMessageType.CALL, seqid_));
      enableTable_args args = new enableTable_args();
      args.tableName = tableName;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_enableTable() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      enableTable_result result = new enableTable_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.io != null) {
        throw result.io;
      }
      return;
    }

    public void disableTable(byte[] tableName) throws IOError, TException
    {
      send_disableTable(tableName);
      recv_disableTable();
    }

    public void send_disableTable(byte[] tableName) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("disableTable", TMessageType.CALL, seqid_));
      disableTable_args args = new disableTable_args();
      args.tableName = tableName;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_disableTable() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      disableTable_result result = new disableTable_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.io != null) {
        throw result.io;
      }
      return;
    }

    public boolean isTableEnabled(byte[] tableName) throws IOError, TException
    {
      send_isTableEnabled(tableName);
      return recv_isTableEnabled();
    }

    public void send_isTableEnabled(byte[] tableName) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("isTableEnabled", TMessageType.CALL, seqid_));
      isTableEnabled_args args = new isTableEnabled_args();
      args.tableName = tableName;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public boolean recv_isTableEnabled() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      isTableEnabled_result result = new isTableEnabled_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "isTableEnabled failed: unknown result");
    }

    public void compact(byte[] tableNameOrRegionName) throws IOError, TException
    {
      send_compact(tableNameOrRegionName);
      recv_compact();
    }

    public void send_compact(byte[] tableNameOrRegionName) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("compact", TMessageType.CALL, seqid_));
      compact_args args = new compact_args();
      args.tableNameOrRegionName = tableNameOrRegionName;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_compact() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      compact_result result = new compact_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.io != null) {
        throw result.io;
      }
      return;
    }

    public void majorCompact(byte[] tableNameOrRegionName) throws IOError, TException
    {
      send_majorCompact(tableNameOrRegionName);
      recv_majorCompact();
    }

    public void send_majorCompact(byte[] tableNameOrRegionName) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("majorCompact", TMessageType.CALL, seqid_));
      majorCompact_args args = new majorCompact_args();
      args.tableNameOrRegionName = tableNameOrRegionName;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_majorCompact() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      majorCompact_result result = new majorCompact_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.io != null) {
        throw result.io;
      }
      return;
    }

    public List<byte[]> getTableNames() throws IOError, TException
    {
      send_getTableNames();
      return recv_getTableNames();
    }

    public void send_getTableNames() throws TException
    {
      oprot_.writeMessageBegin(new TMessage("getTableNames", TMessageType.CALL, seqid_));
      getTableNames_args args = new getTableNames_args();
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public List<byte[]> recv_getTableNames() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      getTableNames_result result = new getTableNames_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "getTableNames failed: unknown result");
    }

    public Map<byte[],ColumnDescriptor> getColumnDescriptors(byte[] tableName) throws IOError, TException
    {
      send_getColumnDescriptors(tableName);
      return recv_getColumnDescriptors();
    }

    public void send_getColumnDescriptors(byte[] tableName) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("getColumnDescriptors", TMessageType.CALL, seqid_));
      getColumnDescriptors_args args = new getColumnDescriptors_args();
      args.tableName = tableName;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public Map<byte[],ColumnDescriptor> recv_getColumnDescriptors() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      getColumnDescriptors_result result = new getColumnDescriptors_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "getColumnDescriptors failed: unknown result");
    }

    public List<TRegionInfo> getTableRegions(byte[] tableName) throws IOError, TException
    {
      send_getTableRegions(tableName);
      return recv_getTableRegions();
    }

    public void send_getTableRegions(byte[] tableName) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("getTableRegions", TMessageType.CALL, seqid_));
      getTableRegions_args args = new getTableRegions_args();
      args.tableName = tableName;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public List<TRegionInfo> recv_getTableRegions() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      getTableRegions_result result = new getTableRegions_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "getTableRegions failed: unknown result");
    }

    public void createTable(byte[] tableName, List<ColumnDescriptor> columnFamilies) throws IOError, IllegalArgument, AlreadyExists, TException
    {
      send_createTable(tableName, columnFamilies);
      recv_createTable();
    }

    public void send_createTable(byte[] tableName, List<ColumnDescriptor> columnFamilies) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("createTable", TMessageType.CALL, seqid_));
      createTable_args args = new createTable_args();
      args.tableName = tableName;
      args.columnFamilies = columnFamilies;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_createTable() throws IOError, IllegalArgument, AlreadyExists, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      createTable_result result = new createTable_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.io != null) {
        throw result.io;
      }
      if (result.ia != null) {
        throw result.ia;
      }
      if (result.exist != null) {
        throw result.exist;
      }
      return;
    }

    public void deleteTable(byte[] tableName) throws IOError, TException
    {
      send_deleteTable(tableName);
      recv_deleteTable();
    }

    public void send_deleteTable(byte[] tableName) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("deleteTable", TMessageType.CALL, seqid_));
      deleteTable_args args = new deleteTable_args();
      args.tableName = tableName;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_deleteTable() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      deleteTable_result result = new deleteTable_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.io != null) {
        throw result.io;
      }
      return;
    }

    public List<TCell> get(byte[] tableName, byte[] row, byte[] column) throws IOError, TException
    {
      send_get(tableName, row, column);
      return recv_get();
    }

    public void send_get(byte[] tableName, byte[] row, byte[] column) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("get", TMessageType.CALL, seqid_));
      get_args args = new get_args();
      args.tableName = tableName;
      args.row = row;
      args.column = column;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public List<TCell> recv_get() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      get_result result = new get_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "get failed: unknown result");
    }

    public List<TCell> getVer(byte[] tableName, byte[] row, byte[] column, int numVersions) throws IOError, TException
    {
      send_getVer(tableName, row, column, numVersions);
      return recv_getVer();
    }

    public void send_getVer(byte[] tableName, byte[] row, byte[] column, int numVersions) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("getVer", TMessageType.CALL, seqid_));
      getVer_args args = new getVer_args();
      args.tableName = tableName;
      args.row = row;
      args.column = column;
      args.numVersions = numVersions;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public List<TCell> recv_getVer() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      getVer_result result = new getVer_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "getVer failed: unknown result");
    }

    public List<TCell> getVerTs(byte[] tableName, byte[] row, byte[] column, long timestamp, int numVersions) throws IOError, TException
    {
      send_getVerTs(tableName, row, column, timestamp, numVersions);
      return recv_getVerTs();
    }

    public void send_getVerTs(byte[] tableName, byte[] row, byte[] column, long timestamp, int numVersions) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("getVerTs", TMessageType.CALL, seqid_));
      getVerTs_args args = new getVerTs_args();
      args.tableName = tableName;
      args.row = row;
      args.column = column;
      args.timestamp = timestamp;
      args.numVersions = numVersions;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public List<TCell> recv_getVerTs() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      getVerTs_result result = new getVerTs_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "getVerTs failed: unknown result");
    }

    public List<TRowResult> getRow(byte[] tableName, byte[] row) throws IOError, TException
    {
      send_getRow(tableName, row);
      return recv_getRow();
    }

    public void send_getRow(byte[] tableName, byte[] row) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("getRow", TMessageType.CALL, seqid_));
      getRow_args args = new getRow_args();
      args.tableName = tableName;
      args.row = row;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public List<TRowResult> recv_getRow() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      getRow_result result = new getRow_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "getRow failed: unknown result");
    }

    public List<TRowResult> getRowWithColumns(byte[] tableName, byte[] row, List<byte[]> columns) throws IOError, TException
    {
      send_getRowWithColumns(tableName, row, columns);
      return recv_getRowWithColumns();
    }

    public void send_getRowWithColumns(byte[] tableName, byte[] row, List<byte[]> columns) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("getRowWithColumns", TMessageType.CALL, seqid_));
      getRowWithColumns_args args = new getRowWithColumns_args();
      args.tableName = tableName;
      args.row = row;
      args.columns = columns;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public List<TRowResult> recv_getRowWithColumns() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      getRowWithColumns_result result = new getRowWithColumns_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "getRowWithColumns failed: unknown result");
    }

    public List<TRowResult> getRowTs(byte[] tableName, byte[] row, long timestamp) throws IOError, TException
    {
      send_getRowTs(tableName, row, timestamp);
      return recv_getRowTs();
    }

    public void send_getRowTs(byte[] tableName, byte[] row, long timestamp) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("getRowTs", TMessageType.CALL, seqid_));
      getRowTs_args args = new getRowTs_args();
      args.tableName = tableName;
      args.row = row;
      args.timestamp = timestamp;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public List<TRowResult> recv_getRowTs() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      getRowTs_result result = new getRowTs_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "getRowTs failed: unknown result");
    }

    public List<TRowResult> getRowWithColumnsTs(byte[] tableName, byte[] row, List<byte[]> columns, long timestamp) throws IOError, TException
    {
      send_getRowWithColumnsTs(tableName, row, columns, timestamp);
      return recv_getRowWithColumnsTs();
    }

    public void send_getRowWithColumnsTs(byte[] tableName, byte[] row, List<byte[]> columns, long timestamp) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("getRowWithColumnsTs", TMessageType.CALL, seqid_));
      getRowWithColumnsTs_args args = new getRowWithColumnsTs_args();
      args.tableName = tableName;
      args.row = row;
      args.columns = columns;
      args.timestamp = timestamp;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public List<TRowResult> recv_getRowWithColumnsTs() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      getRowWithColumnsTs_result result = new getRowWithColumnsTs_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "getRowWithColumnsTs failed: unknown result");
    }

    public void mutateRow(byte[] tableName, byte[] row, List<Mutation> mutations) throws IOError, IllegalArgument, TException
    {
      send_mutateRow(tableName, row, mutations);
      recv_mutateRow();
    }

    public void send_mutateRow(byte[] tableName, byte[] row, List<Mutation> mutations) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("mutateRow", TMessageType.CALL, seqid_));
      mutateRow_args args = new mutateRow_args();
      args.tableName = tableName;
      args.row = row;
      args.mutations = mutations;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_mutateRow() throws IOError, IllegalArgument, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      mutateRow_result result = new mutateRow_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.io != null) {
        throw result.io;
      }
      if (result.ia != null) {
        throw result.ia;
      }
      return;
    }

    public void mutateRowTs(byte[] tableName, byte[] row, List<Mutation> mutations, long timestamp) throws IOError, IllegalArgument, TException
    {
      send_mutateRowTs(tableName, row, mutations, timestamp);
      recv_mutateRowTs();
    }

    public void send_mutateRowTs(byte[] tableName, byte[] row, List<Mutation> mutations, long timestamp) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("mutateRowTs", TMessageType.CALL, seqid_));
      mutateRowTs_args args = new mutateRowTs_args();
      args.tableName = tableName;
      args.row = row;
      args.mutations = mutations;
      args.timestamp = timestamp;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_mutateRowTs() throws IOError, IllegalArgument, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      mutateRowTs_result result = new mutateRowTs_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.io != null) {
        throw result.io;
      }
      if (result.ia != null) {
        throw result.ia;
      }
      return;
    }

    public void mutateRows(byte[] tableName, List<BatchMutation> rowBatches) throws IOError, IllegalArgument, TException
    {
      send_mutateRows(tableName, rowBatches);
      recv_mutateRows();
    }

    public void send_mutateRows(byte[] tableName, List<BatchMutation> rowBatches) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("mutateRows", TMessageType.CALL, seqid_));
      mutateRows_args args = new mutateRows_args();
      args.tableName = tableName;
      args.rowBatches = rowBatches;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_mutateRows() throws IOError, IllegalArgument, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      mutateRows_result result = new mutateRows_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.io != null) {
        throw result.io;
      }
      if (result.ia != null) {
        throw result.ia;
      }
      return;
    }

    public void mutateRowsTs(byte[] tableName, List<BatchMutation> rowBatches, long timestamp) throws IOError, IllegalArgument, TException
    {
      send_mutateRowsTs(tableName, rowBatches, timestamp);
      recv_mutateRowsTs();
    }

    public void send_mutateRowsTs(byte[] tableName, List<BatchMutation> rowBatches, long timestamp) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("mutateRowsTs", TMessageType.CALL, seqid_));
      mutateRowsTs_args args = new mutateRowsTs_args();
      args.tableName = tableName;
      args.rowBatches = rowBatches;
      args.timestamp = timestamp;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_mutateRowsTs() throws IOError, IllegalArgument, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      mutateRowsTs_result result = new mutateRowsTs_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.io != null) {
        throw result.io;
      }
      if (result.ia != null) {
        throw result.ia;
      }
      return;
    }

    public long atomicIncrement(byte[] tableName, byte[] row, byte[] column, long value) throws IOError, IllegalArgument, TException
    {
      send_atomicIncrement(tableName, row, column, value);
      return recv_atomicIncrement();
    }

    public void send_atomicIncrement(byte[] tableName, byte[] row, byte[] column, long value) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("atomicIncrement", TMessageType.CALL, seqid_));
      atomicIncrement_args args = new atomicIncrement_args();
      args.tableName = tableName;
      args.row = row;
      args.column = column;
      args.value = value;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public long recv_atomicIncrement() throws IOError, IllegalArgument, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      atomicIncrement_result result = new atomicIncrement_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      if (result.ia != null) {
        throw result.ia;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "atomicIncrement failed: unknown result");
    }

    public void deleteAll(byte[] tableName, byte[] row, byte[] column) throws IOError, TException
    {
      send_deleteAll(tableName, row, column);
      recv_deleteAll();
    }

    public void send_deleteAll(byte[] tableName, byte[] row, byte[] column) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("deleteAll", TMessageType.CALL, seqid_));
      deleteAll_args args = new deleteAll_args();
      args.tableName = tableName;
      args.row = row;
      args.column = column;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_deleteAll() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      deleteAll_result result = new deleteAll_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.io != null) {
        throw result.io;
      }
      return;
    }

    public void deleteAllTs(byte[] tableName, byte[] row, byte[] column, long timestamp) throws IOError, TException
    {
      send_deleteAllTs(tableName, row, column, timestamp);
      recv_deleteAllTs();
    }

    public void send_deleteAllTs(byte[] tableName, byte[] row, byte[] column, long timestamp) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("deleteAllTs", TMessageType.CALL, seqid_));
      deleteAllTs_args args = new deleteAllTs_args();
      args.tableName = tableName;
      args.row = row;
      args.column = column;
      args.timestamp = timestamp;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_deleteAllTs() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      deleteAllTs_result result = new deleteAllTs_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.io != null) {
        throw result.io;
      }
      return;
    }

    public void deleteAllRow(byte[] tableName, byte[] row) throws IOError, TException
    {
      send_deleteAllRow(tableName, row);
      recv_deleteAllRow();
    }

    public void send_deleteAllRow(byte[] tableName, byte[] row) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("deleteAllRow", TMessageType.CALL, seqid_));
      deleteAllRow_args args = new deleteAllRow_args();
      args.tableName = tableName;
      args.row = row;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_deleteAllRow() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      deleteAllRow_result result = new deleteAllRow_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.io != null) {
        throw result.io;
      }
      return;
    }

    public void deleteAllRowTs(byte[] tableName, byte[] row, long timestamp) throws IOError, TException
    {
      send_deleteAllRowTs(tableName, row, timestamp);
      recv_deleteAllRowTs();
    }

    public void send_deleteAllRowTs(byte[] tableName, byte[] row, long timestamp) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("deleteAllRowTs", TMessageType.CALL, seqid_));
      deleteAllRowTs_args args = new deleteAllRowTs_args();
      args.tableName = tableName;
      args.row = row;
      args.timestamp = timestamp;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_deleteAllRowTs() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      deleteAllRowTs_result result = new deleteAllRowTs_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.io != null) {
        throw result.io;
      }
      return;
    }

    public int scannerOpen(byte[] tableName, byte[] startRow, List<byte[]> columns) throws IOError, TException
    {
      send_scannerOpen(tableName, startRow, columns);
      return recv_scannerOpen();
    }

    public void send_scannerOpen(byte[] tableName, byte[] startRow, List<byte[]> columns) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("scannerOpen", TMessageType.CALL, seqid_));
      scannerOpen_args args = new scannerOpen_args();
      args.tableName = tableName;
      args.startRow = startRow;
      args.columns = columns;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public int recv_scannerOpen() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      scannerOpen_result result = new scannerOpen_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "scannerOpen failed: unknown result");
    }

    public int scannerOpenWithStop(byte[] tableName, byte[] startRow, byte[] stopRow, List<byte[]> columns) throws IOError, TException
    {
      send_scannerOpenWithStop(tableName, startRow, stopRow, columns);
      return recv_scannerOpenWithStop();
    }

    public void send_scannerOpenWithStop(byte[] tableName, byte[] startRow, byte[] stopRow, List<byte[]> columns) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("scannerOpenWithStop", TMessageType.CALL, seqid_));
      scannerOpenWithStop_args args = new scannerOpenWithStop_args();
      args.tableName = tableName;
      args.startRow = startRow;
      args.stopRow = stopRow;
      args.columns = columns;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public int recv_scannerOpenWithStop() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      scannerOpenWithStop_result result = new scannerOpenWithStop_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "scannerOpenWithStop failed: unknown result");
    }

    public int scannerOpenWithPrefix(byte[] tableName, byte[] startAndPrefix, List<byte[]> columns) throws IOError, TException
    {
      send_scannerOpenWithPrefix(tableName, startAndPrefix, columns);
      return recv_scannerOpenWithPrefix();
    }

    public void send_scannerOpenWithPrefix(byte[] tableName, byte[] startAndPrefix, List<byte[]> columns) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("scannerOpenWithPrefix", TMessageType.CALL, seqid_));
      scannerOpenWithPrefix_args args = new scannerOpenWithPrefix_args();
      args.tableName = tableName;
      args.startAndPrefix = startAndPrefix;
      args.columns = columns;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public int recv_scannerOpenWithPrefix() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      scannerOpenWithPrefix_result result = new scannerOpenWithPrefix_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "scannerOpenWithPrefix failed: unknown result");
    }

    public int scannerOpenTs(byte[] tableName, byte[] startRow, List<byte[]> columns, long timestamp) throws IOError, TException
    {
      send_scannerOpenTs(tableName, startRow, columns, timestamp);
      return recv_scannerOpenTs();
    }

    public void send_scannerOpenTs(byte[] tableName, byte[] startRow, List<byte[]> columns, long timestamp) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("scannerOpenTs", TMessageType.CALL, seqid_));
      scannerOpenTs_args args = new scannerOpenTs_args();
      args.tableName = tableName;
      args.startRow = startRow;
      args.columns = columns;
      args.timestamp = timestamp;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public int recv_scannerOpenTs() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      scannerOpenTs_result result = new scannerOpenTs_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "scannerOpenTs failed: unknown result");
    }

    public int scannerOpenWithStopTs(byte[] tableName, byte[] startRow, byte[] stopRow, List<byte[]> columns, long timestamp) throws IOError, TException
    {
      send_scannerOpenWithStopTs(tableName, startRow, stopRow, columns, timestamp);
      return recv_scannerOpenWithStopTs();
    }

    public void send_scannerOpenWithStopTs(byte[] tableName, byte[] startRow, byte[] stopRow, List<byte[]> columns, long timestamp) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("scannerOpenWithStopTs", TMessageType.CALL, seqid_));
      scannerOpenWithStopTs_args args = new scannerOpenWithStopTs_args();
      args.tableName = tableName;
      args.startRow = startRow;
      args.stopRow = stopRow;
      args.columns = columns;
      args.timestamp = timestamp;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public int recv_scannerOpenWithStopTs() throws IOError, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      scannerOpenWithStopTs_result result = new scannerOpenWithStopTs_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "scannerOpenWithStopTs failed: unknown result");
    }

    public List<TRowResult> scannerGet(int id) throws IOError, IllegalArgument, TException
    {
      send_scannerGet(id);
      return recv_scannerGet();
    }

    public void send_scannerGet(int id) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("scannerGet", TMessageType.CALL, seqid_));
      scannerGet_args args = new scannerGet_args();
      args.id = id;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public List<TRowResult> recv_scannerGet() throws IOError, IllegalArgument, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      scannerGet_result result = new scannerGet_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      if (result.ia != null) {
        throw result.ia;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "scannerGet failed: unknown result");
    }

    public List<TRowResult> scannerGetList(int id, int nbRows) throws IOError, IllegalArgument, TException
    {
      send_scannerGetList(id, nbRows);
      return recv_scannerGetList();
    }

    public void send_scannerGetList(int id, int nbRows) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("scannerGetList", TMessageType.CALL, seqid_));
      scannerGetList_args args = new scannerGetList_args();
      args.id = id;
      args.nbRows = nbRows;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public List<TRowResult> recv_scannerGetList() throws IOError, IllegalArgument, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      scannerGetList_result result = new scannerGetList_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      if (result.io != null) {
        throw result.io;
      }
      if (result.ia != null) {
        throw result.ia;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "scannerGetList failed: unknown result");
    }

    public void scannerClose(int id) throws IOError, IllegalArgument, TException
    {
      send_scannerClose(id);
      recv_scannerClose();
    }

    public void send_scannerClose(int id) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("scannerClose", TMessageType.CALL, seqid_));
      scannerClose_args args = new scannerClose_args();
      args.id = id;
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public void recv_scannerClose() throws IOError, IllegalArgument, TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      scannerClose_result result = new scannerClose_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.io != null) {
        throw result.io;
      }
      if (result.ia != null) {
        throw result.ia;
      }
      return;
    }

  }
  public static class Processor implements TProcessor {
    public Processor(Iface iface)
    {
      iface_ = iface;
      processMap_.put("enableTable", new enableTable());
      processMap_.put("disableTable", new disableTable());
      processMap_.put("isTableEnabled", new isTableEnabled());
      processMap_.put("compact", new compact());
      processMap_.put("majorCompact", new majorCompact());
      processMap_.put("getTableNames", new getTableNames());
      processMap_.put("getColumnDescriptors", new getColumnDescriptors());
      processMap_.put("getTableRegions", new getTableRegions());
      processMap_.put("createTable", new createTable());
      processMap_.put("deleteTable", new deleteTable());
      processMap_.put("get", new get());
      processMap_.put("getVer", new getVer());
      processMap_.put("getVerTs", new getVerTs());
      processMap_.put("getRow", new getRow());
      processMap_.put("getRowWithColumns", new getRowWithColumns());
      processMap_.put("getRowTs", new getRowTs());
      processMap_.put("getRowWithColumnsTs", new getRowWithColumnsTs());
      processMap_.put("mutateRow", new mutateRow());
      processMap_.put("mutateRowTs", new mutateRowTs());
      processMap_.put("mutateRows", new mutateRows());
      processMap_.put("mutateRowsTs", new mutateRowsTs());
      processMap_.put("atomicIncrement", new atomicIncrement());
      processMap_.put("deleteAll", new deleteAll());
      processMap_.put("deleteAllTs", new deleteAllTs());
      processMap_.put("deleteAllRow", new deleteAllRow());
      processMap_.put("deleteAllRowTs", new deleteAllRowTs());
      processMap_.put("scannerOpen", new scannerOpen());
      processMap_.put("scannerOpenWithStop", new scannerOpenWithStop());
      processMap_.put("scannerOpenWithPrefix", new scannerOpenWithPrefix());
      processMap_.put("scannerOpenTs", new scannerOpenTs());
      processMap_.put("scannerOpenWithStopTs", new scannerOpenWithStopTs());
      processMap_.put("scannerGet", new scannerGet());
      processMap_.put("scannerGetList", new scannerGetList());
      processMap_.put("scannerClose", new scannerClose());
    }

    protected static interface ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException;
    }

    private Iface iface_;
    protected final HashMap<String,ProcessFunction> processMap_ = new HashMap<String,ProcessFunction>();

    public boolean process(TProtocol iprot, TProtocol oprot) throws TException
    {
      TMessage msg = iprot.readMessageBegin();
      ProcessFunction fn = processMap_.get(msg.name);
      if (fn == null) {
        TProtocolUtil.skip(iprot, TType.STRUCT);
        iprot.readMessageEnd();
        TApplicationException x = new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: '"+msg.name+"'");
        oprot.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
        x.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
        return true;
      }
      fn.process(msg.seqid, iprot, oprot);
      return true;
    }

    private class enableTable implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        enableTable_args args = new enableTable_args();
        args.read(iprot);
        iprot.readMessageEnd();
        enableTable_result result = new enableTable_result();
        try {
          iface_.enableTable(args.tableName);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("enableTable", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class disableTable implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        disableTable_args args = new disableTable_args();
        args.read(iprot);
        iprot.readMessageEnd();
        disableTable_result result = new disableTable_result();
        try {
          iface_.disableTable(args.tableName);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("disableTable", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class isTableEnabled implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        isTableEnabled_args args = new isTableEnabled_args();
        args.read(iprot);
        iprot.readMessageEnd();
        isTableEnabled_result result = new isTableEnabled_result();
        try {
          result.success = iface_.isTableEnabled(args.tableName);
          result.__isset.success = true;
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("isTableEnabled", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class compact implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        compact_args args = new compact_args();
        args.read(iprot);
        iprot.readMessageEnd();
        compact_result result = new compact_result();
        try {
          iface_.compact(args.tableNameOrRegionName);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("compact", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class majorCompact implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        majorCompact_args args = new majorCompact_args();
        args.read(iprot);
        iprot.readMessageEnd();
        majorCompact_result result = new majorCompact_result();
        try {
          iface_.majorCompact(args.tableNameOrRegionName);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("majorCompact", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class getTableNames implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        getTableNames_args args = new getTableNames_args();
        args.read(iprot);
        iprot.readMessageEnd();
        getTableNames_result result = new getTableNames_result();
        try {
          result.success = iface_.getTableNames();
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("getTableNames", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class getColumnDescriptors implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        getColumnDescriptors_args args = new getColumnDescriptors_args();
        args.read(iprot);
        iprot.readMessageEnd();
        getColumnDescriptors_result result = new getColumnDescriptors_result();
        try {
          result.success = iface_.getColumnDescriptors(args.tableName);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("getColumnDescriptors", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class getTableRegions implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        getTableRegions_args args = new getTableRegions_args();
        args.read(iprot);
        iprot.readMessageEnd();
        getTableRegions_result result = new getTableRegions_result();
        try {
          result.success = iface_.getTableRegions(args.tableName);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("getTableRegions", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class createTable implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        createTable_args args = new createTable_args();
        args.read(iprot);
        iprot.readMessageEnd();
        createTable_result result = new createTable_result();
        try {
          iface_.createTable(args.tableName, args.columnFamilies);
        } catch (IOError io) {
          result.io = io;
        } catch (IllegalArgument ia) {
          result.ia = ia;
        } catch (AlreadyExists exist) {
          result.exist = exist;
        }
        oprot.writeMessageBegin(new TMessage("createTable", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class deleteTable implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        deleteTable_args args = new deleteTable_args();
        args.read(iprot);
        iprot.readMessageEnd();
        deleteTable_result result = new deleteTable_result();
        try {
          iface_.deleteTable(args.tableName);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("deleteTable", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class get implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        get_args args = new get_args();
        args.read(iprot);
        iprot.readMessageEnd();
        get_result result = new get_result();
        try {
          result.success = iface_.get(args.tableName, args.row, args.column);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("get", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class getVer implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        getVer_args args = new getVer_args();
        args.read(iprot);
        iprot.readMessageEnd();
        getVer_result result = new getVer_result();
        try {
          result.success = iface_.getVer(args.tableName, args.row, args.column, args.numVersions);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("getVer", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class getVerTs implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        getVerTs_args args = new getVerTs_args();
        args.read(iprot);
        iprot.readMessageEnd();
        getVerTs_result result = new getVerTs_result();
        try {
          result.success = iface_.getVerTs(args.tableName, args.row, args.column, args.timestamp, args.numVersions);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("getVerTs", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class getRow implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        getRow_args args = new getRow_args();
        args.read(iprot);
        iprot.readMessageEnd();
        getRow_result result = new getRow_result();
        try {
          result.success = iface_.getRow(args.tableName, args.row);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("getRow", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class getRowWithColumns implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        getRowWithColumns_args args = new getRowWithColumns_args();
        args.read(iprot);
        iprot.readMessageEnd();
        getRowWithColumns_result result = new getRowWithColumns_result();
        try {
          result.success = iface_.getRowWithColumns(args.tableName, args.row, args.columns);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("getRowWithColumns", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class getRowTs implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        getRowTs_args args = new getRowTs_args();
        args.read(iprot);
        iprot.readMessageEnd();
        getRowTs_result result = new getRowTs_result();
        try {
          result.success = iface_.getRowTs(args.tableName, args.row, args.timestamp);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("getRowTs", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class getRowWithColumnsTs implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        getRowWithColumnsTs_args args = new getRowWithColumnsTs_args();
        args.read(iprot);
        iprot.readMessageEnd();
        getRowWithColumnsTs_result result = new getRowWithColumnsTs_result();
        try {
          result.success = iface_.getRowWithColumnsTs(args.tableName, args.row, args.columns, args.timestamp);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("getRowWithColumnsTs", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class mutateRow implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        mutateRow_args args = new mutateRow_args();
        args.read(iprot);
        iprot.readMessageEnd();
        mutateRow_result result = new mutateRow_result();
        try {
          iface_.mutateRow(args.tableName, args.row, args.mutations);
        } catch (IOError io) {
          result.io = io;
        } catch (IllegalArgument ia) {
          result.ia = ia;
        }
        oprot.writeMessageBegin(new TMessage("mutateRow", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class mutateRowTs implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        mutateRowTs_args args = new mutateRowTs_args();
        args.read(iprot);
        iprot.readMessageEnd();
        mutateRowTs_result result = new mutateRowTs_result();
        try {
          iface_.mutateRowTs(args.tableName, args.row, args.mutations, args.timestamp);
        } catch (IOError io) {
          result.io = io;
        } catch (IllegalArgument ia) {
          result.ia = ia;
        }
        oprot.writeMessageBegin(new TMessage("mutateRowTs", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class mutateRows implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        mutateRows_args args = new mutateRows_args();
        args.read(iprot);
        iprot.readMessageEnd();
        mutateRows_result result = new mutateRows_result();
        try {
          iface_.mutateRows(args.tableName, args.rowBatches);
        } catch (IOError io) {
          result.io = io;
        } catch (IllegalArgument ia) {
          result.ia = ia;
        }
        oprot.writeMessageBegin(new TMessage("mutateRows", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class mutateRowsTs implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        mutateRowsTs_args args = new mutateRowsTs_args();
        args.read(iprot);
        iprot.readMessageEnd();
        mutateRowsTs_result result = new mutateRowsTs_result();
        try {
          iface_.mutateRowsTs(args.tableName, args.rowBatches, args.timestamp);
        } catch (IOError io) {
          result.io = io;
        } catch (IllegalArgument ia) {
          result.ia = ia;
        }
        oprot.writeMessageBegin(new TMessage("mutateRowsTs", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class atomicIncrement implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        atomicIncrement_args args = new atomicIncrement_args();
        args.read(iprot);
        iprot.readMessageEnd();
        atomicIncrement_result result = new atomicIncrement_result();
        try {
          result.success = iface_.atomicIncrement(args.tableName, args.row, args.column, args.value);
          result.__isset.success = true;
        } catch (IOError io) {
          result.io = io;
        } catch (IllegalArgument ia) {
          result.ia = ia;
        }
        oprot.writeMessageBegin(new TMessage("atomicIncrement", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class deleteAll implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        deleteAll_args args = new deleteAll_args();
        args.read(iprot);
        iprot.readMessageEnd();
        deleteAll_result result = new deleteAll_result();
        try {
          iface_.deleteAll(args.tableName, args.row, args.column);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("deleteAll", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class deleteAllTs implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        deleteAllTs_args args = new deleteAllTs_args();
        args.read(iprot);
        iprot.readMessageEnd();
        deleteAllTs_result result = new deleteAllTs_result();
        try {
          iface_.deleteAllTs(args.tableName, args.row, args.column, args.timestamp);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("deleteAllTs", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class deleteAllRow implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        deleteAllRow_args args = new deleteAllRow_args();
        args.read(iprot);
        iprot.readMessageEnd();
        deleteAllRow_result result = new deleteAllRow_result();
        try {
          iface_.deleteAllRow(args.tableName, args.row);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("deleteAllRow", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class deleteAllRowTs implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        deleteAllRowTs_args args = new deleteAllRowTs_args();
        args.read(iprot);
        iprot.readMessageEnd();
        deleteAllRowTs_result result = new deleteAllRowTs_result();
        try {
          iface_.deleteAllRowTs(args.tableName, args.row, args.timestamp);
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("deleteAllRowTs", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class scannerOpen implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        scannerOpen_args args = new scannerOpen_args();
        args.read(iprot);
        iprot.readMessageEnd();
        scannerOpen_result result = new scannerOpen_result();
        try {
          result.success = iface_.scannerOpen(args.tableName, args.startRow, args.columns);
          result.__isset.success = true;
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("scannerOpen", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class scannerOpenWithStop implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        scannerOpenWithStop_args args = new scannerOpenWithStop_args();
        args.read(iprot);
        iprot.readMessageEnd();
        scannerOpenWithStop_result result = new scannerOpenWithStop_result();
        try {
          result.success = iface_.scannerOpenWithStop(args.tableName, args.startRow, args.stopRow, args.columns);
          result.__isset.success = true;
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("scannerOpenWithStop", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class scannerOpenWithPrefix implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        scannerOpenWithPrefix_args args = new scannerOpenWithPrefix_args();
        args.read(iprot);
        iprot.readMessageEnd();
        scannerOpenWithPrefix_result result = new scannerOpenWithPrefix_result();
        try {
          result.success = iface_.scannerOpenWithPrefix(args.tableName, args.startAndPrefix, args.columns);
          result.__isset.success = true;
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("scannerOpenWithPrefix", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class scannerOpenTs implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        scannerOpenTs_args args = new scannerOpenTs_args();
        args.read(iprot);
        iprot.readMessageEnd();
        scannerOpenTs_result result = new scannerOpenTs_result();
        try {
          result.success = iface_.scannerOpenTs(args.tableName, args.startRow, args.columns, args.timestamp);
          result.__isset.success = true;
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("scannerOpenTs", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class scannerOpenWithStopTs implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        scannerOpenWithStopTs_args args = new scannerOpenWithStopTs_args();
        args.read(iprot);
        iprot.readMessageEnd();
        scannerOpenWithStopTs_result result = new scannerOpenWithStopTs_result();
        try {
          result.success = iface_.scannerOpenWithStopTs(args.tableName, args.startRow, args.stopRow, args.columns, args.timestamp);
          result.__isset.success = true;
        } catch (IOError io) {
          result.io = io;
        }
        oprot.writeMessageBegin(new TMessage("scannerOpenWithStopTs", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class scannerGet implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        scannerGet_args args = new scannerGet_args();
        args.read(iprot);
        iprot.readMessageEnd();
        scannerGet_result result = new scannerGet_result();
        try {
          result.success = iface_.scannerGet(args.id);
        } catch (IOError io) {
          result.io = io;
        } catch (IllegalArgument ia) {
          result.ia = ia;
        }
        oprot.writeMessageBegin(new TMessage("scannerGet", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class scannerGetList implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        scannerGetList_args args = new scannerGetList_args();
        args.read(iprot);
        iprot.readMessageEnd();
        scannerGetList_result result = new scannerGetList_result();
        try {
          result.success = iface_.scannerGetList(args.id, args.nbRows);
        } catch (IOError io) {
          result.io = io;
        } catch (IllegalArgument ia) {
          result.ia = ia;
        }
        oprot.writeMessageBegin(new TMessage("scannerGetList", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

    private class scannerClose implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        scannerClose_args args = new scannerClose_args();
        args.read(iprot);
        iprot.readMessageEnd();
        scannerClose_result result = new scannerClose_result();
        try {
          iface_.scannerClose(args.id);
        } catch (IOError io) {
          result.io = io;
        } catch (IllegalArgument ia) {
          result.ia = ia;
        }
        oprot.writeMessageBegin(new TMessage("scannerClose", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

  }

  public static class enableTable_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("enableTable_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);

    public byte[] tableName;
    public static final int TABLENAME = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(enableTable_args.class, metaDataMap);
    }

    public enableTable_args() {
    }

    public enableTable_args(
      byte[] tableName)
    {
      this();
      this.tableName = tableName;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public enableTable_args(enableTable_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
    }

    @Override
    public enableTable_args clone() {
      return new enableTable_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof enableTable_args)
        return this.equals((enableTable_args)that);
      return false;
    }

    public boolean equals(enableTable_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("enableTable_args(");

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class enableTable_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("enableTable_result");
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(enableTable_result.class, metaDataMap);
    }

    public enableTable_result() {
    }

    public enableTable_result(
      IOError io)
    {
      this();
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public enableTable_result(enableTable_result other) {
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public enableTable_result clone() {
      return new enableTable_result(this);
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof enableTable_result)
        return this.equals((enableTable_result)that);
      return false;
    }

    public boolean equals(enableTable_result that) {
      if (that == null)
        return false;

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("enableTable_result(");

      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class disableTable_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("disableTable_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);

    public byte[] tableName;
    public static final int TABLENAME = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(disableTable_args.class, metaDataMap);
    }

    public disableTable_args() {
    }

    public disableTable_args(
      byte[] tableName)
    {
      this();
      this.tableName = tableName;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public disableTable_args(disableTable_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
    }

    @Override
    public disableTable_args clone() {
      return new disableTable_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof disableTable_args)
        return this.equals((disableTable_args)that);
      return false;
    }

    public boolean equals(disableTable_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("disableTable_args(");

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class disableTable_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("disableTable_result");
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(disableTable_result.class, metaDataMap);
    }

    public disableTable_result() {
    }

    public disableTable_result(
      IOError io)
    {
      this();
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public disableTable_result(disableTable_result other) {
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public disableTable_result clone() {
      return new disableTable_result(this);
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof disableTable_result)
        return this.equals((disableTable_result)that);
      return false;
    }

    public boolean equals(disableTable_result that) {
      if (that == null)
        return false;

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("disableTable_result(");

      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class isTableEnabled_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("isTableEnabled_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);

    public byte[] tableName;
    public static final int TABLENAME = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(isTableEnabled_args.class, metaDataMap);
    }

    public isTableEnabled_args() {
    }

    public isTableEnabled_args(
      byte[] tableName)
    {
      this();
      this.tableName = tableName;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public isTableEnabled_args(isTableEnabled_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
    }

    @Override
    public isTableEnabled_args clone() {
      return new isTableEnabled_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof isTableEnabled_args)
        return this.equals((isTableEnabled_args)that);
      return false;
    }

    public boolean equals(isTableEnabled_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("isTableEnabled_args(");

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class isTableEnabled_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("isTableEnabled_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.BOOL, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public boolean success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean success = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.BOOL)));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(isTableEnabled_result.class, metaDataMap);
    }

    public isTableEnabled_result() {
    }

    public isTableEnabled_result(
      boolean success,
      IOError io)
    {
      this();
      this.success = success;
      this.__isset.success = true;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public isTableEnabled_result(isTableEnabled_result other) {
      __isset.success = other.__isset.success;
      this.success = other.success;
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public isTableEnabled_result clone() {
      return new isTableEnabled_result(this);
    }

    public boolean isSuccess() {
      return this.success;
    }

    public void setSuccess(boolean success) {
      this.success = success;
      this.__isset.success = true;
    }

    public void unsetSuccess() {
      this.__isset.success = false;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.__isset.success;
    }

    public void setSuccessIsSet(boolean value) {
      this.__isset.success = value;
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((Boolean)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return Boolean.valueOf(isSuccess());

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof isTableEnabled_result)
        return this.equals((isTableEnabled_result)that);
      return false;
    }

    public boolean equals(isTableEnabled_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true;
      boolean that_present_success = true;
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (this.success != that.success)
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.BOOL) {
              this.success = iprot.readBool();
              this.__isset.success = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        oprot.writeBool(this.success);
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("isTableEnabled_result(");
      boolean first = true;

      sb.append("success:");
      sb.append(this.success);
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class compact_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("compact_args");
    private static final TField TABLE_NAME_OR_REGION_NAME_FIELD_DESC = new TField("tableNameOrRegionName", TType.STRING, (short)1);

    public byte[] tableNameOrRegionName;
    public static final int TABLENAMEORREGIONNAME = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAMEORREGIONNAME, new FieldMetaData("tableNameOrRegionName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(compact_args.class, metaDataMap);
    }

    public compact_args() {
    }

    public compact_args(
      byte[] tableNameOrRegionName)
    {
      this();
      this.tableNameOrRegionName = tableNameOrRegionName;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public compact_args(compact_args other) {
      if (other.isSetTableNameOrRegionName()) {
        this.tableNameOrRegionName = other.tableNameOrRegionName;
      }
    }

    @Override
    public compact_args clone() {
      return new compact_args(this);
    }

    public byte[] getTableNameOrRegionName() {
      return this.tableNameOrRegionName;
    }

    public void setTableNameOrRegionName(byte[] tableNameOrRegionName) {
      this.tableNameOrRegionName = tableNameOrRegionName;
    }

    public void unsetTableNameOrRegionName() {
      this.tableNameOrRegionName = null;
    }

    // Returns true if field tableNameOrRegionName is set (has been asigned a value) and false otherwise
    public boolean isSetTableNameOrRegionName() {
      return this.tableNameOrRegionName != null;
    }

    public void setTableNameOrRegionNameIsSet(boolean value) {
      if (!value) {
        this.tableNameOrRegionName = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAMEORREGIONNAME:
        if (value == null) {
          unsetTableNameOrRegionName();
        } else {
          setTableNameOrRegionName((byte[])value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAMEORREGIONNAME:
        return getTableNameOrRegionName();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAMEORREGIONNAME:
        return isSetTableNameOrRegionName();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof compact_args)
        return this.equals((compact_args)that);
      return false;
    }

    public boolean equals(compact_args that) {
      if (that == null)
        return false;

      boolean this_present_tableNameOrRegionName = true && this.isSetTableNameOrRegionName();
      boolean that_present_tableNameOrRegionName = true && that.isSetTableNameOrRegionName();
      if (this_present_tableNameOrRegionName || that_present_tableNameOrRegionName) {
        if (!(this_present_tableNameOrRegionName && that_present_tableNameOrRegionName))
          return false;
        if (!java.util.Arrays.equals(this.tableNameOrRegionName, that.tableNameOrRegionName))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAMEORREGIONNAME:
            if (field.type == TType.STRING) {
              this.tableNameOrRegionName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableNameOrRegionName != null) {
        oprot.writeFieldBegin(TABLE_NAME_OR_REGION_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableNameOrRegionName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("compact_args(");

      sb.append("tableNameOrRegionName:");
      if (this.tableNameOrRegionName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.tableNameOrRegionName));
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class compact_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
   private static final TStruct STRUCT_DESC = new TStruct("compact_result");
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(compact_result.class, metaDataMap);
    }

    public compact_result() {
    }

    public compact_result(
      IOError io)
    {
      this();
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public compact_result(compact_result other) {
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public compact_result clone() {
      return new compact_result(this);
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof compact_result)
        return this.equals((compact_result)that);
      return false;
    }

    public boolean equals(compact_result that) {
      if (that == null)
        return false;

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("compact_result(");

      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class majorCompact_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("majorCompact_args");
    private static final TField TABLE_NAME_OR_REGION_NAME_FIELD_DESC = new TField("tableNameOrRegionName", TType.STRING, (short)1);

    public byte[] tableNameOrRegionName;
    public static final int TABLENAMEORREGIONNAME = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAMEORREGIONNAME, new FieldMetaData("tableNameOrRegionName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(majorCompact_args.class, metaDataMap);
    }

    public majorCompact_args() {
    }

    public majorCompact_args(
      byte[] tableNameOrRegionName)
    {
      this();
      this.tableNameOrRegionName = tableNameOrRegionName;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public majorCompact_args(majorCompact_args other) {
      if (other.isSetTableNameOrRegionName()) {
        this.tableNameOrRegionName = other.tableNameOrRegionName;
      }
    }

    @Override
    public majorCompact_args clone() {
      return new majorCompact_args(this);
    }

    public byte[] getTableNameOrRegionName() {
      return this.tableNameOrRegionName;
    }

    public void setTableNameOrRegionName(byte[] tableNameOrRegionName) {
      this.tableNameOrRegionName = tableNameOrRegionName;
    }

    public void unsetTableNameOrRegionName() {
      this.tableNameOrRegionName = null;
    }

    // Returns true if field tableNameOrRegionName is set (has been asigned a value) and false otherwise
    public boolean isSetTableNameOrRegionName() {
      return this.tableNameOrRegionName != null;
    }

    public void setTableNameOrRegionNameIsSet(boolean value) {
      if (!value) {
        this.tableNameOrRegionName = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAMEORREGIONNAME:
        if (value == null) {
          unsetTableNameOrRegionName();
        } else {
          setTableNameOrRegionName((byte[])value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAMEORREGIONNAME:
        return getTableNameOrRegionName();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAMEORREGIONNAME:
        return isSetTableNameOrRegionName();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof majorCompact_args)
        return this.equals((majorCompact_args)that);
      return false;
    }

    public boolean equals(majorCompact_args that) {
      if (that == null)
        return false;

      boolean this_present_tableNameOrRegionName = true && this.isSetTableNameOrRegionName();
      boolean that_present_tableNameOrRegionName = true && that.isSetTableNameOrRegionName();
      if (this_present_tableNameOrRegionName || that_present_tableNameOrRegionName) {
        if (!(this_present_tableNameOrRegionName && that_present_tableNameOrRegionName))
          return false;
        if (!java.util.Arrays.equals(this.tableNameOrRegionName, that.tableNameOrRegionName))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAMEORREGIONNAME:
            if (field.type == TType.STRING) {
              this.tableNameOrRegionName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableNameOrRegionName != null) {
        oprot.writeFieldBegin(TABLE_NAME_OR_REGION_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableNameOrRegionName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("majorCompact_args(");

      sb.append("tableNameOrRegionName:");
      if (this.tableNameOrRegionName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.tableNameOrRegionName));
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class majorCompact_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("majorCompact_result");
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(majorCompact_result.class, metaDataMap);
    }

    public majorCompact_result() {
    }

    public majorCompact_result(
      IOError io)
    {
      this();
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public majorCompact_result(majorCompact_result other) {
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public majorCompact_result clone() {
      return new majorCompact_result(this);
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof majorCompact_result)
        return this.equals((majorCompact_result)that);
      return false;
    }

    public boolean equals(majorCompact_result that) {
      if (that == null)
        return false;

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("majorCompact_result(");

      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getTableNames_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getTableNames_args");

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getTableNames_args.class, metaDataMap);
    }

    public getTableNames_args() {
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getTableNames_args(getTableNames_args other) {
    }

    @Override
    public getTableNames_args clone() {
      return new getTableNames_args(this);
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getTableNames_args)
        return this.equals((getTableNames_args)that);
      return false;
    }

    public boolean equals(getTableNames_args that) {
      if (that == null)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getTableNames_args(");
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getTableNames_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getTableNames_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.LIST, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public List<byte[]> success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new FieldValueMetaData(TType.STRING))));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getTableNames_result.class, metaDataMap);
    }

    public getTableNames_result() {
    }

    public getTableNames_result(
      List<byte[]> success,
      IOError io)
    {
      this();
      this.success = success;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getTableNames_result(getTableNames_result other) {
      if (other.isSetSuccess()) {
        List<byte[]> __this__success = new ArrayList<byte[]>();
        for (byte[] other_element : other.success) {
          __this__success.add(other_element);
        }
        this.success = __this__success;
      }
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public getTableNames_result clone() {
      return new getTableNames_result(this);
    }

    public int getSuccessSize() {
      return (this.success == null) ? 0 : this.success.size();
    }

    public java.util.Iterator<byte[]> getSuccessIterator() {
      return (this.success == null) ? null : this.success.iterator();
    }

    public void addToSuccess(byte[] elem) {
      if (this.success == null) {
        this.success = new ArrayList<byte[]>();
      }
      this.success.add(elem);
    }

    public List<byte[]> getSuccess() {
      return this.success;
    }

    public void setSuccess(List<byte[]> success) {
      this.success = success;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((List<byte[]>)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getTableNames_result)
        return this.equals((getTableNames_result)that);
      return false;
    }

    public boolean equals(getTableNames_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.LIST) {
              {
                TList _list9 = iprot.readListBegin();
                this.success = new ArrayList<byte[]>(_list9.size);
                for (int _i10 = 0; _i10 < _list9.size; ++_i10)
                {
                  byte[] _elem11;
                  _elem11 = iprot.readBinary();
                  this.success.add(_elem11);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRING, this.success.size()));
          for (byte[] _iter12 : this.success)          {
            oprot.writeBinary(_iter12);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getTableNames_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getColumnDescriptors_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getColumnDescriptors_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);

    public byte[] tableName;
    public static final int TABLENAME = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getColumnDescriptors_args.class, metaDataMap);
    }

    public getColumnDescriptors_args() {
    }

    public getColumnDescriptors_args(
      byte[] tableName)
    {
      this();
      this.tableName = tableName;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getColumnDescriptors_args(getColumnDescriptors_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
    }

    @Override
    public getColumnDescriptors_args clone() {
      return new getColumnDescriptors_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getColumnDescriptors_args)
        return this.equals((getColumnDescriptors_args)that);
      return false;
    }

    public boolean equals(getColumnDescriptors_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getColumnDescriptors_args(");

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getColumnDescriptors_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getColumnDescriptors_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.MAP, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public Map<byte[],ColumnDescriptor> success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new MapMetaData(TType.MAP, 
              new FieldValueMetaData(TType.STRING), 
              new StructMetaData(TType.STRUCT, ColumnDescriptor.class))));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getColumnDescriptors_result.class, metaDataMap);
    }

    public getColumnDescriptors_result() {
    }

    public getColumnDescriptors_result(
      Map<byte[],ColumnDescriptor> success,
      IOError io)
    {
      this();
      this.success = success;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getColumnDescriptors_result(getColumnDescriptors_result other) {
      if (other.isSetSuccess()) {
        Map<byte[],ColumnDescriptor> __this__success = new HashMap<byte[],ColumnDescriptor>();
        for (Map.Entry<byte[], ColumnDescriptor> other_element : other.success.entrySet()) {

          byte[] other_element_key = other_element.getKey();
          ColumnDescriptor other_element_value = other_element.getValue();

          byte[] __this__success_copy_key = other_element_key;

          ColumnDescriptor __this__success_copy_value = new ColumnDescriptor(other_element_value);

          __this__success.put(__this__success_copy_key, __this__success_copy_value);
        }
        this.success = __this__success;
      }
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public getColumnDescriptors_result clone() {
      return new getColumnDescriptors_result(this);
    }

    public int getSuccessSize() {
      return (this.success == null) ? 0 : this.success.size();
    }

    public void putToSuccess(byte[] key, ColumnDescriptor val) {
      if (this.success == null) {
        this.success = new HashMap<byte[],ColumnDescriptor>();
      }
      this.success.put(key, val);
    }

    public Map<byte[],ColumnDescriptor> getSuccess() {
      return this.success;
    }

    public void setSuccess(Map<byte[],ColumnDescriptor> success) {
      this.success = success;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((Map<byte[],ColumnDescriptor>)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getColumnDescriptors_result)
        return this.equals((getColumnDescriptors_result)that);
      return false;
    }

    public boolean equals(getColumnDescriptors_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.MAP) {
              {
                TMap _map13 = iprot.readMapBegin();
                this.success = new HashMap<byte[],ColumnDescriptor>(2*_map13.size);
                for (int _i14 = 0; _i14 < _map13.size; ++_i14)
                {
                  byte[] _key15;
                  ColumnDescriptor _val16;
                  _key15 = iprot.readBinary();
                  _val16 = new ColumnDescriptor();
                  _val16.read(iprot);
                  this.success.put(_key15, _val16);
                }
                iprot.readMapEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        {
          oprot.writeMapBegin(new TMap(TType.STRING, TType.STRUCT, this.success.size()));
          for (Map.Entry<byte[], ColumnDescriptor> _iter17 : this.success.entrySet())          {
            oprot.writeBinary(_iter17.getKey());
            _iter17.getValue().write(oprot);
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getColumnDescriptors_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getTableRegions_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getTableRegions_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);

    public byte[] tableName;
    public static final int TABLENAME = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getTableRegions_args.class, metaDataMap);
    }

    public getTableRegions_args() {
    }

    public getTableRegions_args(
      byte[] tableName)
    {
      this();
      this.tableName = tableName;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getTableRegions_args(getTableRegions_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
    }

    @Override
    public getTableRegions_args clone() {
      return new getTableRegions_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getTableRegions_args)
        return this.equals((getTableRegions_args)that);
      return false;
    }

    public boolean equals(getTableRegions_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getTableRegions_args(");

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getTableRegions_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getTableRegions_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.LIST, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public List<TRegionInfo> success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new StructMetaData(TType.STRUCT, TRegionInfo.class))));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getTableRegions_result.class, metaDataMap);
    }

    public getTableRegions_result() {
    }

    public getTableRegions_result(
      List<TRegionInfo> success,
      IOError io)
    {
      this();
      this.success = success;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getTableRegions_result(getTableRegions_result other) {
      if (other.isSetSuccess()) {
        List<TRegionInfo> __this__success = new ArrayList<TRegionInfo>();
        for (TRegionInfo other_element : other.success) {
          __this__success.add(new TRegionInfo(other_element));
        }
        this.success = __this__success;
      }
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public getTableRegions_result clone() {
      return new getTableRegions_result(this);
    }

    public int getSuccessSize() {
      return (this.success == null) ? 0 : this.success.size();
    }

    public java.util.Iterator<TRegionInfo> getSuccessIterator() {
      return (this.success == null) ? null : this.success.iterator();
    }

    public void addToSuccess(TRegionInfo elem) {
      if (this.success == null) {
        this.success = new ArrayList<TRegionInfo>();
      }
      this.success.add(elem);
    }

    public List<TRegionInfo> getSuccess() {
      return this.success;
    }

    public void setSuccess(List<TRegionInfo> success) {
      this.success = success;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((List<TRegionInfo>)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getTableRegions_result)
        return this.equals((getTableRegions_result)that);
      return false;
    }

    public boolean equals(getTableRegions_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.LIST) {
              {
                TList _list18 = iprot.readListBegin();
                this.success = new ArrayList<TRegionInfo>(_list18.size);
                for (int _i19 = 0; _i19 < _list18.size; ++_i19)
                {
                  TRegionInfo _elem20;
                  _elem20 = new TRegionInfo();
                  _elem20.read(iprot);
                  this.success.add(_elem20);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.success.size()));
          for (TRegionInfo _iter21 : this.success)          {
            _iter21.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getTableRegions_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class createTable_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("createTable_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField COLUMN_FAMILIES_FIELD_DESC = new TField("columnFamilies", TType.LIST, (short)2);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public List<ColumnDescriptor> columnFamilies;
    public static final int COLUMNFAMILIES = 2;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(COLUMNFAMILIES, new FieldMetaData("columnFamilies", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new StructMetaData(TType.STRUCT, ColumnDescriptor.class))));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(createTable_args.class, metaDataMap);
    }

    public createTable_args() {
    }

    public createTable_args(
      byte[] tableName,
      List<ColumnDescriptor> columnFamilies)
    {
      this();
      this.tableName = tableName;
      this.columnFamilies = columnFamilies;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public createTable_args(createTable_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetColumnFamilies()) {
        List<ColumnDescriptor> __this__columnFamilies = new ArrayList<ColumnDescriptor>();
        for (ColumnDescriptor other_element : other.columnFamilies) {
          __this__columnFamilies.add(new ColumnDescriptor(other_element));
        }
        this.columnFamilies = __this__columnFamilies;
      }
    }

    @Override
    public createTable_args clone() {
      return new createTable_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public int getColumnFamiliesSize() {
      return (this.columnFamilies == null) ? 0 : this.columnFamilies.size();
    }

    public java.util.Iterator<ColumnDescriptor> getColumnFamiliesIterator() {
      return (this.columnFamilies == null) ? null : this.columnFamilies.iterator();
    }

    public void addToColumnFamilies(ColumnDescriptor elem) {
      if (this.columnFamilies == null) {
        this.columnFamilies = new ArrayList<ColumnDescriptor>();
      }
      this.columnFamilies.add(elem);
    }

    public List<ColumnDescriptor> getColumnFamilies() {
      return this.columnFamilies;
    }

    public void setColumnFamilies(List<ColumnDescriptor> columnFamilies) {
      this.columnFamilies = columnFamilies;
    }

    public void unsetColumnFamilies() {
      this.columnFamilies = null;
    }

    // Returns true if field columnFamilies is set (has been asigned a value) and false otherwise
    public boolean isSetColumnFamilies() {
      return this.columnFamilies != null;
    }

    public void setColumnFamiliesIsSet(boolean value) {
      if (!value) {
        this.columnFamilies = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case COLUMNFAMILIES:
        if (value == null) {
          unsetColumnFamilies();
        } else {
          setColumnFamilies((List<ColumnDescriptor>)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case COLUMNFAMILIES:
        return getColumnFamilies();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case COLUMNFAMILIES:
        return isSetColumnFamilies();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof createTable_args)
        return this.equals((createTable_args)that);
      return false;
    }

    public boolean equals(createTable_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_columnFamilies = true && this.isSetColumnFamilies();
      boolean that_present_columnFamilies = true && that.isSetColumnFamilies();
      if (this_present_columnFamilies || that_present_columnFamilies) {
        if (!(this_present_columnFamilies && that_present_columnFamilies))
          return false;
        if (!this.columnFamilies.equals(that.columnFamilies))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case COLUMNFAMILIES:
            if (field.type == TType.LIST) {
              {
                TList _list22 = iprot.readListBegin();
                this.columnFamilies = new ArrayList<ColumnDescriptor>(_list22.size);
                for (int _i23 = 0; _i23 < _list22.size; ++_i23)
                {
                  ColumnDescriptor _elem24;
                  _elem24 = new ColumnDescriptor();
                  _elem24.read(iprot);
                  this.columnFamilies.add(_elem24);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.columnFamilies != null) {
        oprot.writeFieldBegin(COLUMN_FAMILIES_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.columnFamilies.size()));
          for (ColumnDescriptor _iter25 : this.columnFamilies)          {
            _iter25.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("createTable_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("columnFamilies:");
      if (this.columnFamilies == null) {
        sb.append("null");
      } else {
        sb.append(this.columnFamilies);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class createTable_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("createTable_result");
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);
    private static final TField IA_FIELD_DESC = new TField("ia", TType.STRUCT, (short)2);
    private static final TField EXIST_FIELD_DESC = new TField("exist", TType.STRUCT, (short)3);

    public IOError io;
    public static final int IO = 1;
    public IllegalArgument ia;
    public static final int IA = 2;
    public AlreadyExists exist;
    public static final int EXIST = 3;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
      put(IA, new FieldMetaData("ia", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
      put(EXIST, new FieldMetaData("exist", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(createTable_result.class, metaDataMap);
    }

    public createTable_result() {
    }

    public createTable_result(
      IOError io,
      IllegalArgument ia,
      AlreadyExists exist)
    {
      this();
      this.io = io;
      this.ia = ia;
      this.exist = exist;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public createTable_result(createTable_result other) {
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
      if (other.isSetIa()) {
        this.ia = new IllegalArgument(other.ia);
      }
      if (other.isSetExist()) {
        this.exist = new AlreadyExists(other.exist);
      }
    }

    @Override
    public createTable_result clone() {
      return new createTable_result(this);
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public IllegalArgument getIa() {
      return this.ia;
    }

    public void setIa(IllegalArgument ia) {
      this.ia = ia;
    }

    public void unsetIa() {
      this.ia = null;
    }

    // Returns true if field ia is set (has been asigned a value) and false otherwise
    public boolean isSetIa() {
      return this.ia != null;
    }

    public void setIaIsSet(boolean value) {
      if (!value) {
        this.ia = null;
      }
    }

    public AlreadyExists getExist() {
      return this.exist;
    }

    public void setExist(AlreadyExists exist) {
      this.exist = exist;
    }

    public void unsetExist() {
      this.exist = null;
    }

    // Returns true if field exist is set (has been asigned a value) and false otherwise
    public boolean isSetExist() {
      return this.exist != null;
    }

    public void setExistIsSet(boolean value) {
      if (!value) {
        this.exist = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      case IA:
        if (value == null) {
          unsetIa();
        } else {
          setIa((IllegalArgument)value);
        }
        break;

      case EXIST:
        if (value == null) {
          unsetExist();
        } else {
          setExist((AlreadyExists)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case IO:
        return getIo();

      case IA:
        return getIa();

      case EXIST:
        return getExist();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case IO:
        return isSetIo();
      case IA:
        return isSetIa();
      case EXIST:
        return isSetExist();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof createTable_result)
        return this.equals((createTable_result)that);
      return false;
    }

    public boolean equals(createTable_result that) {
      if (that == null)
        return false;

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      boolean this_present_ia = true && this.isSetIa();
      boolean that_present_ia = true && that.isSetIa();
      if (this_present_ia || that_present_ia) {
        if (!(this_present_ia && that_present_ia))
          return false;
        if (!this.ia.equals(that.ia))
          return false;
      }

      boolean this_present_exist = true && this.isSetExist();
      boolean that_present_exist = true && that.isSetExist();
      if (this_present_exist || that_present_exist) {
        if (!(this_present_exist && that_present_exist))
          return false;
        if (!this.exist.equals(that.exist))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IA:
            if (field.type == TType.STRUCT) {
              this.ia = new IllegalArgument();
              this.ia.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case EXIST:
            if (field.type == TType.STRUCT) {
              this.exist = new AlreadyExists();
              this.exist.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      } else if (this.isSetIa()) {
        oprot.writeFieldBegin(IA_FIELD_DESC);
        this.ia.write(oprot);
        oprot.writeFieldEnd();
      } else if (this.isSetExist()) {
        oprot.writeFieldBegin(EXIST_FIELD_DESC);
        this.exist.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("createTable_result(");
      boolean first = true;

      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("ia:");
      if (this.ia == null) {
        sb.append("null");
      } else {
        sb.append(this.ia);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("exist:");
      if (this.exist == null) {
        sb.append("null");
      } else {
        sb.append(this.exist);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class deleteTable_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("deleteTable_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);

    public byte[] tableName;
    public static final int TABLENAME = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(deleteTable_args.class, metaDataMap);
    }

    public deleteTable_args() {
    }

    public deleteTable_args(
      byte[] tableName)
    {
      this();
      this.tableName = tableName;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public deleteTable_args(deleteTable_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
    }

    @Override
    public deleteTable_args clone() {
      return new deleteTable_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof deleteTable_args)
        return this.equals((deleteTable_args)that);
      return false;
    }

    public boolean equals(deleteTable_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("deleteTable_args(");

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class deleteTable_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("deleteTable_result");
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(deleteTable_result.class, metaDataMap);
    }

    public deleteTable_result() {
    }

    public deleteTable_result(
      IOError io)
    {
      this();
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public deleteTable_result(deleteTable_result other) {
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public deleteTable_result clone() {
      return new deleteTable_result(this);
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof deleteTable_result)
        return this.equals((deleteTable_result)that);
      return false;
    }

    public boolean equals(deleteTable_result that) {
      if (that == null)
        return false;

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("deleteTable_result(");

      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class get_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("get_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)2);
    private static final TField COLUMN_FIELD_DESC = new TField("column", TType.STRING, (short)3);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] row;
    public static final int ROW = 2;
    public byte[] column;
    public static final int COLUMN = 3;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(COLUMN, new FieldMetaData("column", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(get_args.class, metaDataMap);
    }

    public get_args() {
    }

    public get_args(
      byte[] tableName,
      byte[] row,
      byte[] column)
    {
      this();
      this.tableName = tableName;
      this.row = row;
      this.column = column;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public get_args(get_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRow()) {
        this.row = other.row;
      }
      if (other.isSetColumn()) {
        this.column = other.column;
      }
    }

    @Override
    public get_args clone() {
      return new get_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getRow() {
      return this.row;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public void unsetRow() {
      this.row = null;
    }

    // Returns true if field row is set (has been asigned a value) and false otherwise
    public boolean isSetRow() {
      return this.row != null;
    }

    public void setRowIsSet(boolean value) {
      if (!value) {
        this.row = null;
      }
    }

    public byte[] getColumn() {
      return this.column;
    }

    public void setColumn(byte[] column) {
      this.column = column;
    }

    public void unsetColumn() {
      this.column = null;
    }

    // Returns true if field column is set (has been asigned a value) and false otherwise
    public boolean isSetColumn() {
      return this.column != null;
    }

    public void setColumnIsSet(boolean value) {
      if (!value) {
        this.column = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROW:
        if (value == null) {
          unsetRow();
        } else {
          setRow((byte[])value);
        }
        break;

      case COLUMN:
        if (value == null) {
          unsetColumn();
        } else {
          setColumn((byte[])value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROW:
        return getRow();

      case COLUMN:
        return getColumn();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROW:
        return isSetRow();
      case COLUMN:
        return isSetColumn();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof get_args)
        return this.equals((get_args)that);
      return false;
    }

    public boolean equals(get_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_row = true && this.isSetRow();
      boolean that_present_row = true && that.isSetRow();
      if (this_present_row || that_present_row) {
        if (!(this_present_row && that_present_row))
          return false;
        if (!java.util.Arrays.equals(this.row, that.row))
          return false;
      }

      boolean this_present_column = true && this.isSetColumn();
      boolean that_present_column = true && that.isSetColumn();
      if (this_present_column || that_present_column) {
        if (!(this_present_column && that_present_column))
          return false;
        if (!java.util.Arrays.equals(this.column, that.column))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROW:
            if (field.type == TType.STRING) {
              this.row = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case COLUMN:
            if (field.type == TType.STRING) {
              this.column = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        oprot.writeBinary(this.row);
        oprot.writeFieldEnd();
      }
      if (this.column != null) {
        oprot.writeFieldBegin(COLUMN_FIELD_DESC);
        oprot.writeBinary(this.column);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("get_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("row:");
      if (this.row == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.row));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("column:");
      if (this.column == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.column));
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class get_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("get_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.LIST, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public List<TCell> success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new StructMetaData(TType.STRUCT, TCell.class))));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(get_result.class, metaDataMap);
    }

    public get_result() {
    }

    public get_result(
      List<TCell> success,
      IOError io)
    {
      this();
      this.success = success;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public get_result(get_result other) {
      if (other.isSetSuccess()) {
        List<TCell> __this__success = new ArrayList<TCell>();
        for (TCell other_element : other.success) {
          __this__success.add(new TCell(other_element));
        }
        this.success = __this__success;
      }
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public get_result clone() {
      return new get_result(this);
    }

    public int getSuccessSize() {
      return (this.success == null) ? 0 : this.success.size();
    }

    public java.util.Iterator<TCell> getSuccessIterator() {
      return (this.success == null) ? null : this.success.iterator();
    }

    public void addToSuccess(TCell elem) {
      if (this.success == null) {
        this.success = new ArrayList<TCell>();
      }
      this.success.add(elem);
    }

    public List<TCell> getSuccess() {
      return this.success;
    }

    public void setSuccess(List<TCell> success) {
      this.success = success;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((List<TCell>)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof get_result)
        return this.equals((get_result)that);
      return false;
    }

    public boolean equals(get_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.LIST) {
              {
                TList _list26 = iprot.readListBegin();
                this.success = new ArrayList<TCell>(_list26.size);
                for (int _i27 = 0; _i27 < _list26.size; ++_i27)
                {
                  TCell _elem28;
                  _elem28 = new TCell();
                  _elem28.read(iprot);
                  this.success.add(_elem28);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.success.size()));
          for (TCell _iter29 : this.success)          {
            _iter29.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("get_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getVer_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getVer_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)2);
    private static final TField COLUMN_FIELD_DESC = new TField("column", TType.STRING, (short)3);
    private static final TField NUM_VERSIONS_FIELD_DESC = new TField("numVersions", TType.I32, (short)4);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] row;
    public static final int ROW = 2;
    public byte[] column;
    public static final int COLUMN = 3;
    public int numVersions;
    public static final int NUMVERSIONS = 4;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean numVersions = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(COLUMN, new FieldMetaData("column", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(NUMVERSIONS, new FieldMetaData("numVersions", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I32)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getVer_args.class, metaDataMap);
    }

    public getVer_args() {
    }

    public getVer_args(
      byte[] tableName,
      byte[] row,
      byte[] column,
      int numVersions)
    {
      this();
      this.tableName = tableName;
      this.row = row;
      this.column = column;
      this.numVersions = numVersions;
      this.__isset.numVersions = true;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getVer_args(getVer_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRow()) {
        this.row = other.row;
      }
      if (other.isSetColumn()) {
        this.column = other.column;
      }
      __isset.numVersions = other.__isset.numVersions;
      this.numVersions = other.numVersions;
    }

    @Override
    public getVer_args clone() {
      return new getVer_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getRow() {
      return this.row;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public void unsetRow() {
      this.row = null;
    }

    // Returns true if field row is set (has been asigned a value) and false otherwise
    public boolean isSetRow() {
      return this.row != null;
    }

    public void setRowIsSet(boolean value) {
      if (!value) {
        this.row = null;
      }
    }

    public byte[] getColumn() {
      return this.column;
    }

    public void setColumn(byte[] column) {
      this.column = column;
    }

    public void unsetColumn() {
      this.column = null;
    }

    // Returns true if field column is set (has been asigned a value) and false otherwise
    public boolean isSetColumn() {
      return this.column != null;
    }

    public void setColumnIsSet(boolean value) {
      if (!value) {
        this.column = null;
      }
    }

    public int getNumVersions() {
      return this.numVersions;
    }

    public void setNumVersions(int numVersions) {
      this.numVersions = numVersions;
      this.__isset.numVersions = true;
    }

    public void unsetNumVersions() {
      this.__isset.numVersions = false;
    }

    // Returns true if field numVersions is set (has been asigned a value) and false otherwise
    public boolean isSetNumVersions() {
      return this.__isset.numVersions;
    }

    public void setNumVersionsIsSet(boolean value) {
      this.__isset.numVersions = value;
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROW:
        if (value == null) {
          unsetRow();
        } else {
          setRow((byte[])value);
        }
        break;

      case COLUMN:
        if (value == null) {
          unsetColumn();
        } else {
          setColumn((byte[])value);
        }
        break;

      case NUMVERSIONS:
        if (value == null) {
          unsetNumVersions();
        } else {
          setNumVersions((Integer)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROW:
        return getRow();

      case COLUMN:
        return getColumn();

      case NUMVERSIONS:
        return getNumVersions();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROW:
        return isSetRow();
      case COLUMN:
        return isSetColumn();
      case NUMVERSIONS:
        return isSetNumVersions();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getVer_args)
        return this.equals((getVer_args)that);
      return false;
    }

    public boolean equals(getVer_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_row = true && this.isSetRow();
      boolean that_present_row = true && that.isSetRow();
      if (this_present_row || that_present_row) {
        if (!(this_present_row && that_present_row))
          return false;
        if (!java.util.Arrays.equals(this.row, that.row))
          return false;
      }

      boolean this_present_column = true && this.isSetColumn();
      boolean that_present_column = true && that.isSetColumn();
      if (this_present_column || that_present_column) {
        if (!(this_present_column && that_present_column))
          return false;
        if (!java.util.Arrays.equals(this.column, that.column))
          return false;
      }

      boolean this_present_numVersions = true;
      boolean that_present_numVersions = true;
      if (this_present_numVersions || that_present_numVersions) {
        if (!(this_present_numVersions && that_present_numVersions))
          return false;
        if (this.numVersions != that.numVersions)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROW:
            if (field.type == TType.STRING) {
              this.row = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case COLUMN:
            if (field.type == TType.STRING) {
              this.column = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case NUMVERSIONS:
            if (field.type == TType.I32) {
              this.numVersions = iprot.readI32();
              this.__isset.numVersions = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        oprot.writeBinary(this.row);
        oprot.writeFieldEnd();
      }
      if (this.column != null) {
        oprot.writeFieldBegin(COLUMN_FIELD_DESC);
        oprot.writeBinary(this.column);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(NUM_VERSIONS_FIELD_DESC);
      oprot.writeI32(this.numVersions);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getVer_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("row:");
      if (this.row == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.row));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("column:");
      if (this.column == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.column));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("numVersions:");
      sb.append(this.numVersions);
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getVer_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getVer_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.LIST, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public List<TCell> success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new StructMetaData(TType.STRUCT, TCell.class))));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getVer_result.class, metaDataMap);
    }

    public getVer_result() {
    }

    public getVer_result(
      List<TCell> success,
      IOError io)
    {
      this();
      this.success = success;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getVer_result(getVer_result other) {
      if (other.isSetSuccess()) {
        List<TCell> __this__success = new ArrayList<TCell>();
        for (TCell other_element : other.success) {
          __this__success.add(new TCell(other_element));
        }
        this.success = __this__success;
      }
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public getVer_result clone() {
      return new getVer_result(this);
    }

    public int getSuccessSize() {
      return (this.success == null) ? 0 : this.success.size();
    }

    public java.util.Iterator<TCell> getSuccessIterator() {
      return (this.success == null) ? null : this.success.iterator();
    }

    public void addToSuccess(TCell elem) {
      if (this.success == null) {
        this.success = new ArrayList<TCell>();
      }
      this.success.add(elem);
    }

    public List<TCell> getSuccess() {
      return this.success;
    }

    public void setSuccess(List<TCell> success) {
      this.success = success;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((List<TCell>)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getVer_result)
        return this.equals((getVer_result)that);
      return false;
    }

    public boolean equals(getVer_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.LIST) {
              {
                TList _list30 = iprot.readListBegin();
                this.success = new ArrayList<TCell>(_list30.size);
                for (int _i31 = 0; _i31 < _list30.size; ++_i31)
                {
                  TCell _elem32;
                  _elem32 = new TCell();
                  _elem32.read(iprot);
                  this.success.add(_elem32);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.success.size()));
          for (TCell _iter33 : this.success)          {
            _iter33.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getVer_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getVerTs_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getVerTs_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)2);
    private static final TField COLUMN_FIELD_DESC = new TField("column", TType.STRING, (short)3);
    private static final TField TIMESTAMP_FIELD_DESC = new TField("timestamp", TType.I64, (short)4);
    private static final TField NUM_VERSIONS_FIELD_DESC = new TField("numVersions", TType.I32, (short)5);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] row;
    public static final int ROW = 2;
    public byte[] column;
    public static final int COLUMN = 3;
    public long timestamp;
    public static final int TIMESTAMP = 4;
    public int numVersions;
    public static final int NUMVERSIONS = 5;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean timestamp = false;
      public boolean numVersions = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(COLUMN, new FieldMetaData("column", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(TIMESTAMP, new FieldMetaData("timestamp", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I64)));
      put(NUMVERSIONS, new FieldMetaData("numVersions", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I32)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getVerTs_args.class, metaDataMap);
    }

    public getVerTs_args() {
    }

    public getVerTs_args(
      byte[] tableName,
      byte[] row,
      byte[] column,
      long timestamp,
      int numVersions)
    {
      this();
      this.tableName = tableName;
      this.row = row;
      this.column = column;
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
      this.numVersions = numVersions;
      this.__isset.numVersions = true;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getVerTs_args(getVerTs_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRow()) {
        this.row = other.row;
      }
      if (other.isSetColumn()) {
        this.column = other.column;
      }
      __isset.timestamp = other.__isset.timestamp;
      this.timestamp = other.timestamp;
      __isset.numVersions = other.__isset.numVersions;
      this.numVersions = other.numVersions;
    }

    @Override
    public getVerTs_args clone() {
      return new getVerTs_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getRow() {
      return this.row;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public void unsetRow() {
      this.row = null;
    }

    // Returns true if field row is set (has been asigned a value) and false otherwise
    public boolean isSetRow() {
      return this.row != null;
    }

    public void setRowIsSet(boolean value) {
      if (!value) {
        this.row = null;
      }
    }

    public byte[] getColumn() {
      return this.column;
    }

    public void setColumn(byte[] column) {
      this.column = column;
    }

    public void unsetColumn() {
      this.column = null;
    }

    // Returns true if field column is set (has been asigned a value) and false otherwise
    public boolean isSetColumn() {
      return this.column != null;
    }

    public void setColumnIsSet(boolean value) {
      if (!value) {
        this.column = null;
      }
    }

    public long getTimestamp() {
      return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    public void unsetTimestamp() {
      this.__isset.timestamp = false;
    }

    // Returns true if field timestamp is set (has been asigned a value) and false otherwise
    public boolean isSetTimestamp() {
      return this.__isset.timestamp;
    }

    public void setTimestampIsSet(boolean value) {
      this.__isset.timestamp = value;
    }

    public int getNumVersions() {
      return this.numVersions;
    }

    public void setNumVersions(int numVersions) {
      this.numVersions = numVersions;
      this.__isset.numVersions = true;
    }

    public void unsetNumVersions() {
      this.__isset.numVersions = false;
    }

    // Returns true if field numVersions is set (has been asigned a value) and false otherwise
    public boolean isSetNumVersions() {
      return this.__isset.numVersions;
    }

    public void setNumVersionsIsSet(boolean value) {
      this.__isset.numVersions = value;
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROW:
        if (value == null) {
          unsetRow();
        } else {
          setRow((byte[])value);
        }
        break;

      case COLUMN:
        if (value == null) {
          unsetColumn();
        } else {
          setColumn((byte[])value);
        }
        break;

      case TIMESTAMP:
        if (value == null) {
          unsetTimestamp();
        } else {
          setTimestamp((Long)value);
        }
        break;

      case NUMVERSIONS:
        if (value == null) {
          unsetNumVersions();
        } else {
          setNumVersions((Integer)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROW:
        return getRow();

      case COLUMN:
        return getColumn();

      case TIMESTAMP:
        return getTimestamp();

      case NUMVERSIONS:
        return getNumVersions();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROW:
        return isSetRow();
      case COLUMN:
        return isSetColumn();
      case TIMESTAMP:
        return isSetTimestamp();
      case NUMVERSIONS:
        return isSetNumVersions();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getVerTs_args)
        return this.equals((getVerTs_args)that);
      return false;
    }

    public boolean equals(getVerTs_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_row = true && this.isSetRow();
      boolean that_present_row = true && that.isSetRow();
      if (this_present_row || that_present_row) {
        if (!(this_present_row && that_present_row))
          return false;
        if (!java.util.Arrays.equals(this.row, that.row))
          return false;
      }

      boolean this_present_column = true && this.isSetColumn();
      boolean that_present_column = true && that.isSetColumn();
      if (this_present_column || that_present_column) {
        if (!(this_present_column && that_present_column))
          return false;
        if (!java.util.Arrays.equals(this.column, that.column))
          return false;
      }

      boolean this_present_timestamp = true;
      boolean that_present_timestamp = true;
      if (this_present_timestamp || that_present_timestamp) {
        if (!(this_present_timestamp && that_present_timestamp))
          return false;
        if (this.timestamp != that.timestamp)
          return false;
      }

      boolean this_present_numVersions = true;
      boolean that_present_numVersions = true;
      if (this_present_numVersions || that_present_numVersions) {
        if (!(this_present_numVersions && that_present_numVersions))
          return false;
        if (this.numVersions != that.numVersions)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROW:
            if (field.type == TType.STRING) {
              this.row = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case COLUMN:
            if (field.type == TType.STRING) {
              this.column = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case TIMESTAMP:
            if (field.type == TType.I64) {
              this.timestamp = iprot.readI64();
              this.__isset.timestamp = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case NUMVERSIONS:
            if (field.type == TType.I32) {
              this.numVersions = iprot.readI32();
              this.__isset.numVersions = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        oprot.writeBinary(this.row);
        oprot.writeFieldEnd();
      }
      if (this.column != null) {
        oprot.writeFieldBegin(COLUMN_FIELD_DESC);
        oprot.writeBinary(this.column);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(TIMESTAMP_FIELD_DESC);
      oprot.writeI64(this.timestamp);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(NUM_VERSIONS_FIELD_DESC);
      oprot.writeI32(this.numVersions);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getVerTs_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("row:");
      if (this.row == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.row));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("column:");
      if (this.column == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.column));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("timestamp:");
      sb.append(this.timestamp);
      first = false;
      if (!first) sb.append(", ");
      sb.append("numVersions:");
      sb.append(this.numVersions);
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getVerTs_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getVerTs_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.LIST, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public List<TCell> success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new StructMetaData(TType.STRUCT, TCell.class))));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getVerTs_result.class, metaDataMap);
    }

    public getVerTs_result() {
    }

    public getVerTs_result(
      List<TCell> success,
      IOError io)
    {
      this();
      this.success = success;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getVerTs_result(getVerTs_result other) {
      if (other.isSetSuccess()) {
        List<TCell> __this__success = new ArrayList<TCell>();
        for (TCell other_element : other.success) {
          __this__success.add(new TCell(other_element));
        }
        this.success = __this__success;
      }
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public getVerTs_result clone() {
      return new getVerTs_result(this);
    }

    public int getSuccessSize() {
      return (this.success == null) ? 0 : this.success.size();
    }

    public java.util.Iterator<TCell> getSuccessIterator() {
      return (this.success == null) ? null : this.success.iterator();
    }

    public void addToSuccess(TCell elem) {
      if (this.success == null) {
        this.success = new ArrayList<TCell>();
      }
      this.success.add(elem);
    }

    public List<TCell> getSuccess() {
      return this.success;
    }

    public void setSuccess(List<TCell> success) {
      this.success = success;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((List<TCell>)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getVerTs_result)
        return this.equals((getVerTs_result)that);
      return false;
    }

    public boolean equals(getVerTs_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.LIST) {
              {
                TList _list34 = iprot.readListBegin();
                this.success = new ArrayList<TCell>(_list34.size);
                for (int _i35 = 0; _i35 < _list34.size; ++_i35)
                {
                  TCell _elem36;
                  _elem36 = new TCell();
                  _elem36.read(iprot);
                  this.success.add(_elem36);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.success.size()));
          for (TCell _iter37 : this.success)          {
            _iter37.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getVerTs_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getRow_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getRow_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)2);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] row;
    public static final int ROW = 2;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getRow_args.class, metaDataMap);
    }

    public getRow_args() {
    }

    public getRow_args(
      byte[] tableName,
      byte[] row)
    {
      this();
      this.tableName = tableName;
      this.row = row;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getRow_args(getRow_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRow()) {
        this.row = other.row;
      }
    }

    @Override
    public getRow_args clone() {
      return new getRow_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getRow() {
      return this.row;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public void unsetRow() {
      this.row = null;
    }

    // Returns true if field row is set (has been asigned a value) and false otherwise
    public boolean isSetRow() {
      return this.row != null;
    }

    public void setRowIsSet(boolean value) {
      if (!value) {
        this.row = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROW:
        if (value == null) {
          unsetRow();
        } else {
          setRow((byte[])value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROW:
        return getRow();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROW:
        return isSetRow();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getRow_args)
        return this.equals((getRow_args)that);
      return false;
    }

    public boolean equals(getRow_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_row = true && this.isSetRow();
      boolean that_present_row = true && that.isSetRow();
      if (this_present_row || that_present_row) {
        if (!(this_present_row && that_present_row))
          return false;
        if (!java.util.Arrays.equals(this.row, that.row))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROW:
            if (field.type == TType.STRING) {
              this.row = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        oprot.writeBinary(this.row);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getRow_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("row:");
      if (this.row == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.row));
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getRow_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getRow_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.LIST, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public List<TRowResult> success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new StructMetaData(TType.STRUCT, TRowResult.class))));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getRow_result.class, metaDataMap);
    }

    public getRow_result() {
    }

    public getRow_result(
      List<TRowResult> success,
      IOError io)
    {
      this();
      this.success = success;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getRow_result(getRow_result other) {
      if (other.isSetSuccess()) {
        List<TRowResult> __this__success = new ArrayList<TRowResult>();
        for (TRowResult other_element : other.success) {
          __this__success.add(new TRowResult(other_element));
        }
        this.success = __this__success;
      }
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public getRow_result clone() {
      return new getRow_result(this);
    }

    public int getSuccessSize() {
      return (this.success == null) ? 0 : this.success.size();
    }

    public java.util.Iterator<TRowResult> getSuccessIterator() {
      return (this.success == null) ? null : this.success.iterator();
    }

    public void addToSuccess(TRowResult elem) {
      if (this.success == null) {
        this.success = new ArrayList<TRowResult>();
      }
      this.success.add(elem);
    }

    public List<TRowResult> getSuccess() {
      return this.success;
    }

    public void setSuccess(List<TRowResult> success) {
      this.success = success;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((List<TRowResult>)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getRow_result)
        return this.equals((getRow_result)that);
      return false;
    }

    public boolean equals(getRow_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.LIST) {
              {
                TList _list38 = iprot.readListBegin();
                this.success = new ArrayList<TRowResult>(_list38.size);
                for (int _i39 = 0; _i39 < _list38.size; ++_i39)
                {
                  TRowResult _elem40;
                  _elem40 = new TRowResult();
                  _elem40.read(iprot);
                  this.success.add(_elem40);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.success.size()));
          for (TRowResult _iter41 : this.success)          {
            _iter41.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getRow_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getRowWithColumns_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getRowWithColumns_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)2);
    private static final TField COLUMNS_FIELD_DESC = new TField("columns", TType.LIST, (short)3);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] row;
    public static final int ROW = 2;
    public List<byte[]> columns;
    public static final int COLUMNS = 3;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(COLUMNS, new FieldMetaData("columns", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new FieldValueMetaData(TType.STRING))));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getRowWithColumns_args.class, metaDataMap);
    }

    public getRowWithColumns_args() {
    }

    public getRowWithColumns_args(
      byte[] tableName,
      byte[] row,
      List<byte[]> columns)
    {
      this();
      this.tableName = tableName;
      this.row = row;
      this.columns = columns;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getRowWithColumns_args(getRowWithColumns_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRow()) {
        this.row = other.row;
      }
      if (other.isSetColumns()) {
        List<byte[]> __this__columns = new ArrayList<byte[]>();
        for (byte[] other_element : other.columns) {
          __this__columns.add(other_element);
        }
        this.columns = __this__columns;
      }
    }

    @Override
    public getRowWithColumns_args clone() {
      return new getRowWithColumns_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getRow() {
      return this.row;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public void unsetRow() {
      this.row = null;
    }

    // Returns true if field row is set (has been asigned a value) and false otherwise
    public boolean isSetRow() {
      return this.row != null;
    }

    public void setRowIsSet(boolean value) {
      if (!value) {
        this.row = null;
      }
    }

    public int getColumnsSize() {
      return (this.columns == null) ? 0 : this.columns.size();
    }

    public java.util.Iterator<byte[]> getColumnsIterator() {
      return (this.columns == null) ? null : this.columns.iterator();
    }

    public void addToColumns(byte[] elem) {
      if (this.columns == null) {
        this.columns = new ArrayList<byte[]>();
      }
      this.columns.add(elem);
    }

    public List<byte[]> getColumns() {
      return this.columns;
    }

    public void setColumns(List<byte[]> columns) {
      this.columns = columns;
    }

    public void unsetColumns() {
      this.columns = null;
    }

    // Returns true if field columns is set (has been asigned a value) and false otherwise
    public boolean isSetColumns() {
      return this.columns != null;
    }

    public void setColumnsIsSet(boolean value) {
      if (!value) {
        this.columns = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROW:
        if (value == null) {
          unsetRow();
        } else {
          setRow((byte[])value);
        }
        break;

      case COLUMNS:
        if (value == null) {
          unsetColumns();
        } else {
          setColumns((List<byte[]>)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROW:
        return getRow();

      case COLUMNS:
        return getColumns();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROW:
        return isSetRow();
      case COLUMNS:
        return isSetColumns();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getRowWithColumns_args)
        return this.equals((getRowWithColumns_args)that);
      return false;
    }

    public boolean equals(getRowWithColumns_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_row = true && this.isSetRow();
      boolean that_present_row = true && that.isSetRow();
      if (this_present_row || that_present_row) {
        if (!(this_present_row && that_present_row))
          return false;
        if (!java.util.Arrays.equals(this.row, that.row))
          return false;
      }

      boolean this_present_columns = true && this.isSetColumns();
      boolean that_present_columns = true && that.isSetColumns();
      if (this_present_columns || that_present_columns) {
        if (!(this_present_columns && that_present_columns))
          return false;
        if (!this.columns.equals(that.columns))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROW:
            if (field.type == TType.STRING) {
              this.row = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case COLUMNS:
            if (field.type == TType.LIST) {
              {
                TList _list42 = iprot.readListBegin();
                this.columns = new ArrayList<byte[]>(_list42.size);
                for (int _i43 = 0; _i43 < _list42.size; ++_i43)
                {
                  byte[] _elem44;
                  _elem44 = iprot.readBinary();
                  this.columns.add(_elem44);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        oprot.writeBinary(this.row);
        oprot.writeFieldEnd();
      }
      if (this.columns != null) {
        oprot.writeFieldBegin(COLUMNS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRING, this.columns.size()));
          for (byte[] _iter45 : this.columns)          {
            oprot.writeBinary(_iter45);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getRowWithColumns_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("row:");
      if (this.row == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.row));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("columns:");
      if (this.columns == null) {
        sb.append("null");
      } else {
        sb.append(this.columns);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getRowWithColumns_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getRowWithColumns_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.LIST, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public List<TRowResult> success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new StructMetaData(TType.STRUCT, TRowResult.class))));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getRowWithColumns_result.class, metaDataMap);
    }

    public getRowWithColumns_result() {
    }

    public getRowWithColumns_result(
      List<TRowResult> success,
      IOError io)
    {
      this();
      this.success = success;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getRowWithColumns_result(getRowWithColumns_result other) {
      if (other.isSetSuccess()) {
        List<TRowResult> __this__success = new ArrayList<TRowResult>();
        for (TRowResult other_element : other.success) {
          __this__success.add(new TRowResult(other_element));
        }
        this.success = __this__success;
      }
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public getRowWithColumns_result clone() {
      return new getRowWithColumns_result(this);
    }

    public int getSuccessSize() {
      return (this.success == null) ? 0 : this.success.size();
    }

    public java.util.Iterator<TRowResult> getSuccessIterator() {
      return (this.success == null) ? null : this.success.iterator();
    }

    public void addToSuccess(TRowResult elem) {
      if (this.success == null) {
        this.success = new ArrayList<TRowResult>();
      }
      this.success.add(elem);
    }

    public List<TRowResult> getSuccess() {
      return this.success;
    }

    public void setSuccess(List<TRowResult> success) {
      this.success = success;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((List<TRowResult>)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getRowWithColumns_result)
        return this.equals((getRowWithColumns_result)that);
      return false;
    }

    public boolean equals(getRowWithColumns_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.LIST) {
              {
                TList _list46 = iprot.readListBegin();
                this.success = new ArrayList<TRowResult>(_list46.size);
                for (int _i47 = 0; _i47 < _list46.size; ++_i47)
                {
                  TRowResult _elem48;
                  _elem48 = new TRowResult();
                  _elem48.read(iprot);
                  this.success.add(_elem48);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.success.size()));
          for (TRowResult _iter49 : this.success)          {
            _iter49.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getRowWithColumns_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getRowTs_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getRowTs_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)2);
    private static final TField TIMESTAMP_FIELD_DESC = new TField("timestamp", TType.I64, (short)3);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] row;
    public static final int ROW = 2;
    public long timestamp;
    public static final int TIMESTAMP = 3;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      private boolean timestamp;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(TIMESTAMP, new FieldMetaData("timestamp", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I64)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getRowTs_args.class, metaDataMap);
    }

    public getRowTs_args() {
    }

    public getRowTs_args(
      byte[] tableName,
      byte[] row,
      long timestamp)
    {
      this();
      this.tableName = tableName;
      this.row = row;
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getRowTs_args(getRowTs_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRow()) {
        this.row = other.row;
      }
      __isset.timestamp = other.__isset.timestamp;
      this.timestamp = other.timestamp;
    }

    @Override
    public getRowTs_args clone() {
      return new getRowTs_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getRow() {
      return this.row;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public void unsetRow() {
      this.row = null;
    }

    // Returns true if field row is set (has been asigned a value) and false otherwise
    public boolean isSetRow() {
      return this.row != null;
    }

    public void setRowIsSet(boolean value) {
      if (!value) {
        this.row = null;
      }
    }

    public long getTimestamp() {
      return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    public void unsetTimestamp() {
      this.__isset.timestamp = false;
    }

    // Returns true if field timestamp is set (has been asigned a value) and false otherwise
    public boolean isSetTimestamp() {
      return this.__isset.timestamp;
    }

    public void setTimestampIsSet(boolean value) {
      this.__isset.timestamp = value;
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROW:
        if (value == null) {
          unsetRow();
        } else {
          setRow((byte[])value);
        }
        break;

      case TIMESTAMP:
        if (value == null) {
          unsetTimestamp();
        } else {
          setTimestamp((Long)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROW:
        return getRow();

      case TIMESTAMP:
        return getTimestamp();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROW:
        return isSetRow();
      case TIMESTAMP:
        return isSetTimestamp();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getRowTs_args)
        return this.equals((getRowTs_args)that);
      return false;
    }

    public boolean equals(getRowTs_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_row = true && this.isSetRow();
      boolean that_present_row = true && that.isSetRow();
      if (this_present_row || that_present_row) {
        if (!(this_present_row && that_present_row))
          return false;
        if (!java.util.Arrays.equals(this.row, that.row))
          return false;
      }

      boolean this_present_timestamp = true;
      boolean that_present_timestamp = true;
      if (this_present_timestamp || that_present_timestamp) {
        if (!(this_present_timestamp && that_present_timestamp))
          return false;
        if (this.timestamp != that.timestamp)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROW:
            if (field.type == TType.STRING) {
              this.row = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case TIMESTAMP:
            if (field.type == TType.I64) {
              this.timestamp = iprot.readI64();
              this.__isset.timestamp = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        oprot.writeBinary(this.row);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(TIMESTAMP_FIELD_DESC);
      oprot.writeI64(this.timestamp);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getRowTs_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("row:");
      if (this.row == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.row));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("timestamp:");
      sb.append(this.timestamp);
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getRowTs_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getRowTs_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.LIST, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public List<TRowResult> success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new StructMetaData(TType.STRUCT, TRowResult.class))));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getRowTs_result.class, metaDataMap);
    }

    public getRowTs_result() {
    }

    public getRowTs_result(
      List<TRowResult> success,
      IOError io)
    {
      this();
      this.success = success;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getRowTs_result(getRowTs_result other) {
      if (other.isSetSuccess()) {
        List<TRowResult> __this__success = new ArrayList<TRowResult>();
        for (TRowResult other_element : other.success) {
          __this__success.add(new TRowResult(other_element));
        }
        this.success = __this__success;
      }
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public getRowTs_result clone() {
      return new getRowTs_result(this);
    }

    public int getSuccessSize() {
      return (this.success == null) ? 0 : this.success.size();
    }

    public java.util.Iterator<TRowResult> getSuccessIterator() {
      return (this.success == null) ? null : this.success.iterator();
    }

    public void addToSuccess(TRowResult elem) {
      if (this.success == null) {
        this.success = new ArrayList<TRowResult>();
      }
      this.success.add(elem);
    }

    public List<TRowResult> getSuccess() {
      return this.success;
    }

    public void setSuccess(List<TRowResult> success) {
      this.success = success;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((List<TRowResult>)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getRowTs_result)
        return this.equals((getRowTs_result)that);
      return false;
    }

    public boolean equals(getRowTs_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.LIST) {
              {
                TList _list50 = iprot.readListBegin();
                this.success = new ArrayList<TRowResult>(_list50.size);
                for (int _i51 = 0; _i51 < _list50.size; ++_i51)
                {
                  TRowResult _elem52;
                  _elem52 = new TRowResult();
                  _elem52.read(iprot);
                  this.success.add(_elem52);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.success.size()));
          for (TRowResult _iter53 : this.success)          {
            _iter53.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getRowTs_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getRowWithColumnsTs_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getRowWithColumnsTs_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)2);
    private static final TField COLUMNS_FIELD_DESC = new TField("columns", TType.LIST, (short)3);
    private static final TField TIMESTAMP_FIELD_DESC = new TField("timestamp", TType.I64, (short)4);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] row;
    public static final int ROW = 2;
    public List<byte[]> columns;
    public static final int COLUMNS = 3;
    public long timestamp;
    public static final int TIMESTAMP = 4;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean timestamp = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(COLUMNS, new FieldMetaData("columns", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new FieldValueMetaData(TType.STRING))));
      put(TIMESTAMP, new FieldMetaData("timestamp", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I64)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getRowWithColumnsTs_args.class, metaDataMap);
    }

    public getRowWithColumnsTs_args() {
    }

    public getRowWithColumnsTs_args(
      byte[] tableName,
      byte[] row,
      List<byte[]> columns,
      long timestamp)
    {
      this();
      this.tableName = tableName;
      this.row = row;
      this.columns = columns;
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getRowWithColumnsTs_args(getRowWithColumnsTs_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRow()) {
        this.row = other.row;
      }
      if (other.isSetColumns()) {
        List<byte[]> __this__columns = new ArrayList<byte[]>();
        for (byte[] other_element : other.columns) {
          __this__columns.add(other_element);
        }
        this.columns = __this__columns;
      }
      __isset.timestamp = other.__isset.timestamp;
      this.timestamp = other.timestamp;
    }

    @Override
    public getRowWithColumnsTs_args clone() {
      return new getRowWithColumnsTs_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getRow() {
      return this.row;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public void unsetRow() {
      this.row = null;
    }

    // Returns true if field row is set (has been asigned a value) and false otherwise
    public boolean isSetRow() {
      return this.row != null;
    }

    public void setRowIsSet(boolean value) {
      if (!value) {
        this.row = null;
      }
    }

    public int getColumnsSize() {
      return (this.columns == null) ? 0 : this.columns.size();
    }

    public java.util.Iterator<byte[]> getColumnsIterator() {
      return (this.columns == null) ? null : this.columns.iterator();
    }

    public void addToColumns(byte[] elem) {
      if (this.columns == null) {
        this.columns = new ArrayList<byte[]>();
      }
      this.columns.add(elem);
    }

    public List<byte[]> getColumns() {
      return this.columns;
    }

    public void setColumns(List<byte[]> columns) {
      this.columns = columns;
    }

    public void unsetColumns() {
      this.columns = null;
    }

    // Returns true if field columns is set (has been asigned a value) and false otherwise
    public boolean isSetColumns() {
      return this.columns != null;
    }

    public void setColumnsIsSet(boolean value) {
      if (!value) {
        this.columns = null;
      }
    }

    public long getTimestamp() {
      return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    public void unsetTimestamp() {
      this.__isset.timestamp = false;
    }

    // Returns true if field timestamp is set (has been asigned a value) and false otherwise
    public boolean isSetTimestamp() {
      return this.__isset.timestamp;
    }

    public void setTimestampIsSet(boolean value) {
      this.__isset.timestamp = value;
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROW:
        if (value == null) {
          unsetRow();
        } else {
          setRow((byte[])value);
        }
        break;

      case COLUMNS:
        if (value == null) {
          unsetColumns();
        } else {
          setColumns((List<byte[]>)value);
        }
        break;

      case TIMESTAMP:
        if (value == null) {
          unsetTimestamp();
        } else {
          setTimestamp((Long)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROW:
        return getRow();

      case COLUMNS:
        return getColumns();

      case TIMESTAMP:
        return getTimestamp();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROW:
        return isSetRow();
      case COLUMNS:
        return isSetColumns();
      case TIMESTAMP:
        return isSetTimestamp();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getRowWithColumnsTs_args)
        return this.equals((getRowWithColumnsTs_args)that);
      return false;
    }

    public boolean equals(getRowWithColumnsTs_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_row = true && this.isSetRow();
      boolean that_present_row = true && that.isSetRow();
      if (this_present_row || that_present_row) {
        if (!(this_present_row && that_present_row))
          return false;
        if (!java.util.Arrays.equals(this.row, that.row))
          return false;
      }

      boolean this_present_columns = true && this.isSetColumns();
      boolean that_present_columns = true && that.isSetColumns();
      if (this_present_columns || that_present_columns) {
        if (!(this_present_columns && that_present_columns))
          return false;
        if (!this.columns.equals(that.columns))
          return false;
      }

      boolean this_present_timestamp = true;
      boolean that_present_timestamp = true;
      if (this_present_timestamp || that_present_timestamp) {
        if (!(this_present_timestamp && that_present_timestamp))
          return false;
        if (this.timestamp != that.timestamp)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROW:
            if (field.type == TType.STRING) {
              this.row = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case COLUMNS:
            if (field.type == TType.LIST) {
              {
                TList _list54 = iprot.readListBegin();
                this.columns = new ArrayList<byte[]>(_list54.size);
                for (int _i55 = 0; _i55 < _list54.size; ++_i55)
                {
                  byte[] _elem56;
                  _elem56 = iprot.readBinary();
                  this.columns.add(_elem56);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case TIMESTAMP:
            if (field.type == TType.I64) {
              this.timestamp = iprot.readI64();
              this.__isset.timestamp = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        oprot.writeBinary(this.row);
        oprot.writeFieldEnd();
      }
      if (this.columns != null) {
        oprot.writeFieldBegin(COLUMNS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRING, this.columns.size()));
          for (byte[] _iter57 : this.columns)          {
            oprot.writeBinary(_iter57);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(TIMESTAMP_FIELD_DESC);
      oprot.writeI64(this.timestamp);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getRowWithColumnsTs_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("row:");
      if (this.row == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.row));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("columns:");
      if (this.columns == null) {
        sb.append("null");
      } else {
        sb.append(this.columns);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("timestamp:");
      sb.append(this.timestamp);
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class getRowWithColumnsTs_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("getRowWithColumnsTs_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.LIST, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public List<TRowResult> success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new StructMetaData(TType.STRUCT, TRowResult.class))));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(getRowWithColumnsTs_result.class, metaDataMap);
    }

    public getRowWithColumnsTs_result() {
    }

    public getRowWithColumnsTs_result(
      List<TRowResult> success,
      IOError io)
    {
      this();
      this.success = success;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getRowWithColumnsTs_result(getRowWithColumnsTs_result other) {
      if (other.isSetSuccess()) {
        List<TRowResult> __this__success = new ArrayList<TRowResult>();
        for (TRowResult other_element : other.success) {
          __this__success.add(new TRowResult(other_element));
        }
        this.success = __this__success;
      }
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public getRowWithColumnsTs_result clone() {
      return new getRowWithColumnsTs_result(this);
    }

    public int getSuccessSize() {
      return (this.success == null) ? 0 : this.success.size();
    }

    public java.util.Iterator<TRowResult> getSuccessIterator() {
      return (this.success == null) ? null : this.success.iterator();
    }

    public void addToSuccess(TRowResult elem) {
      if (this.success == null) {
        this.success = new ArrayList<TRowResult>();
      }
      this.success.add(elem);
    }

    public List<TRowResult> getSuccess() {
      return this.success;
    }

    public void setSuccess(List<TRowResult> success) {
      this.success = success;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((List<TRowResult>)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getRowWithColumnsTs_result)
        return this.equals((getRowWithColumnsTs_result)that);
      return false;
    }

    public boolean equals(getRowWithColumnsTs_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.LIST) {
              {
                TList _list58 = iprot.readListBegin();
                this.success = new ArrayList<TRowResult>(_list58.size);
                for (int _i59 = 0; _i59 < _list58.size; ++_i59)
                {
                  TRowResult _elem60;
                  _elem60 = new TRowResult();
                  _elem60.read(iprot);
                  this.success.add(_elem60);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.success.size()));
          for (TRowResult _iter61 : this.success)          {
            _iter61.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getRowWithColumnsTs_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class mutateRow_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("mutateRow_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)2);
    private static final TField MUTATIONS_FIELD_DESC = new TField("mutations", TType.LIST, (short)3);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] row;
    public static final int ROW = 2;
    public List<Mutation> mutations;
    public static final int MUTATIONS = 3;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(MUTATIONS, new FieldMetaData("mutations", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new StructMetaData(TType.STRUCT, Mutation.class))));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(mutateRow_args.class, metaDataMap);
    }

    public mutateRow_args() {
    }

    public mutateRow_args(
      byte[] tableName,
      byte[] row,
      List<Mutation> mutations)
    {
      this();
      this.tableName = tableName;
      this.row = row;
      this.mutations = mutations;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public mutateRow_args(mutateRow_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRow()) {
        this.row = other.row;
      }
      if (other.isSetMutations()) {
        List<Mutation> __this__mutations = new ArrayList<Mutation>();
        for (Mutation other_element : other.mutations) {
          __this__mutations.add(new Mutation(other_element));
        }
        this.mutations = __this__mutations;
      }
    }

    @Override
    public mutateRow_args clone() {
      return new mutateRow_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getRow() {
      return this.row;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public void unsetRow() {
      this.row = null;
    }

    // Returns true if field row is set (has been asigned a value) and false otherwise
    public boolean isSetRow() {
      return this.row != null;
    }

    public void setRowIsSet(boolean value) {
      if (!value) {
        this.row = null;
      }
    }

    public int getMutationsSize() {
      return (this.mutations == null) ? 0 : this.mutations.size();
    }

    public java.util.Iterator<Mutation> getMutationsIterator() {
      return (this.mutations == null) ? null : this.mutations.iterator();
    }

    public void addToMutations(Mutation elem) {
      if (this.mutations == null) {
        this.mutations = new ArrayList<Mutation>();
      }
      this.mutations.add(elem);
    }

    public List<Mutation> getMutations() {
      return this.mutations;
    }

    public void setMutations(List<Mutation> mutations) {
      this.mutations = mutations;
    }

    public void unsetMutations() {
      this.mutations = null;
    }

    // Returns true if field mutations is set (has been asigned a value) and false otherwise
    public boolean isSetMutations() {
      return this.mutations != null;
    }

    public void setMutationsIsSet(boolean value) {
      if (!value) {
        this.mutations = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROW:
        if (value == null) {
          unsetRow();
        } else {
          setRow((byte[])value);
        }
        break;

      case MUTATIONS:
        if (value == null) {
          unsetMutations();
        } else {
          setMutations((List<Mutation>)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROW:
        return getRow();

      case MUTATIONS:
        return getMutations();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROW:
        return isSetRow();
      case MUTATIONS:
        return isSetMutations();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof mutateRow_args)
        return this.equals((mutateRow_args)that);
      return false;
    }

    public boolean equals(mutateRow_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_row = true && this.isSetRow();
      boolean that_present_row = true && that.isSetRow();
      if (this_present_row || that_present_row) {
        if (!(this_present_row && that_present_row))
          return false;
        if (!java.util.Arrays.equals(this.row, that.row))
          return false;
      }

      boolean this_present_mutations = true && this.isSetMutations();
      boolean that_present_mutations = true && that.isSetMutations();
      if (this_present_mutations || that_present_mutations) {
        if (!(this_present_mutations && that_present_mutations))
          return false;
        if (!this.mutations.equals(that.mutations))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROW:
            if (field.type == TType.STRING) {
              this.row = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case MUTATIONS:
            if (field.type == TType.LIST) {
              {
                TList _list62 = iprot.readListBegin();
                this.mutations = new ArrayList<Mutation>(_list62.size);
                for (int _i63 = 0; _i63 < _list62.size; ++_i63)
                {
                  Mutation _elem64;
                  _elem64 = new Mutation();
                  _elem64.read(iprot);
                  this.mutations.add(_elem64);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        oprot.writeBinary(this.row);
        oprot.writeFieldEnd();
      }
      if (this.mutations != null) {
        oprot.writeFieldBegin(MUTATIONS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.mutations.size()));
          for (Mutation _iter65 : this.mutations)          {
            _iter65.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("mutateRow_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("row:");
      if (this.row == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.row));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("mutations:");
      if (this.mutations == null) {
        sb.append("null");
      } else {
        sb.append(this.mutations);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class mutateRow_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("mutateRow_result");
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);
    private static final TField IA_FIELD_DESC = new TField("ia", TType.STRUCT, (short)2);

    public IOError io;
    public static final int IO = 1;
    public IllegalArgument ia;
    public static final int IA = 2;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
      put(IA, new FieldMetaData("ia", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(mutateRow_result.class, metaDataMap);
    }

    public mutateRow_result() {
    }

    public mutateRow_result(
      IOError io,
      IllegalArgument ia)
    {
      this();
      this.io = io;
      this.ia = ia;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public mutateRow_result(mutateRow_result other) {
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
      if (other.isSetIa()) {
        this.ia = new IllegalArgument(other.ia);
      }
    }

    @Override
    public mutateRow_result clone() {
      return new mutateRow_result(this);
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public IllegalArgument getIa() {
      return this.ia;
    }

    public void setIa(IllegalArgument ia) {
      this.ia = ia;
    }

    public void unsetIa() {
      this.ia = null;
    }

    // Returns true if field ia is set (has been asigned a value) and false otherwise
    public boolean isSetIa() {
      return this.ia != null;
    }

    public void setIaIsSet(boolean value) {
      if (!value) {
        this.ia = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      case IA:
        if (value == null) {
          unsetIa();
        } else {
          setIa((IllegalArgument)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case IO:
        return getIo();

      case IA:
        return getIa();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case IO:
        return isSetIo();
      case IA:
        return isSetIa();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof mutateRow_result)
        return this.equals((mutateRow_result)that);
      return false;
    }

    public boolean equals(mutateRow_result that) {
      if (that == null)
        return false;

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      boolean this_present_ia = true && this.isSetIa();
      boolean that_present_ia = true && that.isSetIa();
      if (this_present_ia || that_present_ia) {
        if (!(this_present_ia && that_present_ia))
          return false;
        if (!this.ia.equals(that.ia))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IA:
            if (field.type == TType.STRUCT) {
              this.ia = new IllegalArgument();
              this.ia.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      } else if (this.isSetIa()) {
        oprot.writeFieldBegin(IA_FIELD_DESC);
        this.ia.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("mutateRow_result(");
      boolean first = true;

      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("ia:");
      if (this.ia == null) {
        sb.append("null");
      } else {
        sb.append(this.ia);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class mutateRowTs_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("mutateRowTs_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)2);
    private static final TField MUTATIONS_FIELD_DESC = new TField("mutations", TType.LIST, (short)3);
    private static final TField TIMESTAMP_FIELD_DESC = new TField("timestamp", TType.I64, (short)4);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] row;
    public static final int ROW = 2;
    public List<Mutation> mutations;
    public static final int MUTATIONS = 3;
    public long timestamp;
    public static final int TIMESTAMP = 4;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean timestamp = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(MUTATIONS, new FieldMetaData("mutations", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new StructMetaData(TType.STRUCT, Mutation.class))));
      put(TIMESTAMP, new FieldMetaData("timestamp", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I64)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(mutateRowTs_args.class, metaDataMap);
    }

    public mutateRowTs_args() {
    }

    public mutateRowTs_args(
      byte[] tableName,
      byte[] row,
      List<Mutation> mutations,
      long timestamp)
    {
      this();
      this.tableName = tableName;
      this.row = row;
      this.mutations = mutations;
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public mutateRowTs_args(mutateRowTs_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRow()) {
        this.row = other.row;
      }
      if (other.isSetMutations()) {
        List<Mutation> __this__mutations = new ArrayList<Mutation>();
        for (Mutation other_element : other.mutations) {
          __this__mutations.add(new Mutation(other_element));
        }
        this.mutations = __this__mutations;
      }
      __isset.timestamp = other.__isset.timestamp;
      this.timestamp = other.timestamp;
    }

    @Override
    public mutateRowTs_args clone() {
      return new mutateRowTs_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getRow() {
      return this.row;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public void unsetRow() {
      this.row = null;
    }

    // Returns true if field row is set (has been asigned a value) and false otherwise
    public boolean isSetRow() {
      return this.row != null;
    }

    public void setRowIsSet(boolean value) {
      if (!value) {
        this.row = null;
      }
    }

    public int getMutationsSize() {
      return (this.mutations == null) ? 0 : this.mutations.size();
    }

    public java.util.Iterator<Mutation> getMutationsIterator() {
      return (this.mutations == null) ? null : this.mutations.iterator();
    }

    public void addToMutations(Mutation elem) {
      if (this.mutations == null) {
        this.mutations = new ArrayList<Mutation>();
      }
      this.mutations.add(elem);
    }

    public List<Mutation> getMutations() {
      return this.mutations;
    }

    public void setMutations(List<Mutation> mutations) {
      this.mutations = mutations;
    }

    public void unsetMutations() {
      this.mutations = null;
    }

    // Returns true if field mutations is set (has been asigned a value) and false otherwise
    public boolean isSetMutations() {
      return this.mutations != null;
    }

    public void setMutationsIsSet(boolean value) {
      if (!value) {
        this.mutations = null;
      }
    }

    public long getTimestamp() {
      return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    public void unsetTimestamp() {
      this.__isset.timestamp = false;
    }

    // Returns true if field timestamp is set (has been asigned a value) and false otherwise
    public boolean isSetTimestamp() {
      return this.__isset.timestamp;
    }

    public void setTimestampIsSet(boolean value) {
      this.__isset.timestamp = value;
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROW:
        if (value == null) {
          unsetRow();
        } else {
          setRow((byte[])value);
        }
        break;

      case MUTATIONS:
        if (value == null) {
          unsetMutations();
        } else {
          setMutations((List<Mutation>)value);
        }
        break;

      case TIMESTAMP:
        if (value == null) {
          unsetTimestamp();
        } else {
          setTimestamp((Long)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROW:
        return getRow();

      case MUTATIONS:
        return getMutations();

      case TIMESTAMP:
        return getTimestamp();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROW:
        return isSetRow();
      case MUTATIONS:
        return isSetMutations();
      case TIMESTAMP:
        return isSetTimestamp();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof mutateRowTs_args)
        return this.equals((mutateRowTs_args)that);
      return false;
    }

    public boolean equals(mutateRowTs_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_row = true && this.isSetRow();
      boolean that_present_row = true && that.isSetRow();
      if (this_present_row || that_present_row) {
        if (!(this_present_row && that_present_row))
          return false;
        if (!java.util.Arrays.equals(this.row, that.row))
          return false;
      }

      boolean this_present_mutations = true && this.isSetMutations();
      boolean that_present_mutations = true && that.isSetMutations();
      if (this_present_mutations || that_present_mutations) {
        if (!(this_present_mutations && that_present_mutations))
          return false;
        if (!this.mutations.equals(that.mutations))
          return false;
      }

      boolean this_present_timestamp = true;
      boolean that_present_timestamp = true;
      if (this_present_timestamp || that_present_timestamp) {
        if (!(this_present_timestamp && that_present_timestamp))
          return false;
        if (this.timestamp != that.timestamp)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROW:
            if (field.type == TType.STRING) {
              this.row = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case MUTATIONS:
            if (field.type == TType.LIST) {
              {
                TList _list66 = iprot.readListBegin();
                this.mutations = new ArrayList<Mutation>(_list66.size);
                for (int _i67 = 0; _i67 < _list66.size; ++_i67)
                {
                  Mutation _elem68;
                  _elem68 = new Mutation();
                  _elem68.read(iprot);
                  this.mutations.add(_elem68);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case TIMESTAMP:
            if (field.type == TType.I64) {
              this.timestamp = iprot.readI64();
              this.__isset.timestamp = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        oprot.writeBinary(this.row);
        oprot.writeFieldEnd();
      }
      if (this.mutations != null) {
        oprot.writeFieldBegin(MUTATIONS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.mutations.size()));
          for (Mutation _iter69 : this.mutations)          {
            _iter69.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(TIMESTAMP_FIELD_DESC);
      oprot.writeI64(this.timestamp);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("mutateRowTs_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("row:");
      if (this.row == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.row));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("mutations:");
      if (this.mutations == null) {
        sb.append("null");
      } else {
        sb.append(this.mutations);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("timestamp:");
      sb.append(this.timestamp);
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class mutateRowTs_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("mutateRowTs_result");
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);
    private static final TField IA_FIELD_DESC = new TField("ia", TType.STRUCT, (short)2);

    public IOError io;
    public static final int IO = 1;
    public IllegalArgument ia;
    public static final int IA = 2;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
      put(IA, new FieldMetaData("ia", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(mutateRowTs_result.class, metaDataMap);
    }

    public mutateRowTs_result() {
    }

    public mutateRowTs_result(
      IOError io,
      IllegalArgument ia)
    {
      this();
      this.io = io;
      this.ia = ia;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public mutateRowTs_result(mutateRowTs_result other) {
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
      if (other.isSetIa()) {
        this.ia = new IllegalArgument(other.ia);
      }
    }

    @Override
    public mutateRowTs_result clone() {
      return new mutateRowTs_result(this);
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public IllegalArgument getIa() {
      return this.ia;
    }

    public void setIa(IllegalArgument ia) {
      this.ia = ia;
    }

    public void unsetIa() {
      this.ia = null;
    }

    // Returns true if field ia is set (has been asigned a value) and false otherwise
    public boolean isSetIa() {
      return this.ia != null;
    }

    public void setIaIsSet(boolean value) {
      if (!value) {
        this.ia = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      case IA:
        if (value == null) {
          unsetIa();
        } else {
          setIa((IllegalArgument)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case IO:
        return getIo();

      case IA:
        return getIa();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case IO:
        return isSetIo();
      case IA:
        return isSetIa();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof mutateRowTs_result)
        return this.equals((mutateRowTs_result)that);
      return false;
    }

    public boolean equals(mutateRowTs_result that) {
      if (that == null)
        return false;

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      boolean this_present_ia = true && this.isSetIa();
      boolean that_present_ia = true && that.isSetIa();
      if (this_present_ia || that_present_ia) {
        if (!(this_present_ia && that_present_ia))
          return false;
        if (!this.ia.equals(that.ia))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IA:
            if (field.type == TType.STRUCT) {
              this.ia = new IllegalArgument();
              this.ia.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      } else if (this.isSetIa()) {
        oprot.writeFieldBegin(IA_FIELD_DESC);
        this.ia.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("mutateRowTs_result(");
      boolean first = true;

      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("ia:");
      if (this.ia == null) {
        sb.append("null");
      } else {
        sb.append(this.ia);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class mutateRows_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("mutateRows_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_BATCHES_FIELD_DESC = new TField("rowBatches", TType.LIST, (short)2);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public List<BatchMutation> rowBatches;
    public static final int ROWBATCHES = 2;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROWBATCHES, new FieldMetaData("rowBatches", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new StructMetaData(TType.STRUCT, BatchMutation.class))));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(mutateRows_args.class, metaDataMap);
    }

    public mutateRows_args() {
    }

    public mutateRows_args(
      byte[] tableName,
      List<BatchMutation> rowBatches)
    {
      this();
      this.tableName = tableName;
      this.rowBatches = rowBatches;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public mutateRows_args(mutateRows_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRowBatches()) {
        List<BatchMutation> __this__rowBatches = new ArrayList<BatchMutation>();
        for (BatchMutation other_element : other.rowBatches) {
          __this__rowBatches.add(new BatchMutation(other_element));
        }
        this.rowBatches = __this__rowBatches;
      }
    }

    @Override
    public mutateRows_args clone() {
      return new mutateRows_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public int getRowBatchesSize() {
      return (this.rowBatches == null) ? 0 : this.rowBatches.size();
    }

    public java.util.Iterator<BatchMutation> getRowBatchesIterator() {
      return (this.rowBatches == null) ? null : this.rowBatches.iterator();
    }

    public void addToRowBatches(BatchMutation elem) {
      if (this.rowBatches == null) {
        this.rowBatches = new ArrayList<BatchMutation>();
      }
      this.rowBatches.add(elem);
    }

    public List<BatchMutation> getRowBatches() {
      return this.rowBatches;
    }

    public void setRowBatches(List<BatchMutation> rowBatches) {
      this.rowBatches = rowBatches;
    }

    public void unsetRowBatches() {
      this.rowBatches = null;
    }

    // Returns true if field rowBatches is set (has been asigned a value) and false otherwise
    public boolean isSetRowBatches() {
      return this.rowBatches != null;
    }

    public void setRowBatchesIsSet(boolean value) {
      if (!value) {
        this.rowBatches = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROWBATCHES:
        if (value == null) {
          unsetRowBatches();
        } else {
          setRowBatches((List<BatchMutation>)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROWBATCHES:
        return getRowBatches();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROWBATCHES:
        return isSetRowBatches();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof mutateRows_args)
        return this.equals((mutateRows_args)that);
      return false;
    }

    public boolean equals(mutateRows_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_rowBatches = true && this.isSetRowBatches();
      boolean that_present_rowBatches = true && that.isSetRowBatches();
      if (this_present_rowBatches || that_present_rowBatches) {
        if (!(this_present_rowBatches && that_present_rowBatches))
          return false;
        if (!this.rowBatches.equals(that.rowBatches))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROWBATCHES:
            if (field.type == TType.LIST) {
              {
                TList _list70 = iprot.readListBegin();
                this.rowBatches = new ArrayList<BatchMutation>(_list70.size);
                for (int _i71 = 0; _i71 < _list70.size; ++_i71)
                {
                  BatchMutation _elem72;
                  _elem72 = new BatchMutation();
                  _elem72.read(iprot);
                  this.rowBatches.add(_elem72);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.rowBatches != null) {
        oprot.writeFieldBegin(ROW_BATCHES_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.rowBatches.size()));
          for (BatchMutation _iter73 : this.rowBatches)          {
            _iter73.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("mutateRows_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("rowBatches:");
      if (this.rowBatches == null) {
        sb.append("null");
      } else {
        sb.append(this.rowBatches);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class mutateRows_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("mutateRows_result");
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);
    private static final TField IA_FIELD_DESC = new TField("ia", TType.STRUCT, (short)2);

    public IOError io;
    public static final int IO = 1;
    public IllegalArgument ia;
    public static final int IA = 2;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
      put(IA, new FieldMetaData("ia", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(mutateRows_result.class, metaDataMap);
    }

    public mutateRows_result() {
    }

    public mutateRows_result(
      IOError io,
      IllegalArgument ia)
    {
      this();
      this.io = io;
      this.ia = ia;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public mutateRows_result(mutateRows_result other) {
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
      if (other.isSetIa()) {
        this.ia = new IllegalArgument(other.ia);
      }
    }

    @Override
    public mutateRows_result clone() {
      return new mutateRows_result(this);
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public IllegalArgument getIa() {
      return this.ia;
    }

    public void setIa(IllegalArgument ia) {
      this.ia = ia;
    }

    public void unsetIa() {
      this.ia = null;
    }

    // Returns true if field ia is set (has been asigned a value) and false otherwise
    public boolean isSetIa() {
      return this.ia != null;
    }

    public void setIaIsSet(boolean value) {
      if (!value) {
        this.ia = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      case IA:
        if (value == null) {
          unsetIa();
        } else {
          setIa((IllegalArgument)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case IO:
        return getIo();

      case IA:
        return getIa();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case IO:
        return isSetIo();
      case IA:
        return isSetIa();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof mutateRows_result)
        return this.equals((mutateRows_result)that);
      return false;
    }

    public boolean equals(mutateRows_result that) {
      if (that == null)
        return false;

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      boolean this_present_ia = true && this.isSetIa();
      boolean that_present_ia = true && that.isSetIa();
      if (this_present_ia || that_present_ia) {
        if (!(this_present_ia && that_present_ia))
          return false;
        if (!this.ia.equals(that.ia))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IA:
            if (field.type == TType.STRUCT) {
              this.ia = new IllegalArgument();
              this.ia.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      } else if (this.isSetIa()) {
        oprot.writeFieldBegin(IA_FIELD_DESC);
        this.ia.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("mutateRows_result(");
      boolean first = true;

      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("ia:");
      if (this.ia == null) {
        sb.append("null");
      } else {
        sb.append(this.ia);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class mutateRowsTs_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("mutateRowsTs_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_BATCHES_FIELD_DESC = new TField("rowBatches", TType.LIST, (short)2);
    private static final TField TIMESTAMP_FIELD_DESC = new TField("timestamp", TType.I64, (short)3);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public List<BatchMutation> rowBatches;
    public static final int ROWBATCHES = 2;
    public long timestamp;
    public static final int TIMESTAMP = 3;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean timestamp = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROWBATCHES, new FieldMetaData("rowBatches", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new StructMetaData(TType.STRUCT, BatchMutation.class))));
      put(TIMESTAMP, new FieldMetaData("timestamp", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I64)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(mutateRowsTs_args.class, metaDataMap);
    }

    public mutateRowsTs_args() {
    }

    public mutateRowsTs_args(
      byte[] tableName,
      List<BatchMutation> rowBatches,
      long timestamp)
    {
      this();
      this.tableName = tableName;
      this.rowBatches = rowBatches;
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public mutateRowsTs_args(mutateRowsTs_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRowBatches()) {
        List<BatchMutation> __this__rowBatches = new ArrayList<BatchMutation>();
        for (BatchMutation other_element : other.rowBatches) {
          __this__rowBatches.add(new BatchMutation(other_element));
        }
        this.rowBatches = __this__rowBatches;
      }
      __isset.timestamp = other.__isset.timestamp;
      this.timestamp = other.timestamp;
    }

    @Override
    public mutateRowsTs_args clone() {
      return new mutateRowsTs_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public int getRowBatchesSize() {
      return (this.rowBatches == null) ? 0 : this.rowBatches.size();
    }

    public java.util.Iterator<BatchMutation> getRowBatchesIterator() {
      return (this.rowBatches == null) ? null : this.rowBatches.iterator();
    }

    public void addToRowBatches(BatchMutation elem) {
      if (this.rowBatches == null) {
        this.rowBatches = new ArrayList<BatchMutation>();
      }
      this.rowBatches.add(elem);
    }

    public List<BatchMutation> getRowBatches() {
      return this.rowBatches;
    }

    public void setRowBatches(List<BatchMutation> rowBatches) {
      this.rowBatches = rowBatches;
    }

    public void unsetRowBatches() {
      this.rowBatches = null;
    }

    // Returns true if field rowBatches is set (has been asigned a value) and false otherwise
    public boolean isSetRowBatches() {
      return this.rowBatches != null;
    }

    public void setRowBatchesIsSet(boolean value) {
      if (!value) {
        this.rowBatches = null;
      }
    }

    public long getTimestamp() {
      return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    public void unsetTimestamp() {
      this.__isset.timestamp = false;
    }

    // Returns true if field timestamp is set (has been asigned a value) and false otherwise
    public boolean isSetTimestamp() {
      return this.__isset.timestamp;
    }

    public void setTimestampIsSet(boolean value) {
      this.__isset.timestamp = value;
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROWBATCHES:
        if (value == null) {
          unsetRowBatches();
        } else {
          setRowBatches((List<BatchMutation>)value);
        }
        break;

      case TIMESTAMP:
        if (value == null) {
          unsetTimestamp();
        } else {
          setTimestamp((Long)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROWBATCHES:
        return getRowBatches();

      case TIMESTAMP:
        return getTimestamp();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROWBATCHES:
        return isSetRowBatches();
      case TIMESTAMP:
        return isSetTimestamp();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof mutateRowsTs_args)
        return this.equals((mutateRowsTs_args)that);
      return false;
    }

    public boolean equals(mutateRowsTs_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_rowBatches = true && this.isSetRowBatches();
      boolean that_present_rowBatches = true && that.isSetRowBatches();
      if (this_present_rowBatches || that_present_rowBatches) {
        if (!(this_present_rowBatches && that_present_rowBatches))
          return false;
        if (!this.rowBatches.equals(that.rowBatches))
          return false;
      }

      boolean this_present_timestamp = true;
      boolean that_present_timestamp = true;
      if (this_present_timestamp || that_present_timestamp) {
        if (!(this_present_timestamp && that_present_timestamp))
          return false;
        if (this.timestamp != that.timestamp)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROWBATCHES:
            if (field.type == TType.LIST) {
              {
                TList _list74 = iprot.readListBegin();
                this.rowBatches = new ArrayList<BatchMutation>(_list74.size);
                for (int _i75 = 0; _i75 < _list74.size; ++_i75)
                {
                  BatchMutation _elem76;
                  _elem76 = new BatchMutation();
                  _elem76.read(iprot);
                  this.rowBatches.add(_elem76);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case TIMESTAMP:
            if (field.type == TType.I64) {
              this.timestamp = iprot.readI64();
              this.__isset.timestamp = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.rowBatches != null) {
        oprot.writeFieldBegin(ROW_BATCHES_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.rowBatches.size()));
          for (BatchMutation _iter77 : this.rowBatches)          {
            _iter77.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(TIMESTAMP_FIELD_DESC);
      oprot.writeI64(this.timestamp);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("mutateRowsTs_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("rowBatches:");
      if (this.rowBatches == null) {
        sb.append("null");
      } else {
        sb.append(this.rowBatches);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("timestamp:");
      sb.append(this.timestamp);
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class mutateRowsTs_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("mutateRowsTs_result");
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);
    private static final TField IA_FIELD_DESC = new TField("ia", TType.STRUCT, (short)2);

    public IOError io;
    public static final int IO = 1;
    public IllegalArgument ia;
    public static final int IA = 2;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
      put(IA, new FieldMetaData("ia", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(mutateRowsTs_result.class, metaDataMap);
    }

    public mutateRowsTs_result() {
    }

    public mutateRowsTs_result(
      IOError io,
      IllegalArgument ia)
    {
      this();
      this.io = io;
      this.ia = ia;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public mutateRowsTs_result(mutateRowsTs_result other) {
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
      if (other.isSetIa()) {
        this.ia = new IllegalArgument(other.ia);
      }
    }

    @Override
    public mutateRowsTs_result clone() {
      return new mutateRowsTs_result(this);
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public IllegalArgument getIa() {
      return this.ia;
    }

    public void setIa(IllegalArgument ia) {
      this.ia = ia;
    }

    public void unsetIa() {
      this.ia = null;
    }

    // Returns true if field ia is set (has been asigned a value) and false otherwise
    public boolean isSetIa() {
      return this.ia != null;
    }

    public void setIaIsSet(boolean value) {
      if (!value) {
        this.ia = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      case IA:
        if (value == null) {
          unsetIa();
        } else {
          setIa((IllegalArgument)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case IO:
        return getIo();

      case IA:
        return getIa();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case IO:
        return isSetIo();
      case IA:
        return isSetIa();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof mutateRowsTs_result)
        return this.equals((mutateRowsTs_result)that);
      return false;
    }

    public boolean equals(mutateRowsTs_result that) {
      if (that == null)
        return false;

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      boolean this_present_ia = true && this.isSetIa();
      boolean that_present_ia = true && that.isSetIa();
      if (this_present_ia || that_present_ia) {
        if (!(this_present_ia && that_present_ia))
          return false;
        if (!this.ia.equals(that.ia))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IA:
            if (field.type == TType.STRUCT) {
              this.ia = new IllegalArgument();
              this.ia.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      } else if (this.isSetIa()) {
        oprot.writeFieldBegin(IA_FIELD_DESC);
        this.ia.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("mutateRowsTs_result(");
      boolean first = true;

      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("ia:");
      if (this.ia == null) {
        sb.append("null");
      } else {
        sb.append(this.ia);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class atomicIncrement_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("atomicIncrement_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)2);
    private static final TField COLUMN_FIELD_DESC = new TField("column", TType.STRING, (short)3);
    private static final TField VALUE_FIELD_DESC = new TField("value", TType.I64, (short)4);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] row;
    public static final int ROW = 2;
    public byte[] column;
    public static final int COLUMN = 3;
    public long value;
    public static final int VALUE = 4;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean value = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(COLUMN, new FieldMetaData("column", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(VALUE, new FieldMetaData("value", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I64)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(atomicIncrement_args.class, metaDataMap);
    }

    public atomicIncrement_args() {
    }

    public atomicIncrement_args(
      byte[] tableName,
      byte[] row,
      byte[] column,
      long value)
    {
      this();
      this.tableName = tableName;
      this.row = row;
      this.column = column;
      this.value = value;
      this.__isset.value = true;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public atomicIncrement_args(atomicIncrement_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRow()) {
        this.row = other.row;
      }
      if (other.isSetColumn()) {
        this.column = other.column;
      }
      __isset.value = other.__isset.value;
      this.value = other.value;
    }

    @Override
    public atomicIncrement_args clone() {
      return new atomicIncrement_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getRow() {
      return this.row;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public void unsetRow() {
      this.row = null;
    }

    // Returns true if field row is set (has been asigned a value) and false otherwise
    public boolean isSetRow() {
      return this.row != null;
    }

    public void setRowIsSet(boolean value) {
      if (!value) {
        this.row = null;
      }
    }

    public byte[] getColumn() {
      return this.column;
    }

    public void setColumn(byte[] column) {
      this.column = column;
    }

    public void unsetColumn() {
      this.column = null;
    }

    // Returns true if field column is set (has been asigned a value) and false otherwise
    public boolean isSetColumn() {
      return this.column != null;
    }

    public void setColumnIsSet(boolean value) {
      if (!value) {
        this.column = null;
      }
    }

    public long getValue() {
      return this.value;
    }

    public void setValue(long value) {
      this.value = value;
      this.__isset.value = true;
    }

    public void unsetValue() {
      this.__isset.value = false;
    }

    // Returns true if field value is set (has been asigned a value) and false otherwise
    public boolean isSetValue() {
      return this.__isset.value;
    }

    public void setValueIsSet(boolean value) {
      this.__isset.value = value;
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROW:
        if (value == null) {
          unsetRow();
        } else {
          setRow((byte[])value);
        }
        break;

      case COLUMN:
        if (value == null) {
          unsetColumn();
        } else {
          setColumn((byte[])value);
        }
        break;

      case VALUE:
        if (value == null) {
          unsetValue();
        } else {
          setValue((Long)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROW:
        return getRow();

      case COLUMN:
        return getColumn();

      case VALUE:
        return getValue();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROW:
        return isSetRow();
      case COLUMN:
        return isSetColumn();
      case VALUE:
        return isSetValue();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof atomicIncrement_args)
        return this.equals((atomicIncrement_args)that);
      return false;
    }

    public boolean equals(atomicIncrement_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_row = true && this.isSetRow();
      boolean that_present_row = true && that.isSetRow();
      if (this_present_row || that_present_row) {
        if (!(this_present_row && that_present_row))
          return false;
        if (!java.util.Arrays.equals(this.row, that.row))
          return false;
      }

      boolean this_present_column = true && this.isSetColumn();
      boolean that_present_column = true && that.isSetColumn();
      if (this_present_column || that_present_column) {
        if (!(this_present_column && that_present_column))
          return false;
        if (!java.util.Arrays.equals(this.column, that.column))
          return false;
      }

      boolean this_present_value = true;
      boolean that_present_value = true;
      if (this_present_value || that_present_value) {
        if (!(this_present_value && that_present_value))
          return false;
        if (this.value != that.value)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROW:
            if (field.type == TType.STRING) {
              this.row = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case COLUMN:
            if (field.type == TType.STRING) {
              this.column = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case VALUE:
            if (field.type == TType.I64) {
              this.value = iprot.readI64();
              this.__isset.value = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        oprot.writeBinary(this.row);
        oprot.writeFieldEnd();
      }
      if (this.column != null) {
        oprot.writeFieldBegin(COLUMN_FIELD_DESC);
        oprot.writeBinary(this.column);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(VALUE_FIELD_DESC);
      oprot.writeI64(this.value);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("atomicIncrement_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("row:");
      if (this.row == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.row));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("column:");
      if (this.column == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.column));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("value:");
      sb.append(this.value);
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class atomicIncrement_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("atomicIncrement_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.I64, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);
    private static final TField IA_FIELD_DESC = new TField("ia", TType.STRUCT, (short)2);

    public long success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;
    public IllegalArgument ia;
    public static final int IA = 2;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean success = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I64)));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
      put(IA, new FieldMetaData("ia", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(atomicIncrement_result.class, metaDataMap);
    }

    public atomicIncrement_result() {
    }

    public atomicIncrement_result(
      long success,
      IOError io,
      IllegalArgument ia)
    {
      this();
      this.success = success;
      this.__isset.success = true;
      this.io = io;
      this.ia = ia;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public atomicIncrement_result(atomicIncrement_result other) {
      __isset.success = other.__isset.success;
      this.success = other.success;
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
      if (other.isSetIa()) {
        this.ia = new IllegalArgument(other.ia);
      }
    }

    @Override
    public atomicIncrement_result clone() {
      return new atomicIncrement_result(this);
    }

    public long getSuccess() {
      return this.success;
    }

    public void setSuccess(long success) {
      this.success = success;
      this.__isset.success = true;
    }

    public void unsetSuccess() {
      this.__isset.success = false;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.__isset.success;
    }

    public void setSuccessIsSet(boolean value) {
      this.__isset.success = value;
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public IllegalArgument getIa() {
      return this.ia;
    }

    public void setIa(IllegalArgument ia) {
      this.ia = ia;
    }

    public void unsetIa() {
      this.ia = null;
    }

    // Returns true if field ia is set (has been asigned a value) and false otherwise
    public boolean isSetIa() {
      return this.ia != null;
    }

    public void setIaIsSet(boolean value) {
      if (!value) {
        this.ia = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((Long)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      case IA:
        if (value == null) {
          unsetIa();
        } else {
          setIa((IllegalArgument)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      case IA:
        return getIa();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      case IA:
        return isSetIa();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof atomicIncrement_result)
        return this.equals((atomicIncrement_result)that);
      return false;
    }

    public boolean equals(atomicIncrement_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true;
      boolean that_present_success = true;
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (this.success != that.success)
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      boolean this_present_ia = true && this.isSetIa();
      boolean that_present_ia = true && that.isSetIa();
      if (this_present_ia || that_present_ia) {
        if (!(this_present_ia && that_present_ia))
          return false;
        if (!this.ia.equals(that.ia))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.I64) {
              this.success = iprot.readI64();
              this.__isset.success = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IA:
            if (field.type == TType.STRUCT) {
              this.ia = new IllegalArgument();
              this.ia.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        oprot.writeI64(this.success);
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      } else if (this.isSetIa()) {
        oprot.writeFieldBegin(IA_FIELD_DESC);
        this.ia.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("atomicIncrement_result(");
      boolean first = true;

      sb.append("success:");
      sb.append(this.success);
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("ia:");
      if (this.ia == null) {
        sb.append("null");
      } else {
        sb.append(this.ia);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class deleteAll_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("deleteAll_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)2);
    private static final TField COLUMN_FIELD_DESC = new TField("column", TType.STRING, (short)3);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] row;
    public static final int ROW = 2;
    public byte[] column;
    public static final int COLUMN = 3;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(COLUMN, new FieldMetaData("column", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(deleteAll_args.class, metaDataMap);
    }

    public deleteAll_args() {
    }

    public deleteAll_args(
      byte[] tableName,
      byte[] row,
      byte[] column)
    {
      this();
      this.tableName = tableName;
      this.row = row;
      this.column = column;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public deleteAll_args(deleteAll_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRow()) {
        this.row = other.row;
      }
      if (other.isSetColumn()) {
        this.column = other.column;
      }
    }

    @Override
    public deleteAll_args clone() {
      return new deleteAll_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getRow() {
      return this.row;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public void unsetRow() {
      this.row = null;
    }

    // Returns true if field row is set (has been asigned a value) and false otherwise
    public boolean isSetRow() {
      return this.row != null;
    }

    public void setRowIsSet(boolean value) {
      if (!value) {
        this.row = null;
      }
    }

    public byte[] getColumn() {
      return this.column;
    }

    public void setColumn(byte[] column) {
      this.column = column;
    }

    public void unsetColumn() {
      this.column = null;
    }

    // Returns true if field column is set (has been asigned a value) and false otherwise
    public boolean isSetColumn() {
      return this.column != null;
    }

    public void setColumnIsSet(boolean value) {
      if (!value) {
        this.column = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROW:
        if (value == null) {
          unsetRow();
        } else {
          setRow((byte[])value);
        }
        break;

      case COLUMN:
        if (value == null) {
          unsetColumn();
        } else {
          setColumn((byte[])value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROW:
        return getRow();

      case COLUMN:
        return getColumn();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROW:
        return isSetRow();
      case COLUMN:
        return isSetColumn();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof deleteAll_args)
        return this.equals((deleteAll_args)that);
      return false;
    }

    public boolean equals(deleteAll_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_row = true && this.isSetRow();
      boolean that_present_row = true && that.isSetRow();
      if (this_present_row || that_present_row) {
        if (!(this_present_row && that_present_row))
          return false;
        if (!java.util.Arrays.equals(this.row, that.row))
          return false;
      }

      boolean this_present_column = true && this.isSetColumn();
      boolean that_present_column = true && that.isSetColumn();
      if (this_present_column || that_present_column) {
        if (!(this_present_column && that_present_column))
          return false;
        if (!java.util.Arrays.equals(this.column, that.column))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROW:
            if (field.type == TType.STRING) {
              this.row = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case COLUMN:
            if (field.type == TType.STRING) {
              this.column = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        oprot.writeBinary(this.row);
        oprot.writeFieldEnd();
      }
      if (this.column != null) {
        oprot.writeFieldBegin(COLUMN_FIELD_DESC);
        oprot.writeBinary(this.column);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("deleteAll_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("row:");
      if (this.row == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.row));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("column:");
      if (this.column == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.column));
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class deleteAll_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("deleteAll_result");
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(deleteAll_result.class, metaDataMap);
    }

    public deleteAll_result() {
    }

    public deleteAll_result(
      IOError io)
    {
      this();
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public deleteAll_result(deleteAll_result other) {
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public deleteAll_result clone() {
      return new deleteAll_result(this);
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof deleteAll_result)
        return this.equals((deleteAll_result)that);
      return false;
    }

    public boolean equals(deleteAll_result that) {
      if (that == null)
        return false;

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("deleteAll_result(");

      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class deleteAllTs_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("deleteAllTs_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)2);
    private static final TField COLUMN_FIELD_DESC = new TField("column", TType.STRING, (short)3);
    private static final TField TIMESTAMP_FIELD_DESC = new TField("timestamp", TType.I64, (short)4);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] row;
    public static final int ROW = 2;
    public byte[] column;
    public static final int COLUMN = 3;
    public long timestamp;
    public static final int TIMESTAMP = 4;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean timestamp = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(COLUMN, new FieldMetaData("column", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(TIMESTAMP, new FieldMetaData("timestamp", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I64)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(deleteAllTs_args.class, metaDataMap);
    }

    public deleteAllTs_args() {
    }

    public deleteAllTs_args(
      byte[] tableName,
      byte[] row,
      byte[] column,
      long timestamp)
    {
      this();
      this.tableName = tableName;
      this.row = row;
      this.column = column;
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public deleteAllTs_args(deleteAllTs_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRow()) {
        this.row = other.row;
      }
      if (other.isSetColumn()) {
        this.column = other.column;
      }
      __isset.timestamp = other.__isset.timestamp;
      this.timestamp = other.timestamp;
    }

    @Override
    public deleteAllTs_args clone() {
      return new deleteAllTs_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getRow() {
      return this.row;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public void unsetRow() {
      this.row = null;
    }

    // Returns true if field row is set (has been asigned a value) and false otherwise
    public boolean isSetRow() {
      return this.row != null;
    }

    public void setRowIsSet(boolean value) {
      if (!value) {
        this.row = null;
      }
    }

    public byte[] getColumn() {
      return this.column;
    }

    public void setColumn(byte[] column) {
      this.column = column;
    }

    public void unsetColumn() {
      this.column = null;
    }

    // Returns true if field column is set (has been asigned a value) and false otherwise
    public boolean isSetColumn() {
      return this.column != null;
    }

    public void setColumnIsSet(boolean value) {
      if (!value) {
        this.column = null;
      }
    }

    public long getTimestamp() {
      return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    public void unsetTimestamp() {
      this.__isset.timestamp = false;
    }

    // Returns true if field timestamp is set (has been asigned a value) and false otherwise
    public boolean isSetTimestamp() {
      return this.__isset.timestamp;
    }

    public void setTimestampIsSet(boolean value) {
      this.__isset.timestamp = value;
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROW:
        if (value == null) {
          unsetRow();
        } else {
          setRow((byte[])value);
        }
        break;

      case COLUMN:
        if (value == null) {
          unsetColumn();
        } else {
          setColumn((byte[])value);
        }
        break;

      case TIMESTAMP:
        if (value == null) {
          unsetTimestamp();
        } else {
          setTimestamp((Long)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROW:
        return getRow();

      case COLUMN:
        return getColumn();

      case TIMESTAMP:
        return getTimestamp();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROW:
        return isSetRow();
      case COLUMN:
        return isSetColumn();
      case TIMESTAMP:
        return isSetTimestamp();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof deleteAllTs_args)
        return this.equals((deleteAllTs_args)that);
      return false;
    }

    public boolean equals(deleteAllTs_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_row = true && this.isSetRow();
      boolean that_present_row = true && that.isSetRow();
      if (this_present_row || that_present_row) {
        if (!(this_present_row && that_present_row))
          return false;
        if (!java.util.Arrays.equals(this.row, that.row))
          return false;
      }

      boolean this_present_column = true && this.isSetColumn();
      boolean that_present_column = true && that.isSetColumn();
      if (this_present_column || that_present_column) {
        if (!(this_present_column && that_present_column))
          return false;
        if (!java.util.Arrays.equals(this.column, that.column))
          return false;
      }

      boolean this_present_timestamp = true;
      boolean that_present_timestamp = true;
      if (this_present_timestamp || that_present_timestamp) {
        if (!(this_present_timestamp && that_present_timestamp))
          return false;
        if (this.timestamp != that.timestamp)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROW:
            if (field.type == TType.STRING) {
              this.row = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case COLUMN:
            if (field.type == TType.STRING) {
              this.column = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case TIMESTAMP:
            if (field.type == TType.I64) {
              this.timestamp = iprot.readI64();
              this.__isset.timestamp = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        oprot.writeBinary(this.row);
        oprot.writeFieldEnd();
      }
      if (this.column != null) {
        oprot.writeFieldBegin(COLUMN_FIELD_DESC);
        oprot.writeBinary(this.column);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(TIMESTAMP_FIELD_DESC);
      oprot.writeI64(this.timestamp);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("deleteAllTs_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("row:");
      if (this.row == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.row));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("column:");
      if (this.column == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.column));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("timestamp:");
      sb.append(this.timestamp);
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class deleteAllTs_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("deleteAllTs_result");
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(deleteAllTs_result.class, metaDataMap);
    }

    public deleteAllTs_result() {
    }

    public deleteAllTs_result(
      IOError io)
    {
      this();
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public deleteAllTs_result(deleteAllTs_result other) {
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public deleteAllTs_result clone() {
      return new deleteAllTs_result(this);
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof deleteAllTs_result)
        return this.equals((deleteAllTs_result)that);
      return false;
    }

    public boolean equals(deleteAllTs_result that) {
      if (that == null)
        return false;

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("deleteAllTs_result(");

      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class deleteAllRow_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("deleteAllRow_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)2);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] row;
    public static final int ROW = 2;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(deleteAllRow_args.class, metaDataMap);
    }

    public deleteAllRow_args() {
    }

    public deleteAllRow_args(
      byte[] tableName,
      byte[] row)
    {
      this();
      this.tableName = tableName;
      this.row = row;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public deleteAllRow_args(deleteAllRow_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRow()) {
        this.row = other.row;
      }
    }

    @Override
    public deleteAllRow_args clone() {
      return new deleteAllRow_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getRow() {
      return this.row;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public void unsetRow() {
      this.row = null;
    }

    // Returns true if field row is set (has been asigned a value) and false otherwise
    public boolean isSetRow() {
      return this.row != null;
    }

    public void setRowIsSet(boolean value) {
      if (!value) {
        this.row = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROW:
        if (value == null) {
          unsetRow();
        } else {
          setRow((byte[])value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROW:
        return getRow();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROW:
        return isSetRow();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof deleteAllRow_args)
        return this.equals((deleteAllRow_args)that);
      return false;
    }

    public boolean equals(deleteAllRow_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_row = true && this.isSetRow();
      boolean that_present_row = true && that.isSetRow();
      if (this_present_row || that_present_row) {
        if (!(this_present_row && that_present_row))
          return false;
        if (!java.util.Arrays.equals(this.row, that.row))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROW:
            if (field.type == TType.STRING) {
              this.row = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        oprot.writeBinary(this.row);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("deleteAllRow_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("row:");
      if (this.row == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.row));
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class deleteAllRow_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("deleteAllRow_result");
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(deleteAllRow_result.class, metaDataMap);
    }

    public deleteAllRow_result() {
    }

    public deleteAllRow_result(
      IOError io)
    {
      this();
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public deleteAllRow_result(deleteAllRow_result other) {
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public deleteAllRow_result clone() {
      return new deleteAllRow_result(this);
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof deleteAllRow_result)
        return this.equals((deleteAllRow_result)that);
      return false;
    }

    public boolean equals(deleteAllRow_result that) {
      if (that == null)
        return false;

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("deleteAllRow_result(");

      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class deleteAllRowTs_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("deleteAllRowTs_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField ROW_FIELD_DESC = new TField("row", TType.STRING, (short)2);
    private static final TField TIMESTAMP_FIELD_DESC = new TField("timestamp", TType.I64, (short)3);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] row;
    public static final int ROW = 2;
    public long timestamp;
    public static final int TIMESTAMP = 3;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean timestamp = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(ROW, new FieldMetaData("row", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(TIMESTAMP, new FieldMetaData("timestamp", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I64)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(deleteAllRowTs_args.class, metaDataMap);
    }

    public deleteAllRowTs_args() {
    }

    public deleteAllRowTs_args(
      byte[] tableName,
      byte[] row,
      long timestamp)
    {
      this();
      this.tableName = tableName;
      this.row = row;
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public deleteAllRowTs_args(deleteAllRowTs_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetRow()) {
        this.row = other.row;
      }
      __isset.timestamp = other.__isset.timestamp;
      this.timestamp = other.timestamp;
    }

    @Override
    public deleteAllRowTs_args clone() {
      return new deleteAllRowTs_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getRow() {
      return this.row;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public void unsetRow() {
      this.row = null;
    }

    // Returns true if field row is set (has been asigned a value) and false otherwise
    public boolean isSetRow() {
      return this.row != null;
    }

    public void setRowIsSet(boolean value) {
      if (!value) {
        this.row = null;
      }
    }

    public long getTimestamp() {
      return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    public void unsetTimestamp() {
      this.__isset.timestamp = false;
    }

    // Returns true if field timestamp is set (has been asigned a value) and false otherwise
    public boolean isSetTimestamp() {
      return this.__isset.timestamp;
    }

    public void setTimestampIsSet(boolean value) {
      this.__isset.timestamp = value;
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case ROW:
        if (value == null) {
          unsetRow();
        } else {
          setRow((byte[])value);
        }
        break;

      case TIMESTAMP:
        if (value == null) {
          unsetTimestamp();
        } else {
          setTimestamp((Long)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case ROW:
        return getRow();

      case TIMESTAMP:
        return getTimestamp();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case ROW:
        return isSetRow();
      case TIMESTAMP:
        return isSetTimestamp();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof deleteAllRowTs_args)
        return this.equals((deleteAllRowTs_args)that);
      return false;
    }

    public boolean equals(deleteAllRowTs_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_row = true && this.isSetRow();
      boolean that_present_row = true && that.isSetRow();
      if (this_present_row || that_present_row) {
        if (!(this_present_row && that_present_row))
          return false;
        if (!java.util.Arrays.equals(this.row, that.row))
          return false;
      }

      boolean this_present_timestamp = true;
      boolean that_present_timestamp = true;
      if (this_present_timestamp || that_present_timestamp) {
        if (!(this_present_timestamp && that_present_timestamp))
          return false;
        if (this.timestamp != that.timestamp)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case ROW:
            if (field.type == TType.STRING) {
              this.row = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case TIMESTAMP:
            if (field.type == TType.I64) {
              this.timestamp = iprot.readI64();
              this.__isset.timestamp = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        oprot.writeBinary(this.row);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(TIMESTAMP_FIELD_DESC);
      oprot.writeI64(this.timestamp);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("deleteAllRowTs_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("row:");
      if (this.row == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.row));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("timestamp:");
      sb.append(this.timestamp);
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class deleteAllRowTs_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("deleteAllRowTs_result");
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public IOError io;
    public static final int IO = 1;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(deleteAllRowTs_result.class, metaDataMap);
    }

    public deleteAllRowTs_result() {
    }

    public deleteAllRowTs_result(
      IOError io)
    {
      this();
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public deleteAllRowTs_result(deleteAllRowTs_result other) {
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public deleteAllRowTs_result clone() {
      return new deleteAllRowTs_result(this);
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof deleteAllRowTs_result)
        return this.equals((deleteAllRowTs_result)that);
      return false;
    }

    public boolean equals(deleteAllRowTs_result that) {
      if (that == null)
        return false;

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("deleteAllRowTs_result(");

      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerOpen_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerOpen_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField START_ROW_FIELD_DESC = new TField("startRow", TType.STRING, (short)2);
    private static final TField COLUMNS_FIELD_DESC = new TField("columns", TType.LIST, (short)3);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] startRow;
    public static final int STARTROW = 2;
    public List<byte[]> columns;
    public static final int COLUMNS = 3;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(STARTROW, new FieldMetaData("startRow", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(COLUMNS, new FieldMetaData("columns", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new FieldValueMetaData(TType.STRING))));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerOpen_args.class, metaDataMap);
    }

    public scannerOpen_args() {
    }

    public scannerOpen_args(
      byte[] tableName,
      byte[] startRow,
      List<byte[]> columns)
    {
      this();
      this.tableName = tableName;
      this.startRow = startRow;
      this.columns = columns;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerOpen_args(scannerOpen_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetStartRow()) {
        this.startRow = other.startRow;
      }
      if (other.isSetColumns()) {
        List<byte[]> __this__columns = new ArrayList<byte[]>();
        for (byte[] other_element : other.columns) {
          __this__columns.add(other_element);
        }
        this.columns = __this__columns;
      }
    }

    @Override
    public scannerOpen_args clone() {
      return new scannerOpen_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getStartRow() {
      return this.startRow;
    }

    public void setStartRow(byte[] startRow) {
      this.startRow = startRow;
    }

    public void unsetStartRow() {
      this.startRow = null;
    }

    // Returns true if field startRow is set (has been asigned a value) and false otherwise
    public boolean isSetStartRow() {
      return this.startRow != null;
    }

    public void setStartRowIsSet(boolean value) {
      if (!value) {
        this.startRow = null;
      }
    }

    public int getColumnsSize() {
      return (this.columns == null) ? 0 : this.columns.size();
    }

    public java.util.Iterator<byte[]> getColumnsIterator() {
      return (this.columns == null) ? null : this.columns.iterator();
    }

    public void addToColumns(byte[] elem) {
      if (this.columns == null) {
        this.columns = new ArrayList<byte[]>();
      }
      this.columns.add(elem);
    }

    public List<byte[]> getColumns() {
      return this.columns;
    }

    public void setColumns(List<byte[]> columns) {
      this.columns = columns;
    }

    public void unsetColumns() {
      this.columns = null;
    }

    // Returns true if field columns is set (has been asigned a value) and false otherwise
    public boolean isSetColumns() {
      return this.columns != null;
    }

    public void setColumnsIsSet(boolean value) {
      if (!value) {
        this.columns = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case STARTROW:
        if (value == null) {
          unsetStartRow();
        } else {
          setStartRow((byte[])value);
        }
        break;

      case COLUMNS:
        if (value == null) {
          unsetColumns();
        } else {
          setColumns((List<byte[]>)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case STARTROW:
        return getStartRow();

      case COLUMNS:
        return getColumns();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case STARTROW:
        return isSetStartRow();
      case COLUMNS:
        return isSetColumns();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerOpen_args)
        return this.equals((scannerOpen_args)that);
      return false;
    }

    public boolean equals(scannerOpen_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_startRow = true && this.isSetStartRow();
      boolean that_present_startRow = true && that.isSetStartRow();
      if (this_present_startRow || that_present_startRow) {
        if (!(this_present_startRow && that_present_startRow))
          return false;
        if (!java.util.Arrays.equals(this.startRow, that.startRow))
          return false;
      }

      boolean this_present_columns = true && this.isSetColumns();
      boolean that_present_columns = true && that.isSetColumns();
      if (this_present_columns || that_present_columns) {
        if (!(this_present_columns && that_present_columns))
          return false;
        if (!this.columns.equals(that.columns))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case STARTROW:
            if (field.type == TType.STRING) {
              this.startRow = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case COLUMNS:
            if (field.type == TType.LIST) {
              {
                TList _list78 = iprot.readListBegin();
                this.columns = new ArrayList<byte[]>(_list78.size);
                for (int _i79 = 0; _i79 < _list78.size; ++_i79)
                {
                  byte[] _elem80;
                  _elem80 = iprot.readBinary();
                  this.columns.add(_elem80);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.startRow != null) {
        oprot.writeFieldBegin(START_ROW_FIELD_DESC);
        oprot.writeBinary(this.startRow);
        oprot.writeFieldEnd();
      }
      if (this.columns != null) {
        oprot.writeFieldBegin(COLUMNS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRING, this.columns.size()));
          for (byte[] _iter81 : this.columns)          {
            oprot.writeBinary(_iter81);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerOpen_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("startRow:");
      if (this.startRow == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.startRow));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("columns:");
      if (this.columns == null) {
        sb.append("null");
      } else {
        sb.append(this.columns);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerOpen_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerOpen_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.I32, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public int success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean success = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I32)));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerOpen_result.class, metaDataMap);
    }

    public scannerOpen_result() {
    }

    public scannerOpen_result(
      int success,
      IOError io)
    {
      this();
      this.success = success;
      this.__isset.success = true;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerOpen_result(scannerOpen_result other) {
      __isset.success = other.__isset.success;
      this.success = other.success;
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public scannerOpen_result clone() {
      return new scannerOpen_result(this);
    }

    public int getSuccess() {
      return this.success;
    }

    public void setSuccess(int success) {
      this.success = success;
      this.__isset.success = true;
    }

    public void unsetSuccess() {
      this.__isset.success = false;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.__isset.success;
    }

    public void setSuccessIsSet(boolean value) {
      this.__isset.success = value;
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((Integer)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerOpen_result)
        return this.equals((scannerOpen_result)that);
      return false;
    }

    public boolean equals(scannerOpen_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true;
      boolean that_present_success = true;
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (this.success != that.success)
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.I32) {
              this.success = iprot.readI32();
              this.__isset.success = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        oprot.writeI32(this.success);
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerOpen_result(");
      boolean first = true;

      sb.append("success:");
      sb.append(this.success);
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerOpenWithStop_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerOpenWithStop_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField START_ROW_FIELD_DESC = new TField("startRow", TType.STRING, (short)2);
    private static final TField STOP_ROW_FIELD_DESC = new TField("stopRow", TType.STRING, (short)3);
    private static final TField COLUMNS_FIELD_DESC = new TField("columns", TType.LIST, (short)4);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] startRow;
    public static final int STARTROW = 2;
    public byte[] stopRow;
    public static final int STOPROW = 3;
    public List<byte[]> columns;
    public static final int COLUMNS = 4;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(STARTROW, new FieldMetaData("startRow", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(STOPROW, new FieldMetaData("stopRow", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(COLUMNS, new FieldMetaData("columns", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new FieldValueMetaData(TType.STRING))));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerOpenWithStop_args.class, metaDataMap);
    }

    public scannerOpenWithStop_args() {
    }

    public scannerOpenWithStop_args(
      byte[] tableName,
      byte[] startRow,
      byte[] stopRow,
      List<byte[]> columns)
    {
      this();
      this.tableName = tableName;
      this.startRow = startRow;
      this.stopRow = stopRow;
      this.columns = columns;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerOpenWithStop_args(scannerOpenWithStop_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetStartRow()) {
        this.startRow = other.startRow;
      }
      if (other.isSetStopRow()) {
        this.stopRow = other.stopRow;
      }
      if (other.isSetColumns()) {
        List<byte[]> __this__columns = new ArrayList<byte[]>();
        for (byte[] other_element : other.columns) {
          __this__columns.add(other_element);
        }
        this.columns = __this__columns;
      }
    }

    @Override
    public scannerOpenWithStop_args clone() {
      return new scannerOpenWithStop_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getStartRow() {
      return this.startRow;
    }

    public void setStartRow(byte[] startRow) {
      this.startRow = startRow;
    }

    public void unsetStartRow() {
      this.startRow = null;
    }

    // Returns true if field startRow is set (has been asigned a value) and false otherwise
    public boolean isSetStartRow() {
      return this.startRow != null;
    }

    public void setStartRowIsSet(boolean value) {
      if (!value) {
        this.startRow = null;
      }
    }

    public byte[] getStopRow() {
      return this.stopRow;
    }

    public void setStopRow(byte[] stopRow) {
      this.stopRow = stopRow;
    }

    public void unsetStopRow() {
      this.stopRow = null;
    }

    // Returns true if field stopRow is set (has been asigned a value) and false otherwise
    public boolean isSetStopRow() {
      return this.stopRow != null;
    }

    public void setStopRowIsSet(boolean value) {
      if (!value) {
        this.stopRow = null;
      }
    }

    public int getColumnsSize() {
      return (this.columns == null) ? 0 : this.columns.size();
    }

    public java.util.Iterator<byte[]> getColumnsIterator() {
      return (this.columns == null) ? null : this.columns.iterator();
    }

    public void addToColumns(byte[] elem) {
      if (this.columns == null) {
        this.columns = new ArrayList<byte[]>();
      }
      this.columns.add(elem);
    }

    public List<byte[]> getColumns() {
      return this.columns;
    }

    public void setColumns(List<byte[]> columns) {
      this.columns = columns;
    }

    public void unsetColumns() {
      this.columns = null;
    }

    // Returns true if field columns is set (has been asigned a value) and false otherwise
    public boolean isSetColumns() {
      return this.columns != null;
    }

    public void setColumnsIsSet(boolean value) {
      if (!value) {
        this.columns = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case STARTROW:
        if (value == null) {
          unsetStartRow();
        } else {
          setStartRow((byte[])value);
        }
        break;

      case STOPROW:
        if (value == null) {
          unsetStopRow();
        } else {
          setStopRow((byte[])value);
        }
        break;

      case COLUMNS:
        if (value == null) {
          unsetColumns();
        } else {
          setColumns((List<byte[]>)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case STARTROW:
        return getStartRow();

      case STOPROW:
        return getStopRow();

      case COLUMNS:
        return getColumns();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case STARTROW:
        return isSetStartRow();
      case STOPROW:
        return isSetStopRow();
      case COLUMNS:
        return isSetColumns();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerOpenWithStop_args)
        return this.equals((scannerOpenWithStop_args)that);
      return false;
    }

    public boolean equals(scannerOpenWithStop_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_startRow = true && this.isSetStartRow();
      boolean that_present_startRow = true && that.isSetStartRow();
      if (this_present_startRow || that_present_startRow) {
        if (!(this_present_startRow && that_present_startRow))
          return false;
        if (!java.util.Arrays.equals(this.startRow, that.startRow))
          return false;
      }

      boolean this_present_stopRow = true && this.isSetStopRow();
      boolean that_present_stopRow = true && that.isSetStopRow();
      if (this_present_stopRow || that_present_stopRow) {
        if (!(this_present_stopRow && that_present_stopRow))
          return false;
        if (!java.util.Arrays.equals(this.stopRow, that.stopRow))
          return false;
      }

      boolean this_present_columns = true && this.isSetColumns();
      boolean that_present_columns = true && that.isSetColumns();
      if (this_present_columns || that_present_columns) {
        if (!(this_present_columns && that_present_columns))
          return false;
        if (!this.columns.equals(that.columns))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case STARTROW:
            if (field.type == TType.STRING) {
              this.startRow = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case STOPROW:
            if (field.type == TType.STRING) {
              this.stopRow = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case COLUMNS:
            if (field.type == TType.LIST) {
              {
                TList _list82 = iprot.readListBegin();
                this.columns = new ArrayList<byte[]>(_list82.size);
                for (int _i83 = 0; _i83 < _list82.size; ++_i83)
                {
                  byte[] _elem84;
                  _elem84 = iprot.readBinary();
                  this.columns.add(_elem84);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.startRow != null) {
        oprot.writeFieldBegin(START_ROW_FIELD_DESC);
        oprot.writeBinary(this.startRow);
        oprot.writeFieldEnd();
      }
      if (this.stopRow != null) {
        oprot.writeFieldBegin(STOP_ROW_FIELD_DESC);
        oprot.writeBinary(this.stopRow);
        oprot.writeFieldEnd();
      }
      if (this.columns != null) {
        oprot.writeFieldBegin(COLUMNS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRING, this.columns.size()));
          for (byte[] _iter85 : this.columns)          {
            oprot.writeBinary(_iter85);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerOpenWithStop_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("startRow:");
      if (this.startRow == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.startRow));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("stopRow:");
      if (this.stopRow == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.stopRow));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("columns:");
      if (this.columns == null) {
        sb.append("null");
      } else {
        sb.append(this.columns);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerOpenWithStop_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerOpenWithStop_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.I32, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public int success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean success = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I32)));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerOpenWithStop_result.class, metaDataMap);
    }

    public scannerOpenWithStop_result() {
    }

    public scannerOpenWithStop_result(
      int success,
      IOError io)
    {
      this();
      this.success = success;
      this.__isset.success = true;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerOpenWithStop_result(scannerOpenWithStop_result other) {
      __isset.success = other.__isset.success;
      this.success = other.success;
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public scannerOpenWithStop_result clone() {
      return new scannerOpenWithStop_result(this);
    }

    public int getSuccess() {
      return this.success;
    }

    public void setSuccess(int success) {
      this.success = success;
      this.__isset.success = true;
    }

    public void unsetSuccess() {
      this.__isset.success = false;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.__isset.success;
    }

    public void setSuccessIsSet(boolean value) {
      this.__isset.success = value;
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((Integer)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerOpenWithStop_result)
        return this.equals((scannerOpenWithStop_result)that);
      return false;
    }

    public boolean equals(scannerOpenWithStop_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true;
      boolean that_present_success = true;
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (this.success != that.success)
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.I32) {
              this.success = iprot.readI32();
              this.__isset.success = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        oprot.writeI32(this.success);
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerOpenWithStop_result(");
      boolean first = true;

      sb.append("success:");
      sb.append(this.success);
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerOpenWithPrefix_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerOpenWithPrefix_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField START_AND_PREFIX_FIELD_DESC = new TField("startAndPrefix", TType.STRING, (short)2);
    private static final TField COLUMNS_FIELD_DESC = new TField("columns", TType.LIST, (short)3);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] startAndPrefix;
    public static final int STARTANDPREFIX = 2;
    public List<byte[]> columns;
    public static final int COLUMNS = 3;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(STARTANDPREFIX, new FieldMetaData("startAndPrefix", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(COLUMNS, new FieldMetaData("columns", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new FieldValueMetaData(TType.STRING))));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerOpenWithPrefix_args.class, metaDataMap);
    }

    public scannerOpenWithPrefix_args() {
    }

    public scannerOpenWithPrefix_args(
      byte[] tableName,
      byte[] startAndPrefix,
      List<byte[]> columns)
    {
      this();
      this.tableName = tableName;
      this.startAndPrefix = startAndPrefix;
      this.columns = columns;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerOpenWithPrefix_args(scannerOpenWithPrefix_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetStartAndPrefix()) {
        this.startAndPrefix = other.startAndPrefix;
      }
      if (other.isSetColumns()) {
        List<byte[]> __this__columns = new ArrayList<byte[]>();
        for (byte[] other_element : other.columns) {
          __this__columns.add(other_element);
        }
        this.columns = __this__columns;
      }
    }

    @Override
    public scannerOpenWithPrefix_args clone() {
      return new scannerOpenWithPrefix_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getStartAndPrefix() {
      return this.startAndPrefix;
    }

    public void setStartAndPrefix(byte[] startAndPrefix) {
      this.startAndPrefix = startAndPrefix;
    }

    public void unsetStartAndPrefix() {
      this.startAndPrefix = null;
    }

    // Returns true if field startAndPrefix is set (has been asigned a value) and false otherwise
    public boolean isSetStartAndPrefix() {
      return this.startAndPrefix != null;
    }

    public void setStartAndPrefixIsSet(boolean value) {
      if (!value) {
        this.startAndPrefix = null;
      }
    }

    public int getColumnsSize() {
      return (this.columns == null) ? 0 : this.columns.size();
    }

    public java.util.Iterator<byte[]> getColumnsIterator() {
      return (this.columns == null) ? null : this.columns.iterator();
    }

    public void addToColumns(byte[] elem) {
      if (this.columns == null) {
        this.columns = new ArrayList<byte[]>();
      }
      this.columns.add(elem);
    }

    public List<byte[]> getColumns() {
      return this.columns;
    }

    public void setColumns(List<byte[]> columns) {
      this.columns = columns;
    }

    public void unsetColumns() {
      this.columns = null;
    }

    // Returns true if field columns is set (has been asigned a value) and false otherwise
    public boolean isSetColumns() {
      return this.columns != null;
    }

    public void setColumnsIsSet(boolean value) {
      if (!value) {
        this.columns = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case STARTANDPREFIX:
        if (value == null) {
          unsetStartAndPrefix();
        } else {
          setStartAndPrefix((byte[])value);
        }
        break;

      case COLUMNS:
        if (value == null) {
          unsetColumns();
        } else {
          setColumns((List<byte[]>)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case STARTANDPREFIX:
        return getStartAndPrefix();

      case COLUMNS:
        return getColumns();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case STARTANDPREFIX:
        return isSetStartAndPrefix();
      case COLUMNS:
        return isSetColumns();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerOpenWithPrefix_args)
        return this.equals((scannerOpenWithPrefix_args)that);
      return false;
    }

    public boolean equals(scannerOpenWithPrefix_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_startAndPrefix = true && this.isSetStartAndPrefix();
      boolean that_present_startAndPrefix = true && that.isSetStartAndPrefix();
      if (this_present_startAndPrefix || that_present_startAndPrefix) {
        if (!(this_present_startAndPrefix && that_present_startAndPrefix))
          return false;
        if (!java.util.Arrays.equals(this.startAndPrefix, that.startAndPrefix))
          return false;
      }

      boolean this_present_columns = true && this.isSetColumns();
      boolean that_present_columns = true && that.isSetColumns();
      if (this_present_columns || that_present_columns) {
        if (!(this_present_columns && that_present_columns))
          return false;
        if (!this.columns.equals(that.columns))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case STARTANDPREFIX:
            if (field.type == TType.STRING) {
              this.startAndPrefix = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case COLUMNS:
            if (field.type == TType.LIST) {
              {
                TList _list86 = iprot.readListBegin();
                this.columns = new ArrayList<byte[]>(_list86.size);
                for (int _i87 = 0; _i87 < _list86.size; ++_i87)
                {
                  byte[] _elem88;
                  _elem88 = iprot.readBinary();
                  this.columns.add(_elem88);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.startAndPrefix != null) {
        oprot.writeFieldBegin(START_AND_PREFIX_FIELD_DESC);
        oprot.writeBinary(this.startAndPrefix);
        oprot.writeFieldEnd();
      }
      if (this.columns != null) {
        oprot.writeFieldBegin(COLUMNS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRING, this.columns.size()));
          for (byte[] _iter89 : this.columns)          {
            oprot.writeBinary(_iter89);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerOpenWithPrefix_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("startAndPrefix:");
      if (this.startAndPrefix == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.startAndPrefix));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("columns:");
      if (this.columns == null) {
        sb.append("null");
      } else {
        sb.append(this.columns);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerOpenWithPrefix_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerOpenWithPrefix_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.I32, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public int success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean success = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I32)));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerOpenWithPrefix_result.class, metaDataMap);
    }

    public scannerOpenWithPrefix_result() {
    }

    public scannerOpenWithPrefix_result(
      int success,
      IOError io)
    {
      this();
      this.success = success;
      this.__isset.success = true;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerOpenWithPrefix_result(scannerOpenWithPrefix_result other) {
      __isset.success = other.__isset.success;
      this.success = other.success;
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public scannerOpenWithPrefix_result clone() {
      return new scannerOpenWithPrefix_result(this);
    }

    public int getSuccess() {
      return this.success;
    }

    public void setSuccess(int success) {
      this.success = success;
      this.__isset.success = true;
    }

    public void unsetSuccess() {
      this.__isset.success = false;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.__isset.success;
    }

    public void setSuccessIsSet(boolean value) {
      this.__isset.success = value;
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((Integer)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerOpenWithPrefix_result)
        return this.equals((scannerOpenWithPrefix_result)that);
      return false;
    }

    public boolean equals(scannerOpenWithPrefix_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true;
      boolean that_present_success = true;
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (this.success != that.success)
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.I32) {
              this.success = iprot.readI32();
              this.__isset.success = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        oprot.writeI32(this.success);
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerOpenWithPrefix_result(");
      boolean first = true;

      sb.append("success:");
      sb.append(this.success);
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerOpenTs_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerOpenTs_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField START_ROW_FIELD_DESC = new TField("startRow", TType.STRING, (short)2);
    private static final TField COLUMNS_FIELD_DESC = new TField("columns", TType.LIST, (short)3);
    private static final TField TIMESTAMP_FIELD_DESC = new TField("timestamp", TType.I64, (short)4);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] startRow;
    public static final int STARTROW = 2;
    public List<byte[]> columns;
    public static final int COLUMNS = 3;
    public long timestamp;
    public static final int TIMESTAMP = 4;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean timestamp = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(STARTROW, new FieldMetaData("startRow", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(COLUMNS, new FieldMetaData("columns", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new FieldValueMetaData(TType.STRING))));
      put(TIMESTAMP, new FieldMetaData("timestamp", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I64)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerOpenTs_args.class, metaDataMap);
    }

    public scannerOpenTs_args() {
    }

    public scannerOpenTs_args(
      byte[] tableName,
      byte[] startRow,
      List<byte[]> columns,
      long timestamp)
    {
      this();
      this.tableName = tableName;
      this.startRow = startRow;
      this.columns = columns;
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerOpenTs_args(scannerOpenTs_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetStartRow()) {
        this.startRow = other.startRow;
      }
      if (other.isSetColumns()) {
        List<byte[]> __this__columns = new ArrayList<byte[]>();
        for (byte[] other_element : other.columns) {
          __this__columns.add(other_element);
        }
        this.columns = __this__columns;
      }
      __isset.timestamp = other.__isset.timestamp;
      this.timestamp = other.timestamp;
    }

    @Override
    public scannerOpenTs_args clone() {
      return new scannerOpenTs_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getStartRow() {
      return this.startRow;
    }

    public void setStartRow(byte[] startRow) {
      this.startRow = startRow;
    }

    public void unsetStartRow() {
      this.startRow = null;
    }

    // Returns true if field startRow is set (has been asigned a value) and false otherwise
    public boolean isSetStartRow() {
      return this.startRow != null;
    }

    public void setStartRowIsSet(boolean value) {
      if (!value) {
        this.startRow = null;
      }
    }

    public int getColumnsSize() {
      return (this.columns == null) ? 0 : this.columns.size();
    }

    public java.util.Iterator<byte[]> getColumnsIterator() {
      return (this.columns == null) ? null : this.columns.iterator();
    }

    public void addToColumns(byte[] elem) {
      if (this.columns == null) {
        this.columns = new ArrayList<byte[]>();
      }
      this.columns.add(elem);
    }

    public List<byte[]> getColumns() {
      return this.columns;
    }

    public void setColumns(List<byte[]> columns) {
      this.columns = columns;
    }

    public void unsetColumns() {
      this.columns = null;
    }

    // Returns true if field columns is set (has been asigned a value) and false otherwise
    public boolean isSetColumns() {
      return this.columns != null;
    }

    public void setColumnsIsSet(boolean value) {
      if (!value) {
        this.columns = null;
      }
    }

    public long getTimestamp() {
      return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    public void unsetTimestamp() {
      this.__isset.timestamp = false;
    }

    // Returns true if field timestamp is set (has been asigned a value) and false otherwise
    public boolean isSetTimestamp() {
      return this.__isset.timestamp;
    }

    public void setTimestampIsSet(boolean value) {
      this.__isset.timestamp = value;
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case STARTROW:
        if (value == null) {
          unsetStartRow();
        } else {
          setStartRow((byte[])value);
        }
        break;

      case COLUMNS:
        if (value == null) {
          unsetColumns();
        } else {
          setColumns((List<byte[]>)value);
        }
        break;

      case TIMESTAMP:
        if (value == null) {
          unsetTimestamp();
        } else {
          setTimestamp((Long)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case STARTROW:
        return getStartRow();

      case COLUMNS:
        return getColumns();

      case TIMESTAMP:
        return getTimestamp();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case STARTROW:
        return isSetStartRow();
      case COLUMNS:
        return isSetColumns();
      case TIMESTAMP:
        return isSetTimestamp();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerOpenTs_args)
        return this.equals((scannerOpenTs_args)that);
      return false;
    }

    public boolean equals(scannerOpenTs_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_startRow = true && this.isSetStartRow();
      boolean that_present_startRow = true && that.isSetStartRow();
      if (this_present_startRow || that_present_startRow) {
        if (!(this_present_startRow && that_present_startRow))
          return false;
        if (!java.util.Arrays.equals(this.startRow, that.startRow))
          return false;
      }

      boolean this_present_columns = true && this.isSetColumns();
      boolean that_present_columns = true && that.isSetColumns();
      if (this_present_columns || that_present_columns) {
        if (!(this_present_columns && that_present_columns))
          return false;
        if (!this.columns.equals(that.columns))
          return false;
      }

      boolean this_present_timestamp = true;
      boolean that_present_timestamp = true;
      if (this_present_timestamp || that_present_timestamp) {
        if (!(this_present_timestamp && that_present_timestamp))
          return false;
        if (this.timestamp != that.timestamp)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case STARTROW:
            if (field.type == TType.STRING) {
              this.startRow = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case COLUMNS:
            if (field.type == TType.LIST) {
              {
                TList _list90 = iprot.readListBegin();
                this.columns = new ArrayList<byte[]>(_list90.size);
                for (int _i91 = 0; _i91 < _list90.size; ++_i91)
                {
                  byte[] _elem92;
                  _elem92 = iprot.readBinary();
                  this.columns.add(_elem92);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case TIMESTAMP:
            if (field.type == TType.I64) {
              this.timestamp = iprot.readI64();
              this.__isset.timestamp = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.startRow != null) {
        oprot.writeFieldBegin(START_ROW_FIELD_DESC);
        oprot.writeBinary(this.startRow);
        oprot.writeFieldEnd();
      }
      if (this.columns != null) {
        oprot.writeFieldBegin(COLUMNS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRING, this.columns.size()));
          for (byte[] _iter93 : this.columns)          {
            oprot.writeBinary(_iter93);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(TIMESTAMP_FIELD_DESC);
      oprot.writeI64(this.timestamp);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerOpenTs_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("startRow:");
      if (this.startRow == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.startRow));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("columns:");
      if (this.columns == null) {
        sb.append("null");
      } else {
        sb.append(this.columns);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("timestamp:");
      sb.append(this.timestamp);
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerOpenTs_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerOpenTs_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.I32, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public int success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean success = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I32)));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerOpenTs_result.class, metaDataMap);
    }

    public scannerOpenTs_result() {
    }

    public scannerOpenTs_result(
      int success,
      IOError io)
    {
      this();
      this.success = success;
      this.__isset.success = true;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerOpenTs_result(scannerOpenTs_result other) {
      __isset.success = other.__isset.success;
      this.success = other.success;
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public scannerOpenTs_result clone() {
      return new scannerOpenTs_result(this);
    }

    public int getSuccess() {
      return this.success;
    }

    public void setSuccess(int success) {
      this.success = success;
      this.__isset.success = true;
    }

    public void unsetSuccess() {
      this.__isset.success = false;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.__isset.success;
    }

    public void setSuccessIsSet(boolean value) {
      this.__isset.success = value;
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((Integer)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerOpenTs_result)
        return this.equals((scannerOpenTs_result)that);
      return false;
    }

    public boolean equals(scannerOpenTs_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true;
      boolean that_present_success = true;
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (this.success != that.success)
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.I32) {
              this.success = iprot.readI32();
              this.__isset.success = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        oprot.writeI32(this.success);
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerOpenTs_result(");
      boolean first = true;

      sb.append("success:");
      sb.append(this.success);
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerOpenWithStopTs_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerOpenWithStopTs_args");
    private static final TField TABLE_NAME_FIELD_DESC = new TField("tableName", TType.STRING, (short)1);
    private static final TField START_ROW_FIELD_DESC = new TField("startRow", TType.STRING, (short)2);
    private static final TField STOP_ROW_FIELD_DESC = new TField("stopRow", TType.STRING, (short)3);
    private static final TField COLUMNS_FIELD_DESC = new TField("columns", TType.LIST, (short)4);
    private static final TField TIMESTAMP_FIELD_DESC = new TField("timestamp", TType.I64, (short)5);

    public byte[] tableName;
    public static final int TABLENAME = 1;
    public byte[] startRow;
    public static final int STARTROW = 2;
    public byte[] stopRow;
    public static final int STOPROW = 3;
    public List<byte[]> columns;
    public static final int COLUMNS = 4;
    public long timestamp;
    public static final int TIMESTAMP = 5;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean timestamp = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(TABLENAME, new FieldMetaData("tableName", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(STARTROW, new FieldMetaData("startRow", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(STOPROW, new FieldMetaData("stopRow", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRING)));
      put(COLUMNS, new FieldMetaData("columns", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new FieldValueMetaData(TType.STRING))));
      put(TIMESTAMP, new FieldMetaData("timestamp", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I64)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerOpenWithStopTs_args.class, metaDataMap);
    }

    public scannerOpenWithStopTs_args() {
    }

    public scannerOpenWithStopTs_args(
      byte[] tableName,
      byte[] startRow,
      byte[] stopRow,
      List<byte[]> columns,
      long timestamp)
    {
      this();
      this.tableName = tableName;
      this.startRow = startRow;
      this.stopRow = stopRow;
      this.columns = columns;
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerOpenWithStopTs_args(scannerOpenWithStopTs_args other) {
      if (other.isSetTableName()) {
        this.tableName = other.tableName;
      }
      if (other.isSetStartRow()) {
        this.startRow = other.startRow;
      }
      if (other.isSetStopRow()) {
        this.stopRow = other.stopRow;
      }
      if (other.isSetColumns()) {
        List<byte[]> __this__columns = new ArrayList<byte[]>();
        for (byte[] other_element : other.columns) {
          __this__columns.add(other_element);
        }
        this.columns = __this__columns;
      }
      __isset.timestamp = other.__isset.timestamp;
      this.timestamp = other.timestamp;
    }

    @Override
    public scannerOpenWithStopTs_args clone() {
      return new scannerOpenWithStopTs_args(this);
    }

    public byte[] getTableName() {
      return this.tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public void unsetTableName() {
      this.tableName = null;
    }

    // Returns true if field tableName is set (has been asigned a value) and false otherwise
    public boolean isSetTableName() {
      return this.tableName != null;
    }

    public void setTableNameIsSet(boolean value) {
      if (!value) {
        this.tableName = null;
      }
    }

    public byte[] getStartRow() {
      return this.startRow;
    }

    public void setStartRow(byte[] startRow) {
      this.startRow = startRow;
    }

    public void unsetStartRow() {
      this.startRow = null;
    }

    // Returns true if field startRow is set (has been asigned a value) and false otherwise
    public boolean isSetStartRow() {
      return this.startRow != null;
    }

    public void setStartRowIsSet(boolean value) {
      if (!value) {
        this.startRow = null;
      }
    }

    public byte[] getStopRow() {
      return this.stopRow;
    }

    public void setStopRow(byte[] stopRow) {
      this.stopRow = stopRow;
    }

    public void unsetStopRow() {
      this.stopRow = null;
    }

    // Returns true if field stopRow is set (has been asigned a value) and false otherwise
    public boolean isSetStopRow() {
      return this.stopRow != null;
    }

    public void setStopRowIsSet(boolean value) {
      if (!value) {
        this.stopRow = null;
      }
    }

    public int getColumnsSize() {
      return (this.columns == null) ? 0 : this.columns.size();
    }

    public java.util.Iterator<byte[]> getColumnsIterator() {
      return (this.columns == null) ? null : this.columns.iterator();
    }

    public void addToColumns(byte[] elem) {
      if (this.columns == null) {
        this.columns = new ArrayList<byte[]>();
      }
      this.columns.add(elem);
    }

    public List<byte[]> getColumns() {
      return this.columns;
    }

    public void setColumns(List<byte[]> columns) {
      this.columns = columns;
    }

    public void unsetColumns() {
      this.columns = null;
    }

    // Returns true if field columns is set (has been asigned a value) and false otherwise
    public boolean isSetColumns() {
      return this.columns != null;
    }

    public void setColumnsIsSet(boolean value) {
      if (!value) {
        this.columns = null;
      }
    }

    public long getTimestamp() {
      return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      this.__isset.timestamp = true;
    }

    public void unsetTimestamp() {
      this.__isset.timestamp = false;
    }

    // Returns true if field timestamp is set (has been asigned a value) and false otherwise
    public boolean isSetTimestamp() {
      return this.__isset.timestamp;
    }

    public void setTimestampIsSet(boolean value) {
      this.__isset.timestamp = value;
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case TABLENAME:
        if (value == null) {
          unsetTableName();
        } else {
          setTableName((byte[])value);
        }
        break;

      case STARTROW:
        if (value == null) {
          unsetStartRow();
        } else {
          setStartRow((byte[])value);
        }
        break;

      case STOPROW:
        if (value == null) {
          unsetStopRow();
        } else {
          setStopRow((byte[])value);
        }
        break;

      case COLUMNS:
        if (value == null) {
          unsetColumns();
        } else {
          setColumns((List<byte[]>)value);
        }
        break;

      case TIMESTAMP:
        if (value == null) {
          unsetTimestamp();
        } else {
          setTimestamp((Long)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return getTableName();

      case STARTROW:
        return getStartRow();

      case STOPROW:
        return getStopRow();

      case COLUMNS:
        return getColumns();

      case TIMESTAMP:
        return getTimestamp();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case TABLENAME:
        return isSetTableName();
      case STARTROW:
        return isSetStartRow();
      case STOPROW:
        return isSetStopRow();
      case COLUMNS:
        return isSetColumns();
      case TIMESTAMP:
        return isSetTimestamp();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerOpenWithStopTs_args)
        return this.equals((scannerOpenWithStopTs_args)that);
      return false;
    }

    public boolean equals(scannerOpenWithStopTs_args that) {
      if (that == null)
        return false;

      boolean this_present_tableName = true && this.isSetTableName();
      boolean that_present_tableName = true && that.isSetTableName();
      if (this_present_tableName || that_present_tableName) {
        if (!(this_present_tableName && that_present_tableName))
          return false;
        if (!java.util.Arrays.equals(this.tableName, that.tableName))
          return false;
      }

      boolean this_present_startRow = true && this.isSetStartRow();
      boolean that_present_startRow = true && that.isSetStartRow();
      if (this_present_startRow || that_present_startRow) {
        if (!(this_present_startRow && that_present_startRow))
          return false;
        if (!java.util.Arrays.equals(this.startRow, that.startRow))
          return false;
      }

      boolean this_present_stopRow = true && this.isSetStopRow();
      boolean that_present_stopRow = true && that.isSetStopRow();
      if (this_present_stopRow || that_present_stopRow) {
        if (!(this_present_stopRow && that_present_stopRow))
          return false;
        if (!java.util.Arrays.equals(this.stopRow, that.stopRow))
          return false;
      }

      boolean this_present_columns = true && this.isSetColumns();
      boolean that_present_columns = true && that.isSetColumns();
      if (this_present_columns || that_present_columns) {
        if (!(this_present_columns && that_present_columns))
          return false;
        if (!this.columns.equals(that.columns))
          return false;
      }

      boolean this_present_timestamp = true;
      boolean that_present_timestamp = true;
      if (this_present_timestamp || that_present_timestamp) {
        if (!(this_present_timestamp && that_present_timestamp))
          return false;
        if (this.timestamp != that.timestamp)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case TABLENAME:
            if (field.type == TType.STRING) {
              this.tableName = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case STARTROW:
            if (field.type == TType.STRING) {
              this.startRow = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case STOPROW:
            if (field.type == TType.STRING) {
              this.stopRow = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case COLUMNS:
            if (field.type == TType.LIST) {
              {
                TList _list94 = iprot.readListBegin();
                this.columns = new ArrayList<byte[]>(_list94.size);
                for (int _i95 = 0; _i95 < _list94.size; ++_i95)
                {
                  byte[] _elem96;
                  _elem96 = iprot.readBinary();
                  this.columns.add(_elem96);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case TIMESTAMP:
            if (field.type == TType.I64) {
              this.timestamp = iprot.readI64();
              this.__isset.timestamp = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (this.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeBinary(this.tableName);
        oprot.writeFieldEnd();
      }
      if (this.startRow != null) {
        oprot.writeFieldBegin(START_ROW_FIELD_DESC);
        oprot.writeBinary(this.startRow);
        oprot.writeFieldEnd();
      }
      if (this.stopRow != null) {
        oprot.writeFieldBegin(STOP_ROW_FIELD_DESC);
        oprot.writeBinary(this.stopRow);
        oprot.writeFieldEnd();
      }
      if (this.columns != null) {
        oprot.writeFieldBegin(COLUMNS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRING, this.columns.size()));
          for (byte[] _iter97 : this.columns)          {
            oprot.writeBinary(_iter97);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(TIMESTAMP_FIELD_DESC);
      oprot.writeI64(this.timestamp);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerOpenWithStopTs_args(");
      boolean first = true;

      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toString(this.tableName));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("startRow:");
      if (this.startRow == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.startRow));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("stopRow:");
      if (this.stopRow == null) {
        sb.append("null");
      } else {
        sb.append(Bytes.toStringBinary(this.stopRow));
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("columns:");
      if (this.columns == null) {
        sb.append("null");
      } else {
        sb.append(this.columns);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("timestamp:");
      sb.append(this.timestamp);
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerOpenWithStopTs_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerOpenWithStopTs_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.I32, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);

    public int success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean success = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I32)));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerOpenWithStopTs_result.class, metaDataMap);
    }

    public scannerOpenWithStopTs_result() {
    }

    public scannerOpenWithStopTs_result(
      int success,
      IOError io)
    {
      this();
      this.success = success;
      this.__isset.success = true;
      this.io = io;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerOpenWithStopTs_result(scannerOpenWithStopTs_result other) {
      __isset.success = other.__isset.success;
      this.success = other.success;
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
    }

    @Override
    public scannerOpenWithStopTs_result clone() {
      return new scannerOpenWithStopTs_result(this);
    }

    public int getSuccess() {
      return this.success;
    }

    public void setSuccess(int success) {
      this.success = success;
      this.__isset.success = true;
    }

    public void unsetSuccess() {
      this.__isset.success = false;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.__isset.success;
    }

    public void setSuccessIsSet(boolean value) {
      this.__isset.success = value;
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((Integer)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerOpenWithStopTs_result)
        return this.equals((scannerOpenWithStopTs_result)that);
      return false;
    }

    public boolean equals(scannerOpenWithStopTs_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true;
      boolean that_present_success = true;
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (this.success != that.success)
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.I32) {
              this.success = iprot.readI32();
              this.__isset.success = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        oprot.writeI32(this.success);
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerOpenWithStopTs_result(");
      boolean first = true;

      sb.append("success:");
      sb.append(this.success);
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerGet_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerGet_args");
    private static final TField ID_FIELD_DESC = new TField("id", TType.I32, (short)1);

    public int id;
    public static final int ID = 1;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean id = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(ID, new FieldMetaData("id", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I32)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerGet_args.class, metaDataMap);
    }

    public scannerGet_args() {
    }

    public scannerGet_args(
      int id)
    {
      this();
      this.id = id;
      this.__isset.id = true;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerGet_args(scannerGet_args other) {
      __isset.id = other.__isset.id;
      this.id = other.id;
    }

    @Override
    public scannerGet_args clone() {
      return new scannerGet_args(this);
    }

    public int getId() {
      return this.id;
    }

    public void setId(int id) {
      this.id = id;
      this.__isset.id = true;
    }

    public void unsetId() {
      this.__isset.id = false;
    }

    // Returns true if field id is set (has been asigned a value) and false otherwise
    public boolean isSetId() {
      return this.__isset.id;
    }

    public void setIdIsSet(boolean value) {
      this.__isset.id = value;
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case ID:
        if (value == null) {
          unsetId();
        } else {
          setId((Integer)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case ID:
        return getId();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case ID:
        return isSetId();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerGet_args)
        return this.equals((scannerGet_args)that);
      return false;
    }

    public boolean equals(scannerGet_args that) {
      if (that == null)
        return false;

      boolean this_present_id = true;
      boolean that_present_id = true;
      if (this_present_id || that_present_id) {
        if (!(this_present_id && that_present_id))
          return false;
        if (this.id != that.id)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case ID:
            if (field.type == TType.I32) {
              this.id = iprot.readI32();
              this.__isset.id = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(ID_FIELD_DESC);
      oprot.writeI32(this.id);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerGet_args(");
      sb.append("id:");
      sb.append(this.id);
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerGet_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerGet_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.LIST, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);
    private static final TField IA_FIELD_DESC = new TField("ia", TType.STRUCT, (short)2);

    public List<TRowResult> success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;
    public IllegalArgument ia;
    public static final int IA = 2;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new StructMetaData(TType.STRUCT, TRowResult.class))));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
      put(IA, new FieldMetaData("ia", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerGet_result.class, metaDataMap);
    }

    public scannerGet_result() {
    }

    public scannerGet_result(
      List<TRowResult> success,
      IOError io,
      IllegalArgument ia)
    {
      this();
      this.success = success;
      this.io = io;
      this.ia = ia;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerGet_result(scannerGet_result other) {
      if (other.isSetSuccess()) {
        List<TRowResult> __this__success = new ArrayList<TRowResult>();
        for (TRowResult other_element : other.success) {
          __this__success.add(new TRowResult(other_element));
        }
        this.success = __this__success;
      }
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
      if (other.isSetIa()) {
        this.ia = new IllegalArgument(other.ia);
      }
    }

    @Override
    public scannerGet_result clone() {
      return new scannerGet_result(this);
    }

    public int getSuccessSize() {
      return (this.success == null) ? 0 : this.success.size();
    }

    public java.util.Iterator<TRowResult> getSuccessIterator() {
      return (this.success == null) ? null : this.success.iterator();
    }

    public void addToSuccess(TRowResult elem) {
      if (this.success == null) {
        this.success = new ArrayList<TRowResult>();
      }
      this.success.add(elem);
    }

    public List<TRowResult> getSuccess() {
      return this.success;
    }

    public void setSuccess(List<TRowResult> success) {
      this.success = success;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public IllegalArgument getIa() {
      return this.ia;
    }

    public void setIa(IllegalArgument ia) {
      this.ia = ia;
    }

    public void unsetIa() {
      this.ia = null;
    }

    // Returns true if field ia is set (has been asigned a value) and false otherwise
    public boolean isSetIa() {
      return this.ia != null;
    }

    public void setIaIsSet(boolean value) {
      if (!value) {
        this.ia = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((List<TRowResult>)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      case IA:
        if (value == null) {
          unsetIa();
        } else {
          setIa((IllegalArgument)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      case IA:
        return getIa();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      case IA:
        return isSetIa();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerGet_result)
        return this.equals((scannerGet_result)that);
      return false;
    }

    public boolean equals(scannerGet_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      boolean this_present_ia = true && this.isSetIa();
      boolean that_present_ia = true && that.isSetIa();
      if (this_present_ia || that_present_ia) {
        if (!(this_present_ia && that_present_ia))
          return false;
        if (!this.ia.equals(that.ia))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.LIST) {
              {
                TList _list98 = iprot.readListBegin();
                this.success = new ArrayList<TRowResult>(_list98.size);
                for (int _i99 = 0; _i99 < _list98.size; ++_i99)
                {
                  TRowResult _elem100;
                  _elem100 = new TRowResult();
                  _elem100.read(iprot);
                  this.success.add(_elem100);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IA:
            if (field.type == TType.STRUCT) {
              this.ia = new IllegalArgument();
              this.ia.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.success.size()));
          for (TRowResult _iter101 : this.success)          {
            _iter101.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      } else if (this.isSetIa()) {
        oprot.writeFieldBegin(IA_FIELD_DESC);
        this.ia.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerGet_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("ia:");
      if (this.ia == null) {
        sb.append("null");
      } else {
        sb.append(this.ia);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerGetList_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerGetList_args");
    private static final TField ID_FIELD_DESC = new TField("id", TType.I32, (short)1);
    private static final TField NB_ROWS_FIELD_DESC = new TField("nbRows", TType.I32, (short)2);

    public int id;
    public static final int ID = 1;
    public int nbRows;
    public static final int NBROWS = 2;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean id = false;
      public boolean nbRows = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(ID, new FieldMetaData("id", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I32)));
      put(NBROWS, new FieldMetaData("nbRows", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I32)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerGetList_args.class, metaDataMap);
    }

    public scannerGetList_args() {
    }

    public scannerGetList_args(
      int id,
      int nbRows)
    {
      this();
      this.id = id;
      this.__isset.id = true;
      this.nbRows = nbRows;
      this.__isset.nbRows = true;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerGetList_args(scannerGetList_args other) {
      __isset.id = other.__isset.id;
      this.id = other.id;
      __isset.nbRows = other.__isset.nbRows;
      this.nbRows = other.nbRows;
    }

    @Override
    public scannerGetList_args clone() {
      return new scannerGetList_args(this);
    }

    public int getId() {
      return this.id;
    }

    public void setId(int id) {
      this.id = id;
      this.__isset.id = true;
    }

    public void unsetId() {
      this.__isset.id = false;
    }

    // Returns true if field id is set (has been asigned a value) and false otherwise
    public boolean isSetId() {
      return this.__isset.id;
    }

    public void setIdIsSet(boolean value) {
      this.__isset.id = value;
    }

    public int getNbRows() {
      return this.nbRows;
    }

    public void setNbRows(int nbRows) {
      this.nbRows = nbRows;
      this.__isset.nbRows = true;
    }

    public void unsetNbRows() {
      this.__isset.nbRows = false;
    }

    // Returns true if field nbRows is set (has been asigned a value) and false otherwise
    public boolean isSetNbRows() {
      return this.__isset.nbRows;
    }

    public void setNbRowsIsSet(boolean value) {
      this.__isset.nbRows = value;
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case ID:
        if (value == null) {
          unsetId();
        } else {
          setId((Integer)value);
        }
        break;

      case NBROWS:
        if (value == null) {
          unsetNbRows();
        } else {
          setNbRows((Integer)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case ID:
        return getId();

      case NBROWS:
        return getNbRows();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case ID:
        return isSetId();
      case NBROWS:
        return isSetNbRows();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerGetList_args)
        return this.equals((scannerGetList_args)that);
      return false;
    }

    public boolean equals(scannerGetList_args that) {
      if (that == null)
        return false;

      boolean this_present_id = true;
      boolean that_present_id = true;
      if (this_present_id || that_present_id) {
        if (!(this_present_id && that_present_id))
          return false;
        if (this.id != that.id)
          return false;
      }

      boolean this_present_nbRows = true;
      boolean that_present_nbRows = true;
      if (this_present_nbRows || that_present_nbRows) {
        if (!(this_present_nbRows && that_present_nbRows))
          return false;
        if (this.nbRows != that.nbRows)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case ID:
            if (field.type == TType.I32) {
              this.id = iprot.readI32();
              this.__isset.id = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case NBROWS:
            if (field.type == TType.I32) {
              this.nbRows = iprot.readI32();
              this.__isset.nbRows = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(ID_FIELD_DESC);
      oprot.writeI32(this.id);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(NB_ROWS_FIELD_DESC);
      oprot.writeI32(this.nbRows);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerGetList_args(");
      boolean first = true;

      sb.append("id:");
      sb.append(this.id);
      first = false;
      if (!first) sb.append(", ");
      sb.append("nbRows:");
      sb.append(this.nbRows);
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerGetList_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerGetList_result");
    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.LIST, (short)0);
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);
    private static final TField IA_FIELD_DESC = new TField("ia", TType.STRUCT, (short)2);

    public List<TRowResult> success;
    public static final int SUCCESS = 0;
    public IOError io;
    public static final int IO = 1;
    public IllegalArgument ia;
    public static final int IA = 2;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new ListMetaData(TType.LIST, 
              new StructMetaData(TType.STRUCT, TRowResult.class))));
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
      put(IA, new FieldMetaData("ia", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerGetList_result.class, metaDataMap);
    }

    public scannerGetList_result() {
    }

    public scannerGetList_result(
      List<TRowResult> success,
      IOError io,
      IllegalArgument ia)
    {
      this();
      this.success = success;
      this.io = io;
      this.ia = ia;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerGetList_result(scannerGetList_result other) {
      if (other.isSetSuccess()) {
        List<TRowResult> __this__success = new ArrayList<TRowResult>();
        for (TRowResult other_element : other.success) {
          __this__success.add(new TRowResult(other_element));
        }
        this.success = __this__success;
      }
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
      if (other.isSetIa()) {
        this.ia = new IllegalArgument(other.ia);
      }
    }

    @Override
    public scannerGetList_result clone() {
      return new scannerGetList_result(this);
    }

    public int getSuccessSize() {
      return (this.success == null) ? 0 : this.success.size();
    }

    public java.util.Iterator<TRowResult> getSuccessIterator() {
      return (this.success == null) ? null : this.success.iterator();
    }

    public void addToSuccess(TRowResult elem) {
      if (this.success == null) {
        this.success = new ArrayList<TRowResult>();
      }
      this.success.add(elem);
    }

    public List<TRowResult> getSuccess() {
      return this.success;
    }

    public void setSuccess(List<TRowResult> success) {
      this.success = success;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    // Returns true if field success is set (has been asigned a value) and false otherwise
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public IllegalArgument getIa() {
      return this.ia;
    }

    public void setIa(IllegalArgument ia) {
      this.ia = ia;
    }

    public void unsetIa() {
      this.ia = null;
    }

    // Returns true if field ia is set (has been asigned a value) and false otherwise
    public boolean isSetIa() {
      return this.ia != null;
    }

    public void setIaIsSet(boolean value) {
      if (!value) {
        this.ia = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((List<TRowResult>)value);
        }
        break;

      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      case IA:
        if (value == null) {
          unsetIa();
        } else {
          setIa((IllegalArgument)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return getSuccess();

      case IO:
        return getIo();

      case IA:
        return getIa();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case SUCCESS:
        return isSetSuccess();
      case IO:
        return isSetIo();
      case IA:
        return isSetIa();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerGetList_result)
        return this.equals((scannerGetList_result)that);
      return false;
    }

    public boolean equals(scannerGetList_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      boolean this_present_ia = true && this.isSetIa();
      boolean that_present_ia = true && that.isSetIa();
      if (this_present_ia || that_present_ia) {
        if (!(this_present_ia && that_present_ia))
          return false;
        if (!this.ia.equals(that.ia))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case SUCCESS:
            if (field.type == TType.LIST) {
              {
                TList _list102 = iprot.readListBegin();
                this.success = new ArrayList<TRowResult>(_list102.size);
                for (int _i103 = 0; _i103 < _list102.size; ++_i103)
                {
                  TRowResult _elem104;
                  _elem104 = new TRowResult();
                  _elem104.read(iprot);
                  this.success.add(_elem104);
                }
                iprot.readListEnd();
              }
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IA:
            if (field.type == TType.STRUCT) {
              this.ia = new IllegalArgument();
              this.ia.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRUCT, this.success.size()));
          for (TRowResult _iter105 : this.success)          {
            _iter105.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      } else if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      } else if (this.isSetIa()) {
        oprot.writeFieldBegin(IA_FIELD_DESC);
        this.ia.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerGetList_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("ia:");
      if (this.ia == null) {
        sb.append("null");
      } else {
        sb.append(this.ia);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerClose_args implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerClose_args");
    private static final TField ID_FIELD_DESC = new TField("id", TType.I32, (short)1);

    public int id;
    public static final int ID = 1;

    private final Isset __isset = new Isset();
    private static final class Isset implements java.io.Serializable {
      private static final long serialVersionUID = 1L;
      public boolean id = false;
    }

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(ID, new FieldMetaData("id", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I32)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerClose_args.class, metaDataMap);
    }

    public scannerClose_args() {
    }

    public scannerClose_args(
      int id)
    {
      this();
      this.id = id;
      this.__isset.id = true;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerClose_args(scannerClose_args other) {
      __isset.id = other.__isset.id;
      this.id = other.id;
    }

    @Override
    public scannerClose_args clone() {
      return new scannerClose_args(this);
    }

    public int getId() {
      return this.id;
    }

    public void setId(int id) {
      this.id = id;
      this.__isset.id = true;
    }

    public void unsetId() {
      this.__isset.id = false;
    }

    // Returns true if field id is set (has been asigned a value) and false otherwise
    public boolean isSetId() {
      return this.__isset.id;
    }

    public void setIdIsSet(boolean value) {
      this.__isset.id = value;
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case ID:
        if (value == null) {
          unsetId();
        } else {
          setId((Integer)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case ID:
        return getId();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case ID:
        return isSetId();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerClose_args)
        return this.equals((scannerClose_args)that);
      return false;
    }

    public boolean equals(scannerClose_args that) {
      if (that == null)
        return false;

      boolean this_present_id = true;
      boolean that_present_id = true;
      if (this_present_id || that_present_id) {
        if (!(this_present_id && that_present_id))
          return false;
        if (this.id != that.id)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case ID:
            if (field.type == TType.I32) {
              this.id = iprot.readI32();
              this.__isset.id = true;
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(ID_FIELD_DESC);
      oprot.writeI32(this.id);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerClose_args(");
      sb.append("id:");
      sb.append(this.id);
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

  public static class scannerClose_result implements TBase, java.io.Serializable, Cloneable   {
    private static final long serialVersionUID = 1L;
    private static final TStruct STRUCT_DESC = new TStruct("scannerClose_result");
    private static final TField IO_FIELD_DESC = new TField("io", TType.STRUCT, (short)1);
    private static final TField IA_FIELD_DESC = new TField("ia", TType.STRUCT, (short)2);

    public IOError io;
    public static final int IO = 1;
    public IllegalArgument ia;
    public static final int IA = 2;

    public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
      put(IO, new FieldMetaData("io", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
      put(IA, new FieldMetaData("ia", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.STRUCT)));
    }});

    static {
      FieldMetaData.addStructMetaDataMap(scannerClose_result.class, metaDataMap);
    }

    public scannerClose_result() {
    }

    public scannerClose_result(
      IOError io,
      IllegalArgument ia)
    {
      this();
      this.io = io;
      this.ia = ia;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public scannerClose_result(scannerClose_result other) {
      if (other.isSetIo()) {
        this.io = new IOError(other.io);
      }
      if (other.isSetIa()) {
        this.ia = new IllegalArgument(other.ia);
      }
    }

    @Override
    public scannerClose_result clone() {
      return new scannerClose_result(this);
    }

    public IOError getIo() {
      return this.io;
    }

    public void setIo(IOError io) {
      this.io = io;
    }

    public void unsetIo() {
      this.io = null;
    }

    // Returns true if field io is set (has been asigned a value) and false otherwise
    public boolean isSetIo() {
      return this.io != null;
    }

    public void setIoIsSet(boolean value) {
      if (!value) {
        this.io = null;
      }
    }

    public IllegalArgument getIa() {
      return this.ia;
    }

    public void setIa(IllegalArgument ia) {
      this.ia = ia;
    }

    public void unsetIa() {
      this.ia = null;
    }

    // Returns true if field ia is set (has been asigned a value) and false otherwise
    public boolean isSetIa() {
      return this.ia != null;
    }

    public void setIaIsSet(boolean value) {
      if (!value) {
        this.ia = null;
      }
    }

    public void setFieldValue(int fieldID, Object value) {
      switch (fieldID) {
      case IO:
        if (value == null) {
          unsetIo();
        } else {
          setIo((IOError)value);
        }
        break;

      case IA:
        if (value == null) {
          unsetIa();
        } else {
          setIa((IllegalArgument)value);
        }
        break;

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    public Object getFieldValue(int fieldID) {
      switch (fieldID) {
      case IO:
        return getIo();

      case IA:
        return getIa();

      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
    public boolean isSet(int fieldID) {
      switch (fieldID) {
      case IO:
        return isSetIo();
      case IA:
        return isSetIa();
      default:
        throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
      }
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof scannerClose_result)
        return this.equals((scannerClose_result)that);
      return false;
    }

    public boolean equals(scannerClose_result that) {
      if (that == null)
        return false;

      boolean this_present_io = true && this.isSetIo();
      boolean that_present_io = true && that.isSetIo();
      if (this_present_io || that_present_io) {
        if (!(this_present_io && that_present_io))
          return false;
        if (!this.io.equals(that.io))
          return false;
      }

      boolean this_present_ia = true && this.isSetIa();
      boolean that_present_ia = true && that.isSetIa();
      if (this_present_ia || that_present_ia) {
        if (!(this_present_ia && that_present_ia))
          return false;
        if (!this.ia.equals(that.ia))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id)
        {
          case IO:
            if (field.type == TType.STRUCT) {
              this.io = new IOError();
              this.io.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case IA:
            if (field.type == TType.STRUCT) {
              this.ia = new IllegalArgument();
              this.ia.read(iprot);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();


      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetIo()) {
        oprot.writeFieldBegin(IO_FIELD_DESC);
        this.io.write(oprot);
        oprot.writeFieldEnd();
      } else if (this.isSetIa()) {
        oprot.writeFieldBegin(IA_FIELD_DESC);
        this.ia.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("scannerClose_result(");
      boolean first = true;

      sb.append("io:");
      if (this.io == null) {
        sb.append("null");
      } else {
        sb.append(this.io);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("ia:");
      if (this.ia == null) {
        sb.append("null");
      } else {
        sb.append(this.ia);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
      // check that fields of type enum have valid values
    }

  }

}
