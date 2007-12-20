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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Used to communicate with a single HBase table
 */
public class HTable implements HConstants {
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());

  protected final HConnection connection;
  protected final Text tableName;
  protected final long pause;
  protected final int numRetries;
  protected Random rand;
  protected volatile SortedMap<Text, HRegionLocation> tableServers;
  protected AtomicReference<BatchUpdate> batch;

  protected volatile boolean tableDoesNotExist;
  
  // For row mutation operations
  
  protected volatile boolean closed;

  protected void checkClosed() {
    if (tableDoesNotExist) {
      throw new IllegalStateException("table does not exist: " + tableName);
    }
    if (closed) {
      throw new IllegalStateException("table is closed");
    }
  }
  
  /**
   * Creates an object to access a HBase table
   * 
   * @param conf configuration object
   * @param tableName name of the table
   * @throws IOException
   */
  public HTable(HBaseConfiguration conf, Text tableName) throws IOException {
    closed = true;
    tableDoesNotExist = true;
    this.connection = HConnectionManager.getConnection(conf);
    this.tableName = tableName;
    this.pause = conf.getLong("hbase.client.pause", 10 * 1000);
    this.numRetries = conf.getInt("hbase.client.retries.number", 5);
    this.rand = new Random();
    this.batch = new AtomicReference<BatchUpdate>();
    tableServers = connection.getTableServers(tableName);
    tableDoesNotExist = false;
    closed = false;
  }

  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return Location of row.
   */
  HRegionLocation getRegionLocation(Text row) {
    checkClosed();
    if (this.tableServers == null) {
      throw new IllegalStateException("Must open table first");
    }
    
    // Only one server will have the row we are looking for
    Text serverKey = (this.tableServers.containsKey(row)) ?
        row : this.tableServers.headMap(row).lastKey();
    return this.tableServers.get(serverKey);
  }

  /** @return the connection */
  public HConnection getConnection() {
    checkClosed();
    return connection;
  }

  /**
   * Releases resources associated with this table. After calling close(), all
   * other methods will throw an IllegalStateException
   */
  public synchronized void close() {
    if (!closed) {
      closed = true;
      tableServers = null;
      batch.set(null);
      connection.close(tableName);
    }
  }
  
  /**
   * Verifies that no update is in progress
   */
  public synchronized void checkUpdateInProgress() {
    updateInProgress(false);
  }
  
  /*
   * Checks to see if an update is in progress
   * 
   * @param updateMustBeInProgress
   *    If true, an update must be in progress. An IllegalStateException will be
   *    thrown if not.
   *    
   *    If false, an update must not be in progress. An IllegalStateException
   *    will be thrown if an update is in progress.
   */
  private void updateInProgress(boolean updateMustBeInProgress) {
    if (updateMustBeInProgress) {
      if (batch.get() == null) {
        throw new IllegalStateException("no update in progress");
      }
    } else {
      if (batch.get() != null) {
        throw new IllegalStateException("update in progress");
      }
    }
  }
  

  /** @return the table name */
  public Text getTableName() {
    return this.tableName;
  }

  /**
   * @return table metadata 
   * @throws IOException
   */
  public HTableDescriptor getMetadata() throws IOException {
    HTableDescriptor [] metas = this.connection.listTables();
    HTableDescriptor result = null;
    for (int i = 0; i < metas.length; i++) {
      if (metas[i].getName().equals(this.tableName)) {
        result = metas[i];
        break;
      }
    }
    return result;
  }

  /**
   * Gets the starting row key for every region in the currently open table
   * @return Array of region starting row keys
   */
  public Text[] getStartKeys() {
    checkClosed();
    Text[] keys = new Text[tableServers.size()];
    int i = 0;
    for(Text key: tableServers.keySet()){
      keys[i++] = key;
    }
    return keys;
  }
  
  /** 
   * Get a single value for the specified row and column
   *
   * @param row row key
   * @param column column name
   * @return value for specified row/column
   * @throws IOException
   */
  public byte[] get(Text row, Text column) throws IOException {
    checkClosed();
    byte [] value = null;
    for(int tries = 0; tries < numRetries; tries++) {
      HRegionLocation r = getRegionLocation(row);
      HRegionInterface server =
        connection.getHRegionConnection(r.getServerAddress());
      
      try {
        value = server.get(r.getRegionInfo().getRegionName(), row, column);
        break;
        
      } catch (IOException e) {
        if (e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        if (tries == numRetries - 1) {
          throw e;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("reloading table servers because: " + e.getMessage());
        }
        tableServers = connection.reloadTableServers(tableName);
      }
      try {
        Thread.sleep(this.pause);
        
      } catch (InterruptedException x) {
        // continue
      }
    }
    return value;
  }
 
  /** 
   * Get the specified number of versions of the specified row and column
   * 
   * @param row         - row key
   * @param column      - column name
   * @param numVersions - number of versions to retrieve
   * @return            - array byte values
   * @throws IOException
   */
  public byte[][] get(Text row, Text column, int numVersions) throws IOException {
    checkClosed();
    byte [][] values = null;
    for (int tries = 0; tries < numRetries; tries++) {
      HRegionLocation r = getRegionLocation(row);
      HRegionInterface server = 
        connection.getHRegionConnection(r.getServerAddress());
      
      try {
        values = server.get(r.getRegionInfo().getRegionName(), row, column,
            numVersions);
        
        break;
        
      } catch (IOException e) {
        if (e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        if (tries == numRetries - 1) {
          // No more tries
          throw e;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("reloading table servers because: " + e.getMessage());
        }
        tableServers = connection.reloadTableServers(tableName);
      }
      try {
        Thread.sleep(this.pause);
        
      } catch (InterruptedException x) {
        // continue
      }
    }

    if (values != null) {
      ArrayList<byte[]> bytes = new ArrayList<byte[]>();
      for (int i = 0 ; i < values.length; i++) {
        bytes.add(values[i]);
      }
      return bytes.toArray(new byte[values.length][]);
    }
    return null;
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
  public byte[][] get(Text row, Text column, long timestamp, int numVersions)
  throws IOException {
    checkClosed();
    byte [][] values = null;
    for (int tries = 0; tries < numRetries; tries++) {
      HRegionLocation r = getRegionLocation(row);
      HRegionInterface server =
        connection.getHRegionConnection(r.getServerAddress());
      
      try {
        values = server.get(r.getRegionInfo().getRegionName(), row, column,
            timestamp, numVersions);
        
        break;
    
      } catch (IOException e) {
        if (e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        if (tries == numRetries - 1) {
          // No more tries
          throw e;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("reloading table servers because: " + e.getMessage());
        }
        tableServers = connection.reloadTableServers(tableName);
      }
      try {
        Thread.sleep(this.pause);
        
      } catch (InterruptedException x) {
        // continue
      }
    }

    if (values != null) {
      ArrayList<byte[]> bytes = new ArrayList<byte[]>();
      for (int i = 0 ; i < values.length; i++) {
        bytes.add(values[i]);
      }
      return bytes.toArray(new byte[values.length][]);
    }
    return null;
  }
    
  /** 
   * Get all the data for the specified row at the latest timestamp
   * 
   * @param row row key
   * @return Map of columns to values.  Map is empty if row does not exist.
   * @throws IOException
   */
  public SortedMap<Text, byte[]> getRow(Text row) throws IOException {
    return getRow(row, HConstants.LATEST_TIMESTAMP);
  }

  /** 
   * Get all the data for the specified row at a specified timestamp
   * 
   * @param row row key
   * @param ts timestamp
   * @return Map of columns to values.  Map is empty if row does not exist.
   * @throws IOException
   */
  public SortedMap<Text, byte[]> getRow(Text row, long ts) throws IOException {
    checkClosed();
    MapWritable value = null;
    for (int tries = 0; tries < numRetries; tries++) {
      HRegionLocation r = getRegionLocation(row);
      HRegionInterface server =
        connection.getHRegionConnection(r.getServerAddress());
      
      try {
        value = server.getRow(r.getRegionInfo().getRegionName(), row, ts);
        break;
        
      } catch (IOException e) {
        if (e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        if (tries == numRetries - 1) {
          // No more tries
          throw e;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("reloading table servers because: " + e.getMessage());
        }
        tableServers = connection.reloadTableServers(tableName);
      }
      try {
        Thread.sleep(this.pause);
        
      } catch (InterruptedException x) {
        // continue
      }
    }
    SortedMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
    if (value != null && value.size() != 0) {
      for (Map.Entry<Writable, Writable> e: value.entrySet()) {
        HStoreKey key = (HStoreKey) e.getKey();
        results.put(key.getColumn(),
            ((ImmutableBytesWritable) e.getValue()).get());
      }
    }
    return results;
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
  public HScannerInterface obtainScanner(Text[] columns, Text startRow)
  throws IOException {
    return obtainScanner(columns, startRow, HConstants.LATEST_TIMESTAMP, null);
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
  public HScannerInterface obtainScanner(Text[] columns, Text startRow,
      long timestamp)
  throws IOException {
    return obtainScanner(columns, startRow, timestamp, null);
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
  public HScannerInterface obtainScanner(Text[] columns, Text startRow,
      RowFilterInterface filter)
  throws IOException { 
    return obtainScanner(columns, startRow, HConstants.LATEST_TIMESTAMP, filter);
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
  public HScannerInterface obtainScanner(final Text[] columns,
      final Text startRow, final Text stopRow)
  throws IOException {
    return obtainScanner(columns, startRow, stopRow,
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
  public HScannerInterface obtainScanner(final Text[] columns,
      final Text startRow, final Text stopRow, final long timestamp)
  throws IOException {
    return obtainScanner(columns, startRow, timestamp,
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
  public HScannerInterface obtainScanner(Text[] columns,
      Text startRow, long timestamp, RowFilterInterface filter)
  throws IOException {
    checkClosed();
    return new ClientScanner(columns, startRow, timestamp, filter);
  }

  /** 
   * Start a batch of row insertions/updates.
   * 
   * No changes are committed until the call to commitBatchUpdate returns.
   * A call to abortBatchUpdate will abandon the entire batch.
   *
   * @param row name of row to be updated
   * @return lockid to be used in subsequent put, delete and commit calls
   * 
   * @deprecated Batch operations are now the default. startBatchUpdate is now
   * implemented by @see {@link #startUpdate(Text)} 
   */
  @Deprecated
  public synchronized long startBatchUpdate(final Text row) {
    return startUpdate(row);
  }
  
  /** 
   * Abort a batch mutation
   * @param lockid lock id returned by startBatchUpdate
   * 
   * @deprecated Batch operations are now the default. abortBatch is now 
   * implemented by @see {@link #abort(long)}
   */
  @Deprecated
  public synchronized void abortBatch(final long lockid) {
    abort(lockid);
  }
  
  /** 
   * Finalize a batch mutation
   *
   * @param lockid lock id returned by startBatchUpdate
   * @throws IOException
   * 
   * @deprecated Batch operations are now the default. commitBatch(long) is now
   * implemented by @see {@link #commit(long)}
   */
  @Deprecated
  public void commitBatch(final long lockid) throws IOException {
    commit(lockid, System.currentTimeMillis());
  }

  /** 
   * Finalize a batch mutation
   *
   * @param lockid lock id returned by startBatchUpdate
   * @param timestamp time to associate with all the changes
   * @throws IOException
   * 
   * @deprecated Batch operations are now the default. commitBatch(long, long)
   * is now implemented by @see {@link #commit(long, long)}
   */
  @Deprecated
  public synchronized void commitBatch(final long lockid, final long timestamp)
  throws IOException {

    commit(lockid, timestamp);
  }
  
  /** 
   * Start an atomic row insertion/update.  No changes are committed until the 
   * call to commit() returns. A call to abort() will abandon any updates in
   * progress.
   * 
   * <p>
   * Example:
   * <br>
   * <pre><span style="font-family: monospace;">
   * long lockid = table.startUpdate(new Text(article.getName()));
   * for (File articleInfo: article.listFiles(new NonDirectories())) {
   *   String article = null;
   *   try {
   *     DataInputStream in = new DataInputStream(new FileInputStream(articleInfo));
   *     article = in.readUTF();
   *   } catch (IOException e) {
   *     // Input error - abandon update
   *     table.abort(lockid);
   *     throw e;
   *   }
   *   try {
   *     table.put(lockid, columnName(articleInfo.getName()), article.getBytes());
   *   } catch (RuntimeException e) {
   *     // Put failed - abandon update
   *     table.abort(lockid);
   *     throw e;
   *   }
   * }
   * table.commit(lockid);
   * </span></pre>
   *
   * 
   * @param row Name of row to start update against.  Note, choose row names
   * with care.  Rows are sorted lexicographically (comparison is done
   * using {@link Text#compareTo(Object)}.  If your keys are numeric,
   * lexicographic sorting means that 46 sorts AFTER 450 (If you want to use
   * numerics for keys, zero-pad).
   * @return Row lock id..
   * @see #commit(long)
   * @see #commit(long, long)
   * @see #abort(long)
   */
  public synchronized long startUpdate(final Text row) {
    checkClosed();
    updateInProgress(false);
    batch.set(new BatchUpdate(rand.nextLong()));
    return batch.get().startUpdate(row);
  }
  
  /** 
   * Update a value for the specified column.
   * Runs {@link #abort(long)} if exception thrown.
   *
   * @param lockid lock id returned from startUpdate
   * @param column column whose value is being set
   * @param val new value for column.  Cannot be null.
   */
  public void put(long lockid, Text column, byte val[]) {
    checkClosed();
    if (val == null) {
      throw new IllegalArgumentException("value cannot be null");
    }
    updateInProgress(true);
    batch.get().put(lockid, column, val);
  }
  
  /** 
   * Update a value for the specified column.
   * Runs {@link #abort(long)} if exception thrown.
   *
   * @param lockid lock id returned from startUpdate
   * @param column column whose value is being set
   * @param val new value for column.  Cannot be null.
   * @throws IOException throws this if the writable can't be
   * converted into a byte array 
   */
  public void put(long lockid, Text column, Writable val) throws IOException {    
    put(lockid, column, Writables.getBytes(val));
  }
  
  /** 
   * Delete the value for a column.
   * Deletes the cell whose row/column/commit-timestamp match those of the
   * delete.
   * @param lockid lock id returned from startUpdate
   * @param column name of column whose value is to be deleted
   */
  public void delete(long lockid, Text column) {
    checkClosed();
    updateInProgress(true);
    batch.get().delete(lockid, column);
  }
  
  /** 
   * Delete all cells that match the passed row and column.
   * @param row Row to update
   * @param column name of column whose value is to be deleted
   * @throws IOException 
   */
  public void deleteAll(final Text row, final Text column) throws IOException {
    deleteAll(row, column, LATEST_TIMESTAMP);
  }
  
  /** 
   * Delete all cells that match the passed row and column and whose
   * timestamp is equal-to or older than the passed timestamp.
   * @param row Row to update
   * @param column name of column whose value is to be deleted
   * @param ts Delete all cells of the same timestamp or older.
   * @throws IOException 
   */
  public void deleteAll(final Text row, final Text column, final long ts)
  throws IOException {
    checkClosed();
    for(int tries = 0; tries < numRetries; tries++) {
      HRegionLocation r = getRegionLocation(row);
      HRegionInterface server =
        connection.getHRegionConnection(r.getServerAddress());
      try {
        server.deleteAll(r.getRegionInfo().getRegionName(), row, column, ts);
        break;
        
      } catch (IOException e) {
        if (e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        if (tries == numRetries - 1) {
          throw e;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("reloading table servers because: " + e.getMessage());
        }
        tableServers = connection.reloadTableServers(tableName);
      }
      try {
        Thread.sleep(this.pause);
      } catch (InterruptedException x) {
        // continue
      }
    }
  }
  
  /**
   * Completely delete the row's cells of the same timestamp or older.
   *
   * @param row Key of the row you want to completely delete.
   * @param ts Timestamp of cells to delete
   */
  public void deleteAll(final Text row, long ts)
  throws IOException {
    checkClosed();
    for(int tries = 0; tries < numRetries; tries++) {
      HRegionLocation r = getRegionLocation(row);
      HRegionInterface server =
        connection.getHRegionConnection(r.getServerAddress());
      try {
        server.deleteAll(r.getRegionInfo().getRegionName(), row, ts);
        break;

      } catch (IOException e) {
        if (e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        if (tries == numRetries - 1) {
          throw e;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("reloading table servers because: " + e.getMessage());
        }
        tableServers = connection.reloadTableServers(tableName);
      }
      try {
        Thread.sleep(this.pause);
      } catch (InterruptedException x) {
        // continue
      }
    }
  }
      
  /**
   * Completely delete the row's cells.
   *
   * @param row Key of the row you want to completely delete.
   */
  public void deleteAll(final Text row)
  throws IOException {
    deleteAll(row, HConstants.LATEST_TIMESTAMP);
  }
  
  /**
   * Delete all cells for a row with matching column family with timestamps
   * less than or equal to <i>timestamp</i>.
   *
   * @param row The row to operate on
   * @param family The column family to match
   * @param timestamp Timestamp to match
   */
  public void deleteFamily(final Text row, final Text family, long timestamp)
  throws IOException {
    checkClosed();
    for(int tries = 0; tries < numRetries; tries++) {
      HRegionLocation r = getRegionLocation(row);
      HRegionInterface server =
        connection.getHRegionConnection(r.getServerAddress());
      try {
        server.deleteFamily(r.getRegionInfo().getRegionName(), row, family, timestamp);
        break;

      } catch (IOException e) {
        if (e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        if (tries == numRetries - 1) {
          throw e;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("reloading table servers because: " + e.getMessage());
        }
        tableServers = connection.reloadTableServers(tableName);
      }
      try {
        Thread.sleep(this.pause);
      } catch (InterruptedException x) {
        // continue
      }
    }
  }

  /**
   * Delete all cells for a row with matching column family at all timestamps.
   *
   * @param row The row to operate on
   * @param family The column family to match
   */  
  public void deleteFamily(final Text row, final Text family)
  throws IOException{
    deleteFamily(row, family, HConstants.LATEST_TIMESTAMP);
  }
  
  /** 
   * Abort a row mutation.
   * 
   * This method should be called only when an update has been started and it
   * is determined that the update should not be committed.
   * 
   * Releases resources being held by the update in progress.
   *
   * @param lockid lock id returned from startUpdate
   */
  public synchronized void abort(long lockid) {
    checkClosed();
    if (batch.get() != null && batch.get().getLockid() != lockid) {
      throw new IllegalArgumentException("invalid lock id " + lockid);
    }
    batch.set(null);
  }
  
  /** 
   * Finalize a row mutation.
   * 
   * When this method is specified, we pass the server a value that says use
   * the 'latest' timestamp.  If we are doing a put, on the server-side, cells
   * will be given the servers's current timestamp.  If the we are commiting
   * deletes, then delete removes the most recently modified cell of stipulated
   * column.
   * 
   * @see #commit(long, long)
   * 
   * @param lockid lock id returned from startUpdate
   * @throws IOException
   */
  public void commit(long lockid) throws IOException {
    commit(lockid, LATEST_TIMESTAMP);
  }

  /** 
   * Finalize a row mutation and release any resources associated with the update.
   * 
   * @param lockid lock id returned from startUpdate
   * @param timestamp time to associate with the change
   * @throws IOException
   */
  public synchronized void commit(long lockid, long timestamp)
  throws IOException {
    checkClosed();
    updateInProgress(true);
    if (batch.get().getLockid() != lockid) {
      throw new IllegalArgumentException("invalid lock id " + lockid);
    }
    
    try {
      for (int tries = 0; tries < numRetries; tries++) {
        HRegionLocation r = getRegionLocation(batch.get().getRow());
        HRegionInterface server =
          connection.getHRegionConnection(r.getServerAddress());
        try {
          server.batchUpdate(r.getRegionInfo().getRegionName(), timestamp,
            batch.get());
          break;
        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException(
                (RemoteException) e);
          }
          if (tries < numRetries -1) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("reloading table servers because: " + e.getMessage());
            }
            tableServers = connection.reloadTableServers(tableName);
          } else {
            throw e;
          }
        }
        try {
          Thread.sleep(pause);

        } catch (InterruptedException e) {
          // continue
        }
      }
    } finally {
      batch.set(null);
    }
  }
  
  /**
   * Renew lease on update
   * 
   * @param lockid              - lock id returned from startUpdate
   * 
   * @deprecated Batch updates are now the default. Consequently this method
   * does nothing.
   */
  @Deprecated
  public synchronized void renewLease(@SuppressWarnings("unused") long lockid) {
    // noop
  }

  /**
   * Implements the scanner interface for the HBase client.
   * If there are multiple regions in a table, this scanner will iterate
   * through them all.
   */
  protected class ClientScanner implements HScannerInterface {
    private final Text EMPTY_COLUMN = new Text();
    private Text[] columns;
    private Text startRow;
    private long scanTime;
    @SuppressWarnings("hiding")
    private boolean closed;
    private AtomicReferenceArray<HRegionLocation> regions;
    @SuppressWarnings("hiding")
    private int currentRegion;
    private HRegionInterface server;
    private long scannerId;
    private RowFilterInterface filter;
    
    private void loadRegions() {
      checkClosed();
      Text firstServer = null;
      if (this.startRow == null || this.startRow.getLength() == 0) {
        firstServer = tableServers.firstKey();

      } else if(tableServers.containsKey(startRow)) {
        firstServer = startRow;

      } else {
        firstServer = tableServers.headMap(startRow).lastKey();
      }
      Collection<HRegionLocation> info =
        tableServers.tailMap(firstServer).values();
      
      this.regions = new AtomicReferenceArray<HRegionLocation>(
          info.toArray(new HRegionLocation[info.size()]));
    }
    
    protected ClientScanner(Text[] columns, Text startRow, long timestamp,
        RowFilterInterface filter) throws IOException {
      
      this.columns = columns;
      this.startRow = startRow;
      this.scanTime = timestamp;
      this.closed = false;
      this.filter = filter;
      if (filter != null) {
        filter.validate(columns);
      }
      loadRegions();
      this.currentRegion = -1;
      this.server = null;
      this.scannerId = -1L;
      nextScanner();
    }
    
    /*
     * Gets a scanner for the next region.
     * Returns false if there are no more scanners.
     */
    private boolean nextScanner() throws IOException {
      checkClosed();
      if (this.scannerId != -1L) {
        this.server.close(this.scannerId);
        this.scannerId = -1L;
      }
      this.currentRegion += 1;
      if (this.currentRegion == this.regions.length()) {
        close();
        return false;
      }
      try {
        for (int tries = 0; tries < numRetries; tries++) {
          HRegionLocation r = this.regions.get(currentRegion);
          this.server =
            connection.getHRegionConnection(r.getServerAddress());
          
          try {
            if (this.filter == null) {
              this.scannerId =
                this.server.openScanner(r.getRegionInfo().getRegionName(),
                    this.columns, currentRegion == 0 ? this.startRow
                        : EMPTY_START_ROW, scanTime, null);
              
            } else {
              this.scannerId =
                this.server.openScanner(r.getRegionInfo().getRegionName(),
                    this.columns, currentRegion == 0 ? this.startRow
                        : EMPTY_START_ROW, scanTime, filter);
            }

            break;
        
          } catch (IOException e) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException(
                  (RemoteException) e);
            }
            if (tries == numRetries - 1) {
              // No more tries
              throw e;
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("reloading table servers because: " + e.getMessage());
            }
            tableServers = connection.reloadTableServers(tableName);
            loadRegions();
          }
        }

      } catch (IOException e) {
        close();
        if (e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        throw e;
      }
      return true;
    }

    public boolean next(HStoreKey key, SortedMap<Text, byte[]> results)
    throws IOException {
      checkClosed();
      if (this.closed) {
        return false;
      }
      MapWritable values = null;
      // Clear the results so we don't inherit any values from any previous
      // calls to next.
      results.clear();
      do {
        values = this.server.next(this.scannerId);
      } while (values != null && values.size() == 0 && nextScanner());

      if (values != null && values.size() != 0) {
        for (Map.Entry<Writable, Writable> e: values.entrySet()) {
          HStoreKey k = (HStoreKey) e.getKey();
          key.setRow(k.getRow());
          key.setVersion(k.getTimestamp());
          key.setColumn(EMPTY_COLUMN);
          results.put(k.getColumn(),
              ((ImmutableBytesWritable) e.getValue()).get());
        }
      }
      return values == null ? false : values.size() != 0;
    }

    /**
     * {@inheritDoc}
     */
    public void close() throws IOException {
      checkClosed();
      if (this.scannerId != -1L) {
        try {
          this.server.close(this.scannerId);
          
        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          if (!(e instanceof NotServingRegionException)) {
            throw e;
          }
        }
        this.scannerId = -1L;
      }
      this.server = null;
      this.closed = true;
    }

    /** {@inheritDoc} */
    public Iterator<Entry<HStoreKey, SortedMap<Text, byte[]>>> iterator() {
      return new Iterator<Entry<HStoreKey, SortedMap<Text, byte[]>>>() {
        HStoreKey key = null;
        SortedMap<Text, byte []> value = null;
        
        public boolean hasNext() {
          boolean hasNext = false;
          try {
            this.key = new HStoreKey();
            this.value = new TreeMap<Text, byte[]>();
            hasNext = ClientScanner.this.next(key, value);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return hasNext;
        }

        public Entry<HStoreKey, SortedMap<Text, byte[]>> next() {
          return new Map.Entry<HStoreKey, SortedMap<Text, byte[]>>() {
            public HStoreKey getKey() {
              return key;
            }

            public SortedMap<Text, byte[]> getValue() {
              return value;
            }

            public SortedMap<Text, byte[]> setValue(@SuppressWarnings("unused")
            SortedMap<Text, byte[]> value) {
              throw new UnsupportedOperationException();
            }
          };
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
