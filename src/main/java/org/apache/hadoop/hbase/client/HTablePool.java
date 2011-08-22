/**
 * Copyright 2010 The Apache Software Foundation
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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PoolMap;
import org.apache.hadoop.hbase.util.PoolMap.PoolType;

/**
 * A simple pool of HTable instances.
 * 
 * Each HTablePool acts as a pool for all tables. To use, instantiate an
 * HTablePool and use {@link #getTable(String)} to get an HTable from the pool.
 *
   * This method is not needed anymore, clients should call
   * HTableInterface.close() rather than returning the tables to the pool
   *
 * Once you are done with it, close your instance of {@link HTableInterface}
 * by calling {@link HTableInterface#close()} rather than returning the tables
 * to the pool with (deprecated) {@link #putTable(HTableInterface)}.
 * 
 * <p>
 * A pool can be created with a <i>maxSize</i> which defines the most HTable
 * references that will ever be retained for each table. Otherwise the default
 * is {@link Integer#MAX_VALUE}.
 * 
 * <p>
 * Pool will manage its own connections to the cluster. See
 * {@link HConnectionManager}.
 */
public class HTablePool implements Closeable {
  private final PoolMap<String, HTableInterface> tables;
  private final int maxSize;
  private final PoolType poolType;
  private final Configuration config;
  private final HTableInterfaceFactory tableFactory;

  /**
   * Default Constructor. Default HBaseConfiguration and no limit on pool size.
   */
  public HTablePool() {
    this(HBaseConfiguration.create(), Integer.MAX_VALUE);
  }

  /**
   * Constructor to set maximum versions and use the specified configuration.
   * 
   * @param config
   *          configuration
   * @param maxSize
   *          maximum number of references to keep for each table
   */
  public HTablePool(final Configuration config, final int maxSize) {
    this(config, maxSize, null, null);
  }

  /**
   * Constructor to set maximum versions and use the specified configuration and
   * table factory.
   * 
   * @param config
   *          configuration
   * @param maxSize
   *          maximum number of references to keep for each table
   * @param tableFactory
   *          table factory
   */
  public HTablePool(final Configuration config, final int maxSize,
      final HTableInterfaceFactory tableFactory) {
    this(config, maxSize, null, PoolType.Reusable);
  }

  /**
   * Constructor to set maximum versions and use the specified configuration and
   * pool type.
   * 
   * @param config
   *          configuration
   * @param maxSize
   *          maximum number of references to keep for each table
   * @param tableFactory
   *          table factory
   * @param poolType
   *          pool type which is one of {@link PoolType#Reusable} or
   *          {@link PoolType#ThreadLocal}
   */
  public HTablePool(final Configuration config, final int maxSize,
      final PoolType poolType) {
    this(config, maxSize, null, poolType);
  }

  /**
   * Constructor to set maximum versions and use the specified configuration,
   * table factory and pool type. The HTablePool supports the
   * {@link PoolType#Reusable} and {@link PoolType#ThreadLocal}. If the pool
   * type is null or not one of those two values, then it will default to
   * {@link PoolType#Reusable}.
   * 
   * @param config
   *          configuration
   * @param maxSize
   *          maximum number of references to keep for each table
   * @param tableFactory
   *          table factory
   * @param poolType
   *          pool type which is one of {@link PoolType#Reusable} or
   *          {@link PoolType#ThreadLocal}
   */
  public HTablePool(final Configuration config, final int maxSize,
      final HTableInterfaceFactory tableFactory, PoolType poolType) {
    // Make a new configuration instance so I can safely cleanup when
    // done with the pool.
    this.config = config == null ? new Configuration() : config;
    this.maxSize = maxSize;
    this.tableFactory = tableFactory == null ? new HTableFactory()
        : tableFactory;
    if (poolType == null) {
      this.poolType = PoolType.Reusable;
    } else {
      switch (poolType) {
      case Reusable:
      case ThreadLocal:
        this.poolType = poolType;
        break;
      default:
        this.poolType = PoolType.Reusable;
        break;
      }
    }
    this.tables = new PoolMap<String, HTableInterface>(this.poolType,
        this.maxSize);
  }

  /**
   * Get a reference to the specified table from the pool.
   * <p>
   * <p/>
   * 
   * @param tableName
   *          table name
   * @return a reference to the specified table
   * @throws RuntimeException
   *           if there is a problem instantiating the HTable
   */
  public HTableInterface getTable(String tableName) {
    // call the old getTable implementation renamed to findOrCreateTable
    HTableInterface table = findOrCreateTable(tableName);
    // return a proxy table so when user closes the proxy, the actual table
    // will be returned to the pool
    try {
      return new PooledHTable(table);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Get a reference to the specified table from the pool.
   * <p>
   * 
   * Create a new one if one is not available.
   * 
   * @param tableName
   *          table name
   * @return a reference to the specified table
   * @throws RuntimeException
   *           if there is a problem instantiating the HTable
   */
  private HTableInterface findOrCreateTable(String tableName) {
    HTableInterface table = tables.get(tableName);
    if (table == null) {
      table = createHTable(tableName);
    }
    return table;
  }

  /**
   * Get a reference to the specified table from the pool.
   * <p>
   * 
   * Create a new one if one is not available.
   * 
   * @param tableName
   *          table name
   * @return a reference to the specified table
   * @throws RuntimeException
   *           if there is a problem instantiating the HTable
   */
  public HTableInterface getTable(byte[] tableName) {
    return getTable(Bytes.toString(tableName));
  }

  /**
   * This method is not needed anymore, clients should call
   * HTableInterface.close() rather than returning the tables to the pool
   * 
   * @param table
   *          the proxy table user got from pool
   * @deprecated
   */
  public void putTable(HTableInterface table) throws IOException {
    // we need to be sure nobody puts a proxy implementation in the pool
    // but if the client code is not updated
    // and it will continue to call putTable() instead of calling close()
    // then we need to return the wrapped table to the pool instead of the
    // proxy
    // table
    if (table instanceof PooledHTable) {
      returnTable(((PooledHTable) table).getWrappedTable());
    } else {
      // normally this should not happen if clients pass back the same
      // table
      // object they got from the pool
      // but if it happens then it's better to reject it
      throw new IllegalArgumentException("not a pooled table: " + table);
    }
  }

  /**
   * Puts the specified HTable back into the pool.
   * <p>
   * 
   * If the pool already contains <i>maxSize</i> references to the table, then
   * the table instance gets closed after flushing buffered edits.
   * 
   * @param table
   *          table
   */
  private void returnTable(HTableInterface table) throws IOException {
    // this is the old putTable method renamed and made private
    String tableName = Bytes.toString(table.getTableName());
    if (tables.size(tableName) >= maxSize) {
      // release table instance since we're not reusing it
      this.tables.remove(tableName, table);
      this.tableFactory.releaseHTableInterface(table);
      return;
    }
    tables.put(tableName, table);
  }

  protected HTableInterface createHTable(String tableName) {
    return this.tableFactory.createHTableInterface(config,
        Bytes.toBytes(tableName));
  }

  /**
   * Closes all the HTable instances , belonging to the given table, in the
   * table pool.
   * <p>
   * Note: this is a 'shutdown' of the given table pool and different from
   * {@link #putTable(HTableInterface)}, that is used to return the table
   * instance to the pool for future re-use.
   * 
   * @param tableName
   */
  public void closeTablePool(final String tableName) throws IOException {
    Collection<HTableInterface> tables = this.tables.values(tableName);
    if (tables != null) {
      for (HTableInterface table : tables) {
        this.tableFactory.releaseHTableInterface(table);
      }
    }
    this.tables.remove(tableName);
  }

  /**
   * See {@link #closeTablePool(String)}.
   * 
   * @param tableName
   */
  public void closeTablePool(final byte[] tableName) throws IOException {
    closeTablePool(Bytes.toString(tableName));
  }

  /**
   * Closes all the HTable instances , belonging to all tables in the table
   * pool.
   * <p>
   * Note: this is a 'shutdown' of all the table pools.
   */
  public void close() throws IOException {
    for (String tableName : tables.keySet()) {
      closeTablePool(tableName);
    }
    this.tables.clear();
  }

  int getCurrentPoolSize(String tableName) {
    return tables.size(tableName);
  }

  /**
   * A proxy class that implements HTableInterface.close method to return the
   * wrapped table back to the table pool
   * 
   */
  class PooledHTable extends HTable {

    private HTableInterface table; // actual table implementation

    public PooledHTable(HTableInterface table) throws IOException {
      super(table.getConfiguration(), table.getTableName());
      this.table = table;
    }

    @Override
    public byte[] getTableName() {
      return table.getTableName();
    }

    @Override
    public Configuration getConfiguration() {
      return table.getConfiguration();
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
      return table.getTableDescriptor();
    }

    @Override
    public boolean exists(Get get) throws IOException {
      return table.exists(get);
    }

    @Override
    public void batch(List<Row> actions, Object[] results) throws IOException,
        InterruptedException {
      table.batch(actions, results);
    }

    @Override
    public Object[] batch(List<Row> actions) throws IOException,
        InterruptedException {
      return table.batch(actions);
    }

    @Override
    public Result get(Get get) throws IOException {
      return table.get(get);
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
      return table.get(gets);
    }

    @Override
    public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
      return table.getRowOrBefore(row, family);
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
      return table.getScanner(scan);
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
      return table.getScanner(family);
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier)
        throws IOException {
      return table.getScanner(family, qualifier);
    }

    @Override
    public void put(Put put) throws IOException {
      table.put(put);
    }

    @Override
    public void put(List<Put> puts) throws IOException {
      table.put(puts);
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
        byte[] value, Put put) throws IOException {
      return table.checkAndPut(row, family, qualifier, value, put);
    }

    @Override
    public void delete(Delete delete) throws IOException {
      table.delete(delete);
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
      table.delete(deletes);
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
        byte[] value, Delete delete) throws IOException {
      return table.checkAndDelete(row, family, qualifier, value, delete);
    }

    @Override
    public Result increment(Increment increment) throws IOException {
      return table.increment(increment);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
        byte[] qualifier, long amount) throws IOException {
      return table.incrementColumnValue(row, family, qualifier, amount);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
        byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
      return table.incrementColumnValue(row, family, qualifier, amount,
          writeToWAL);
    }

    @Override
    public boolean isAutoFlush() {
      return table.isAutoFlush();
    }

    @Override
    public void flushCommits() throws IOException {
      table.flushCommits();
    }

    /**
     * Returns the actual table back to the pool
     * 
     * @throws IOException
     */
    public void close() throws IOException {
      returnTable(table);
    }

    @Override
    public RowLock lockRow(byte[] row) throws IOException {
      return table.lockRow(row);
    }

    @Override
    public void unlockRow(RowLock rl) throws IOException {
      table.unlockRow(rl);
    }

    @Override
    public <T extends CoprocessorProtocol> T coprocessorProxy(
        Class<T> protocol, byte[] row) {
      return table.coprocessorProxy(protocol, row);
    }

    @Override
    public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(
        Class<T> protocol, byte[] startKey, byte[] endKey,
        Batch.Call<T, R> callable) throws IOException, Throwable {
      return table.coprocessorExec(protocol, startKey, endKey, callable);
    }

    @Override
    public <T extends CoprocessorProtocol, R> void coprocessorExec(
        Class<T> protocol, byte[] startKey, byte[] endKey,
        Batch.Call<T, R> callable, Batch.Callback<R> callback)
        throws IOException, Throwable {
      table.coprocessorExec(protocol, startKey, endKey, callable, callback);
    }

    @Override
    public String toString() {
      return "PooledHTable{" + ", table=" + table + '}';
    }

    /**
     * Expose the wrapped HTable to tests in the same package
     * 
     * @return wrapped htable
     */
    HTableInterface getWrappedTable() {
      return table;
    }
  }
}
