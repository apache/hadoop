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
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A simple pool of HTable instances.<p>
 *
 * Each HTablePool acts as a pool for all tables.  To use, instantiate an
 * HTablePool and use {@link #getTable(String)} to get an HTable from the pool.
 * Once you are done with it, return it to the pool with {@link #putTable(HTableInterface)}.
 * 
 * <p>A pool can be created with a <i>maxSize</i> which defines the most HTable
 * references that will ever be retained for each table.  Otherwise the default
 * is {@link Integer#MAX_VALUE}.
 *
 * <p>Pool will manage its own cluster to the cluster. See {@link HConnectionManager}.
 */
public class HTablePool implements Closeable {
  private final Map<String, Queue<HTableInterface>> tables =
    new ConcurrentHashMap<String, Queue<HTableInterface>>();
  private final Configuration config;
  private final int maxSize;
  private final HTableInterfaceFactory tableFactory;

  /**
   * Default Constructor.  Default HBaseConfiguration and no limit on pool size.
   */
  public HTablePool() {
    this(HBaseConfiguration.create(), Integer.MAX_VALUE);
  }

  /**
   * Constructor to set maximum versions and use the specified configuration.
   * @param config configuration
   * @param maxSize maximum number of references to keep for each table
   */
  public HTablePool(final Configuration config, final int maxSize) {
    this(config, maxSize, null);
  }

  public HTablePool(final Configuration config, final int maxSize,
      final HTableInterfaceFactory tableFactory) {
    // Make a new configuration instance so I can safely cleanup when
    // done with the pool.
    this.config = config == null? new Configuration(): config;
    this.maxSize = maxSize;
    this.tableFactory = tableFactory == null? new HTableFactory(): tableFactory;
  }

  /**
   * Get a reference to the specified table from the pool.<p>
   *
   * Create a new one if one is not available.
   * @param tableName table name
   * @return a reference to the specified table
   * @throws RuntimeException if there is a problem instantiating the HTable
   */
  public HTableInterface getTable(String tableName) {
    Queue<HTableInterface> queue = tables.get(tableName);
    if(queue == null) {
      queue = new ConcurrentLinkedQueue<HTableInterface>();
      tables.put(tableName, queue);
      return createHTable(tableName);
    }
    HTableInterface table = queue.poll();
    if(table == null) {
      return createHTable(tableName);
    }
    return table;
  }

  /**
   * Get a reference to the specified table from the pool.<p>
   *
   * Create a new one if one is not available.
   * @param tableName table name
   * @return a reference to the specified table
   * @throws RuntimeException if there is a problem instantiating the HTable
   */
  public HTableInterface getTable(byte [] tableName) {
    return getTable(Bytes.toString(tableName));
  }

  /**
   * Puts the specified HTable back into the pool.<p>
   *
   * If the pool already contains <i>maxSize</i> references to the table,
   * then the table instance gets closed after flushing buffered edits.
   * @param table table
   */
  public void putTable(HTableInterface table) throws IOException {
    Queue<HTableInterface> queue = tables.get(Bytes.toString(table.getTableName()));
    if(queue.size() >= maxSize) {
      // release table instance since we're not reusing it
      this.tableFactory.releaseHTableInterface(table);
      return;
    }
    queue.add(table);
  }

  protected HTableInterface createHTable(String tableName) {
    return this.tableFactory.createHTableInterface(config, Bytes.toBytes(tableName));
  }

  /**
   * Closes all the HTable instances , belonging to the given table, in the table pool.
   * <p>
   * Note: this is a 'shutdown' of the given table pool and different from
   * {@link #putTable(HTableInterface)}, that is used to return the table
   * instance to the pool for future re-use.
   *
   * @param tableName
   */
  public void closeTablePool(final String tableName) throws IOException {
    Queue<HTableInterface> queue = tables.get(tableName);
    if (queue != null) {
      HTableInterface table = queue.poll();
      while (table != null) {
        this.tableFactory.releaseHTableInterface(table);
        table = queue.poll();
      }
    }
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
   * Closes all the HTable instances , belonging to all tables in the table pool.
   * <p>
   * Note: this is a 'shutdown' of all the table pools.
   */
  public void close() throws IOException {
    for (String tableName : tables.keySet()) {
      closeTablePool(tableName);
    }
  }

  int getCurrentPoolSize(String tableName) {
    Queue<HTableInterface> queue = tables.get(tableName);
    return queue.size();
  }
}
