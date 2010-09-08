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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * A simple pool of HTable instances.<p>
 *
 * Each HTablePool acts as a pool for all tables.  To use, instantiate an
 * HTablePool and use {@link #getTable(String)} to get an HTable from the pool.
 * Once you are done with it, return it to the pool with {@link #putTable(HTableInterface)}.<p>
 *
 * A pool can be created with a <i>maxSize</i> which defines the most HTable
 * references that will ever be retained for each table.  Otherwise the default
 * is {@link Integer#MAX_VALUE}.<p>
 */
public class HTablePool {
  private final ConcurrentMap<String, LinkedList<HTableInterface>> tables =
    new ConcurrentHashMap<String, LinkedList<HTableInterface>>();
  private final Configuration config;
  private final int maxSize;
  private HTableInterfaceFactory tableFactory = new HTableFactory();

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
  public HTablePool(Configuration config, int maxSize) {
    this.config = config;
    this.maxSize = maxSize;
  }

  public HTablePool(Configuration config, int maxSize, HTableInterfaceFactory tableFactory) {
    this.config = config;
    this.maxSize = maxSize;
    this.tableFactory = tableFactory;
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
    LinkedList<HTableInterface> queue = tables.get(tableName);
    if(queue == null) {
      queue = new LinkedList<HTableInterface>();
      tables.putIfAbsent(tableName, queue);
      return createHTable(tableName);
    }
    HTableInterface table;
    synchronized(queue) {
      table = queue.poll();
    }
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
   * then nothing happens.
   * @param table table
   */
  public void putTable(HTableInterface table) {
    LinkedList<HTableInterface> queue = tables.get(Bytes.toString(table.getTableName()));
    synchronized(queue) {
      if(queue.size() >= maxSize) return;
      queue.add(table);
    }
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
  public void closeTablePool(final String tableName)  {
    Queue<HTableInterface> queue = tables.get(tableName);
    synchronized (queue) {
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
  public void closeTablePool(final byte[] tableName)  {
    closeTablePool(Bytes.toString(tableName));
  }

  int getCurrentPoolSize(String tableName) {
    Queue<HTableInterface> queue = tables.get(tableName);
    synchronized(queue) {
      return queue.size();
    }
  }
}
