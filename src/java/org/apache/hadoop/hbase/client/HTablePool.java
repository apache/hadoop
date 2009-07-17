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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A simple pool of HTable instances.<p>
 * 
 * Each HTablePool acts as a pool for all tables.  To use, instantiate an
 * HTablePool and use {@link #getTable(String)} to get an HTable from the pool.
 * Once you are done with it, return it to the pool with {@link #putTable(HTable)}.<p>
 * 
 * A pool can be created with a <i>maxSize</i> which defines the most HTable
 * references that will ever be retained for each table.  Otherwise the default
 * is {@link Integer#MAX_VALUE}.<p>
 */
public class HTablePool {
  private final Map<String, LinkedList<HTable>> tables = 
      Collections.synchronizedMap(new HashMap<String, LinkedList<HTable>>());
  private final HBaseConfiguration config;
  private final int maxSize;

  /**
   * Default Constructor.  Default HBaseConfiguration and no limit on pool size.
   */
  public HTablePool() {
    this(new HBaseConfiguration(), Integer.MAX_VALUE);
  }

  /**
   * Constructor to set maximum versions and use the specified configuration.
   * @param config configuration
   * @param maxSize maximum number of references to keep for each table
   */
  public HTablePool(HBaseConfiguration config, int maxSize) {
    this.config = config;
    this.maxSize = maxSize;
  }

  /**
   * Get a reference to the specified table from the pool.<p>
   * 
   * Create a new one if one is not available.
   * @param tableName
   * @return a reference to the specified table
   * @throws RuntimeException if there is a problem instantiating the HTable
   */
  public HTable getTable(String tableName) {
    LinkedList<HTable> queue = tables.get(tableName);
    if(queue == null) {
      queue = new LinkedList<HTable>();
      tables.put(tableName, queue);
      return newHTable(tableName);
    }
    HTable table;
    synchronized(queue) {
      table = queue.poll();
    }
    if(table == null) {
      return newHTable(tableName);
    }
    return table;
  }

  /**
   * Get a reference to the specified table from the pool.<p>
   * 
   * Create a new one if one is not available.
   * @param tableName
   * @return a reference to the specified table
   * @throws RuntimeException if there is a problem instantiating the HTable
   */
  public HTable getTable(byte [] tableName) {
    return getTable(Bytes.toString(tableName));
  }

  /**
   * Puts the specified HTable back into the pool.<p>
   * 
   * If the pool already contains <i>maxSize</i> references to the table,
   * then nothing happens.
   * @param table
   */
  public void putTable(HTable table) {
    LinkedList<HTable> queue = tables.get(Bytes.toString(table.getTableName()));
    synchronized(queue) {
      if(queue.size() >= maxSize) return;
      queue.add(table);
    }
  }

  private HTable newHTable(String tableName) {
    try {
      return new HTable(config, Bytes.toBytes(tableName));
    } catch(IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
