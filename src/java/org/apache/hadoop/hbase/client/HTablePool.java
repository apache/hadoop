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
import java.util.ArrayDeque;
/* using a stack instead of a FIFO might have some small positive performance
   impact wrt. cache */
import java.util.Deque;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A simple pool of HTable instances.
 * <p>
 * The default pool size is 10.
 */
public class HTablePool {
  private static final Map<byte[], HTablePool> poolMap = 
    new TreeMap<byte[], HTablePool>(Bytes.BYTES_COMPARATOR);

  private final HBaseConfiguration config;
  private final byte[] tableName;
  private final Deque<HTable> pool;
  private final int maxSize;

  /**
   * Get a shared table pool.
   * @param tableName the table name
   * @return the table pool
   */
  public static HTablePool getPool(HBaseConfiguration config, 
      byte[] tableName) {
    return getPool(config, tableName, 10);
  }

  /**
   * Get a shared table pool.
   * @param tableName the table name
   * @return the table pool
   */
  public static HTablePool getPool(byte[] tableName) {
    return getPool(new HBaseConfiguration(), tableName, 10);
  }

  /**
   * Get a shared table pool.
   * <p>
   * NOTE: <i>maxSize</i> is advisory. If the pool does not yet exist, a new
   * shared pool will be allocated with <i>maxSize</i> as the size limit.
   * However, if the shared pool already exists, and was created with a 
   * different (or default) value for <i>maxSize</i>, it will not be changed.
   * @param config HBase configuration
   * @param tableName the table name
   * @param maxSize the maximum size of the pool
   * @return the table pool
   */
  public static HTablePool getPool(HBaseConfiguration config, byte[] tableName,
      int maxSize) {
    synchronized (poolMap) {
      HTablePool pool = poolMap.get(tableName);
      if (pool == null) {
        pool = new HTablePool(config, tableName, maxSize);
        poolMap.put(tableName, pool);
      }
      return pool;
    }
  }

  /**
   * Constructor 
   * @param config HBase configuration
   * @param tableName the table name
   * @param maxSize maximum pool size
   */
  public HTablePool(HBaseConfiguration config, byte[] tableName,
      int maxSize) {
    this.config = config;
    this.tableName = tableName;
    this.maxSize = maxSize;
    this.pool = new ArrayDeque<HTable>(this.maxSize);
  }

  /**
   * Constructor
   * @param tableName the table name
   * @param maxSize maximum pool size
   */
  public HTablePool(byte[] tableName, int maxSize) {
    this(new HBaseConfiguration(), tableName, maxSize);
  }

  /**
   * Constructor
   * @param tableName the table name
   */
  public HTablePool(byte[] tableName) {
    this(new HBaseConfiguration(), tableName, 10);
  }

  /**
   * Get a HTable instance, possibly from the pool, if one is available.
   * @return HTable a HTable instance
   * @throws IOException
   */
  public HTable get() throws IOException {
    synchronized (pool) {
      // peek then pop inside a synchronized block avoids the overhead of a
      // NoSuchElementException
      HTable table = pool.peek();
      if (table != null) {
        return pool.pop();
      }
    }
    return new HTable(config, tableName);
  }

  /**
   * Return a HTable instance to the pool.
   * @param table a HTable instance
   */
  public void put(HTable table) {
    synchronized (pool) {
      if (pool.size() < maxSize) {
        pool.push(table);
      }
    }
  }

}
