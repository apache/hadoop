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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.stargate;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

import com.sun.jersey.server.impl.container.servlet.ServletAdaptor;

/**
 * Singleton class encapsulating global REST servlet state and functions.
 */
public class RESTServlet extends ServletAdaptor {
  
  private static final long serialVersionUID = 1L;  
  public static final int DEFAULT_MAX_AGE = 60 * 60 * 4;       // 4 hours
  public static final String VERSION_STRING = "0.0.1";

  private static RESTServlet instance;

  private final HBaseConfiguration conf;
  protected Map<String,Integer> maxAgeMap = 
    Collections.synchronizedMap(new HashMap<String,Integer>());

  /**
   * @return the RESTServlet singleton instance
   * @throws IOException
   */
  public synchronized static RESTServlet getInstance() throws IOException {
    if (instance == null) {
      instance = new RESTServlet();
    }
    return instance;
  }

  /**
   * Constructor
   * @throws IOException
   */
  public RESTServlet() throws IOException {
    this.conf = new HBaseConfiguration();
  }


  /**
   * Get or create a table pool for the given table. 
   * @param name the table name
   * @return the table pool
   */
  protected HTablePool getTablePool(String name) {
    return HTablePool.getPool(conf, Bytes.toBytes(name));
  }

  /**
   * @return the servlet's global HBase configuration
   */
  protected HBaseConfiguration getConfiguration() {
    return conf;
  }

  /**
   * @param tableName the table name
   * @return the maximum cache age suitable for use with this table, in
   *  seconds 
   * @throws IOException
   */
  public int getMaxAge(String tableName) throws IOException {
    Integer i = maxAgeMap.get(tableName);
    if (i != null) {
      return i.intValue();
    }
    HTablePool pool = this.getTablePool(tableName);
    HTable table = pool.get();
    if (table != null) {
      int maxAge = DEFAULT_MAX_AGE;
      for (HColumnDescriptor family:
          table.getTableDescriptor().getFamilies()) {
        int ttl = family.getTimeToLive();
        if (ttl < 0) {
          continue;
        }
        if (ttl < maxAge) {
          maxAge = ttl;
        }
      }
      maxAgeMap.put(tableName, maxAge);
      return maxAge;
    }
    return DEFAULT_MAX_AGE;
  }

  /**
   * Signal that a previously calculated maximum cache age has been
   * invalidated by a schema change.
   * @param tableName the table name
   */
  public void invalidateMaxAge(String tableName) {
    maxAgeMap.remove(tableName);
  }
}
