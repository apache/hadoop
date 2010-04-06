/*
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

package org.apache.hadoop.hbase.stargate.util;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.stargate.Constants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

/**
 * A HTable-backed token bucket.
 * <p>
 * Can be configured with <t>rate</t>, the number of tokens to add to the
 * bucket each second, and <t>size</t>, the maximum number of tokens allowed
 * to burst. Configuration is stored in the HTable adjacent to the token
 * count and is periodically refreshed.
 * <p>
 * Expected columns:
 * <p>
 * <ul>
 *   <li>user:
 *   <ul>
 *     <li>user:tokens</li>
 *     <li>user:tokens.rate</li>
 *     <li>user:tokens.size</li>
 *   </ul></li>
 * </ul>
 */
public class HTableTokenBucket implements Constants {

  static final Log LOG = LogFactory.getLog(HTableTokenBucket.class);

  static final byte[] USER = Bytes.toBytes("user");
  static final byte[] TOKENS = Bytes.toBytes("tokens");
  static final byte[] TOKENS_RATE = Bytes.toBytes("tokens.rate");
  static final byte[] TOKENS_SIZE = Bytes.toBytes("tokens.size");

  Configuration conf;
  String tableName;
  HTable table;
  byte[] row;
  int tokens;
  double rate = 20.0; // default, 20 ops added per second
  int size = 100;     // burst
  long lastUpdated = System.currentTimeMillis();
  long configUpdateInterval;
  long lastConfigUpdated = System.currentTimeMillis();

  void updateConfig() throws IOException {
    Get get = new Get(row);
    get.addColumn(USER, TOKENS_RATE);
    get.addColumn(USER, TOKENS_SIZE);
    Result result = table.get(get);
    byte[] value = result.getValue(USER, TOKENS_RATE);
    if (value != null) {
      this.rate = (int)Bytes.toDouble(value);
    }
    value = result.getValue(USER, TOKENS_SIZE);
    if (value != null) {
      this.size = (int)Bytes.toLong(value);
    }
  }

  /**
   * Constructor
   * @param conf configuration
   * @param row row key for user
   * @throws IOException
   */
  public HTableTokenBucket(Configuration conf, byte[] row) 
      throws IOException {
    this(conf, conf.get("stargate.tb.htable.name", USERS_TABLE), row);
  }

  /**
   * Constructor
   * @param conf configuration
   * @param tableName the table to use
   * @param row row key for user
   * @throws IOException
   */
  public HTableTokenBucket(Configuration conf, String tableName,
      byte[] row) throws IOException {
    this.conf = conf;
    this.tableName = tableName;
    this.row = row;
    this.table = new HTable(conf, tableName);
    this.configUpdateInterval = 
      conf.getLong("stargate.tb.update.interval", 1000 * 60);
    updateConfig();
  }

  /**
   * @return the number of remaining tokens in the bucket (roughly)
   * @throws IOException
   */
  public int available() throws IOException {
    long now = System.currentTimeMillis();
    if (now - lastConfigUpdated > configUpdateInterval) {
      try {
        updateConfig();
      } catch (IOException e) { 
        LOG.warn(StringUtils.stringifyException(e));
      }
      lastConfigUpdated = now;
    }

    // We can't simply use incrementColumnValue here because the timestamp of
    // the keyvalue will not be changed as long as it remains in memstore, so
    // there will be some unavoidable contention on the row if multiple 
    // Stargate instances are concurrently serving the same user, and three
    // more round trips than otherwise.
    RowLock rl = table.lockRow(row);
    try {
      Get get = new Get(row, rl);
      get.addColumn(USER, TOKENS);
      List<KeyValue> kvs = table.get(get).list();
      if (kvs != null && !kvs.isEmpty()) {
        KeyValue kv = kvs.get(0);
        tokens = (int)Bytes.toLong(kv.getValue());
        lastUpdated = kv.getTimestamp();
      } else {
        tokens = (int)rate;
      }
      long elapsed = now - lastUpdated;
      int i = (int)((elapsed / 1000) * rate); // convert sec <-> ms
      if (tokens + i > size) {
        i = size - tokens;
      }
      if (i > 0) {
        tokens += i;
        Put put = new Put(row, rl);
        put.add(USER, TOKENS, Bytes.toBytes((long)tokens));
        put.setWriteToWAL(false);
        table.put(put);
        table.flushCommits();
      }
    } finally {
      table.unlockRow(rl);
    }
    return tokens;
  }

  /**
   * @param t the number of tokens to consume from the bucket
   * @throws IOException
   */
  public void remove(int t) throws IOException {
    // Here we don't care about timestamp changes; actually it's advantageous
    // if they are not updated, otherwise available() and remove() must be
    // used as near to each other in time as possible.
    table.incrementColumnValue(row, USER, TOKENS, (long) -t, false);
  }

  public double getRate() {
    return rate;
  }

  public int getSize() {
    return size;
  }

}
