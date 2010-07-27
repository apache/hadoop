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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is responsible for replicating the edits coming
 * from another cluster.
 * <p/>
 * This replication process is currently waiting for the edits to be applied
 * before the method can return. This means that the replication of edits
 * is synchronized (after reading from HLogs in ReplicationSource) and that a
 * single region server cannot receive edits from two sources at the same time
 * <p/>
 * This class uses the native HBase client in order to replicate entries.
 * <p/>
 *
 * TODO make this class more like ReplicationSource wrt log handling
 */
public class ReplicationSink {

  private static final Log LOG = LogFactory.getLog(ReplicationSink.class);
  // Name of the HDFS directory that contains the temporary rep logs
  public static final String REPLICATION_LOG_DIR = ".replogs";
  private final Configuration conf;
  // Pool used to replicated
  private final HTablePool pool;
  // boolean coming from HRS to know when the process stops
  private final AtomicBoolean stop;
  private final ReplicationSinkMetrics metrics;

  /**
   * Create a sink for replication
   *
   * @param conf                conf object
   * @param stopper             boolean to tell this thread to stop
   * @throws IOException thrown when HDFS goes bad or bad file name
   */
  public ReplicationSink(Configuration conf, AtomicBoolean stopper)
      throws IOException {
    this.conf = conf;
    this.pool = new HTablePool(this.conf,
        conf.getInt("replication.sink.htablepool.capacity", 10));
    this.stop = stopper;
    this.metrics = new ReplicationSinkMetrics();
  }

  /**
   * Replicate this array of entries directly into the local cluster
   * using the native client.
   *
   * @param entries
   * @throws IOException
   */
  public synchronized void replicateEntries(HLog.Entry[] entries)
      throws IOException {
    if (entries.length == 0) {
      return;
    }
    // Very simple optimization where we batch sequences of rows going
    // to the same table.
    try {
      long totalReplicated = 0;
      byte[] lastTable = HConstants.EMPTY_BYTE_ARRAY;
      List<Put> puts = new ArrayList<Put>();
      for (HLog.Entry entry : entries) {
        WALEdit edit = entry.getEdit();
        List<KeyValue> kvs = edit.getKeyValues();
        if (kvs.get(0).isDelete()) {
          Delete delete = new Delete(kvs.get(0).getRow(),
              kvs.get(0).getTimestamp(), null);
          for (KeyValue kv : kvs) {
            if (kv.isDeleteFamily()) {
              delete.deleteFamily(kv.getFamily());
            } else if (!kv.isEmptyColumn()) {
              delete.deleteColumn(kv.getFamily(),
                  kv.getQualifier());
            }
          }
          delete(entry.getKey().getTablename(), delete);
        } else {
          // Switching table, flush
          if (!Bytes.equals(lastTable, entry.getKey().getTablename())) {
            put(lastTable, puts);
          }
          // With mini-batching, we need to expect multiple rows per edit
          byte[] lastKey = kvs.get(0).getRow();
          Put put = new Put(kvs.get(0).getRow(),
              kvs.get(0).getTimestamp());
          for (KeyValue kv : kvs) {
            if (!Bytes.equals(lastKey, kv.getRow())) {
              puts.add(put);
              put = new Put(kv.getRow(), kv.getTimestamp());
            }
            put.add(kv.getFamily(), kv.getQualifier(), kv.getValue());
            lastKey = kv.getRow();
          }
          puts.add(put);
          lastTable = entry.getKey().getTablename();
        }
        totalReplicated++;
      }
      put(lastTable, puts);
      this.metrics.setAgeOfLastAppliedOp(
          entries[entries.length-1].getKey().getWriteTime());
      this.metrics.appliedBatchesRate.inc(1);
      LOG.info("Total replicated: " + totalReplicated);
    } catch (IOException ex) {
      if (ex.getCause() instanceof TableNotFoundException) {
        LOG.warn("Losing edits because: ", ex);
      } else {
        // Should we log rejected edits in a file for replay?
        LOG.error("Unable to accept edit because", ex);
        this.stop.set(true);
        throw ex;
      }
    } catch (RuntimeException re) {
      if (re.getCause() instanceof TableNotFoundException) {
        LOG.warn("Losing edits because: ", re);
      } else {
        this.stop.set(true);
        throw re;
      }
    }
  }

  /**
   * Do the puts and handle the pool
   * @param tableName table to insert into
   * @param puts list of puts
   * @throws IOException
   */
  private void put(byte[] tableName, List<Put> puts) throws IOException {
    if (puts.isEmpty()) {
      return;
    }
    HTableInterface table = null;
    try {
      table = this.pool.getTable(tableName);
      table.put(puts);
      this.metrics.appliedOpsRate.inc(puts.size());
      this.pool.putTable(table);
      puts.clear();
    } finally {
      if (table != null) {
        this.pool.putTable(table);
      }
    }
  }

  /**
   * Do the delete and handle the pool
   * @param tableName table to delete in
   * @param delete the delete to use
   * @throws IOException
   */
  private void delete(byte[] tableName, Delete delete) throws IOException {
    HTableInterface table = null;
    try {
      table = this.pool.getTable(tableName);
      table.delete(delete);
      this.metrics.appliedOpsRate.inc(1);
      this.pool.putTable(table);
    } finally {
      if (table != null) {
        this.pool.putTable(table);
      }
    }
  }
}
