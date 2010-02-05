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
package org.apache.hadoop.hbase.regionserver.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is responsible for replicating the edits coming
 * from another cluster. All edits are first put into a log that will
 * be read later by the main thread.
 *
 * This replication process is currently waiting for the edits to be applied
 * before any other entry can be appended to the log.
 *
 * The log is rolled but old ones aren't kept at the moment.
 */
public class ReplicationSink extends Thread {

  public static final String REPLICATION_LOG_DIR = ".replogs";

  static final Log LOG = LogFactory.getLog(ReplicationSink.class);
  private final Configuration conf;

  private final HTablePool pool;

  private final AtomicBoolean stop;

  private HLog.Reader reader;

  private HLog.Writer writer;

  private final FileSystem fs;

  private Path path;

  private long position = 0;

  private final Lock lock = new ReentrantLock();

  private final Condition newData  = lock.newCondition();

  private final AtomicLong editsSize = new AtomicLong(0);

  private long lastEditSize = 0;

  private final long logrollsize;

  private final long threadWakeFrequency;

  /**
   * Create a sink for replication
   * @param conf conf object
   * @param stopper boolean to tell this thread to stop
   * @param path the path to the log
   * @param fs the filesystem to use
   * @param threadWakeFrequency how long should the thread wait for edits
   * @throws IOException thrown when HDFS goes bad or bad file name
   */
  public ReplicationSink(final Configuration conf,
                         final AtomicBoolean stopper, Path path,
                         FileSystem fs, long threadWakeFrequency)
                         throws IOException {
    this.conf = conf;
    this.pool = new HTablePool(this.conf, 10);
    this.stop = stopper;
    this.fs = fs;
    this.path = path;
    long blocksize = conf.getLong("hbase.regionserver.hlog.blocksize",
      this.fs.getDefaultBlockSize());
    float multi = conf.getFloat("hbase.regionserver.logroll.multiplier", 0.95f);
    this.logrollsize = (long)(blocksize * multi);
    this.threadWakeFrequency = threadWakeFrequency;
    rollLog();

  }

  /**
   * Put this array of entries into a log that will be read later
   * @param entries
   * @throws IOException
   */
  public void replicateEntries(HLog.Entry[] entries)
      throws IOException {
    try {
      this.lock.lock();
      if(!this.stop.get()) {
        // add to WAL and defer actual inserts
        try {
          for(HLog.Entry entry : entries) {

            this.writer.append(entry);
            this.editsSize.addAndGet(entry.getKey().heapSize() +
                entry.getEdit().heapSize());
          }
          this.writer.sync();
          this.newData.signal();

        } catch (IOException ioe) {
          LOG.error("Unable to accept edit because", ioe);
          throw ioe;
        }
      } else {
        LOG.info("Won't be replicating data as we are shutting down");
      }
    } finally {
      this.lock.unlock();

    }
  }

  public void run() {

    try {
      HTableInterface table = null;
      this.lock.lock();
      while (!this.stop.get()) {
        this.newData.await(this.threadWakeFrequency, TimeUnit.MILLISECONDS);
        try {
          if(this.lastEditSize == this.editsSize.get()) {
            continue;
          }
          // There's no tailing in HDFS so we create a new reader
          // and seek every time
          this.reader = HLog.getReader(this.fs, this.path, this.conf);

          if (position != 0) {
            this.reader.seek(position);
          }

          byte[] lastTable = HConstants.EMPTY_BYTE_ARRAY;
          List<Put> puts = new ArrayList<Put>();

          // Very simple optimization where we batch sequences of rows going
          // to the same table.
          HLog.Entry entry = new HLog.Entry();
          while (this.reader.next(entry) != null) {
            KeyValue kv = entry.getEdit();

            if (kv.isDelete()) {
              Delete delete = new Delete(kv.getRow(), kv.getTimestamp(), null);
              if (kv.isDeleteFamily()) {
                delete.deleteFamily(kv.getFamily());
              } else if (!kv.isEmptyColumn()) {
                delete.deleteColumn(entry.getEdit().getFamily(),
                    kv.getQualifier());
              }
              table = pool.getTable(entry.getKey().getTablename());
              table.delete(delete);
              pool.putTable(table);

            } else {
              Put put = new Put(kv.getRow(), kv.getTimestamp(), null);
              put.add(entry.getEdit().getFamily(),
                  kv.getQualifier(), kv.getValue());
              // Switching table, flush
              if (!Bytes.equals(lastTable, entry.getKey().getTablename())
                  && !puts.isEmpty()) {
                table = pool.getTable(lastTable);
                table.put(puts);
                pool.putTable(table);
                puts.clear();
              }
              lastTable = entry.getKey().getTablename();
              puts.add(put);
            }
          }

          if (!puts.isEmpty()) {
            table = pool.getTable(lastTable);
            table.put(puts);
            pool.putTable(table);
          }

          position = this.reader.getPosition();

          if(this.editsSize.get() > this.logrollsize) {
            rollLog();
          }
          this.lastEditSize = editsSize.get();


        } catch (EOFException eof) {
          LOG.warn("Got EOF while reading, will continue on next notify");
        } catch (TableNotFoundException ex) {
          LOG.warn("Losing edits because: " + ex);
        } finally {
          this.newData.signal();
          if(this.reader != null) {
            this.reader.close();
          }
          this.reader = null;
        }

      }
      close();
    } catch (Exception ex) {
      // Should we log rejected edits in a file for replay?
      LOG.error("Unable to accept edit because", ex);
      this.stop.set(true);
    } finally {
      this.lock.unlock();
    }
  }

  private void close() throws IOException {
    this.writer.close();
    if(reader != null) {
      this.reader.close();
    }
    this.fs.delete(this.path,true);
  }

  // Delete the current log and start a new one with the same name
  // TODO keep the old versions so that the writing thread isn't help up
  // by the reading thead and this latter one could be reading older logs.
  // At this point we are under the lock.
  protected void rollLog() throws IOException {
    if(! (this.editsSize.get() == 0)) {
      this.writer.close();
      if(this.reader != null) {
        this.reader.close();
      }
      this.fs.delete(this.path,true);
    }
    this.writer = HLog.createWriter(this.fs, this.path, this.conf);
    this.editsSize.set(0);
    this.position = 0;
    LOG.debug("New replication log");
  }

  /**
   * Get the path of the file for this server
   * @param serverName
   * @return
   */
  public static String getRepLogPath(String serverName) {
    StringBuilder dirName = new StringBuilder(REPLICATION_LOG_DIR);
    dirName.append("/");
    dirName.append(serverName);
    return dirName.toString();
  }
}
