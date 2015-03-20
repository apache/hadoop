/**
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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.io.Closeable;

import static org.apache.hadoop.util.Time.monotonicNow;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A generic abstract class to support journaling of edits logs into 
 * a persistent storage.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class EditLogOutputStream implements Closeable {
  // these are statistics counters
  private long numSync;        // number of sync(s) to disk
  private long totalTimeSync;  // total time to sync

  public EditLogOutputStream() throws IOException {
    numSync = totalTimeSync = 0;
  }

  /**
   * Write edits log operation to the stream.
   * 
   * @param op operation
   * @throws IOException
   */
  abstract public void write(FSEditLogOp op) throws IOException;

  /**
   * Write raw data to an edit log. This data should already have
   * the transaction ID, checksum, etc included. It is for use
   * within the BackupNode when replicating edits from the
   * NameNode.
   *
   * @param bytes the bytes to write.
   * @param offset offset in the bytes to write from
   * @param length number of bytes to write
   * @throws IOException
   */
  abstract public void writeRaw(byte[] bytes, int offset, int length)
      throws IOException;

  /**
   * Create and initialize underlying persistent edits log storage.
   * 
   * @param layoutVersion The LayoutVersion of the journal
   * @throws IOException
   */
  abstract public void create(int layoutVersion) throws IOException;

  /**
   * Close the journal.
   * @throws IOException if the journal can't be closed,
   *         or if there are unflushed edits
   */
  @Override
  abstract public void close() throws IOException;

  /**
   * Close the stream without necessarily flushing any pending data.
   * This may be called after a previous write or close threw an exception.
   */
  abstract public void abort() throws IOException;
  
  /**
   * All data that has been written to the stream so far will be flushed.
   * New data can be still written to the stream while flushing is performed.
   */
  abstract public void setReadyToFlush() throws IOException;

  /**
   * Flush and sync all data that is ready to be flush 
   * {@link #setReadyToFlush()} into underlying persistent store.
   * @param durable if true, the edits should be made truly durable before
   * returning
   * @throws IOException
   */
  abstract protected void flushAndSync(boolean durable) throws IOException;

  /**
   * Flush data to persistent store.
   * Collect sync metrics.
   */
  public void flush() throws IOException {
    flush(true);
  }
  
  public void flush(boolean durable) throws IOException {
    numSync++;
    long start = monotonicNow();
    flushAndSync(durable);
    long end = monotonicNow();
    totalTimeSync += (end - start);
  }

  /**
   * Implement the policy when to automatically sync the buffered edits log
   * The buffered edits can be flushed when the buffer becomes full or
   * a certain period of time is elapsed.
   * 
   * @return true if the buffered data should be automatically synced to disk
   */
  public boolean shouldForceSync() {
    return false;
  }
  
  /**
   * Return total time spent in {@link #flushAndSync(boolean)}
   */
  long getTotalSyncTime() {
    return totalTimeSync;
  }

  /**
   * Return number of calls to {@link #flushAndSync(boolean)}
   */
  protected long getNumSync() {
    return numSync;
  }

  /**
   * @return a short text snippet suitable for describing the current
   * status of the stream
   */
  public String generateReport() {
    return toString();
  }
}
