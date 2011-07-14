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
import java.io.OutputStream;
import java.util.zip.Checksum;

import static org.apache.hadoop.hdfs.server.common.Util.now;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;

/**
 * A generic abstract class to support journaling of edits logs into 
 * a persistent storage.
 */
abstract class EditLogOutputStream 
implements JournalStream {
  // these are statistics counters
  private long numSync;        // number of sync(s) to disk
  private long totalTimeSync;  // total time to sync

  EditLogOutputStream() throws IOException {
    numSync = totalTimeSync = 0;
  }

  /** {@inheritDoc} */
  abstract public void write(int b) throws IOException;

  /**
   * Write edits log record into the stream.
   * The record is represented by operation name and
   * an array of Writable arguments.
   * 
   * @param op operation
   * @param txid the transaction ID of this operation
   * @param writables array of Writable arguments
   * @throws IOException
   */
  abstract void write(byte op, long txid, Writable ... writables)
  throws IOException;
  abstract void write(byte[] data, int i, int length) throws IOException;
  
  /**
   * Create and initialize underlying persistent edits log storage.
   * 
   * @throws IOException
   */
  abstract void create() throws IOException;

  /** {@inheritDoc} */
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
  abstract void setReadyToFlush() throws IOException;

  /**
   * Flush and sync all data that is ready to be flush 
   * {@link #setReadyToFlush()} into underlying persistent store.
   * @throws IOException
   */
  abstract protected void flushAndSync() throws IOException;

  /**
   * Flush data to persistent store.
   * Collect sync metrics.
   */
  public void flush() throws IOException {
    numSync++;
    long start = now();
    flushAndSync();
    long end = now();
    totalTimeSync += (end - start);
  }

  /**
   * Return the size of the current edits log.
   * Length is used to check when it is large enough to start a checkpoint.
   */
  abstract long length() throws IOException;

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
  
  boolean isOperationSupported(byte op) {
    return true;
  }

  /**
   * Return total time spent in {@link #flushAndSync()}
   */
  long getTotalSyncTime() {
    return totalTimeSync;
  }

  /**
   * Return number of calls to {@link #flushAndSync()}
   */
  long getNumSync() {
    return numSync;
  }

  @Override // Object
  public String toString() {
    return getName();
  }

  /**
   * Write the given operation to the specified buffer, including
   * the transaction ID and checksum.
   */
  protected static void writeChecksummedOp(
      DataOutputBuffer buf, byte op, long txid, Writable... writables)
      throws IOException {
    int start = buf.getLength();
    buf.write(op);
    buf.writeLong(txid);
    for (Writable w : writables) {
      w.write(buf);
    }
    // write transaction checksum
    int end = buf.getLength();
    Checksum checksum = FSEditLog.getChecksum();
    checksum.reset();
    checksum.update(buf.getData(), start, end-start);
    int sum = (int)checksum.getValue();
    buf.writeInt(sum);
  }
}
