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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * {@code BufferedBlockWriter} defines an abstraction for a reusable, memory-backed
 * buffer used in the DataNode write pipeline.
 *
 * Implementations may use off-heap (e.g., Netty) or direct I/O buffers to
 * reduce IOPs and improve disk write performance.
 *
 * <p>
 * This interface defines essential operations to manage the lifecycle of a
 * pooled write buffer, including writing data, flushing to disk, syncing for
 * durability, and releasing resources back to the pool.
 * </p>
 */
public interface BufferedBlockWriter {

  /**
   * Writes a range of data from the provided {@link ByteBuffer}
   * into the target buffer.
   *
   * @param dataBuf           the data buffer containing bytes to be written
   * @param startByteToDisk   starting byte offset within the buffer
   * @param numBytesToDisk    number of bytes to write
   * @throws IOException      if a write error occurs
   *
   * <p>Implementations are responsible for handling alignment and
   * ensuring that data is written atomically where required.</p>
   */
  void writeData(ByteBuffer dataBuf, int startByteToDisk, int numBytesToDisk)
      throws IOException;

  /**
   * Performs a data sync operation for the given block.
   *
   * @param blockName the HDFS block name associated with this buffer
   * @param isClosed whether the block file is being closed as part of sync
   *
   *          <p>
   *          Implementations may use this to ensure any pending writes are
   *          persisted to disk before marking the block as complete.
   *          </p>
   */
  void syncData(String blockName, boolean isClosed) throws IOException;

  /**
   * Flushes any in-memory data to the underlying storage target (e.g., disk or
   * channel), ensuring that buffered content is physically written but not
   * necessarily fsynced.
   *
   * @throws IOException if the flush operation fails
   */
  void flush() throws IOException;

  /**
   * Flushes or syncs data depending on the specified flags.
   *
   * @param fsync if {@code true}, ensures data is physically persisted using
   *          fsync or fdatasync
   * @param bufferFlush if {@code true}, flushes in-memory data buffers to disk
   * @param isClosed indicates if the underlying file is being closed
   * @throws IOException if any I/O error occurs during flush or sync
   *
   *           <p>
   *           This method provides a unified entry point for conditional buffer
   *           management, depending on whether only a memory flush, a full
   *           fsync, or a close operation is required.
   *           </p>
   */
  void flushOrSync(boolean fsync, boolean bufferFlush, boolean isClosed)
      throws IOException;

  /**
   * Releases this buffer and returns any associated resources (such as memory,
   * file descriptors, or concurrency permits) back to the pool.
   *
   * <p>
   * Once released, the buffer should not be used again.
   * </p>
   */
  void release();

  /**
   * Get total flushed bytes to disk.
   *
   * @return total flushed bytes.
   */
  long getFlushedBytes();

  /**
   * @return {@code true} if the buffer may hold data not yet flushed to disk.
   *
   *         <p>
   *         Used by the DataNode's idle buffer-flush task to cheaply skip a
   *         flush when nothing is pending. The default is a conservative
   *         {@code true}; a spurious {@code true} only costs an extra no-op
   *         {@link #flush()}.
   *         </p>
   */
  default boolean hasPendingData() {
    return true;
  }

}
