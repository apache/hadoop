/*
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

package org.apache.hadoop.fs.bos;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import java.io.Closeable;
import java.io.IOException;

/**
 * A buffer that holds data for a single block during
 * multipart upload. Data is written to an output buffer and
 * then moved to an input buffer for uploading.
 */
public class BosBlockBuffer implements Closeable {

  /** The output buffer for writing data. */
  DataOutputBuffer outBuffer;

  /** The input buffer for reading data during upload. */
  DataInputBuffer inBuffer = new DataInputBuffer();

  private String key;
  private int blkId;

  /**
   * Constructs a BosBlockBuffer.
   *
   * @param key   the object key
   * @param blkId the block identifier
   * @param size  the initial capacity of the output buffer
   */
  public BosBlockBuffer(
      String key, int blkId, int size) {
    this.key = key;
    this.blkId = blkId;
    outBuffer = new DataOutputBuffer(size);
  }

  /**
   * Returns the object key.
   *
   * @return the key
   */
  public String getKey() {
    return key;
  }

  /**
   * Returns the block identifier.
   *
   * @return the block ID
   */
  public int getBlkId() {
    return blkId;
  }

  /**
   * Sets the block identifier.
   *
   * @param blkId the new block ID
   */
  public void setBlkId(int blkId) {
    this.blkId = blkId;
  }

  /**
   * Moves data from the output buffer to the input buffer
   * and resets the output buffer for reuse.
   */
  void moveData() {
    inBuffer.reset(
        outBuffer.getData(), outBuffer.getLength());
    outBuffer.reset();
  }

  /**
   * Clears the input buffer.
   */
  void clear() {
    inBuffer.reset(null, 0);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    if (outBuffer != null) {
      outBuffer.close();
    }
    if (inBuffer != null) {
      inBuffer.close();
    }
  }
}
