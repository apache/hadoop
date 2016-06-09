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
 *
 */

package org.apache.hadoop.hdfs.web;

/**
 * Responsible for holding buffered data in the process. Hold only 1 and only
 * 1 buffer block in the memory. Buffer block
 * information is for the given file and the offset from the which the block
 * is fetched. Across the webhdfs instances if
 * same buffer block has been used then backend trip is avoided. Buffer block
 * is certainly important since ADL fetches
 * large amount of data (Default is 4MB however can be configured through
 * core-site.xml) from the backend.
 * Observation is in case of ORC/Avro kind of compressed file, buffer block
 * does not avoid few backend calls across
 * webhdfs
 * instances.
 */
final class BufferManager {
  private static final BufferManager BUFFER_MANAGER_INSTANCE = new
      BufferManager();
  private static Object lock = new Object();
  private Buffer buffer = null;
  private String fileName;

  /**
   * Constructor.
   */
  private BufferManager() {
  }

  public static Object getLock() {
    return lock;
  }

  public static BufferManager getInstance() {
    return BUFFER_MANAGER_INSTANCE;
  }

  /**
   * Validate if the current buffer block is of given stream.
   *
   * @param path   ADL stream path
   * @param offset Stream offset that caller is interested in
   * @return True if the buffer block is available otherwise false
   */
  boolean hasValidDataForOffset(String path, long offset) {
    if (this.fileName == null) {
      return false;
    }

    if (!this.fileName.equals(path)) {
      return false;
    }

    if (buffer == null) {
      return false;
    }

    if ((offset < buffer.offset) || (offset >= (buffer.offset
        + buffer.data.length))) {
      return false;
    }

    return true;
  }

  /**
   * Clean buffer block.
   */
  void clear() {
    buffer = null;
  }

  /**
   * Validate if the current buffer block is of given stream. For now partial
   * data available is not supported.
   * Data must be available exactly or within the range of offset and size
   * passed as parameter.
   *
   * @param path   Stream path
   * @param offset Offset of the stream
   * @param size   Size of the data from the offset of the stream caller
   *               interested in
   * @return True if the data is available from the given offset and of the
   * size caller is interested in.
   */
  boolean hasData(String path, long offset, int size) {

    if (!hasValidDataForOffset(path, offset)) {
      return false;
    }

    if ((size + offset) > (buffer.data.length + buffer.offset)) {
      return false;
    }
    return true;
  }

  /**
   * Return the buffer block from the requested offset. It is caller
   * responsibility to check if the buffer block is
   * of there interest and offset is valid.
   *
   * @param data   Byte array to be filed from the buffer block
   * @param offset Data to be fetched from the offset.
   */
  void get(byte[] data, long offset) {
    System.arraycopy(buffer.data, (int) (offset - buffer.offset), data, 0,
        data.length);
  }

  /**
   * Create new empty buffer block of the given size.
   *
   * @param len Size of the buffer block.
   * @return Empty byte array.
   */
  byte[] getEmpty(int len) {
    return new byte[len];
  }

  /**
   * This function allows caller to specify new buffer block for the stream
   * which is pulled from the backend.
   *
   * @param data   Buffer
   * @param path   Stream path to which buffer belongs to
   * @param offset Stream offset where buffer start with
   */
  void add(byte[] data, String path, long offset) {
    if (data == null) {
      return;
    }

    buffer = new Buffer();
    buffer.data = data;
    buffer.offset = offset;
    this.fileName = path;
  }

  /**
   * @return Size of the buffer.
   */
  int getBufferSize() {
    return buffer.data.length;
  }

  /**
   * @return Stream offset where buffer start with
   */
  long getBufferOffset() {
    return buffer.offset;
  }

  /**
   * Buffer container.
   */
  static class Buffer {
    private byte[] data;
    private long offset;
  }
}
