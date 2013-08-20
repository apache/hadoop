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
package org.apache.hadoop.fs;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A ZeroCopyCursor allows you to make zero-copy reads.
 * 
 * Cursors should be closed when they are no longer needed.
 * 
 * Example:
 *   FSDataInputStream fis = fs.open("/file");
 *   ZeroCopyCursor cursor = fis.createZeroCopyCursor();
 *   try {
 *     cursor.read(128);
 *     ByteBuffer data = cursor.getData();
 *     processData(data);
 *   } finally {
 *     cursor.close();
 *   }
 */
public interface ZeroCopyCursor extends Closeable {
  /**
   * Set the fallback buffer used for this zero copy cursor.
   * The fallback buffer is used when a true zero-copy read is impossible.
   * If there is no fallback buffer, UnsupportedOperationException is thrown
   * when a true zero-copy read cannot be done.
   * 
   * @param fallbackBuffer          The fallback buffer to set, or null for none.
   */
  public void setFallbackBuffer(ByteBuffer fallbackBuffer);

  /**
   * @return the fallback buffer in use, or null if there is none.
   */
  public ByteBuffer getFallbackBuffer();
  
  /**
   * @param skipChecksums   Whether we should skip checksumming with this 
   *                        zero copy cursor.
   */
  public void setSkipChecksums(boolean skipChecksums);

  /**
   * @return                Whether we should skip checksumming with this
   *                        zero copy cursor.
   */
  public boolean getSkipChecksums();
  
  /**
   * @param allowShortReads   Whether we should allow short reads.
   */
  public void setAllowShortReads(boolean allowShortReads);

  /**
   * @return                  Whether we should allow short reads.
   */
  public boolean getAllowShortReads();

  /**
   * Perform a zero-copy read.
   *
   * @param toRead          The minimum number of bytes to read.
   *                        Must not be negative.  If we hit EOF before
   *                        reading this many bytes, we will either throw
   *                        EOFException (if allowShortReads = false), or
   *                        return a short read (if allowShortReads = true).
   *                        A short read could be as short as 0 bytes.
   * @throws UnsupportedOperationException
   *             If a true zero-copy read cannot be done, and no fallback
   *             buffer was set.
   * @throws EOFException
   *             If allowShortReads = false, and we can't read all the bytes
   *             that were requested.  This will never be thrown if
   *             allowShortReads = true.
   * @throws IOException
   *             If there was an error while reading the data.
   */
  public void read(int toRead)
      throws UnsupportedOperationException, EOFException, IOException;

  /**
   * Get the current data buffer.
   *
   * This buffer will remain valid until either this cursor is closed, or we
   * call read() again on this same cursor.  You can find the amount of data
   * that was read previously by calling ByteBuffer#remaining.
   * 
   * @return                The current data buffer.
   */
  public ByteBuffer getData();
}
