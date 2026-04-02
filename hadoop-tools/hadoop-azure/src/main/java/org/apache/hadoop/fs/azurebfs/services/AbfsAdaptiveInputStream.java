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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.constants.ReadType;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

/**
 * Input stream implementation optimized for adaptive read patterns.
 * This is the default implementation used for cases where user does not specify any input policy.
 * It switches between sequential and random read optimizations based on the detected read pattern.
 * It also keeps footer read and small file optimizations enabled.
 */
public class AbfsAdaptiveInputStream extends AbfsInputStream {

  /**
   * Constructs AbfsAdaptiveInputStream instance.
   * @param client to be used for read operations
   * @param statistics to record input stream statistics
   * @param path file path
   * @param contentLength file content length
   * @param abfsInputStreamContext input stream context
   * @param eTag file eTag
   * @param tracingContext tracing context to trace the read operations
   */
  public AbfsAdaptiveInputStream(
      final AbfsClient client,
      final FileSystem.Statistics statistics,
      final String path,
      final long contentLength,
      final AbfsInputStreamContext abfsInputStreamContext,
      final String eTag,
      TracingContext tracingContext) {
    super(client, statistics, path, contentLength,
        abfsInputStreamContext, eTag, tracingContext);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected int readOneBlock(final byte[] b, final int off, final int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    if (!validate(b, off, len)) {
      return -1;
    }
    // If buffer is empty, then fill the buffer.
    if (getBCursor() == getLimit()) {
      // If EOF, then return -1
      if (!(shouldRestrictGpsOnOpenFile() && isFirstRead()) && getFCursor() >= getContentLength()) {
        return -1;
      }

      long bytesRead = 0;
      // reset buffer to initial state - i.e., throw away existing data
      setBCursor(0);
      setLimit(0);
      if (getBuffer() == null) {
        LOG.debug("created new buffer size {}", getBufferSize());
        setBuffer(new byte[getBufferSize()]);
      }

      // Reset Read Type back to normal and set again based on code flow.
      getTracingContext().setReadType(ReadType.NORMAL_READ);

      // If restrictGpsOnOpenFile config is enabled, skip prefetch for the first read since contentLength
      // is not available yet.
      if (shouldRestrictGpsOnOpenFile() && isFirstRead()) {
        LOG.debug("RestrictGpsOnOpenFile is enabled. Skip readahead for first read.");
        bytesRead = readInternal(getFCursor(), getBuffer(), 0, getBufferSize(), true);
      }
      else if (shouldAlwaysReadBufferSize()) {
        bytesRead = readInternal(getFCursor(), getBuffer(), 0, getBufferSize(), false);
      } else {
        // Enable readAhead when reading sequentially
        if (-1 == getFCursorAfterLastRead() || getFCursorAfterLastRead() == getFCursor() || b.length >= getBufferSize()) {
          LOG.debug("Sequential read with read ahead size of {}", getBufferSize());
          bytesRead = readInternal(getFCursor(), getBuffer(), 0, getBufferSize(), false);
        } else {
          /*
           * Disable queuing prefetches when random read pattern detected.
           * Instead, read ahead only for readAheadRange above what is asked by caller.
           */
          getTracingContext().setReadType(ReadType.RANDOM_READ);
          int lengthWithReadAhead = Math.min(b.length + getReadAheadRange(), getBufferSize());
          LOG.debug("Random read with read ahead size of {}", lengthWithReadAhead);
          bytesRead = readInternal(getFCursor(), getBuffer(), 0, lengthWithReadAhead, true);
        }
      }
      if (isFirstRead()) {
        setFirstRead(false);
      }
      if (bytesRead == -1) {
        return -1;
      }

      setLimit(getLimit() + (int) bytesRead);
      setFCursor(getFCursor() + bytesRead);
      setFCursorAfterLastRead(getFCursor());
    }
    return copyToUserBuffer(b, off, len);
  }
}
