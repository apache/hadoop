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
 * Input stream implementation optimized for prefetching data.
 * This implementation always prefetches data in advance if enabled
 * to optimize for sequential read patterns.
 */
public class AbfsPrefetchInputStream extends AbfsInputStream {

  /**
   * Constructs AbfsPrefetchInputStream
   * @param client AbfsClient to be used for read operations
   * @param statistics to recordinput stream statistics
   * @param path file path
   * @param contentLength file content length
   * @param abfsInputStreamContext input stream context
   * @param eTag file eTag
   * @param tracingContext tracing context to trace the read operations
   */
  public AbfsPrefetchInputStream(
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

      /*
        Skips prefetch for the first read if restrictGpsOnOpenFile config is enabled.
        This is required since contentLength is not available yet to determine prefetch block size.
       */
      if (shouldRestrictGpsOnOpenFile() && isFirstRead()) {
        getTracingContext().setReadType(ReadType.NORMAL_READ);
        LOG.debug("RestrictGpsOnOpenFile is enabled. Skip readahead for first read even for sequential input policy.");
        bytesRead = readInternal(getFCursor(), getBuffer(), 0, getBufferSize(), true);
      }
      else {
        /*
         * Always start with Prefetch even from first read UNLESS restrictGpsOnOpenFile config is enabled.
         * Even if out of order seek comes, prefetches will be triggered for next set of blocks.
         */
        bytesRead = readInternal(getFCursor(), getBuffer(), 0, getBufferSize(), false);
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
