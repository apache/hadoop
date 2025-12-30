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

import static java.lang.Math.max;

public class AbfsAdaptiveInputStream extends AbfsInputStream {

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

  @Override
  protected int readOneBlock(final byte[] b, final int off, final int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    if (!validate(b, off, len)) {
      return -1;
    }
    //If buffer is empty, then fill the buffer.
    if (bCursor == limit) {
      //If EOF, then return -1
      if (fCursor >= contentLength) {
        return -1;
      }

      long bytesRead = 0;
      //reset buffer to initial state - i.e., throw away existing data
      bCursor = 0;
      limit = 0;
      if (buffer == null) {
        LOG.debug("created new buffer size {}", bufferSize);
        buffer = new byte[bufferSize];
      }

      // Reset Read Type back to normal and set again based on code flow.
      tracingContext.setReadType(ReadType.NORMAL_READ);
      if (alwaysReadBufferSize) {
        bytesRead = readInternal(fCursor, buffer, 0, bufferSize, false);
      } else {
        // Enable readAhead when reading sequentially
        if (-1 == fCursorAfterLastRead || fCursorAfterLastRead == fCursor || b.length >= bufferSize) {
          LOG.debug("Sequential read with read ahead size of {}", bufferSize);
          bytesRead = readInternal(fCursor, buffer, 0, bufferSize, false);
        } else {
          /*
           * Disable queuing prefetches when random read pattern detected.
           * Instead, read ahead only for readAheadRange above what is asked by caller.
           */
          int lengthWithReadAhead = Math.min(b.length + readAheadRange, bufferSize);
          LOG.debug("Random read with read ahead size of {}", lengthWithReadAhead);
          bytesRead = readInternal(fCursor, buffer, 0, lengthWithReadAhead, true);
        }
      }
      if (firstRead) {
        firstRead = false;
      }
      if (bytesRead == -1) {
        return -1;
      }

      limit += bytesRead;
      fCursor += bytesRead;
      fCursorAfterLastRead = fCursor;
    }
    return copyToUserBuffer(b, off, len);
  }
}
