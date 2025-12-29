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
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    return super.read(position, buffer, offset, length);
  }

  @Override
  public int read() throws IOException {
    return super.read();
  }

  @Override
  public synchronized int read(final byte[] b, final int off, final int len) throws IOException {
    // check if buffer is null before logging the length
    if (b != null) {
      LOG.debug("read requested b.length = {} offset = {} len = {}", b.length,
          off, len);
    } else {
      LOG.debug("read requested b = null offset = {} len = {}", off, len);
    }

    int currentOff = off;
    int currentLen = len;
    int lastReadBytes;
    int totalReadBytes = 0;
    if (streamStatistics != null) {
      streamStatistics.readOperationStarted();
    }
    incrementReadOps();
    do {

      // limit is the maximum amount of data present in buffer.
      // fCursor is the current file pointer. Thus maximum we can
      // go back and read from buffer is fCursor - limit.
      // There maybe case that we read less than requested data.
      long filePosAtStartOfBuffer = fCursor - limit;
      if (abfsReadFooterMetrics != null) {
        abfsReadFooterMetrics.updateReadMetrics(filePathIdentifier, len, contentLength, nextReadPos);
      }
      if (nextReadPos >= filePosAtStartOfBuffer && nextReadPos <= fCursor) {
        // Determining position in buffer from where data is to be read.
        bCursor = (int) (nextReadPos - filePosAtStartOfBuffer);

        // When bCursor == limit, buffer will be filled again.
        // So in this case we are not actually reading from buffer.
        if (bCursor != limit && streamStatistics != null) {
          streamStatistics.seekInBuffer();
        }
      } else {
        // Clearing the buffer and setting the file pointer
        // based on previous seek() call.
        fCursor = nextReadPos;
        limit = 0;
        bCursor = 0;
      }
      if (shouldReadFully()) {
        lastReadBytes = readFileCompletely(b, currentOff, currentLen);
      } else if (shouldReadLastBlock()) {
        lastReadBytes = readLastBlock(b, currentOff, currentLen);
      } else {
        lastReadBytes = readOneBlock(b, currentOff, currentLen);
      }
      if (lastReadBytes > 0) {
        currentOff += lastReadBytes;
        currentLen -= lastReadBytes;
        totalReadBytes += lastReadBytes;
      }
      if (currentLen <= 0 || currentLen > b.length - currentOff) {
        break;
      }
    } while (lastReadBytes > 0);
    return totalReadBytes > 0 ? totalReadBytes : lastReadBytes;
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
          // Enabling read ahead for random reads as well to reduce number of remote calls.
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

  private boolean shouldReadFully() {
    return this.firstRead && this.context.readSmallFilesCompletely()
        && this.contentLength <= this.bufferSize;
  }

  private boolean shouldReadLastBlock() {
    long footerStart = max(0, this.contentLength - FOOTER_SIZE);
    return this.firstRead && this.context.optimizeFooterRead()
        && this.fCursor >= footerStart;
  }
}
