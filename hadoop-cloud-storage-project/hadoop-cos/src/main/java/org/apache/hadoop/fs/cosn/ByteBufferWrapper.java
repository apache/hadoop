/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cosn;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.util.CleanerUtil;

/**
 * The wrapper for memory buffers and disk buffers.
 */
public class ByteBufferWrapper {
  private static final Logger LOG =
      LoggerFactory.getLogger(ByteBufferWrapper.class);
  private ByteBuffer byteBuffer;
  private File file;
  private RandomAccessFile randomAccessFile;

  ByteBufferWrapper(ByteBuffer byteBuffer) {
    this(byteBuffer, null, null);
  }

  ByteBufferWrapper(ByteBuffer byteBuffer, RandomAccessFile randomAccessFile,
      File file) {
    this.byteBuffer = byteBuffer;
    this.file = file;
    this.randomAccessFile = randomAccessFile;
  }

  public ByteBuffer getByteBuffer() {
    return this.byteBuffer;
  }

  boolean isDiskBuffer() {
    return this.file != null && this.randomAccessFile != null;
  }

  private void munmap(MappedByteBuffer buffer) {
    if (CleanerUtil.UNMAP_SUPPORTED) {
      try {
        CleanerUtil.getCleaner().freeBuffer(buffer);
      } catch (IOException e) {
        LOG.warn("Failed to unmap the buffer", e);
      }
    } else {
      LOG.trace(CleanerUtil.UNMAP_NOT_SUPPORTED_REASON);
    }
  }

  void close() throws IOException {
    if (null != this.byteBuffer) {
      this.byteBuffer.clear();
    }

    IOException exception = null;
    // catch all exceptions, and try to free up resources that can be freed.
    try {
      if (null != randomAccessFile) {
        this.randomAccessFile.close();
      }
    } catch (IOException e) {
      LOG.error("Close the random access file occurs an exception.", e);
      exception = e;
    }

    if (this.byteBuffer instanceof MappedByteBuffer) {
      munmap((MappedByteBuffer) this.byteBuffer);
    }

    if (null != this.file && this.file.exists()) {
      if (!this.file.delete()) {
        LOG.warn("Delete the tmp file: [{}] failed.",
            this.file.getAbsolutePath());
      }
    }

    if (null != exception) {
      throw exception;
    }
  }
}
