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
package org.apache.hadoop.hdfs;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ZeroCopyCursor;
import org.apache.hadoop.hdfs.client.ClientMmap;

public class HdfsZeroCopyCursor implements ZeroCopyCursor  {
  public static final Log LOG = LogFactory.getLog(HdfsZeroCopyCursor.class);
  private DFSInputStream stream;
  private boolean skipChecksums;
  private boolean allowShortReads;
  private ClientMmap mmap;
  private ByteBuffer fallbackBuffer;
  private ByteBuffer readBuffer;
  
  HdfsZeroCopyCursor(DFSInputStream stream, boolean skipChecksums) {
    this.stream = stream;
    this.skipChecksums = skipChecksums;
    this.allowShortReads = false;
    this.mmap = null;
    this.fallbackBuffer = null;
    this.readBuffer = null;
  }

  @Override
  public void close() throws IOException {
    stream = null;
    if (mmap != null) {
      mmap.unref();
      mmap = null;
    }
    fallbackBuffer = null;
    readBuffer = null;
  }

  @Override
  public void setFallbackBuffer(ByteBuffer fallbackBuffer) {
    this.fallbackBuffer = fallbackBuffer;
  }

  @Override
  public ByteBuffer getFallbackBuffer() {
    return this.fallbackBuffer;
  }

  @Override
  public void setSkipChecksums(boolean skipChecksums) {
    this.skipChecksums = skipChecksums;
  }

  @Override
  public boolean getSkipChecksums() {
    return this.skipChecksums;
  }

  @Override
  public void setAllowShortReads(boolean allowShortReads) {
    this.allowShortReads = allowShortReads;
  }

  @Override
  public boolean getAllowShortReads() {
    return this.allowShortReads;
  }

  @Override
  public void read(int toRead) throws UnsupportedOperationException,
      EOFException, IOException {
    if (toRead < 0) {
      throw new IllegalArgumentException("can't read " + toRead + " bytes.");
    }
    stream.readZeroCopy(this, toRead);
  }

  @Override
  public ByteBuffer getData() {
    return readBuffer;
  }
  
  int readViaSlowPath(int toRead) throws EOFException, IOException {
    if (fallbackBuffer == null) {
      throw new UnsupportedOperationException("unable to read via " +
          "the fastpath, and there was no fallback buffer provided.");
    }
    fallbackBuffer.clear();
    fallbackBuffer.limit(toRead); // will throw if toRead is too large
  
    int totalRead = 0;
    readBuffer = fallbackBuffer;
    try {
      while (toRead > 0) {
        int nread = stream.read(fallbackBuffer);
        if (nread < 0) {
          break;
        }
        toRead -= nread;
        totalRead += nread;
        if (allowShortReads) {
          break;
        }
      }
    } finally {
      fallbackBuffer.flip();
    }
    if ((toRead > 0) && (!allowShortReads)) {
      throw new EOFException("only read " + totalRead + " bytes out of " +
          "a requested " + toRead + " before hitting EOF");
    }
    return totalRead;
  }
  
  void setMmap(ClientMmap mmap, ByteBuffer readBuffer) {
    if (this.mmap != mmap) {
      if (this.mmap != null) {
        this.mmap.unref();
      }
    }
    this.mmap = mmap;
    mmap.ref();
    this.readBuffer = readBuffer;
  }

  ClientMmap getMmap() {
    return mmap;
  }
}
