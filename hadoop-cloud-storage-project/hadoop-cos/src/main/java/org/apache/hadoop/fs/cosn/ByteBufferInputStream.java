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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;

/**
 * The input stream class is used for buffered files.
 * The purpose of providing this class is to optimize buffer read performance.
 */
public class ByteBufferInputStream extends InputStream {
  private ByteBuffer byteBuffer;
  private boolean isClosed;

  public ByteBufferInputStream(ByteBuffer byteBuffer) throws IOException {
    if (null == byteBuffer) {
      throw new IOException("byte buffer is null");
    }
    this.byteBuffer = byteBuffer;
    this.isClosed = false;
  }

  @Override
  public int read() throws IOException {
    if (null == this.byteBuffer) {
      throw new IOException("this byte buffer for InputStream is null");
    }
    if (!this.byteBuffer.hasRemaining()) {
      return -1;
    }
    return this.byteBuffer.get() & 0xFF;
  }

  @Override
  public synchronized void mark(int readLimit) {
    if (!this.markSupported()) {
      return;
    }
    this.byteBuffer.mark();
    // Parameter readLimit is ignored
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public synchronized void reset() throws IOException {
    if (this.isClosed) {
      throw new IOException("Closed in InputStream");
    }
    try {
      this.byteBuffer.reset();
    } catch (InvalidMarkException e) {
      throw new IOException("Invalid mark");
    }
  }

  @Override
  public int available() {
    return this.byteBuffer.remaining();
  }

  @Override
  public void close() {
    this.byteBuffer.rewind();
    this.byteBuffer = null;
    this.isClosed = true;
  }
}
