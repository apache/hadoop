/*
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Not thread safe!
 */
public class ByteBufferOutputStream extends OutputStream {

  protected ByteBuffer buf;

  public ByteBufferOutputStream(int capacity) {
    this(capacity, false);
  }

  public ByteBufferOutputStream(int capacity, boolean useDirectByteBuffer) {
    if (useDirectByteBuffer) {
      buf = ByteBuffer.allocateDirect(capacity);
    } else {
      buf = ByteBuffer.allocate(capacity);
    }
  }

  public int size() {
    return buf.position();
  }

  /**
   * This flips the underlying BB so be sure to use it _last_!
   * @return ByteBuffer
   */
  public ByteBuffer getByteBuffer() {
    buf.flip();
    return buf;
  }

  private void checkSizeAndGrow(int extra) {
    if ( (buf.position() + extra) > buf.limit()) {
      // size calculation is complex, because we could overflow negative,
      // and/or not allocate enough space. this fixes that.
      int newSize = (int)Math.min((((long)buf.capacity()) * 2),
          (long)(Integer.MAX_VALUE));
      newSize = Math.max(newSize, buf.position() + extra);

      ByteBuffer newBuf = ByteBuffer.allocate(newSize);
      buf.flip();
      newBuf.put(buf);
      buf = newBuf;
    }
  }

  // OutputStream
  @Override
  public void write(int b) throws IOException {
    checkSizeAndGrow(Bytes.SIZEOF_BYTE);

    buf.put((byte)b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    checkSizeAndGrow(b.length);

    buf.put(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkSizeAndGrow(len);

    buf.put(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    // noop
  }

  @Override
  public void close() throws IOException {
    // noop again. heh
  }
}
