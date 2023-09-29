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
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * The input stream class is used for buffered files.
 * The purpose of providing this class is to optimize buffer write performance.
 */
public class ByteBufferOutputStream extends OutputStream {
  private ByteBuffer byteBuffer;
  private boolean isFlush;
  private boolean isClosed;

  public ByteBufferOutputStream(ByteBuffer byteBuffer) throws IOException {
    if (null == byteBuffer) {
      throw new IOException("byte buffer is null");
    }
    this.byteBuffer = byteBuffer;
    this.byteBuffer.clear();
    this.isFlush = false;
    this.isClosed = false;
  }

  @Override
  public void write(int b) {
    byte[] singleBytes = new byte[1];
    singleBytes[0] = (byte) b;
    this.byteBuffer.put(singleBytes, 0, 1);
    this.isFlush = false;
  }

  @Override
  public void flush() {
    if (this.isFlush) {
      return;
    }
    this.isFlush = true;
  }

  @Override
  public void close() throws IOException {
    if (this.isClosed) {
      return;
    }
    if (null == this.byteBuffer) {
      throw new IOException("Can not close a null object");
    }

    this.flush();
    this.byteBuffer.flip();
    this.byteBuffer = null;
    this.isFlush = false;
    this.isClosed = true;
  }
}
