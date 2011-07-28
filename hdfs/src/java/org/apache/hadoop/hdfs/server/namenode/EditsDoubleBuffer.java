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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.Writer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;

import com.google.common.base.Preconditions;

/**
 * A double-buffer for edits. New edits are written into the first buffer
 * while the second is available to be flushed. Each time the double-buffer
 * is flushed, the two internal buffers are swapped. This allows edits
 * to progress concurrently to flushes without allocating new buffers each
 * time.
 */
class EditsDoubleBuffer {

  private DataOutputBuffer bufCurrent; // current buffer for writing
  private DataOutputBuffer bufReady; // buffer ready for flushing
  private final int initBufferSize;
  private Writer writer;

  public EditsDoubleBuffer(int defaultBufferSize) {
    initBufferSize = defaultBufferSize;
    bufCurrent = new DataOutputBuffer(initBufferSize);
    bufReady = new DataOutputBuffer(initBufferSize);
    writer = new FSEditLogOp.Writer(bufCurrent);
  }
    
  public void writeOp(FSEditLogOp op) throws IOException {
    writer.writeOp(op);
  }

  void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    bufCurrent.write(bytes, offset, length);
  }
  
  void close() throws IOException {
    Preconditions.checkNotNull(bufCurrent);
    Preconditions.checkNotNull(bufReady);

    int bufSize = bufCurrent.size();
    if (bufSize != 0) {
      throw new IOException("FSEditStream has " + bufSize
          + " bytes still to be flushed and cannot be closed.");
    }

    IOUtils.cleanup(null, bufCurrent, bufReady);
    bufCurrent = bufReady = null;
  }
  
  void setReadyToFlush() {
    assert isFlushed() : "previous data not flushed yet";
    DataOutputBuffer tmp = bufReady;
    bufReady = bufCurrent;
    bufCurrent = tmp;
    writer = new FSEditLogOp.Writer(bufCurrent);
  }
  
  /**
   * Writes the content of the "ready" buffer to the given output stream,
   * and resets it. Does not swap any buffers.
   */
  void flushTo(OutputStream out) throws IOException {
    bufReady.writeTo(out); // write data to file
    bufReady.reset(); // erase all data in the buffer
  }
  
  boolean shouldForceSync() {
    return bufReady.size() >= initBufferSize;
  }

  DataOutputBuffer getCurrentBuf() {
    return bufCurrent;
  }

  public boolean isFlushed() {
    return bufReady.size() == 0;
  }

  public int countBufferedBytes() {
    return bufReady.size() + bufCurrent.size();
  }

}
