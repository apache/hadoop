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
package org.apache.hadoop.mapred.nativetask.handlers;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.buffer.BufferType;
import org.apache.hadoop.mapred.nativetask.buffer.ByteBufferDataReader;
import org.apache.hadoop.mapred.nativetask.buffer.InputBuffer;
import org.apache.hadoop.mapred.nativetask.serde.KVSerializer;
import org.apache.hadoop.mapred.nativetask.util.SizedWritable;

/**
 * collect data when signaled
 */
@InterfaceAudience.Private
public class BufferPushee<OK, OV> implements Closeable {

  private static Log LOG = LogFactory.getLog(BufferPushee.class);
  
  public final static int KV_HEADER_LENGTH = Constants.SIZEOF_KV_LENGTH;

  private InputBuffer asideBuffer;
  private final SizedWritable<OK> tmpOutputKey;
  private final SizedWritable<OV> tmpOutputValue;
  private RecordWriter<OK, OV> writer;
  private ByteBufferDataReader nativeReader;

  private KVSerializer<OK, OV> deserializer;
  private boolean closed = false;

  public BufferPushee(Class<OK> oKClass, Class<OV> oVClass,
                      RecordWriter<OK, OV> writer) throws IOException {
    tmpOutputKey = new SizedWritable<OK>(oKClass);
    tmpOutputValue = new SizedWritable<OV>(oVClass);

    this.writer = writer;

    if (null != oKClass && null != oVClass) {
      this.deserializer = new KVSerializer<OK, OV>(oKClass, oVClass);
    }
    this.nativeReader = new ByteBufferDataReader(null);
  }

  public boolean collect(InputBuffer buffer) throws IOException {
    if (closed) {
      return false;
    }
    
    final ByteBuffer input = buffer.getByteBuffer();
    if (null != asideBuffer && asideBuffer.length() > 0) {
      if (asideBuffer.remaining() > 0) {
        final byte[] output = asideBuffer.getByteBuffer().array();
        final int write = Math.min(asideBuffer.remaining(), input.remaining());
        input.get(output, asideBuffer.position(), write);
        asideBuffer.position(asideBuffer.position() + write);
      }

      if (asideBuffer.remaining() == 0 && asideBuffer.position() > 0) {
        asideBuffer.position(0);
        write(asideBuffer);
        asideBuffer.rewind(0, 0);
      }
    }

    if (input.remaining() == 0) {
      return true;
    }

    if (input.remaining() < KV_HEADER_LENGTH) {
      throw new IOException("incomplete data, input length is: " + input.remaining());
    }
    final int position = input.position();
    final int keyLength = input.getInt();
    final int valueLength = input.getInt();
    input.position(position);
    final int kvLength = keyLength + valueLength + KV_HEADER_LENGTH;
    final int remaining = input.remaining();

    if (kvLength > remaining) {
      if (null == asideBuffer || asideBuffer.capacity() < kvLength) {
        asideBuffer = new InputBuffer(BufferType.HEAP_BUFFER, kvLength);
      }
      asideBuffer.rewind(0, kvLength);

      input.get(asideBuffer.array(), 0, remaining);
      asideBuffer.position(remaining);
    } else {
      write(buffer);
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  private boolean write(InputBuffer input) throws IOException {
    if (closed) {
      return false;
    }
    int totalRead = 0;
    final int remain = input.remaining();
    this.nativeReader.reset(input);
    while (remain > totalRead) {
      final int read = deserializer.deserializeKV(nativeReader, tmpOutputKey, tmpOutputValue);
      if (read != 0) {
        totalRead += read;
        writer.write((OK) (tmpOutputKey.v), (OV) (tmpOutputValue.v));
      }
    }
    if (remain != totalRead) {
      throw new IOException("We expect to read " + remain +
                            ", but we actually read: " + totalRead);
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    if (null != writer) {
      writer.close(null);
    }
    if (null != nativeReader) {
      nativeReader.close();
    }
    closed = true;
  }
}
