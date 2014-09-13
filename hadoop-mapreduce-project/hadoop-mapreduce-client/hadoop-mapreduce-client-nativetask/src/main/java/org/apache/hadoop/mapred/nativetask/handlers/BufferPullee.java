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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.NativeDataTarget;
import org.apache.hadoop.mapred.nativetask.buffer.ByteBufferDataWriter;
import org.apache.hadoop.mapred.nativetask.buffer.OutputBuffer;
import org.apache.hadoop.mapred.nativetask.serde.KVSerializer;
import org.apache.hadoop.mapred.nativetask.util.SizedWritable;

/**
 * load data into a buffer signaled by a {@link BufferPuller}
 */
@InterfaceAudience.Private
public class BufferPullee<IK, IV> implements IDataLoader {

  public static final int KV_HEADER_LENGTH = Constants.SIZEOF_KV_LENGTH;

  private final SizedWritable<IK> tmpInputKey;
  private final SizedWritable<IV> tmpInputValue;
  private boolean inputKVBufferd = false;
  private RawKeyValueIterator rIter;
  private ByteBufferDataWriter nativeWriter;
  protected KVSerializer<IK, IV> serializer;
  private final OutputBuffer outputBuffer;
  private final NativeDataTarget target;
  private boolean closed = false;
  
  public BufferPullee(Class<IK> iKClass, Class<IV> iVClass,
                      RawKeyValueIterator rIter, NativeDataTarget target)
      throws IOException {
    this.rIter = rIter;
    tmpInputKey = new SizedWritable<IK>(iKClass);
    tmpInputValue = new SizedWritable<IV>(iVClass);

    if (null != iKClass && null != iVClass) {
      this.serializer = new KVSerializer<IK, IV>(iKClass, iVClass);
    }
    this.outputBuffer = target.getOutputBuffer();
    this.target = target;
  }

  @Override
  public int load() throws IOException {
    if (closed) {
      return 0;
    }
    
    if (null == outputBuffer) {
      throw new IOException("output buffer not set");
    }

    this.nativeWriter = new ByteBufferDataWriter(target);
    outputBuffer.rewind();

    int written = 0;
    boolean firstKV = true;

    if (inputKVBufferd) {
      written += serializer.serializeKV(nativeWriter, tmpInputKey, tmpInputValue);
      inputKVBufferd = false;
      firstKV = false;
    }

    while (rIter.next()) {
      inputKVBufferd = false;
      tmpInputKey.readFields(rIter.getKey());
      tmpInputValue.readFields(rIter.getValue());
      serializer.updateLength(tmpInputKey, tmpInputValue);

      final int kvSize = tmpInputKey.length + tmpInputValue.length + KV_HEADER_LENGTH;

      if (!firstKV && nativeWriter.shortOfSpace(kvSize)) {
        inputKVBufferd = true;
        break;
      } else {
        written += serializer.serializeKV(nativeWriter, tmpInputKey, tmpInputValue);
        firstKV = false;
      }
    }

    if (nativeWriter.hasUnFlushedData()) {
      nativeWriter.flush();
    }
    return written;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    if (null != rIter) {
      rIter.close();
    }
    if (null != nativeWriter) {
      nativeWriter.close();
    }
    closed = true;
  }
}