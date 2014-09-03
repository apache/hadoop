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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.nativetask.NativeDataTarget;
import org.apache.hadoop.mapred.nativetask.buffer.ByteBufferDataWriter;
import org.apache.hadoop.mapred.nativetask.serde.IKVSerializer;
import org.apache.hadoop.mapred.nativetask.serde.KVSerializer;
import org.apache.hadoop.mapred.nativetask.util.SizedWritable;

/**
 * actively push data into a buffer and signal a {@link BufferPushee} to collect it
 */
@InterfaceAudience.Private
public class BufferPusher<K, V> implements OutputCollector<K, V> {
  
  private static Log LOG = LogFactory.getLog(BufferPusher.class);

  private final SizedWritable<K> tmpInputKey;
  private final SizedWritable<V> tmpInputValue;
  private ByteBufferDataWriter out;
  IKVSerializer serializer;
  private boolean closed = false;

  public BufferPusher(Class<K> iKClass, Class<V> iVClass,
                      NativeDataTarget target) throws IOException {
    tmpInputKey = new SizedWritable<K>(iKClass);
    tmpInputValue = new SizedWritable<V>(iVClass);

    if (null != iKClass && null != iVClass) {
      this.serializer = new KVSerializer<K, V>(iKClass, iVClass);
    }
    this.out = new ByteBufferDataWriter(target);
  }

  public void collect(K key, V value, int partition) throws IOException {
    tmpInputKey.reset(key);
    tmpInputValue.reset(value);
    serializer.serializePartitionKV(out, partition, tmpInputKey, tmpInputValue);
  };

  @Override
  public void collect(K key, V value) throws IOException {
    if (closed) {
      return;
    }
    tmpInputKey.reset(key);
    tmpInputValue.reset(value);
    serializer.serializeKV(out, tmpInputKey, tmpInputValue);
  };

  public void flush() throws IOException {
    if (null != out) {
      if (out.hasUnFlushedData()) {
        out.flush();
      }
    }
  }
  
  public void close() throws IOException {
    if (closed) {
      return;
    }
    if (null != out) {
      out.close();
    }
    closed = true;
  }
}
