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

package org.apache.hadoop.mapred.nativetask.serde;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.buffer.DataInputStream;
import org.apache.hadoop.mapred.nativetask.buffer.DataOutputStream;
import org.apache.hadoop.mapred.nativetask.util.SizedWritable;



@InterfaceAudience.Private
public class KVSerializer<K, V> implements IKVSerializer {

  private static final Log LOG = LogFactory.getLog(KVSerializer.class);
  
  public static final int KV_HEAD_LENGTH = Constants.SIZEOF_KV_LENGTH;

  private final INativeSerializer<Writable> keySerializer;
  private final INativeSerializer<Writable> valueSerializer;

  public KVSerializer(Class<K> kclass, Class<V> vclass) throws IOException {
    
    this.keySerializer = NativeSerialization.getInstance().getSerializer(kclass);
    this.valueSerializer = NativeSerialization.getInstance().getSerializer(vclass);
  }

  @Override
  public void updateLength(SizedWritable<?> key, SizedWritable<?> value) throws IOException {
    key.length = keySerializer.getLength(key.v);
    value.length = valueSerializer.getLength(value.v);
    return;
  }

  @Override
  public int serializeKV(DataOutputStream out, SizedWritable<?> key, SizedWritable<?> value)
    throws IOException {
    return serializePartitionKV(out, -1, key, value);
  }

  @Override
  public int serializePartitionKV(DataOutputStream out, int partitionId,
      SizedWritable<?> key, SizedWritable<?> value)
      throws IOException {

    if (key.length == SizedWritable.INVALID_LENGTH ||
        value.length == SizedWritable.INVALID_LENGTH) {
      updateLength(key, value);
    }

    final int keyLength = key.length;
    final int valueLength = value.length;

    int bytesWritten = KV_HEAD_LENGTH + keyLength + valueLength;
    if (partitionId != -1) {
      bytesWritten += Constants.SIZEOF_PARTITION_LENGTH;
    }

    if (out.hasUnFlushedData() && out.shortOfSpace(bytesWritten)) {
      out.flush();
    }

    if (partitionId != -1) {
      out.writeInt(partitionId);
    }
        
    out.writeInt(keyLength);
    out.writeInt(valueLength);
    
    keySerializer.serialize(key.v, out);
    valueSerializer.serialize(value.v, out);

    return bytesWritten;
  }

  @Override
  public int deserializeKV(DataInputStream in, SizedWritable<?> key,
      SizedWritable<?> value) throws IOException {

    if (!in.hasUnReadData()) {
      return 0;
    }

    key.length = in.readInt();
    value.length = in.readInt();

    keySerializer.deserialize(in, key.length, key.v);
    valueSerializer.deserialize(in, value.length, value.v);

    return key.length + value.length + KV_HEAD_LENGTH;
  }

}
