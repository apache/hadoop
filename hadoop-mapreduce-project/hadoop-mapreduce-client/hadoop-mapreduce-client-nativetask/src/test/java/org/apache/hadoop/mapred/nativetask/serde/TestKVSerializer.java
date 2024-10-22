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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.buffer.DataInputStream;
import org.apache.hadoop.mapred.nativetask.buffer.DataOutputStream;
import org.apache.hadoop.mapred.nativetask.testutil.TestInput;
import org.apache.hadoop.mapred.nativetask.testutil.TestInput.KV;
import org.apache.hadoop.mapred.nativetask.util.SizedWritable;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestKVSerializer {

  int inputArraySize = 1000; // 1000 bytesWriable elements
  int bufferSize = 100; // bytes
  private KV<BytesWritable, BytesWritable>[] inputArray;

  final ByteArrayOutputStream result = new ByteArrayOutputStream();
  private SizedWritable key;
  private SizedWritable value;
  private KVSerializer serializer;

  @BeforeEach
  public void setUp() throws IOException {
    this.inputArray = TestInput.getMapInputs(inputArraySize);
    this.key = new SizedWritable(BytesWritable.class);
    this.value = new SizedWritable(BytesWritable.class);

    this.serializer = new KVSerializer(BytesWritable.class, BytesWritable.class);

    key.reset(inputArray[4].key);
    value.reset(inputArray[4].value);
    serializer.updateLength(key, value);
  }

  @Test
  void testUpdateLength() throws IOException {
    Mockito.mock(DataOutputStream.class);

    int kvLength = 0;
    for (int i = 0; i < inputArraySize; i++) {
      key.reset(inputArray[i].key);
      value.reset(inputArray[i].value);
      serializer.updateLength(key, value);

      // verify whether the size increase
      assertTrue(key.length + value.length > kvLength);
      kvLength = key.length + value.length;
    }
  }

  @Test
  void testSerializeKV() throws IOException {
    final DataOutputStream dataOut = Mockito.mock(DataOutputStream.class);

    Mockito.when(dataOut.hasUnFlushedData()).thenReturn(true);
    Mockito.when(dataOut.shortOfSpace(key.length + value.length +
        Constants.SIZEOF_KV_LENGTH)).thenReturn(true);
    final int written = serializer.serializeKV(dataOut, key, value);

    // flush once, write 4 int, and 2 byte array
    Mockito.verify(dataOut, Mockito.times(1)).flush();
    Mockito.verify(dataOut, Mockito.times(4)).writeInt(anyInt());
    Mockito.verify(dataOut, Mockito.times(2)).write(any(byte[].class),
        anyInt(), anyInt());

    assertEquals(written, key.length + value.length + Constants.SIZEOF_KV_LENGTH);
  }

  @Test
  void testSerializeNoFlush() throws IOException {
    final DataOutputStream dataOut = Mockito.mock(DataOutputStream.class);

    // suppose there are enough space
    Mockito.when(dataOut.hasUnFlushedData()).thenReturn(true);
    Mockito.when(dataOut.shortOfSpace(anyInt())).thenReturn(false);
    final int written = serializer.serializeKV(dataOut, key, value);

    // flush 0, write 4 int, and 2 byte array
    Mockito.verify(dataOut, Mockito.times(0)).flush();
    Mockito.verify(dataOut, Mockito.times(4)).writeInt(anyInt());
    Mockito.verify(dataOut, Mockito.times(2)).write(any(byte[].class),
        anyInt(), anyInt());

    assertEquals(written, key.length + value.length + Constants.SIZEOF_KV_LENGTH);
  }

  @Test
  void testSerializePartitionKV() throws IOException {
    final DataOutputStream dataOut = Mockito.mock(DataOutputStream.class);

    Mockito.when(dataOut.hasUnFlushedData()).thenReturn(true);
    Mockito.when(
        dataOut
            .shortOfSpace(key.length + value.length +
                Constants.SIZEOF_KV_LENGTH + Constants.SIZEOF_PARTITION_LENGTH))
        .thenReturn(true);
    final int written = serializer.serializePartitionKV(dataOut, 100, key, value);

    // flush once, write 4 int, and 2 byte array
    Mockito.verify(dataOut, Mockito.times(1)).flush();
    Mockito.verify(dataOut, Mockito.times(5)).writeInt(anyInt());
    Mockito.verify(dataOut, Mockito.times(2)).write(any(byte[].class),
        anyInt(), anyInt());

    assertEquals(written, key.length + value.length + Constants.SIZEOF_KV_LENGTH
        + Constants.SIZEOF_PARTITION_LENGTH);
  }

  @Test
  void testDeserializerNoData() throws IOException {
    final DataInputStream in = Mockito.mock(DataInputStream.class);
    Mockito.when(in.hasUnReadData()).thenReturn(false);
    assertEquals(0, serializer.deserializeKV(in, key, value));
  }

  @Test
  void testDeserializer() throws IOException {
    final DataInputStream in = Mockito.mock(DataInputStream.class);
    Mockito.when(in.hasUnReadData()).thenReturn(true);
    assertTrue(serializer.deserializeKV(in, key, value) > 0);

    Mockito.verify(in, Mockito.times(4)).readInt();
    Mockito.verify(in, Mockito.times(2)).readFully(any(byte[].class),
        anyInt(), anyInt());
  }
}
