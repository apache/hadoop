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

package org.apache.hadoop.mapred.nativetask.buffer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.nativetask.DataReceiver;
import org.apache.hadoop.mapred.nativetask.NativeDataSource;
import org.apache.hadoop.mapred.nativetask.NativeDataTarget;
import org.apache.hadoop.mapred.nativetask.handlers.BufferPullee;
import org.apache.hadoop.mapred.nativetask.handlers.BufferPuller;
import org.apache.hadoop.mapred.nativetask.handlers.BufferPushee;
import org.apache.hadoop.mapred.nativetask.handlers.BufferPusher;
import org.apache.hadoop.mapred.nativetask.handlers.IDataLoader;
import org.apache.hadoop.mapred.nativetask.testutil.TestInput;
import org.apache.hadoop.mapred.nativetask.testutil.TestInput.KV;
import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;
import org.apache.hadoop.util.Progress;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings({ "rawtypes", "unchecked"})
public class TestBufferPushPull {

  public static int BUFFER_LENGTH = 100; // 100 bytes
  public static int INPUT_KV_COUNT = 1000;
  private KV<BytesWritable, BytesWritable>[] dataInput;

  @Before
  public void setUp() {
    this.dataInput = TestInput.getMapInputs(INPUT_KV_COUNT);
  }

  @Test
  public void testPush() throws Exception {
    final byte[] buff = new byte[BUFFER_LENGTH];

    final InputBuffer input = new InputBuffer(buff);

    final OutputBuffer out = new OutputBuffer(buff);

    final Class<BytesWritable> iKClass = BytesWritable.class;
    final Class<BytesWritable> iVClass = BytesWritable.class;

    final RecordWriterForPush writer = new RecordWriterForPush() {
      @Override
      public void write(BytesWritable key, BytesWritable value) throws IOException {
        final KV expect = dataInput[count++];
        Assert.assertEquals(expect.key.toString(), key.toString());
        Assert.assertEquals(expect.value.toString(), value.toString());
      }
    };

    final BufferPushee pushee = new BufferPushee(iKClass, iVClass, writer);

    final PushTarget handler = new PushTarget(out) {

      @Override
      public void sendData() throws IOException {
        final int outputLength = out.length();
        input.rewind(0, outputLength);
        out.rewind();
        pushee.collect(input);
      }
    };

    final BufferPusher pusher = new BufferPusher(iKClass, iVClass, handler);

    writer.reset();
    for (int i = 0; i < INPUT_KV_COUNT; i++) {
      pusher.collect(dataInput[i].key, dataInput[i].value);
    }
    pusher.close();
    pushee.close();
  }

  @Test
  public void testPull() throws Exception {
    final byte[] buff = new byte[BUFFER_LENGTH];

    final InputBuffer input = new InputBuffer(buff);

    final OutputBuffer out = new OutputBuffer(buff);

    final Class<BytesWritable> iKClass = BytesWritable.class;
    final Class<BytesWritable> iVClass = BytesWritable.class;

    final NativeHandlerForPull handler = new NativeHandlerForPull(input, out);

    final KeyValueIterator iter = new KeyValueIterator();
    final BufferPullee pullee = new BufferPullee(iKClass, iVClass, iter, handler);
    handler.setDataLoader(pullee);

    final BufferPuller puller = new BufferPuller(handler);
    handler.setDataReceiver(puller);

    int count = 0;

    while (puller.next()) {
      final DataInputBuffer key = puller.getKey();
      final DataInputBuffer value = puller.getValue();

      final BytesWritable keyBytes = new BytesWritable();
      final BytesWritable valueBytes = new BytesWritable();

      keyBytes.readFields(key);
      valueBytes.readFields(value);

      Assert.assertEquals(dataInput[count].key.toString(), keyBytes.toString());
      Assert.assertEquals(dataInput[count].value.toString(), valueBytes.toString());

      count++;
    }

    puller.close();
    pullee.close();
  }

  public abstract class PushTarget implements NativeDataTarget {
    OutputBuffer out;

    PushTarget(OutputBuffer out) {
      this.out = out;
    }

    @Override
    public abstract void sendData() throws IOException;

    @Override
    public void finishSendData() throws IOException {
      sendData();
    }

    @Override
    public OutputBuffer getOutputBuffer() {
      return out;
    }
  }

  public abstract class RecordWriterForPush implements RecordWriter<BytesWritable, BytesWritable> {

    protected int count = 0;

    RecordWriterForPush() {
    }

    @Override
    public abstract void write(BytesWritable key, BytesWritable value) throws IOException;

    @Override
    public void close(Reporter reporter) throws IOException {
    }

    public void reset() {
      count = 0;
    }
  };

  public static class NativeHandlerForPull implements NativeDataSource, NativeDataTarget {

    InputBuffer in;
    private final OutputBuffer out;

    private IDataLoader dataLoader;
    private DataReceiver dataReceiver;

    public NativeHandlerForPull(InputBuffer input, OutputBuffer out) {
      this.in = input;
      this.out = out;
    }

    @Override
    public InputBuffer getInputBuffer() {
      return in;
    }

    @Override
    public void setDataReceiver(DataReceiver handler) {
      this.dataReceiver = handler;
    }

    @Override
    public void loadData() throws IOException {
      final int size = dataLoader.load();
    }

    public void setDataLoader(IDataLoader dataLoader) {
      this.dataLoader = dataLoader;
    }

    @Override
    public void sendData() throws IOException {
      final int len = out.length();
      out.rewind();
      in.rewind(0, len);
      dataReceiver.receiveData();
    }

    @Override
    public void finishSendData() throws IOException {
      dataReceiver.receiveData();
    }

    @Override
    public OutputBuffer getOutputBuffer() {
      return this.out;
    }
  }

  public class KeyValueIterator implements RawKeyValueIterator {
    int count = 0;
    BytesWritable key;
    BytesWritable value;

    @Override
    public DataInputBuffer getKey() throws IOException {
      return convert(key);
    }

    @Override
    public DataInputBuffer getValue() throws IOException {
      return convert(value);
    }

    private DataInputBuffer convert(BytesWritable b) throws IOException {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      b.write(new DataOutputStream(out));
      final byte[] array = out.toByteArray();
      final DataInputBuffer result = new DataInputBuffer();
      result.reset(array, array.length);
      return result;
    }

    @Override
    public boolean next() throws IOException {
      if (count < INPUT_KV_COUNT) {
        key = dataInput[count].key;
        value = dataInput[count].key;
        count++;
        return true;
      }
      return false;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Progress getProgress() {
      return null;
    }
  };
}
