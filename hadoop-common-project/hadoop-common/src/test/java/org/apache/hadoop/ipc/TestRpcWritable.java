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

package org.apache.hadoop.ipc;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.thirdparty.protobuf.Message;

public class TestRpcWritable {//extends TestRpcBase {

  static Writable writable = new LongWritable(Time.now());
  static Message message1 =
      EchoRequestProto.newBuilder().setMessage("testing1").build();
  static Message message2 =
      EchoRequestProto.newBuilder().setMessage("testing2").build();

  @Test
  public void testWritableWrapper() throws IOException {
    // serial writable in byte buffer
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    writable.write(new DataOutputStream(baos));
    ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());

    // deserial
    LongWritable actual = RpcWritable.wrap(new LongWritable())
        .readFrom(bb);
    Assert.assertEquals(writable, actual);
    Assert.assertEquals(0, bb.remaining());
  }

  @Test
  public void testProtobufWrapper() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    message1.writeDelimitedTo(baos);
    ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());

    Message actual = RpcWritable.wrap(EchoRequestProto.getDefaultInstance())
        .readFrom(bb);
    Assert.assertEquals(message1, actual);
    Assert.assertEquals(0, bb.remaining());
  }

  @Test
  public void testBufferWrapper() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    message1.writeDelimitedTo(dos);
    message2.writeDelimitedTo(dos);
    writable.write(dos);

    ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
    RpcWritable.Buffer buf = RpcWritable.Buffer.wrap(bb);
    Assert.assertEquals(baos.size(), bb.remaining());
    Assert.assertEquals(baos.size(), buf.remaining());

    Object actual = buf.getValue(EchoRequestProto.getDefaultInstance());
    Assert.assertEquals(message1, actual);
    Assert.assertTrue(bb.remaining() > 0);
    Assert.assertEquals(bb.remaining(), buf.remaining());

    actual = buf.getValue(EchoRequestProto.getDefaultInstance());
    Assert.assertEquals(message2, actual);
    Assert.assertTrue(bb.remaining() > 0);
    Assert.assertEquals(bb.remaining(), buf.remaining());

    actual = buf.newInstance(LongWritable.class, null);
    Assert.assertEquals(writable, actual);
    Assert.assertEquals(0, bb.remaining());
    Assert.assertEquals(0, buf.remaining());
  }

  @Test
  public void testBufferWrapperNested() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    writable.write(dos);
    message1.writeDelimitedTo(dos);
    message2.writeDelimitedTo(dos);
    ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
    RpcWritable.Buffer buf1 = RpcWritable.Buffer.wrap(bb);
    Assert.assertEquals(baos.size(), bb.remaining());
    Assert.assertEquals(baos.size(), buf1.remaining());

    Object actual = buf1.newInstance(LongWritable.class, null);
    Assert.assertEquals(writable, actual);
    int left = bb.remaining();
    Assert.assertTrue(left > 0);
    Assert.assertEquals(left, buf1.remaining());

    // original bb now appears empty, but rpc writable has a slice of the bb.
    RpcWritable.Buffer buf2 = buf1.newInstance(RpcWritable.Buffer.class, null);
    Assert.assertEquals(0, bb.remaining());
    Assert.assertEquals(0, buf1.remaining());
    Assert.assertEquals(left, buf2.remaining());

    actual = buf2.getValue(EchoRequestProto.getDefaultInstance());
    Assert.assertEquals(message1, actual);
    Assert.assertTrue(buf2.remaining() > 0);

    actual = buf2.getValue(EchoRequestProto.getDefaultInstance());
    Assert.assertEquals(message2, actual);
    Assert.assertEquals(0, buf2.remaining());
  }
}
