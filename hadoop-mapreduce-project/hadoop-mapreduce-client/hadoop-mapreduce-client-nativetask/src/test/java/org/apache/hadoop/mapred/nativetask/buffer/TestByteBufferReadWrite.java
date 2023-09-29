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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.mapred.nativetask.NativeDataTarget;

import org.junit.Test;

import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

public class TestByteBufferReadWrite {
  @Test
  public void testReadWrite() throws IOException {
    byte[] buff = new byte[10000];

    InputBuffer input = new InputBuffer(buff);
    MockDataTarget target = new MockDataTarget(buff);
    ByteBufferDataWriter writer = new ByteBufferDataWriter(target);
    
    writer.write(1);
    writer.write(new byte[] {2, 2}, 0, 2);
    writer.writeBoolean(true);
    writer.writeByte(4);
    writer.writeShort(5);
    writer.writeChar(6);
    writer.writeInt(7);
    writer.writeLong(8);
    writer.writeFloat(9);
    writer.writeDouble(10);
    writer.writeBytes("goodboy");
    writer.writeChars("hello");
    writer.writeUTF("native task");

    int length = target.getOutputBuffer().length();
    input.rewind(0, length);
    ByteBufferDataReader reader = new ByteBufferDataReader(input);
    
    assertThat(reader.read()).isOne();
    byte[] two = new byte[2];
    reader.read(two);
    assertThat(two[0]).isEqualTo(two[1]);
    assertThat(two[0]).isEqualTo((byte) 2);
    
    
    assertThat(reader.readBoolean()).isTrue();
    assertThat(reader.readByte()).isEqualTo((byte) 4);
    assertThat(reader.readShort()).isEqualTo((short) 5);
    assertThat(reader.readChar()).isEqualTo((char) 6);
    assertThat(reader.readInt()).isEqualTo(7);
    assertThat(reader.readLong()).isEqualTo(8);
    assertThat(reader.readFloat()).isEqualTo(9f, offset(0.0001f));
    assertThat(reader.readDouble()).isEqualTo(10, offset(0.0001));
    
    byte[] goodboy = new byte["goodboy".length()];
    reader.read(goodboy);
    assertThat(toString(goodboy)).isEqualTo("goodboy");
    
    char[] hello = new char["hello".length()];
    for (int i = 0; i < hello.length; i++) {
      hello[i] = reader.readChar();
    }
    
    String helloString = new String(hello);
    assertThat(helloString).isEqualTo("hello");
    assertThat(reader.readUTF()).isEqualTo("native task");
    assertThat(input.remaining()).isZero();
  }

  /**
   * Test that Unicode characters outside the basic multilingual plane,
   * such as this cat face, are properly encoded.
   */
  @Test
  public void testCatFace() throws IOException {
    byte[] buff = new byte[10];
    MockDataTarget target = new MockDataTarget(buff);
    ByteBufferDataWriter writer = new ByteBufferDataWriter(target);
    String catFace = "\uD83D\uDE38";
    writer.writeUTF(catFace);

    // Check that our own decoder can read it
    InputBuffer input = new InputBuffer(buff);
    input.rewind(0, buff.length);
    ByteBufferDataReader reader = new ByteBufferDataReader(input);
    assertThat(reader.readUTF()).isEqualTo(catFace);

    // Check that the standard Java one can read it too
    String fromJava = new java.io.DataInputStream(new ByteArrayInputStream(buff)).readUTF();
    assertThat(fromJava).isEqualTo(catFace);
  }

  @Test
  public void testShortOfSpace() throws IOException {
    byte[] buff = new byte[10];
    MockDataTarget target = new MockDataTarget(buff);
    ByteBufferDataWriter writer = new ByteBufferDataWriter(target);
    assertThat(writer.hasUnFlushedData()).isFalse();
    
    writer.write(1);
    writer.write(new byte[] {2, 2}, 0, 2);
    assertThat(writer.hasUnFlushedData()).isTrue();
    
    assertThat(writer.shortOfSpace(100)).isTrue();
  }


  @Test
  public void testFlush() throws IOException {
    byte[] buff = new byte[10];
    MockDataTarget target = Mockito.spy(new MockDataTarget(buff));

    ByteBufferDataWriter writer = new ByteBufferDataWriter(target);
    assertThat(writer.hasUnFlushedData()).isFalse();
    
    writer.write(1);
    writer.write(new byte[100]);

    assertThat(writer.hasUnFlushedData()).isTrue();
    writer.close();
    Mockito.verify(target, Mockito.times(11)).sendData();
    Mockito.verify(target).finishSendData();
  }
  
  private static String toString(byte[] str) throws UnsupportedEncodingException {
    return new String(str, 0, str.length, "UTF-8");
  }
  
  private static class MockDataTarget implements NativeDataTarget {

    private OutputBuffer out;

    MockDataTarget(byte[] buffer) {
      this.out = new OutputBuffer(buffer);
    }
    
    @Override
    public void sendData() throws IOException {}

    @Override
    public void finishSendData() throws IOException {}

    @Override
    public OutputBuffer getOutputBuffer() {
      return out;
    }    
  }
}
