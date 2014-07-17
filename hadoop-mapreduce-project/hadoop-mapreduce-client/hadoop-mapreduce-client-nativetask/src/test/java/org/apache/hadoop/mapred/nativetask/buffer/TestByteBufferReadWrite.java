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

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.mapred.nativetask.NativeDataTarget;

import junit.framework.Assert;
import junit.framework.TestCase;

public class TestByteBufferReadWrite extends TestCase{
  
  
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
    
    Assert.assertEquals(1, reader.read());
    byte[] two = new byte[2];
    reader.read(two);
    Assert.assertTrue(two[0] == two[1] && two[0] == 2);
    
    
    Assert.assertEquals(true, reader.readBoolean());
    Assert.assertEquals(4, reader.readByte());
    Assert.assertEquals(5, reader.readShort());
    Assert.assertEquals(6, reader.readChar());
    Assert.assertEquals(7, reader.readInt());
    Assert.assertEquals(8, reader.readLong());
    Assert.assertTrue(reader.readFloat() - 9 < 0.0001);
    Assert.assertTrue(reader.readDouble() - 10 < 0.0001);
    
    byte[] goodboy = new byte["goodboy".length()];
    reader.read(goodboy);
    Assert.assertEquals("goodboy", toString(goodboy));
    
    char[] hello = new char["hello".length()];
    for (int i = 0; i < hello.length; i++) {
      hello[i] = reader.readChar();
    }
    
    String helloString = new String(hello);
    Assert.assertEquals("hello", helloString);
    
    Assert.assertEquals("native task", reader.readUTF());
    
    Assert.assertEquals(0, input.remaining());
  }
  
  public void testShortOfSpace() throws IOException {
    byte[] buff = new byte[10];
    MockDataTarget target = new MockDataTarget(buff);
    ByteBufferDataWriter writer = new ByteBufferDataWriter(target);
    Assert.assertEquals(false, writer.hasUnFlushedData()); 
    
    writer.write(1);
    writer.write(new byte[] {2, 2}, 0, 2);
    Assert.assertEquals(true, writer.hasUnFlushedData()); 
    
    Assert.assertEquals(true, writer.shortOfSpace(100));
  }

  public void testFlush() throws IOException {
    byte[] buff = new byte[10];
    final Counter flushCount = new Counter();
    final Flag finishFlag = new Flag();
    MockDataTarget target = new MockDataTarget(buff) {
      @Override
      public void sendData() throws IOException {
        flushCount.increase();
      }
      
      @Override
      public void finishSendData() throws IOException {
        finishFlag.set(true);
      }
    };
    
    ByteBufferDataWriter writer = new ByteBufferDataWriter(target);
    Assert.assertEquals(false, writer.hasUnFlushedData()); 
    
    writer.write(1);
    writer.write(new byte[100]);

    Assert.assertEquals(true, writer.hasUnFlushedData()); 
    writer.close();    
    Assert.assertEquals(11, flushCount.get());
    Assert.assertEquals(true, finishFlag.get()); 

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
    public void sendData() throws IOException {
      
    }

    @Override
    public void finishSendData() throws IOException {
       
    }

    @Override
    public OutputBuffer getOutputBuffer() {
      return out;
    }    
  }
  
  private static class Counter {
    private int count;
    
    public int get() {
      return count;
    }
    
    public void increase() {
      count++;
    }
  }
  
  private static class Flag {
    private boolean value;
    
    public void set(boolean status) {
      this.value = status;
    }
    
    public boolean get() {
      return this.value;
    }
  }
}
