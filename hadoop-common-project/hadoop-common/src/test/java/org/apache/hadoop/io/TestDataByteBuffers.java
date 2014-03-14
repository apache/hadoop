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

package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestDataByteBuffers {

  private static void readJunk(DataInput in, Random r, long seed, int iter) 
      throws IOException {
    r.setSeed(seed);
    for (int i = 0; i < iter; ++i) {
      switch (r.nextInt(7)) {
        case 0:
          assertEquals((byte)(r.nextInt() & 0xFF), in.readByte()); break;
        case 1:
          assertEquals((short)(r.nextInt() & 0xFFFF), in.readShort()); break;
        case 2:
          assertEquals(r.nextInt(), in.readInt()); break;
        case 3:
          assertEquals(r.nextLong(), in.readLong()); break;
        case 4:
          assertEquals(Double.doubleToLongBits(r.nextDouble()),
                       Double.doubleToLongBits(in.readDouble())); break;
        case 5:
          assertEquals(Float.floatToIntBits(r.nextFloat()),
                       Float.floatToIntBits(in.readFloat())); break;
        case 6:
          int len = r.nextInt(1024);
          byte[] vb = new byte[len];
          r.nextBytes(vb);
          byte[] b = new byte[len];
          in.readFully(b, 0, len);
          assertArrayEquals(vb, b);
          break;
      }
    }
  }

  private static void writeJunk(DataOutput out, Random r, long seed, int iter)
      throws IOException  {
    r.setSeed(seed);
    for (int i = 0; i < iter; ++i) {
      switch (r.nextInt(7)) {
        case 0: out.writeByte(r.nextInt()); break;
        case 1: out.writeShort((short)(r.nextInt() & 0xFFFF)); break;
        case 2: out.writeInt(r.nextInt()); break;
        case 3: out.writeLong(r.nextLong()); break;
        case 4: out.writeDouble(r.nextDouble()); break;
        case 5: out.writeFloat(r.nextFloat()); break;
        case 6:
          byte[] b = new byte[r.nextInt(1024)];
          r.nextBytes(b);
          out.write(b);
          break;
      }
    }
  }

  @Test
  public void testBaseBuffers() throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED: " + seed);
    writeJunk(dob, r, seed, 1000);
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), 0, dob.getLength());
    readJunk(dib, r, seed, 1000);

    dob.reset();
    writeJunk(dob, r, seed, 1000);
    dib.reset(dob.getData(), 0, dob.getLength());
    readJunk(dib, r, seed, 1000);
  }

  @Test
  public void testByteBuffers() throws IOException {
    DataOutputByteBuffer dob = new DataOutputByteBuffer();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED: " + seed);
    writeJunk(dob, r, seed, 1000);
    DataInputByteBuffer dib = new DataInputByteBuffer();
    dib.reset(dob.getData());
    readJunk(dib, r, seed, 1000);

    dob.reset();
    writeJunk(dob, r, seed, 1000);
    dib.reset(dob.getData());
    readJunk(dib, r, seed, 1000);
  }

  private static byte[] toBytes(ByteBuffer[] bufs, int len) {
    byte[] ret = new byte[len];
    int pos = 0;
    for (int i = 0; i < bufs.length; ++i) {
      int rem = bufs[i].remaining();
      bufs[i].get(ret, pos, rem);
      pos += rem;
    }
    return ret;
  }

  @Test
  public void testDataOutputByteBufferCompatibility() throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    DataOutputByteBuffer dobb = new DataOutputByteBuffer();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED: " + seed);
    writeJunk(dob, r, seed, 1000);
    writeJunk(dobb, r, seed, 1000);
    byte[] check = toBytes(dobb.getData(), dobb.getLength());
    assertEquals(check.length, dob.getLength());
    assertArrayEquals(check, Arrays.copyOf(dob.getData(), dob.getLength()));

    dob.reset();
    dobb.reset();
    writeJunk(dob, r, seed, 3000);
    writeJunk(dobb, r, seed, 3000);
    check = toBytes(dobb.getData(), dobb.getLength());
    assertEquals(check.length, dob.getLength());
    assertArrayEquals(check, Arrays.copyOf(dob.getData(), dob.getLength()));

    dob.reset();
    dobb.reset();
    writeJunk(dob, r, seed, 1000);
    writeJunk(dobb, r, seed, 1000);
    check = toBytes(dobb.getData(), dobb.getLength());
    assertEquals("Failed Checking length = " + check.length,
            check.length, dob.getLength());
    assertArrayEquals(check, Arrays.copyOf(dob.getData(), dob.getLength()));
  }

  @Test
  public void TestDataInputByteBufferCompatibility() throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED: " + seed);
    writeJunk(dob, r, seed, 1000);
    ByteBuffer buf = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    DataInputByteBuffer dib = new DataInputByteBuffer();
    dib.reset(buf);
    readJunk(dib, r, seed, 1000);
  }

  @Test
  public void TestDataOutputByteBufferCompatibility() throws IOException {
    DataOutputByteBuffer dob = new DataOutputByteBuffer();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED: " + seed);
    writeJunk(dob, r, seed, 1000);
    ByteBuffer buf = ByteBuffer.allocate(dob.getLength());
    for (ByteBuffer b : dob.getData()) {
      buf.put(b);
    }
    buf.flip();
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(buf.array(), 0, buf.remaining());
    readJunk(dib, r, seed, 1000);
  }

}
