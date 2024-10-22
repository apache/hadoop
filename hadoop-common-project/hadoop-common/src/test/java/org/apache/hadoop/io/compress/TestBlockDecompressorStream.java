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
package org.apache.hadoop.io.compress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.junit.Test;

public class TestBlockDecompressorStream {
  
  private byte[] buf;
  private ByteArrayInputStream bytesIn;
  private ByteArrayOutputStream bytesOut;

  @Test
  public void testRead1() throws IOException {
    testRead(0);
  }

  @Test
  public void testRead2() throws IOException {
    // Test eof after getting non-zero block size info
    testRead(4);
  }

  @Test
  public void testDecompressorStreamSkipOutOfBound() throws IOException {
    ByteArrayInputStream bytesIn;
    DecompressorStream decompStream;
    int bufLen = 4;
    bytesOut = new ByteArrayOutputStream();
    bytesOut.write(ByteBuffer.allocate(bufLen).putInt(1024).array(), 0, bufLen);
    buf = bytesOut.toByteArray();
    bytesIn = new ByteArrayInputStream(buf);

    decompStream = new DecompressorStream(bytesIn, new FakeDecompressor());
    try {
      // check that skipping a length longer than Integer.MAX_VALUE does not
      // throw IndexOutOfBoundsException
      decompStream.skip((long)Integer.MAX_VALUE + 1);
    } catch (EOFException e) {
      // expected exception, because the input is not that long
    }
  }

  private void testRead(int bufLen) throws IOException {
    // compress empty stream
    bytesOut = new ByteArrayOutputStream();
    if (bufLen > 0) {
      bytesOut.write(ByteBuffer.allocate(bufLen).putInt(1024).array(), 0,
          bufLen);
    }
    BlockCompressorStream blockCompressorStream = 
      new BlockCompressorStream(bytesOut, 
          new FakeCompressor(), 1024, 0);
    // close without any write
    blockCompressorStream.close();
    
    // check compressed output 
    buf = bytesOut.toByteArray();
    assertEquals("empty file compressed output size is not " + (bufLen + 4),
        bufLen + 4, buf.length);
    
    // use compressed output as input for decompression
    bytesIn = new ByteArrayInputStream(buf);
    
    // get decompression stream
    try (BlockDecompressorStream blockDecompressorStream =
      new BlockDecompressorStream(bytesIn, new FakeDecompressor(), 1024)) {
      assertEquals("return value is not -1", 
          -1 , blockDecompressorStream.read());
    } catch (IOException e) {
      fail("unexpected IOException : " + e);
    }
  }

  @Test
  public void testReadWhenIoExceptionOccure() throws IOException {
    File file = new File("testReadWhenIOException");
    try {
      file.createNewFile();
      InputStream io = new FileInputStream(file) {
        @Override
        public int read() throws IOException {
          throw new IOException("File blocks missing");
        }
      };

      try (BlockDecompressorStream blockDecompressorStream =
          new BlockDecompressorStream(io, new FakeDecompressor(), 1024)) {
        int byteRead = blockDecompressorStream.read();
        fail("Should not return -1 in case of IOException. Byte read "
            + byteRead);
      } catch (IOException e) {
        assertTrue(e.getMessage().contains("File blocks missing"));
      }
    } finally {
      file.delete();
    }
  }
}