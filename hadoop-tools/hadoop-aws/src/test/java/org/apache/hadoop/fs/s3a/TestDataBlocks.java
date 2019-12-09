/*
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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for {@link S3ADataBlocks}.
 */
public class TestDataBlocks extends Assert {

  @Rule
  public Timeout testTimeout = new Timeout(30 * 1000);

  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  /**
   * Test the {@link S3ADataBlocks.ByteBufferBlockFactory}.
   * That code implements an input stream over a ByteBuffer, and has to
   * return the buffer to the pool after the read complete.
   *
   * This test verifies the basic contract of the process.
   */
  @Test
  public void testByteBufferIO() throws Throwable {
    try (S3ADataBlocks.ByteBufferBlockFactory factory =
             new S3ADataBlocks.ByteBufferBlockFactory(null)) {
      int limit = 128;
      S3ADataBlocks.ByteBufferBlockFactory.ByteBufferBlock block
          = factory.create(1, limit, null);
      assertOutstandingBuffers(factory, 1);

      byte[] buffer = ContractTestUtils.toAsciiByteArray("test data");
      int bufferLen = buffer.length;
      block.write(buffer, 0, bufferLen);
      assertEquals(bufferLen, block.dataSize());
      assertEquals("capacity in " + block,
          limit - bufferLen, block.remainingCapacity());
      assertTrue("hasCapacity(64) in " + block, block.hasCapacity(64));
      assertTrue("No capacity in " + block,
          block.hasCapacity(limit - bufferLen));

      // now start the write
      S3ADataBlocks.BlockUploadData blockUploadData = block.startUpload();
      S3ADataBlocks.ByteBufferBlockFactory.ByteBufferBlock.ByteBufferInputStream
          stream =
          (S3ADataBlocks.ByteBufferBlockFactory.ByteBufferBlock.ByteBufferInputStream)
              blockUploadData.getUploadStream();
      assertTrue("Mark not supported in " + stream, stream.markSupported());
      assertTrue("!hasRemaining() in " + stream, stream.hasRemaining());
      int expected = bufferLen;
      assertEquals("wrong available() in " + stream,
          expected, stream.available());

      assertEquals('t', stream.read());
      stream.mark(limit);
      expected--;
      assertEquals("wrong available() in " + stream,
          expected, stream.available());


      // read into a byte array with an offset
      int offset = 5;
      byte[] in = new byte[limit];
      assertEquals(2, stream.read(in, offset, 2));
      assertEquals('e', in[offset]);
      assertEquals('s', in[offset + 1]);
      expected -= 2;
      assertEquals("wrong available() in " + stream,
          expected, stream.available());

      // read to end
      byte[] remainder = new byte[limit];
      int c;
      int index = 0;
      while ((c = stream.read()) >= 0) {
        remainder[index++] = (byte) c;
      }
      assertEquals(expected, index);
      assertEquals('a', remainder[--index]);

      assertEquals("wrong available() in " + stream,
          0, stream.available());
      assertTrue("hasRemaining() in " + stream, !stream.hasRemaining());

      // go the mark point
      stream.reset();
      assertEquals('e', stream.read());

      // when the stream is closed, the data should be returned
      stream.close();
      assertOutstandingBuffers(factory, 1);
      block.close();
      assertOutstandingBuffers(factory, 0);
      stream.close();
      assertOutstandingBuffers(factory, 0);
    }

  }

  /**
   * Assert the number of buffers active for a block factory.
   * @param factory factory
   * @param expectedCount expected count.
   */
  private static void assertOutstandingBuffers(
      S3ADataBlocks.ByteBufferBlockFactory factory,
      int expectedCount) {
    assertEquals("outstanding buffers in " + factory,
        expectedCount, factory.getOutstandingBufferCount());
  }

}
