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
package org.apache.hadoop.io.compress.bzip2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.io.compress.bzip2.Bzip2Compressor;
import org.apache.hadoop.io.compress.bzip2.Bzip2Decompressor;
import org.apache.hadoop.test.MultithreadedTestUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Random;

import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.junit.Assume.assumeTrue;

public class TestBzip2CompressorDecompressor {
  
  private static final Random rnd = new Random(12345l);

  @Before
  public void before() {
    assumeTrue(Bzip2Factory.isNativeBzip2Loaded(new Configuration()));
  }

  // test compress/decompress process 
  @Test
  public void testCompressDecompress() {
    byte[] rawData = null;
    int rawDataSize = 0;
    rawDataSize = 1024 * 64;
    rawData = generate(rawDataSize);
    try {
      Bzip2Compressor compressor = new Bzip2Compressor();
      Bzip2Decompressor decompressor = new Bzip2Decompressor();
      assertFalse("testBzip2CompressDecompress finished error",
          compressor.finished());
      compressor.setInput(rawData, 0, rawData.length);
      assertTrue("testBzip2CompressDecompress getBytesRead before error",
          compressor.getBytesRead() == 0);
      compressor.finish();

      byte[] compressedResult = new byte[rawDataSize];
      int cSize = compressor.compress(compressedResult, 0, rawDataSize);
      assertTrue("testBzip2CompressDecompress getBytesRead after error",
          compressor.getBytesRead() == rawDataSize);
      assertTrue(
          "testBzip2CompressDecompress compressed size no less than original size",
          cSize < rawDataSize);
      decompressor.setInput(compressedResult, 0, cSize);
      byte[] decompressedBytes = new byte[rawDataSize];
      decompressor.decompress(decompressedBytes, 0, decompressedBytes.length);
      assertArrayEquals("testBzip2CompressDecompress arrays not equals ",
          rawData, decompressedBytes);
      compressor.reset();
      decompressor.reset();
    } catch (IOException ex) {
      fail("testBzip2CompressDecompress ex !!!" + ex);
    }
  }

  public static byte[] generate(int size) {
    byte[] array = new byte[size];
    for (int i = 0; i < size; i++)
      array[i] = (byte)rnd.nextInt(16);
    return array;
  }

  @Test
  public void testBzip2CompressDecompressInMultiThreads() throws Exception {
    MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext();
    for(int i=0;i<10;i++) {
      ctx.addThread( new MultithreadedTestUtil.TestingThread(ctx) {
        @Override
        public void doWork() throws Exception {
          testCompressDecompress();
        }
      });
    }
    ctx.startThreads();

    ctx.waitFor(60000);
  }
}
