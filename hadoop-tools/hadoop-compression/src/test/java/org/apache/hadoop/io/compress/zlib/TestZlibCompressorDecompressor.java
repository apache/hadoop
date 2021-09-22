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
package org.apache.hadoop.io.compress.zlib;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.DeflaterOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.compress.CompressDecompressTester;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor.ZlibDirectDecompressor;
import org.apache.hadoop.test.MultithreadedTestUtil;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;

public class TestZlibCompressorDecompressor {

  private static final Random random = new Random(12345L);

  @Before
  public void before() {
    assumeTrue(ZlibFactory.isNativeZlibLoaded(new Configuration()));
  }  
  
  @Test
  public void testZlibCompressorDecompressor() {
    try {
      int SIZE = 44 * 1024;
      byte[] rawData = generate(SIZE);
      
      CompressDecompressTester.of(rawData)
        .withCompressDecompressPair(new ZlibCompressor(), new ZlibDecompressor())
        .withTestCases(ImmutableSet.of(CompressionTestStrategy.COMPRESS_DECOMPRESS_SINGLE_BLOCK,
           CompressionTestStrategy.COMPRESS_DECOMPRESS_BLOCK,
           CompressionTestStrategy.COMPRESS_DECOMPRESS_ERRORS,
           CompressionTestStrategy.COMPRESS_DECOMPRESS_WITH_EMPTY_STREAM))
         .test();
    } catch (Exception ex) {
      fail("testCompressorDecompressor error !!!" + ex);
    }
  }
  
  @Test
  public void testCompressorDecompressorWithExeedBufferLimit() {
    int BYTE_SIZE = 100 * 1024;
    byte[] rawData = generate(BYTE_SIZE);
    try {
      CompressDecompressTester.of(rawData)
      .withCompressDecompressPair(
        new ZlibCompressor(
            org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel.BEST_COMPRESSION,
            CompressionStrategy.DEFAULT_STRATEGY,
            org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader.DEFAULT_HEADER,
            BYTE_SIZE),
         new ZlibDecompressor(
            org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader.DEFAULT_HEADER,
            BYTE_SIZE))
         .withTestCases(ImmutableSet.of(CompressionTestStrategy.COMPRESS_DECOMPRESS_SINGLE_BLOCK,
            CompressionTestStrategy.COMPRESS_DECOMPRESS_BLOCK,
            CompressionTestStrategy.COMPRESS_DECOMPRESS_ERRORS,
            CompressionTestStrategy.COMPRESS_DECOMPRESS_WITH_EMPTY_STREAM))
          .test();
    } catch (Exception ex) {
      fail("testCompressorDecompressorWithExeedBufferLimit error !!!" + ex);
    } 
  }
  
  
  @Test
  public void testZlibCompressorDecompressorWithConfiguration() {
    Configuration conf = new Configuration();
    if (ZlibFactory.isNativeZlibLoaded(conf)) {
      byte[] rawData;
      int tryNumber = 5;
      int BYTE_SIZE = 10 * 1024;
      Compressor zlibCompressor = ZlibFactory.getZlibCompressor(conf);
      Decompressor zlibDecompressor = ZlibFactory.getZlibDecompressor(conf);
      rawData = generate(BYTE_SIZE);
      try {
        for (int i = 0; i < tryNumber; i++)
          compressDecompressZlib(rawData, (ZlibCompressor) zlibCompressor,
              (ZlibDecompressor) zlibDecompressor);
        zlibCompressor.reinit(conf);
      } catch (Exception ex) {
        fail("testZlibCompressorDecompressorWithConfiguration ex error " + ex);
      }
    } else {
      assertTrue("ZlibFactory is using native libs against request",
          ZlibFactory.isNativeZlibLoaded(conf));
    }
  }

  @Test
  public void testZlibCompressorDecompressorWithCompressionLevels() {
    Configuration conf = new Configuration();
    conf.set("zlib.compress.level","FOUR");
    if (ZlibFactory.isNativeZlibLoaded(conf)) {
      byte[] rawData;
      int tryNumber = 5;
      int BYTE_SIZE = 10 * 1024;
      Compressor zlibCompressor = ZlibFactory.getZlibCompressor(conf);
      Decompressor zlibDecompressor = ZlibFactory.getZlibDecompressor(conf);
      rawData = generate(BYTE_SIZE);
      try {
        for (int i = 0; i < tryNumber; i++)
          compressDecompressZlib(rawData, (ZlibCompressor) zlibCompressor,
                  (ZlibDecompressor) zlibDecompressor);
        zlibCompressor.reinit(conf);
      } catch (Exception ex) {
        fail("testZlibCompressorDecompressorWithConfiguration ex error " + ex);
      }
    } else {
      assertTrue("ZlibFactory is using native libs against request",
              ZlibFactory.isNativeZlibLoaded(conf));
    }
  }

  @Test
  public void testZlibCompressDecompress() {
    byte[] rawData = null;
    int rawDataSize = 0;
    rawDataSize = 1024 * 64;
    rawData = generate(rawDataSize);
    try {
      ZlibCompressor compressor = new ZlibCompressor();
      ZlibDecompressor decompressor = new ZlibDecompressor();
      assertFalse("testZlibCompressDecompress finished error",
          compressor.finished());
      compressor.setInput(rawData, 0, rawData.length);
      assertTrue("testZlibCompressDecompress getBytesRead before error",
          compressor.getBytesRead() == 0);
      compressor.finish();

      byte[] compressedResult = new byte[rawDataSize];
      int cSize = compressor.compress(compressedResult, 0, rawDataSize);
      assertTrue("testZlibCompressDecompress getBytesRead ather error",
          compressor.getBytesRead() == rawDataSize);
      assertTrue(
          "testZlibCompressDecompress compressed size no less then original size",
          cSize < rawDataSize);
      decompressor.setInput(compressedResult, 0, cSize);
      byte[] decompressedBytes = new byte[rawDataSize];
      decompressor.decompress(decompressedBytes, 0, decompressedBytes.length);
      assertArrayEquals("testZlibCompressDecompress arrays not equals ",
          rawData, decompressedBytes);
      compressor.reset();
      decompressor.reset();
    } catch (IOException ex) {
      fail("testZlibCompressDecompress ex !!!" + ex);
    }
  }
  
  
  private void compressDecompressLoop(int rawDataSize) throws IOException {
    byte[] rawData = null;
    rawData = generate(rawDataSize);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(rawDataSize+12);
    DeflaterOutputStream dos = new DeflaterOutputStream(baos);
    dos.write(rawData);
    dos.flush();
    dos.close();
    byte[] compressedResult = baos.toByteArray();
    int compressedSize = compressedResult.length;
    ZlibDirectDecompressor decompressor = new ZlibDirectDecompressor();
   
    ByteBuffer inBuf = ByteBuffer.allocateDirect(compressedSize);
    ByteBuffer outBuf = ByteBuffer.allocateDirect(rawDataSize);

    inBuf.put(compressedResult, 0, compressedSize);
    inBuf.flip();    

    ByteBuffer expected = ByteBuffer.wrap(rawData);
    
    outBuf.clear();
    while(!decompressor.finished()) {
      decompressor.decompress(inBuf, outBuf);
      if (outBuf.remaining() == 0) {
        outBuf.flip();
        while (outBuf.remaining() > 0) {        
          assertEquals(expected.get(), outBuf.get());
        }
        outBuf.clear();
      }
    }
    outBuf.flip();
    while (outBuf.remaining() > 0) {        
      assertEquals(expected.get(), outBuf.get());
    }
    outBuf.clear();
    
    assertEquals(0, expected.remaining());
  }

  @Test
  public void testZlibDirectCompressDecompress() {
    int[] size = { 1, 4, 16, 4 * 1024, 64 * 1024, 128 * 1024, 1024 * 1024 };
    assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    try {
      for (int i = 0; i < size.length; i++) {
        compressDecompressLoop(size[i]);
      }
    } catch (IOException ex) {
      fail("testZlibDirectCompressDecompress ex !!!" + ex);
    }
  }
  
  @Test
  public void testZlibCompressorDecompressorSetDictionary() {
    Configuration conf = new Configuration();
    if (ZlibFactory.isNativeZlibLoaded(conf)) {
      Compressor zlibCompressor = ZlibFactory.getZlibCompressor(conf);
      Decompressor zlibDecompressor = ZlibFactory.getZlibDecompressor(conf);

      checkSetDictionaryNullPointerException(zlibCompressor);
      checkSetDictionaryNullPointerException(zlibDecompressor);

      checkSetDictionaryArrayIndexOutOfBoundsException(zlibDecompressor);
      checkSetDictionaryArrayIndexOutOfBoundsException(zlibCompressor);
    } else {
      assertTrue("ZlibFactory is using native libs against request",
          ZlibFactory.isNativeZlibLoaded(conf));
    }
  }

  @Test
  public void testZlibFactory() {
    Configuration cfg = new Configuration();

    assertTrue("testZlibFactory compression level error !!!",
        CompressionLevel.DEFAULT_COMPRESSION == ZlibFactory
            .getCompressionLevel(cfg));

    assertTrue("testZlibFactory compression strategy error !!!",
        CompressionStrategy.DEFAULT_STRATEGY == ZlibFactory
            .getCompressionStrategy(cfg));

    ZlibFactory.setCompressionLevel(cfg, CompressionLevel.BEST_COMPRESSION);
    assertTrue("testZlibFactory compression strategy error !!!",
        CompressionLevel.BEST_COMPRESSION == ZlibFactory
            .getCompressionLevel(cfg));

    ZlibFactory.setCompressionStrategy(cfg, CompressionStrategy.FILTERED);
    assertTrue("testZlibFactory compression strategy error !!!",
        CompressionStrategy.FILTERED == ZlibFactory.getCompressionStrategy(cfg));
  }
  

  private boolean checkSetDictionaryNullPointerException(
      Decompressor decompressor) {
    try {
      decompressor.setDictionary(null, 0, 1);
    } catch (NullPointerException ex) {
      return true;
    } catch (Exception ex) {
    }
    return false;
  }

  private boolean checkSetDictionaryNullPointerException(Compressor compressor) {
    try {
      compressor.setDictionary(null, 0, 1);
    } catch (NullPointerException ex) {
      return true;
    } catch (Exception ex) {
    }
    return false;
  }

  private boolean checkSetDictionaryArrayIndexOutOfBoundsException(
      Compressor compressor) {
    try {
      compressor.setDictionary(new byte[] { (byte) 0 }, 0, -1);
    } catch (ArrayIndexOutOfBoundsException e) {
      return true;
    } catch (Exception e) {
    }
    return false;
  }

  private boolean checkSetDictionaryArrayIndexOutOfBoundsException(
      Decompressor decompressor) {
    try {
      decompressor.setDictionary(new byte[] { (byte) 0 }, 0, -1);
    } catch (ArrayIndexOutOfBoundsException e) {
      return true;
    } catch (Exception e) {
    }
    return false;
  }

  private byte[] compressDecompressZlib(byte[] rawData,
      ZlibCompressor zlibCompressor, ZlibDecompressor zlibDecompressor)
      throws IOException {
    int cSize = 0;
    byte[] compressedByte = new byte[rawData.length];
    byte[] decompressedRawData = new byte[rawData.length];
    zlibCompressor.setInput(rawData, 0, rawData.length);
    zlibCompressor.finish();
    while (!zlibCompressor.finished()) {
      cSize = zlibCompressor.compress(compressedByte, 0, compressedByte.length);
    }
    zlibCompressor.reset();

    assertTrue(zlibDecompressor.getBytesWritten() == 0);
    assertTrue(zlibDecompressor.getBytesRead() == 0);
    assertTrue(zlibDecompressor.needsInput());
    zlibDecompressor.setInput(compressedByte, 0, cSize);
    assertFalse(zlibDecompressor.needsInput());
    while (!zlibDecompressor.finished()) {
      zlibDecompressor.decompress(decompressedRawData, 0,
          decompressedRawData.length);
    }
    assertTrue(zlibDecompressor.getBytesWritten() == rawData.length);
    assertTrue(zlibDecompressor.getBytesRead() == cSize);
    zlibDecompressor.reset();
    assertTrue(zlibDecompressor.getRemaining() == 0);
    assertArrayEquals(
        "testZlibCompressorDecompressorWithConfiguration array equals error",
        rawData, decompressedRawData);

    return decompressedRawData;
  }

  @Test
  public void testBuiltInGzipDecompressorExceptions() {
    BuiltInGzipDecompressor decompresser = new BuiltInGzipDecompressor();
    try {
      decompresser.setInput(null, 0, 1);
    } catch (NullPointerException ex) {
      // expected
    } catch (Exception ex) {
      fail("testBuiltInGzipDecompressorExceptions npe error " + ex);
    }

    try {
      decompresser.setInput(new byte[] { 0 }, 0, -1);
    } catch (ArrayIndexOutOfBoundsException ex) {
      // expected
    } catch (Exception ex) {
      fail("testBuiltInGzipDecompressorExceptions aioob error" + ex);
    }        
    
    assertTrue("decompresser.getBytesRead error",
        decompresser.getBytesRead() == 0);
    assertTrue("decompresser.getRemaining error",
        decompresser.getRemaining() == 0);
    decompresser.reset();
    decompresser.end();

    InputStream decompStream = null;
    try {
      // invalid 0 and 1 bytes , must be 31, -117
      int buffSize = 1 * 1024;
      byte buffer[] = new byte[buffSize];
      Decompressor decompressor = new BuiltInGzipDecompressor();
      DataInputBuffer gzbuf = new DataInputBuffer();
      decompStream = new DecompressorStream(gzbuf, decompressor);
      gzbuf.reset(new byte[] { 0, 0, 1, 1, 1, 1, 11, 1, 1, 1, 1 }, 11);
      decompStream.read(buffer);
    } catch (IOException ioex) {
      // expected
    } catch (Exception ex) {
      fail("invalid 0 and 1 byte in gzip stream" + ex);
    }

    // invalid 2 byte, must be 8
    try {
      int buffSize = 1 * 1024;
      byte buffer[] = new byte[buffSize];
      Decompressor decompressor = new BuiltInGzipDecompressor();
      DataInputBuffer gzbuf = new DataInputBuffer();
      decompStream = new DecompressorStream(gzbuf, decompressor);
      gzbuf.reset(new byte[] { 31, -117, 7, 1, 1, 1, 1, 11, 1, 1, 1, 1 }, 11);
      decompStream.read(buffer);
    } catch (IOException ioex) {
      // expected
    } catch (Exception ex) {
      fail("invalid 2 byte in gzip stream" + ex);
    }

    try {
      int buffSize = 1 * 1024;
      byte buffer[] = new byte[buffSize];
      Decompressor decompressor = new BuiltInGzipDecompressor();
      DataInputBuffer gzbuf = new DataInputBuffer();
      decompStream = new DecompressorStream(gzbuf, decompressor);
      gzbuf.reset(new byte[] { 31, -117, 8, -32, 1, 1, 1, 11, 1, 1, 1, 1 }, 11);
      decompStream.read(buffer);
    } catch (IOException ioex) {
      // expected
    } catch (Exception ex) {
      fail("invalid 3 byte in gzip stream" + ex);
    }
    try {
      int buffSize = 1 * 1024;
      byte buffer[] = new byte[buffSize];
      Decompressor decompressor = new BuiltInGzipDecompressor();
      DataInputBuffer gzbuf = new DataInputBuffer();
      decompStream = new DecompressorStream(gzbuf, decompressor);
      gzbuf.reset(new byte[] { 31, -117, 8, 4, 1, 1, 1, 11, 1, 1, 1, 1 }, 11);
      decompStream.read(buffer);
    } catch (IOException ioex) {
      // expected
    } catch (Exception ex) {
      fail("invalid 3 byte make hasExtraField" + ex);
    }
  }
  
  public static byte[] generate(int size) {
    byte[] data = new byte[size];
    for (int i = 0; i < size; i++)
      data[i] = (byte)random.nextInt(16);
    return data;
  }

  @Test
  public void testZlibCompressDecompressInMultiThreads() throws Exception {
    MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext();
    for(int i=0;i<10;i++) {
      ctx.addThread( new MultithreadedTestUtil.TestingThread(ctx) {
        @Override
        public void doWork() throws Exception {
          testZlibCompressDecompress();
        }
      });
    }
    ctx.startThreads();

    ctx.waitFor(60000);
  }
}
