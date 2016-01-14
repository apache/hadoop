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
package org.apache.hadoop.io.compress;

import java.util.Random;
import org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy;
import org.apache.hadoop.io.compress.lz4.Lz4Compressor;
import org.apache.hadoop.io.compress.lz4.Lz4Decompressor;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.apache.hadoop.io.compress.zlib.BuiltInZlibDeflater;
import org.apache.hadoop.io.compress.zlib.BuiltInZlibInflater;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import com.google.common.collect.ImmutableSet;

/** 
 * Test for pairs:
 * <pre>
 * SnappyCompressor/SnappyDecompressor
 * Lz4Compressor/Lz4Decompressor
 * BuiltInZlibDeflater/new BuiltInZlibInflater
 *
 *
 * Note: we can't use ZlibCompressor/ZlibDecompressor here 
 * because his constructor can throw exception (if native libraries not found)
 * For ZlibCompressor/ZlibDecompressor pair testing used {@code TestZlibCompressorDecompressor}   
 *
 * </pre>
 *
 */
public class TestCompressorDecompressor {
  
  private static final Random rnd = new Random(12345L);
  
  @Test
  public void testCompressorDecompressor() {
    // no more for this data
    int SIZE = 44 * 1024;
    
    byte[] rawData = generate(SIZE);
    try {
      CompressDecompressTester.of(rawData)
          .withCompressDecompressPair(new SnappyCompressor(), new SnappyDecompressor())
          .withCompressDecompressPair(new Lz4Compressor(), new Lz4Decompressor())
          .withCompressDecompressPair(new BuiltInZlibDeflater(), new BuiltInZlibInflater())
          .withTestCases(ImmutableSet.of(CompressionTestStrategy.COMPRESS_DECOMPRESS_SINGLE_BLOCK,
                      CompressionTestStrategy.COMPRESS_DECOMPRESS_BLOCK,
                      CompressionTestStrategy.COMPRESS_DECOMPRESS_ERRORS,
                      CompressionTestStrategy.COMPRESS_DECOMPRESS_WITH_EMPTY_STREAM))
          .test();

    } catch (Exception ex) {
      GenericTestUtils.assertExceptionContains(
          "testCompressorDecompressor error !!!", ex);
    }
  }
  
  @Test
  public void testCompressorDecompressorWithExeedBufferLimit() {
    int BYTE_SIZE = 100 * 1024;
    byte[] rawData = generate(BYTE_SIZE);
    try {
      CompressDecompressTester.of(rawData)
          .withCompressDecompressPair(
              new SnappyCompressor(BYTE_SIZE + BYTE_SIZE / 2),
              new SnappyDecompressor(BYTE_SIZE + BYTE_SIZE / 2))
          .withCompressDecompressPair(new Lz4Compressor(BYTE_SIZE),
              new Lz4Decompressor(BYTE_SIZE))
          .withTestCases(ImmutableSet.of(CompressionTestStrategy.COMPRESS_DECOMPRESS_SINGLE_BLOCK,
                      CompressionTestStrategy.COMPRESS_DECOMPRESS_BLOCK,
                      CompressionTestStrategy.COMPRESS_DECOMPRESS_ERRORS,
                      CompressionTestStrategy.COMPRESS_DECOMPRESS_WITH_EMPTY_STREAM))
          .test();

    } catch (Exception ex) {
      GenericTestUtils.assertExceptionContains(
          "testCompressorDecompressorWithExeedBufferLimit error !!!", ex);
    }
  }
       
  public static byte[] generate(int size) {
    byte[] array = new byte[size];
    for (int i = 0; i < size; i++)
      array[i] = (byte) rnd.nextInt(16);
    return array;
  }
}
