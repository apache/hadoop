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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RandomDatum;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy;
import org.apache.hadoop.util.ReflectionUtils;

import junit.framework.TestCase;

public class TestCompressionStreamReuse extends TestCase {
  private static final Log LOG = LogFactory
      .getLog(TestCompressionStreamReuse.class);

  private Configuration conf = new Configuration();
  private int count = 10000;
  private int seed = new Random().nextInt();

  public void testBZip2Codec() throws IOException {
    resetStateTest(conf, seed, count,
        "org.apache.hadoop.io.compress.BZip2Codec");
  }

  public void testGzipCompressStreamReuse() throws IOException {
    resetStateTest(conf, seed, count,
        "org.apache.hadoop.io.compress.GzipCodec");
  }

  public void testGzipCompressStreamReuseWithParam() throws IOException {
    Configuration conf = new Configuration(this.conf);
    ZlibFactory
        .setCompressionLevel(conf, CompressionLevel.BEST_COMPRESSION);
    ZlibFactory.setCompressionStrategy(conf,
        CompressionStrategy.HUFFMAN_ONLY);
    resetStateTest(conf, seed, count,
        "org.apache.hadoop.io.compress.GzipCodec");
  }

  private static void resetStateTest(Configuration conf, int seed, int count,
      String codecClass) throws IOException {
    // Create the codec
    CompressionCodec codec = null;
    try {
      codec = (CompressionCodec) ReflectionUtils.newInstance(conf
          .getClassByName(codecClass), conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Illegal codec!");
    }
    LOG.info("Created a Codec object of type: " + codecClass);

    // Generate data
    DataOutputBuffer data = new DataOutputBuffer();
    RandomDatum.Generator generator = new RandomDatum.Generator(seed);
    for (int i = 0; i < count; ++i) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();

      key.write(data);
      value.write(data);
    }
    LOG.info("Generated " + count + " records");

    // Compress data
    DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
    DataOutputStream deflateOut = new DataOutputStream(
        new BufferedOutputStream(compressedDataBuffer));
    CompressionOutputStream deflateFilter = codec
        .createOutputStream(deflateOut);
    deflateFilter.write(data.getData(), 0, data.getLength());
    deflateFilter.finish();
    deflateFilter.flush();
    LOG.info("Finished compressing data");

    // reset deflator
    deflateFilter.resetState();
    LOG.info("Finished reseting deflator");

    // re-generate data
    data.reset();
    generator = new RandomDatum.Generator(seed);
    for (int i = 0; i < count; ++i) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();

      key.write(data);
      value.write(data);
    }
    DataInputBuffer originalData = new DataInputBuffer();
    DataInputStream originalIn = new DataInputStream(
        new BufferedInputStream(originalData));
    originalData.reset(data.getData(), 0, data.getLength());

    // re-compress data
    compressedDataBuffer.reset();
    deflateOut = new DataOutputStream(new BufferedOutputStream(
        compressedDataBuffer));
    deflateFilter = codec.createOutputStream(deflateOut);

    deflateFilter.write(data.getData(), 0, data.getLength());
    deflateFilter.finish();
    deflateFilter.flush();
    LOG.info("Finished re-compressing data");

    // De-compress data
    DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
    deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0,
        compressedDataBuffer.getLength());
    CompressionInputStream inflateFilter = codec
        .createInputStream(deCompressedDataBuffer);
    DataInputStream inflateIn = new DataInputStream(
        new BufferedInputStream(inflateFilter));

    // Check
    for (int i = 0; i < count; ++i) {
      RandomDatum k1 = new RandomDatum();
      RandomDatum v1 = new RandomDatum();
      k1.readFields(originalIn);
      v1.readFields(originalIn);

      RandomDatum k2 = new RandomDatum();
      RandomDatum v2 = new RandomDatum();
      k2.readFields(inflateIn);
      v2.readFields(inflateIn);
      assertTrue(
          "original and compressed-then-decompressed-output not equal",
          k1.equals(k2) && v1.equals(v2));
    }
    LOG.info("SUCCESS! Completed checking " + count + " records");
  }
}
