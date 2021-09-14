/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.hadoop.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.*;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RandomDatum;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ensure that we can perform codec operations given the API and runtime jars
 * by performing some simple smoke tests.
 */
public class ITUseHadoopCodecs {

  private static final Logger LOG = LoggerFactory.getLogger(ITUseHadoopCodecs.class);

  private Configuration haddopConf = new Configuration();
  private int dataCount = 100;
  private int dataSeed = new Random().nextInt();

  @Test
  public void testGzipCodec() throws IOException {
    ZlibFactory.setNativeZlibLoaded(false);
    assertFalse(ZlibFactory.isNativeZlibLoaded(haddopConf));
    codecTest(haddopConf, dataSeed, 0, "org.apache.hadoop.io.compress.GzipCodec");
    codecTest(haddopConf, dataSeed, dataCount, "org.apache.hadoop.io.compress.GzipCodec");
  }

  @Test
  public void testSnappyCodec() throws IOException {
    codecTest(haddopConf, dataSeed, 0, "org.apache.hadoop.io.compress.SnappyCodec");
    codecTest(haddopConf, dataSeed, dataCount, "org.apache.hadoop.io.compress.SnappyCodec");
  }

  @Test
  public void testLz4Codec() {
    Arrays.asList(false, true).forEach(config -> {
      haddopConf.setBoolean(
          CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_USELZ4HC_KEY,
          config);
      try {
        codecTest(haddopConf, dataSeed, 0, "org.apache.hadoop.io.compress.Lz4Codec");
        codecTest(haddopConf, dataSeed, dataCount, "org.apache.hadoop.io.compress.Lz4Codec");
      } catch (IOException e) {
        throw new RuntimeException("failed when running codecTest", e);
      }
    });
  }

  private void codecTest(Configuration conf, int seed, int count, String codecClass)
      throws IOException {

    // Create the codec
    CompressionCodec codec = null;
    try {
      codec = (CompressionCodec)
              ReflectionUtils.newInstance(conf.getClassByName(codecClass), conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Illegal codec!");
    }
    LOG.info("Created a Codec object of type: " + codecClass);

    // Generate data
    DataOutputBuffer data = new DataOutputBuffer();
    RandomDatum.Generator generator = new RandomDatum.Generator(seed);
    for(int i = 0; i < count; ++i) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();

      key.write(data);
      value.write(data);
    }
    LOG.info("Generated " + count + " records");

    // Compress data
    DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
    try (CompressionOutputStream deflateFilter =
        codec.createOutputStream(compressedDataBuffer);
        DataOutputStream deflateOut =
            new DataOutputStream(new BufferedOutputStream(deflateFilter))) {
      deflateOut.write(data.getData(), 0, data.getLength());
      deflateOut.flush();
      deflateFilter.finish();
    }

    // De-compress data
    DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
    deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0,
            compressedDataBuffer.getLength());
    DataInputBuffer originalData = new DataInputBuffer();
    originalData.reset(data.getData(), 0, data.getLength());
    try (CompressionInputStream inflateFilter =
        codec.createInputStream(deCompressedDataBuffer);
        DataInputStream originalIn =
            new DataInputStream(new BufferedInputStream(originalData))) {

      // Check
      int expected;
      do {
        expected = originalIn.read();
        assertEquals("Inflated stream read by byte does not match",
                expected, inflateFilter.read());
      } while (expected != -1);
    }

    LOG.info("SUCCESS! Completed checking " + count + " records");
  }
}
