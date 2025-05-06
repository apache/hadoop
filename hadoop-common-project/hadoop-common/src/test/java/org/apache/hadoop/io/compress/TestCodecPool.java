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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.zlib.BuiltInGzipCompressor;
import org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashSet;
import java.util.Set;

public class TestCodecPool {
  private final String LEASE_COUNT_ERR =
      "Incorrect number of leased (de)compressors";
  DefaultCodec codec;

  @BeforeEach
  public void setup() {
    this.codec = new DefaultCodec();
    this.codec.setConf(new Configuration());
  }

  @Test
  @Timeout(value = 10)
  public void testCompressorPoolCounts() {
    // Get two compressors and return them
    Compressor comp1 = CodecPool.getCompressor(codec);
    Compressor comp2 = CodecPool.getCompressor(codec);
    assertEquals(2,
        CodecPool.getLeasedCompressorsCount(codec),
        LEASE_COUNT_ERR);

    CodecPool.returnCompressor(comp2);
    assertEquals(1,
        CodecPool.getLeasedCompressorsCount(codec),
        LEASE_COUNT_ERR);

    CodecPool.returnCompressor(comp1);
    assertEquals(0,
        CodecPool.getLeasedCompressorsCount(codec),
        LEASE_COUNT_ERR);

    CodecPool.returnCompressor(comp1);
    assertEquals(0,
        CodecPool.getLeasedCompressorsCount(codec),
        LEASE_COUNT_ERR);
  }

  @Test
  @Timeout(value = 10)
  public void testCompressorNotReturnSameInstance() {
    Compressor comp = CodecPool.getCompressor(codec);
    CodecPool.returnCompressor(comp);
    CodecPool.returnCompressor(comp);
    Set<Compressor> compressors = new HashSet<>();
    for (int i = 0; i < 10; ++i) {
      compressors.add(CodecPool.getCompressor(codec));
    }
    assertEquals(10, compressors.size());
    for (Compressor compressor : compressors) {
      CodecPool.returnCompressor(compressor);
    }
  }

  @Test
  @Timeout(value = 10)
  public void testCompressorConf() throws Exception {
    DefaultCodec codec1 = new DefaultCodec();
    Configuration conf = new Configuration();
    ZlibFactory.setCompressionLevel(conf, CompressionLevel.TWO);
    codec1.setConf(conf);
    Compressor comp1 = CodecPool.getCompressor(codec1);
    CodecPool.returnCompressor(comp1);

    DefaultCodec codec2 = new DefaultCodec();
    Configuration conf2 = new Configuration();
    CompressionLevel newCompressionLevel = CompressionLevel.THREE;
    ZlibFactory.setCompressionLevel(conf2, newCompressionLevel);
    codec2.setConf(conf2);
    Compressor comp2 = CodecPool.getCompressor(codec2);
    List<Field> fields = ReflectionUtils.getDeclaredFieldsIncludingInherited(comp2.getClass());
    for (Field field : fields) {
      if (field.getName().equals("level")) {
        field.setAccessible(true);
        Object levelValue = field.get(comp2);
        if (levelValue instanceof CompressionLevel) {
          assertEquals(newCompressionLevel, levelValue);
        } else {
          assertEquals(3, levelValue);
        }
      }
    }
    CodecPool.returnCompressor(comp2);
  }

  @Test
  @Timeout(value = 10)
  public void testDecompressorPoolCounts() {
    // Get two decompressors and return them
    Decompressor decomp1 = CodecPool.getDecompressor(codec);
    Decompressor decomp2 = CodecPool.getDecompressor(codec);
    assertEquals(2,
        CodecPool.getLeasedDecompressorsCount(codec), LEASE_COUNT_ERR);

    CodecPool.returnDecompressor(decomp2);
    assertEquals(1,
        CodecPool.getLeasedDecompressorsCount(codec), LEASE_COUNT_ERR);

    CodecPool.returnDecompressor(decomp1);
    assertEquals(0,
        CodecPool.getLeasedDecompressorsCount(codec), LEASE_COUNT_ERR);

    CodecPool.returnDecompressor(decomp1);
    assertEquals(0,
        CodecPool.getLeasedCompressorsCount(codec), LEASE_COUNT_ERR);
  }

  @Test
  @Timeout(value = 10)
  public void testMultiThreadedCompressorPool() throws InterruptedException {
    final int iterations = 4;
    ExecutorService threadpool = Executors.newFixedThreadPool(3);
    final LinkedBlockingDeque<Compressor> queue = new LinkedBlockingDeque<Compressor>(
        2 * iterations);

    Callable<Boolean> consumer = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Compressor c = queue.take();
        CodecPool.returnCompressor(c);
        return c != null;
      }
    };

    Callable<Boolean> producer = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Compressor c = CodecPool.getCompressor(codec);
        queue.put(c);
        return c != null;
      }
    };

    for (int i = 0; i < iterations; i++) {
      threadpool.submit(consumer);
      threadpool.submit(producer);
    }

    // wait for completion
    threadpool.shutdown();
    threadpool.awaitTermination(1000, TimeUnit.SECONDS);

    assertEquals(0, CodecPool.getLeasedCompressorsCount(codec),
        LEASE_COUNT_ERR);
  }

  @Test
  @Timeout(value = 10)
  public void testMultiThreadedDecompressorPool() throws InterruptedException {
    final int iterations = 4;
    ExecutorService threadpool = Executors.newFixedThreadPool(3);
    final LinkedBlockingDeque<Decompressor> queue = new LinkedBlockingDeque<Decompressor>(
        2 * iterations);

    Callable<Boolean> consumer = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Decompressor dc = queue.take();
        CodecPool.returnDecompressor(dc);
        return dc != null;
      }
    };

    Callable<Boolean> producer = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Decompressor c = CodecPool.getDecompressor(codec);
        queue.put(c);
        return c != null;
      }
    };

    for (int i = 0; i < iterations; i++) {
      threadpool.submit(consumer);
      threadpool.submit(producer);
    }

    // wait for completion
    threadpool.shutdown();
    threadpool.awaitTermination(1000, TimeUnit.SECONDS);

    assertEquals(0,
        CodecPool.getLeasedDecompressorsCount(codec), LEASE_COUNT_ERR);
  }

  @Test
  @Timeout(value = 10)
  public void testDecompressorNotReturnSameInstance() {
    Decompressor decomp = CodecPool.getDecompressor(codec);
    CodecPool.returnDecompressor(decomp);
    CodecPool.returnDecompressor(decomp);
    Set<Decompressor> decompressors = new HashSet<>();
    for (int i = 0; i < 10; ++i) {
      decompressors.add(CodecPool.getDecompressor(codec));
    }
    assertEquals(10, decompressors.size());
    for (Decompressor decompressor : decompressors) {
      CodecPool.returnDecompressor(decompressor);
    }
  }

  @Test
  @Timeout(value = 10)
  public void testDoNotPoolCompressorNotUseableAfterReturn() throws Exception {

    final GzipCodec gzipCodec = new GzipCodec();
    gzipCodec.setConf(new Configuration());

    // BuiltInGzipCompressor is an explicit example of a Compressor with the @DoNotPool annotation
    final Compressor compressor = new BuiltInGzipCompressor(new Configuration());
    CodecPool.returnCompressor(compressor);

    final CompressionOutputStream outputStream =
            gzipCodec.createOutputStream(new ByteArrayOutputStream(), compressor);
    LambdaTestUtils.intercept(
            AlreadyClosedException.class,
            "compress called on closed compressor",
            "Compressor from Codec with @DoNotPool should not be " +
                    "useable after returning to CodecPool",
        () -> outputStream.write(1));
  }

  @Test
  @Timeout(value = 10)
  public void testDoNotPoolDecompressorNotUseableAfterReturn() throws Exception {

    final GzipCodec gzipCodec = new GzipCodec();
    gzipCodec.setConf(new Configuration());

    final Random random = new Random();
    final byte[] bytes = new byte[1024];
    random.nextBytes(bytes);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (OutputStream outputStream = gzipCodec.createOutputStream(baos)) {
      outputStream.write(bytes);
    }

    final byte[] gzipBytes = baos.toByteArray();
    final ByteArrayInputStream bais = new ByteArrayInputStream(gzipBytes);

    // BuiltInGzipDecompressor is an explicit example of a Decompressor
    // with the @DoNotPool annotation
    final Decompressor decompressor = new BuiltInGzipDecompressor();
    CodecPool.returnDecompressor(decompressor);

    final CompressionInputStream inputStream = gzipCodec.createInputStream(bais, decompressor);
    LambdaTestUtils.intercept(
            AlreadyClosedException.class,
            "decompress called on closed decompressor",
            "Decompressor from Codec with @DoNotPool should not be " +
                    "useable after returning to CodecPool",
        () -> inputStream.read());
  }
}
