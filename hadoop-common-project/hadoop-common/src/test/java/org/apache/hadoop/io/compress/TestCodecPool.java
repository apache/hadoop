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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class TestCodecPool {
  private final String LEASE_COUNT_ERR =
      "Incorrect number of leased (de)compressors";
  DefaultCodec codec;

  @Before
  public void setup() {
    this.codec = new DefaultCodec();
    this.codec.setConf(new Configuration());
  }

  @Test(timeout = 10000)
  public void testCompressorPoolCounts() {
    // Get two compressors and return them
    Compressor comp1 = CodecPool.getCompressor(codec);
    Compressor comp2 = CodecPool.getCompressor(codec);
    assertEquals(LEASE_COUNT_ERR, 2,
        CodecPool.getLeasedCompressorsCount(codec));

    CodecPool.returnCompressor(comp2);
    assertEquals(LEASE_COUNT_ERR, 1,
        CodecPool.getLeasedCompressorsCount(codec));

    CodecPool.returnCompressor(comp1);
    assertEquals(LEASE_COUNT_ERR, 0,
        CodecPool.getLeasedCompressorsCount(codec));

    CodecPool.returnCompressor(comp1);
    assertEquals(LEASE_COUNT_ERR, 0,
        CodecPool.getLeasedCompressorsCount(codec));
  }

  @Test(timeout = 10000)
  public void testCompressorNotReturnSameInstance() {
    Compressor comp = CodecPool.getCompressor(codec);
    CodecPool.returnCompressor(comp);
    CodecPool.returnCompressor(comp);
    Set<Compressor> compressors = new HashSet<Compressor>();
    for (int i = 0; i < 10; ++i) {
      compressors.add(CodecPool.getCompressor(codec));
    }
    assertEquals(10, compressors.size());
    for (Compressor compressor : compressors) {
      CodecPool.returnCompressor(compressor);
    }
  }

  @Test(timeout = 10000)
  public void testDecompressorPoolCounts() {
    // Get two decompressors and return them
    Decompressor decomp1 = CodecPool.getDecompressor(codec);
    Decompressor decomp2 = CodecPool.getDecompressor(codec);
    assertEquals(LEASE_COUNT_ERR, 2,
        CodecPool.getLeasedDecompressorsCount(codec));

    CodecPool.returnDecompressor(decomp2);
    assertEquals(LEASE_COUNT_ERR, 1,
        CodecPool.getLeasedDecompressorsCount(codec));

    CodecPool.returnDecompressor(decomp1);
    assertEquals(LEASE_COUNT_ERR, 0,
        CodecPool.getLeasedDecompressorsCount(codec));

    CodecPool.returnDecompressor(decomp1);
    assertEquals(LEASE_COUNT_ERR, 0,
        CodecPool.getLeasedCompressorsCount(codec));
  }

  @Test(timeout = 10000)
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

    assertEquals(LEASE_COUNT_ERR, 0, CodecPool.getLeasedCompressorsCount(codec));
  }

  @Test(timeout = 10000)
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

    assertEquals(LEASE_COUNT_ERR, 0,
        CodecPool.getLeasedDecompressorsCount(codec));
  }

  @Test(timeout = 10000)
  public void testDecompressorNotReturnSameInstance() {
    Decompressor decomp = CodecPool.getDecompressor(codec);
    CodecPool.returnDecompressor(decomp);
    CodecPool.returnDecompressor(decomp);
    Set<Decompressor> decompressors = new HashSet<Decompressor>();
    for (int i = 0; i < 10; ++i) {
      decompressors.add(CodecPool.getDecompressor(codec));
    }
    assertEquals(10, decompressors.size());
    for (Decompressor decompressor : decompressors) {
      CodecPool.returnDecompressor(decompressor);
    }
  }
}
