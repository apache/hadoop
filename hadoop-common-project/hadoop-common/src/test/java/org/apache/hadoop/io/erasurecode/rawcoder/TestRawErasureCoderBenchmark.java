/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Tests for the raw erasure coder benchmark tool.
 */
public class TestRawErasureCoderBenchmark {

  @Test
  public void testDummyCoder() throws Exception {
    // Dummy coder
    RawErasureCoderBenchmark.performBench("encode",
        RawErasureCoderBenchmark.CODER.DUMMY_CODER, 2, 100, 1024);
    RawErasureCoderBenchmark.performBench("decode",
        RawErasureCoderBenchmark.CODER.DUMMY_CODER, 5, 150, 100);

    try {
      RawErasureCoderBenchmark.performBench("decode",
          RawErasureCoderBenchmark.CODER.DUMMY_CODER, 5, -150, 100);
      Assert.fail("should have thrown an IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // intentionally swallow exception
    }
  }

  @Test
  public void testLegacyRSCoder() throws Exception {
    // Legacy RS Java coder
    RawErasureCoderBenchmark.performBench("encode",
        RawErasureCoderBenchmark.CODER.LEGACY_RS_CODER, 2, 80, 200);
    RawErasureCoderBenchmark.performBench("decode",
        RawErasureCoderBenchmark.CODER.LEGACY_RS_CODER, 5, 300, 350);
  }

  @Test
  public void testRSCoder() throws Exception {
    // RS Java coder
    RawErasureCoderBenchmark.performBench("encode",
        RawErasureCoderBenchmark.CODER.RS_CODER, 3, 200, 200);
    RawErasureCoderBenchmark.performBench("decode",
        RawErasureCoderBenchmark.CODER.RS_CODER, 4, 135, 20);
  }

  @Test
  public void testISALCoder() throws Exception {
    Assume.assumeTrue(ErasureCodeNative.isNativeCodeLoaded());
    // ISA-L coder
    RawErasureCoderBenchmark.performBench("encode",
        RawErasureCoderBenchmark.CODER.ISAL_CODER, 5, 300, 64);
    RawErasureCoderBenchmark.performBench("decode",
        RawErasureCoderBenchmark.CODER.ISAL_CODER, 6, 200, 128);
  }
}
