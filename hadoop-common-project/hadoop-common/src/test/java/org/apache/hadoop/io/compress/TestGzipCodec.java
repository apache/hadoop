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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Verify resettable compressor.
 */
public class TestGzipCodec {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestGzipCodec.class);

  private static final String DATA1 = "Dogs don't know it's not bacon!\n";
  private static final String DATA2 = "It's baconnnn!!\n";
  private GzipCodec codec = new GzipCodec();

  @Before
  public void setUp() {
    codec.setConf(new Configuration(false));
  }

  // Test simple compression.
  @Test
  public void testSingleCompress() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CompressionOutputStream cmpOut = codec.createOutputStream(baos);
    cmpOut.write(DATA1.getBytes(StandardCharsets.UTF_8));
    cmpOut.finish();
    cmpOut.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    GZIPInputStream cmpIn = new GZIPInputStream(bais);
    byte[] buf = new byte[1024];
    int len = cmpIn.read(buf);
    String result = new String(buf, 0, len, StandardCharsets.UTF_8);
    assertEquals("Input must match output", DATA1, result);
  }

  // Test multi-member gzip file created via finish(), resetState().
  @Test
  public void testResetCompress() throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    CompressionOutputStream cmpOut = codec.createOutputStream(dob);
    cmpOut.write(DATA1.getBytes(StandardCharsets.UTF_8));
    cmpOut.finish();
    cmpOut.resetState();
    cmpOut.write(DATA2.getBytes(StandardCharsets.UTF_8));
    cmpOut.finish();
    cmpOut.close();
    dob.close();

    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), 0, dob.getLength());
    CompressionInputStream cmpIn = codec.createInputStream(dib);
    byte[] buf = new byte[1024];
    StringBuilder result = new StringBuilder();
    int len = 0;
    while (true) {
      len = cmpIn.read(buf);
      if (len < 0) {
        break;
      }
      result.append(new String(buf, 0, len, StandardCharsets.UTF_8));
    }
    assertEquals("Output must match input", DATA1 + DATA2, result.toString());
  }

  // ensure all necessary methods are overwritten
  @Test
  public void testWriteOverride() throws IOException {
    Random r = new Random();
    long seed = r.nextLong();
    LOG.info("seed: " + seed);
    r.setSeed(seed);
    byte[] buf = new byte[128];
    r.nextBytes(buf);
    DataOutputBuffer dob = new DataOutputBuffer();
    CompressionOutputStream cmpOut = codec.createOutputStream(dob);
    cmpOut.write(buf);
    int i = r.nextInt(128 - 10);
    int l = r.nextInt(128 - i);
    cmpOut.write(buf, i, l);
    cmpOut.write((byte)(r.nextInt() & 0xFF));
    cmpOut.close();

    r.setSeed(seed);
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), 0, dob.getLength());
    CompressionInputStream cmpIn = codec.createInputStream(dib);
    byte[] vbuf = new byte[128];
    assertEquals(128, cmpIn.read(vbuf));
    assertArrayEquals(buf, vbuf);
    r.nextBytes(vbuf);
    int vi = r.nextInt(128 - 10);
    int vl = r.nextInt(128 - vi);
    assertEquals(vl, cmpIn.read(vbuf, 0, vl));
    assertArrayEquals(Arrays.copyOfRange(buf, i,  i + l),
        Arrays.copyOf(vbuf, vl));
    assertEquals(r.nextInt() & 0xFF, cmpIn.read());
    assertEquals(-1, cmpIn.read());
  }

  // don't write a new header if no data are written after reset
  @Test
  public void testIdempotentResetState() throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    CompressionOutputStream cmpOut = codec.createOutputStream(dob);
    cmpOut.write(DATA1.getBytes(StandardCharsets.UTF_8));
    cmpOut.finish();
    cmpOut.finish();
    cmpOut.finish();
    cmpOut.resetState();
    cmpOut.resetState();
    cmpOut.finish();
    cmpOut.resetState();
    cmpOut.close();
    dob.close();

    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), 0, dob.getLength());
    CompressionInputStream cmpIn = codec.createInputStream(dib);
    byte[] buf = new byte[1024];
    StringBuilder result = new StringBuilder();
    int len = 0;
    while (true) {
      len = cmpIn.read(buf);
      if (len < 0) {
        break;
      }
      result.append(new String(buf, 0, len, StandardCharsets.UTF_8));
    }
    assertEquals("Output must match input", DATA1, result.toString());
  }
}
