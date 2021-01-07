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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class TestDecompressorStream {
  private static final String TEST_STRING =
      "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

  private ByteArrayInputStream bytesIn;
  private Decompressor decompressor;
  private DecompressorStream decompressorStream;

  @Before
  public void setUp() throws IOException {
    bytesIn = new ByteArrayInputStream(TEST_STRING.getBytes());
    decompressor = new FakeDecompressor();
    decompressorStream =
        new DecompressorStream(bytesIn, decompressor, 20, 13);
  }

  @Test
  public void testReadOneByte() throws IOException {
    for (int i = 0; i < TEST_STRING.length(); ++i) {
      assertThat(decompressorStream.read(), is((int) TEST_STRING.charAt(i)));
    }
    try {
      int ret = decompressorStream.read();
      fail("Not reachable but got ret " + ret);
    } catch (EOFException e) {
      // Expect EOF exception
    }
  }

  @Test
  public void testReadBuffer() throws IOException {
    // 32 buf.length < 52 TEST_STRING.length()
    byte[] buf = new byte[32];
    int bytesToRead = TEST_STRING.length();
    int i = 0;
    while (bytesToRead > 0) {
      int n = Math.min(bytesToRead, buf.length);
      int bytesRead = decompressorStream.read(buf, 0, n);
      assertTrue(bytesRead > 0 && bytesRead <= n);
      assertThat(new String(buf, 0, bytesRead),
          is(TEST_STRING.substring(i, i + bytesRead)));
      bytesToRead = bytesToRead - bytesRead;
      i = i + bytesRead;
    }
    try {
      int ret = decompressorStream.read(buf, 0, buf.length);
      fail("Not reachable but got ret " + ret);
    } catch (EOFException e) {
      // Expect EOF exception
    }
  }

  @Test
  public void testSkip() throws IOException {
    assertThat(decompressorStream.skip(12), is(12L));
    assertThat(decompressorStream.read(), is((int)TEST_STRING.charAt(12)));
    assertThat(decompressorStream.read(), is((int)TEST_STRING.charAt(13)));
    assertThat(decompressorStream.read(), is((int)TEST_STRING.charAt(14)));
    assertThat(decompressorStream.skip(10), is(10L));
    assertThat(decompressorStream.read(), is((int)TEST_STRING.charAt(25)));
    try {
      long ret = decompressorStream.skip(1000);
      fail("Not reachable but got ret " + ret);
    } catch (EOFException e) {
      // Expect EOF exception
    }
  }
}