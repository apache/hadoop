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

package org.apache.hadoop.fs.tosfs.object.tos;

import com.volcengine.tos.model.object.GetObjectBasicOutput;
import com.volcengine.tos.model.object.GetObjectV2Output;
import org.apache.hadoop.fs.tosfs.common.Bytes;
import org.apache.hadoop.fs.tosfs.object.Constants;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestChainTOSInputStream {

  private static final int DATA_SIZE = 1 << 20;
  private static final byte[] DATA = TestUtility.rand(DATA_SIZE);

  @Test
  public void testRetryReadData() throws IOException {
    int readLen = DATA_SIZE - 1;
    int cutOff = readLen / 2;
    try (ChainTOSInputStream stream = createTestChainTOSInputStream(DATA, 0, DATA_SIZE - 1, 1024,
        cutOff)) {
      // The read length is more than the cut-off position, and equal to data length,
      // so the first stream will throw IOException, and fallback to the second stream.
      byte[] data = new byte[readLen];
      int n = stream.read(data);
      assertEquals(readLen, n);
      assertArrayEquals(Bytes.toBytes(DATA, 0, readLen), data);
    }

    try (ChainTOSInputStream stream = createTestChainTOSInputStream(DATA, 0, DATA_SIZE - 1, 1024,
        cutOff)) {
      // The read length is more than data length, so the first stream will throw IOException,
      // and fallback to the second stream.
      byte[] data = new byte[readLen + 2];
      int n = stream.read(data);
      assertEquals(readLen, n);
      assertArrayEquals(Bytes.toBytes(DATA, 0, readLen), Bytes.toBytes(data, 0, n));
    }

    readLen = DATA_SIZE / 3;
    cutOff = DATA_SIZE / 2;
    try (ChainTOSInputStream stream = createTestChainTOSInputStream(DATA, 0, DATA_SIZE, 1024,
        cutOff)) {
      for (int i = 0; i <= 3; i++) {
        // The cut-off position is between (readLen, 2 * readLen), so the data of first read come
        // from the first stream, and then the second read will meet IOException, and fallback to
        // the second stream.
        byte[] data = new byte[readLen];
        int n = stream.read(data);

        int off = i * readLen;
        int len = Math.min(readLen, DATA_SIZE - off);

        assertEquals(len, n);
        assertArrayEquals(Bytes.toBytes(DATA, off, len), Bytes.toBytes(data, 0, len));
      }
    }

    int smallDataSize = 1 << 10;
    cutOff = smallDataSize / 2;
    byte[] smallData = TestUtility.rand(1 << 10);
    try (ChainTOSInputStream stream = createTestChainTOSInputStream(smallData, 0, smallDataSize,
        1024, cutOff)) {
      for (int i = 0; i < smallDataSize; i++) {
        // The cut-off position is 512, the 512th read operation will meet IOException,
        // and then fallback to the second stream.
        int read = stream.read();
        assertEquals(smallData[i] & 0xFF, read);
      }
    }
  }

  @Test
  public void testSkipAndRead() throws IOException {
    int cutOff = (DATA_SIZE - 1) / 2;
    try (ChainTOSInputStream stream = createTestChainTOSInputStream(DATA, 0, DATA_SIZE - 1, 1024,
        cutOff)) {
      // The skip pos is equal to cut-off pos, once skip finished, the first read operation will
      // meet IOException, and the fallback to the second stream.
      int readPos = (DATA_SIZE - 1) / 2;
      stream.skip(readPos);

      int readLen = 1024;
      byte[] data = new byte[readLen];
      int n = stream.read(data);
      assertEquals(readLen, n);
      assertArrayEquals(Bytes.toBytes(DATA, readPos, readLen), data);
    }

    try (ChainTOSInputStream stream = createTestChainTOSInputStream(DATA, 0, DATA_SIZE - 1, 1024,
        cutOff)) {
      // The skip pos is more than cut-off pos, the skip operation will throw IOException,
      // and the fallback to the second stream and skip(readPos) again
      int readPos = cutOff + 1024;
      stream.skip(readPos);

      int readLen = 1024;
      byte[] data = new byte[readLen];
      int n = stream.read(data);
      assertEquals(readLen, n);
      assertArrayEquals(Bytes.toBytes(DATA, readPos, readLen), data);
    }

    try (ChainTOSInputStream stream = createTestChainTOSInputStream(DATA, 0, DATA_SIZE - 1, 1024,
        cutOff)) {
      // The skip pos = cut-off pos - 1025, the skip operation will succeed on the first stream,
      // the 1024 bytes read operation also succeed on the first stream,
      // but the next 1024 bytes read operation will fail on the first stream, and fallback to the
      // second stream
      int readPos = cutOff - 1024 - 1;
      stream.skip(readPos);

      int readLen = 1024;
      byte[] data = new byte[readLen];
      int n = stream.read(data);
      assertEquals(readLen, n);
      assertArrayEquals(Bytes.toBytes(DATA, readPos, readLen), data);

      n = stream.read(data);
      assertEquals(readLen, n);
      assertArrayEquals(Bytes.toBytes(DATA, readPos + 1024, readLen), data);
    }

    try (ChainTOSInputStream stream = createTestChainTOSInputStream(DATA, 0, DATA_SIZE - 1, 1024,
        cutOff)) {
      // 1. Skip 1024 bytes and then read 1024 bytes from the first stream.
      // 2. And then skip cut-off - 512 bytes, the target off = 1024 + 1024 + cut-off - 512,
      // which is bigger than cut-off pos, so the second skip operation will fail,
      // and then fallback to the second stream.
      // 3. Read 1024 bytes
      int readPos = 1024;
      stream.skip(readPos);

      int readLen = 1024;
      byte[] data = new byte[readLen];
      int n = stream.read(data);
      assertEquals(readLen, n);
      assertArrayEquals(Bytes.toBytes(DATA, readPos, readLen), data);

      int skipPos = cutOff - 512;
      stream.skip(skipPos);

      n = stream.read(data);
      assertEquals(readLen, n);
      int targetOff = readPos + 1024 + skipPos;
      assertArrayEquals(Bytes.toBytes(DATA, targetOff, readLen), data);
    }
  }

  /**
   * The ChainTOSInputStream contains two stream created by TestObjectFactory.
   * Once the read pos of first stream is more than cutPos, the stream will throw IOException with
   * unexpect end of stream error msg, but the second stream will contain the remaining data.
   */
  private ChainTOSInputStream createTestChainTOSInputStream(byte[] data, long startOff, long endOff,
      long maxDrainSize, long cutPos) {
    String key = "dummy-key";
    TOS.GetObjectFactory factory = new TestObjectFactory(data, Arrays.asList(cutPos, -1L));
    return new ChainTOSInputStream(factory, key, startOff, endOff, maxDrainSize, 1);
  }

  private static class TestObjectFactory implements TOS.GetObjectFactory {
    private final byte[] data;
    private final List<Long> streamBreakPoses;
    private int streamIndex = 0;

    TestObjectFactory(byte[] data, List<Long> streamBreakPoses) {
      this.data = data;
      this.streamBreakPoses = streamBreakPoses;
    }

    @Override
    public GetObjectOutput create(String key, long offset, long end) {
      long len = Math.min(end, data.length) - offset;
      ByteArrayInputStream dataIn = new ByteArrayInputStream(this.data, (int) offset, (int) len);

      if (streamIndex < streamBreakPoses.size()) {
        return new GetObjectOutput(new GetObjectV2Output(new GetObjectBasicOutput(),
            new UnExpectedEndOfStream(dataIn, streamBreakPoses.get(streamIndex++))),
            Constants.MAGIC_CHECKSUM);
      } else {
        throw new RuntimeException("No more output");
      }
    }
  }

  private static class UnExpectedEndOfStream extends InputStream {
    private final ByteArrayInputStream delegate;
    private final long breakPos;
    private int readPos;

    UnExpectedEndOfStream(ByteArrayInputStream stream, long breakPos) {
      delegate = stream;
      this.breakPos = breakPos;
    }

    @Override
    public int read() throws IOException {
      if (breakPos != -1 && readPos >= breakPos) {
        throw new IOException("unexpected end of stream on dummy source.");
      } else {
        int n = delegate.read();
        readPos += 1;
        return n;
      }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (breakPos != -1 && readPos >= breakPos) {
        throw new IOException("unexpected end of stream on dummy source.");
      } else {
        int n = delegate.read(b, off, len);
        readPos += n;
        return n;
      }
    }
  }
}
