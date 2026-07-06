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
package org.apache.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.util.DataChecksum;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link FSOutputSummer}.
 */
public class TestFSOutputSummer {

  /**
   * A minimal concrete subclass of FSOutputSummer for testing the
   * constructor in isolation, without needing a real output stream.
   */
  private static class DummyFSOutputSummer extends FSOutputSummer {
    DummyFSOutputSummer(DataChecksum sum) {
      super(sum);
    }

    @Override
    protected void writeChunk(byte[] b, int bOffset, int bLen,
        byte[] checksum, int checksumOffset, int checksumLen)
        throws IOException {
      // no-op for this test
    }

    @Override
    protected void checkClosed() throws IOException {
      // no-op for this test
    }
  }

  /**
   * HADOOP-18896: a large value of file.bytes-per-checksum causes the
   * buffer size computation (bytesPerChecksum * BUFFER_NUM_CHUNKS) to
   * overflow a signed int and become negative, which previously caused
   * a NegativeArraySizeException when allocating the buffer array.
   * After the fix, the constructor should instead fail fast with an
   * IllegalArgumentException.
   */
  @Test
  public void testLargeBytesPerChecksumOverflow() {
    // Chosen so that bytesPerChecksum * 9 (BUFFER_NUM_CHUNKS) overflows
    // Integer.MAX_VALUE and wraps around to a negative value.
    final int largeBytesPerChecksum = 238609295;
    DataChecksum sum = DataChecksum.newDataChecksum(
        DataChecksum.Type.CRC32, largeBytesPerChecksum);

    assertThrows(IllegalArgumentException.class,
        () -> new DummyFSOutputSummer(sum));
  }
}
