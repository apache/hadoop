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

package org.apache.hadoop.fs.s3a.statistics;

import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.statistics.IOStatisticAssertions;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;

public class ITestS3AFileSystemStatistic extends AbstractS3ATestBase {

  private static final int ONE_KB = 1024;
  private static final int TWO_KB = 2 * ONE_KB;

  /**
   * Verify the fs statistic bytesRead after reading from 2 different
   * InputStreams for the same filesystem instance.
   */
  @Test
  public void testBytesReadWithStream() throws IOException {
    S3AFileSystem fs = getFileSystem();
    Path filePath = path(getMethodName());
    byte[] oneKbBuf = new byte[ONE_KB];

    // Writing 1KB in a file.
    try (FSDataOutputStream out = fs.create(filePath)) {
      out.write(oneKbBuf);
      // Verify if correct number of bytes were written.
      IOStatisticAssertions.assertThatStatisticCounter(out.getIOStatistics(),
          StreamStatisticNames.STREAM_WRITE_BYTES)
          .describedAs("Bytes written by OutputStream "
              + "should match the actual bytes")
          .isEqualTo(ONE_KB);
    }

    // Reading 1KB from first InputStream.
    try (FSDataInputStream in = fs.open(filePath, ONE_KB)) {
      in.readFully(0, oneKbBuf);
    }

    // Reading 1KB from second InputStream.
    try (FSDataInputStream in2 = fs.open(filePath, ONE_KB)) {
      in2.readFully(0, oneKbBuf);
    }

    FileSystem.Statistics fsStats = fs.getFsStatistics();
    // Verifying that total bytes read by FS is equal to 2KB.
    assertEquals("Mismatch in number of FS bytes read by InputStreams", TWO_KB,
        fsStats.getBytesRead());
  }
}
