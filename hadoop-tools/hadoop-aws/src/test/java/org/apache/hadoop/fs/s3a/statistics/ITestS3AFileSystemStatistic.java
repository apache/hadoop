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

public class ITestS3AFileSystemStatistic extends AbstractS3ATestBase {

  private static final int ONE_MB = 1024 * 1024;
  private static final int TWO_MB = 2 * 1024 * 1024;

  /**
   * Verify the fs statistic bytesRead after reading from 2 different
   * InputStreams for the same filesystem instance.
   */
  @Test
  public void testBytesReadWithStream() throws IOException {
    S3AFileSystem fs = getFileSystem();
    Path filePath = path(getMethodName());
    byte[] oneMbBuf = new byte[ONE_MB];

    // Writing 1MB in a file.
    FSDataOutputStream out = fs.create(filePath);
    out.write(oneMbBuf);
    out.close();

    // Reading 1MB from first InputStream.
    FSDataInputStream in = fs.open(filePath, ONE_MB);
    in.readFully(0, oneMbBuf);
    in.close();

    // Reading 1MB from second InputStream.
    FSDataInputStream in2 = fs.open(filePath, ONE_MB);
    in2.readFully(0, oneMbBuf);
    in2.close();

    FileSystem.Statistics fsStats = fs.getFsStatistics();
    // Verifying that total bytes read by FS is equal to 2MB.
    assertEquals(TWO_MB, fsStats.getBytesRead());
  }
}
