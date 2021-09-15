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

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.assume;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_WRITE_BUFFER_SIZE;

/**
 * Testing Huge file for AbfsOutputStream.
 */
@RunWith(Parameterized.class)
public class ITestAbfsHugeFiles extends AbstractAbfsScaleTest {
  private static final int ONE_MB = 1024 * 1024;
  private static final int EIGHT_MB = 8 * ONE_MB;
  private final int size;

  @Parameterized.Parameters(name = "Size={0}")
  public static Iterable<Object[]> sizes() {
    return Arrays.asList(new Object[][] {
        { DEFAULT_WRITE_BUFFER_SIZE },
        { getHugeFileUploadValue() } });
  }

  public ITestAbfsHugeFiles(int size) throws Exception {
    this.size = size;
  }

  /**
   * Testing Huge files written at once on AbfsOutputStream.
   */
  @Test
  public void testHugeFileWrite() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    Path filePath = path(getMethodName());
    final byte[] b = new byte[size];
    new Random().nextBytes(b);
    try (FSDataOutputStream out = fs.create(filePath)) {
      out.write(b);
    }
    // Verify correct length was uploaded. Don't want to verify contents
    // here, as this would increase the test time significantly.
    assertEquals("Mismatch in content length of file uploaded", size,
        fs.getFileStatus(filePath).getLen());
  }

  /**
   * Testing Huge files written in chunks of 8M in lots of writes.
   */
  @Test
  public void testLotsOfWrites() throws IOException {
    assume("If the size isn't a multiple of 8M this test would not pass, so "
        + "skip", size % EIGHT_MB == 0);
    AzureBlobFileSystem fs = getFileSystem();
    Path filePath = path(getMethodName());
    final byte[] b = new byte[size];
    new Random().nextBytes(b);
    try (FSDataOutputStream out = fs.create(filePath)) {
      int offset = 0;
      for (int i = 0; i < size / EIGHT_MB; i++) {
        out.write(b, offset, EIGHT_MB);
        offset += EIGHT_MB;
      }
    }
    LOG.info(String.valueOf(size % EIGHT_MB));
    // Verify correct length was uploaded. Don't want to verify contents
    // here, as this would increase the test time significantly.
    assertEquals("Mismatch in content length of file uploaded", size,
        fs.getFileStatus(filePath).getLen());
  }
}
