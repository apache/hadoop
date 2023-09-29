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
import java.util.Collection;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.store.DataBlocks;

import static org.apache.hadoop.fs.azure.integration.AzureTestConstants.AZURE_SCALE_HUGE_FILE_UPLOAD;
import static org.apache.hadoop.fs.azure.integration.AzureTestConstants.AZURE_SCALE_HUGE_FILE_UPLOAD_DEFAULT;
import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.assume;
import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.getTestPropertyInt;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.DATA_BLOCKS_BUFFER;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_WRITE_BUFFER_SIZE;

/**
 * Testing Huge file for AbfsOutputStream.
 */
@RunWith(Parameterized.class)
public class ITestAbfsHugeFiles extends AbstractAbfsScaleTest {
  private static final int ONE_MB = 1024 * 1024;
  private static final int EIGHT_MB = 8 * ONE_MB;
  // Configurable huge file upload: "fs.azure.scale.test.huge.upload",
  // default is 2 * DEFAULT_WRITE_BUFFER_SIZE(8M).
  private static final int HUGE_FILE;

  // Set the HUGE_FILE.
  static {
    HUGE_FILE = getTestPropertyInt(new Configuration(),
        AZURE_SCALE_HUGE_FILE_UPLOAD, AZURE_SCALE_HUGE_FILE_UPLOAD_DEFAULT);
  }

  // Writing block size to be used in this test.
  private int size;
  // Block Factory to be used in this test.
  private String blockFactoryName;

  @Parameterized.Parameters(name = "size [{0}] ; blockFactoryName "
      + "[{1}]")
  public static Collection<Object[]> sizes() {
    return Arrays.asList(new Object[][] {
        { DEFAULT_WRITE_BUFFER_SIZE, DataBlocks.DATA_BLOCKS_BUFFER_DISK },
        { HUGE_FILE, DataBlocks.DATA_BLOCKS_BUFFER_DISK },
        { DEFAULT_WRITE_BUFFER_SIZE, DataBlocks.DATA_BLOCKS_BUFFER_ARRAY },
        { HUGE_FILE, DataBlocks.DATA_BLOCKS_BUFFER_ARRAY },
        { DEFAULT_WRITE_BUFFER_SIZE, DataBlocks.DATA_BLOCKS_BYTEBUFFER },
        { HUGE_FILE, DataBlocks.DATA_BLOCKS_BYTEBUFFER },
    });
  }

  public ITestAbfsHugeFiles(int size, String blockFactoryName)
      throws Exception {
    this.size = size;
    this.blockFactoryName = blockFactoryName;
  }

  @Before
  public void setUp() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.unset(DATA_BLOCKS_BUFFER);
    configuration.set(DATA_BLOCKS_BUFFER, blockFactoryName);
    super.setup();
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
            + "skip",
        size % EIGHT_MB == 0);
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
    // Verify correct length was uploaded. Don't want to verify contents
    // here, as this would increase the test time significantly.
    assertEquals("Mismatch in content length of file uploaded", size,
        fs.getFileStatus(filePath).getLen());
  }
}
