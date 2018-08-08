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

package org.apache.hadoop.fs.azurebfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;

/**
 * Test copy operation.
 */
public class ITestAzureBlobFileSystemCopy extends AbstractAbfsIntegrationTest {
  public ITestAzureBlobFileSystemCopy() {
    super();
  }

  @Test
  public void testCopyFromLocalFileSystem() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path localFilePath = new Path(System.getProperty("test.build.data",
        "azure_test"));
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    localFs.delete(localFilePath, true);
    try {
      writeString(localFs, localFilePath, "Testing");
      Path dstPath = new Path("copiedFromLocal");
      assertTrue(FileUtil.copy(localFs, localFilePath, fs, dstPath, false,
          fs.getConf()));
      assertIsFile(fs, dstPath);
      assertEquals("Testing", readString(fs, dstPath));
      fs.delete(dstPath, true);
    } finally {
      localFs.delete(localFilePath, true);
    }
  }

  private String readString(FileSystem fs, Path testFile) throws IOException {
    return readString(fs.open(testFile));
  }

  private String readString(FSDataInputStream inputStream) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        inputStream))) {
      final int bufferSize = 1024;
      char[] buffer = new char[bufferSize];
      int count = reader.read(buffer, 0, bufferSize);
      if (count > bufferSize) {
        throw new IOException("Exceeded buffer size");
      }
      return new String(buffer, 0, count);
    }
  }

  private void writeString(FileSystem fs, Path path, String value)
      throws IOException {
    writeString(fs.create(path, true), value);
  }

  private void writeString(FSDataOutputStream outputStream, String value)
      throws IOException {
    try(BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(outputStream))) {
      writer.write(value);
    }
  }
}
