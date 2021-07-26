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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

import org.junit.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.contract.ContractTestUtils;

/**
 * Test append operations.
 */
public class ITestAzureBlobFileSystemAppend extends
    AbstractAbfsIntegrationTest {
  private static final String TEST_FILE_PATH = "testfile";
  private static final String TEST_FOLDER_PATH = "testFolder";

  public ITestAzureBlobFileSystemAppend() throws Exception {
    super();
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendDirShouldFail() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = path(TEST_FILE_PATH);
    fs.mkdirs(filePath);
    fs.append(filePath, 0).close();
  }

  @Test
  public void testAppendWithLength0() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    try(FSDataOutputStream stream = fs.create(path(TEST_FILE_PATH))) {
      final byte[] b = new byte[1024];
      new Random().nextBytes(b);
      stream.write(b, 1000, 0);
      assertEquals(0, stream.getPos());
    }
  }


  @Test(expected = FileNotFoundException.class)
  public void testAppendFileAfterDelete() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = path(TEST_FILE_PATH);
    ContractTestUtils.touch(fs, filePath);
    fs.delete(filePath, false);

    fs.append(filePath).close();
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path folderPath = path(TEST_FOLDER_PATH);
    fs.mkdirs(folderPath);
    fs.append(folderPath).close();
  }

  @Test
  public void testTracingForAppend() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    Path testPath = path(TEST_FILE_PATH);
    fs.create(testPath).close();
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.APPEND, false, 0));
    fs.append(testPath, 10);
  }
}
