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
import java.lang.reflect.Field;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.mockito.Mockito;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_ENABLE_SMALL_WRITE_OPTIMIZATION;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Test append operations.
 */
public class ITestAzureBlobFileSystemAppend extends
    AbstractAbfsIntegrationTest {
  private static final Path TEST_FILE_PATH = new Path("testfile");
  private static final Path TEST_FOLDER_PATH = new Path("testFolder");

  public ITestAzureBlobFileSystemAppend() throws Exception {
    super();
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendDirShouldFail() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = TEST_FILE_PATH;
    fs.mkdirs(filePath);
    fs.append(filePath, 0);
  }

  @Test
  public void testAppendWithLength0() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    try(FSDataOutputStream stream = fs.create(TEST_FILE_PATH)) {
      final byte[] b = new byte[1024];
      new Random().nextBytes(b);
      stream.write(b, 1000, 0);
      assertEquals(0, stream.getPos());
    }
  }


  @Test(expected = FileNotFoundException.class)
  public void testAppendFileAfterDelete() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = TEST_FILE_PATH;
    ContractTestUtils.touch(fs, filePath);
    fs.delete(filePath, false);
    fs.append(filePath);
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path folderPath = TEST_FOLDER_PATH;
    fs.mkdirs(folderPath);
    fs.append(folderPath);
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendImplicitDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path folderPath = TEST_FOLDER_PATH;
    fs.mkdirs(folderPath);
    fs.append(folderPath.getParent());
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendFileNotExists() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.append(TEST_FOLDER_PATH);
  }

  @Test(expected = IOException.class)
  public void testIsAppendBlob() throws Exception {
    final AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(true).when(store).isAppendBlobKey(anyString());
    fs.getAbfsStore().getAbfsConfiguration().setPrefixMode(PrefixMode.BLOB);

    // Set abfsStore as our mocked value.
    Field privateField = AzureBlobFileSystem.class.getDeclaredField("abfsStore");
    privateField.setAccessible(true);
    privateField.set(fs, store);

    fs.create(TEST_FILE_PATH);
  }

  @Test(expected = IOException.class)
  public void testSmallWriteBlob() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(AZURE_ENABLE_SMALL_WRITE_OPTIMIZATION, "true");
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    fs.getAbfsStore().getAbfsConfiguration().setPrefixMode(PrefixMode.BLOB);

    fs.create(TEST_FILE_PATH);
  }

  /** Create file over dfs endpoint and append over blob endpoint **/
  @Test
  public void testCreateOverDfsAppendOverBlob() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.getAbfsStore().getAbfsConfiguration().getPrefixMode() == PrefixMode.BLOB);
    fs.getAbfsClient().createPath(makeQualified(TEST_FILE_PATH).toUri().getPath(), true, false,
            null, null, false,
            null, getTestTracingContext(fs, true));
    FSDataOutputStream outputStream = fs.append(TEST_FILE_PATH);
    outputStream.write(10);
    outputStream.hsync();
    outputStream.write(20);
    outputStream.hsync();
    outputStream.write(30);
    outputStream.hsync();
  }

  /**
   * Create directory over dfs endpoint and append over blob endpoint.
   * Should return error as append is not supported for directory.
   * **/
  @Test(expected = IOException.class)
  public void testCreateExplicitDirectoryOverDfsAppendOverBlob() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.getAbfsStore().getAbfsConfiguration().getPrefixMode() == PrefixMode.BLOB);
    fs.getAbfsClient().createPath(makeQualified(TEST_FOLDER_PATH).toUri().getPath(), false, false,
            null, null, false,
            null, getTestTracingContext(fs, true));
    FSDataOutputStream outputStream = fs.append(TEST_FOLDER_PATH);
    outputStream.write(10);
    outputStream.hsync();
  }

  /**
   * Recreate file between append and flush. Etag mismatch happens.
   **/
  @Test(expected = IOException.class)
  public void testRecreateAppendAndFlush() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.getAbfsStore().getAbfsConfiguration().getPrefixMode() == PrefixMode.BLOB);
    Assume.assumeTrue(!fs.getAbfsStore().getAbfsConfiguration().shouldIngressFallbackToDfs());
    fs.create(TEST_FILE_PATH);
    FSDataOutputStream outputStream = fs.append(TEST_FILE_PATH);
    outputStream.write(10);
    final AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration());
    FSDataOutputStream outputStream1 = fs1.create(TEST_FILE_PATH);
    outputStream.hsync();
  }

  /**
   * Recreate file between append and flush using dfs. Etag mismatch happens.
   **/
  @Test(expected = IOException.class)
  public void testRecreateDirectoryAppendAndFlush() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.getAbfsStore().getAbfsConfiguration().getPrefixMode() == PrefixMode.BLOB);
    fs.create(TEST_FILE_PATH);
    FSDataOutputStream outputStream = fs.append(TEST_FILE_PATH);
    outputStream.write(10);
    final AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration());
    fs1.mkdirs(TEST_FILE_PATH);
    outputStream.hsync();
  }

  @Test
  public void testTracingForAppend() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    fs.create(TEST_FILE_PATH);
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.APPEND, false, 0));
    fs.append(TEST_FILE_PATH, 10);
  }
}
