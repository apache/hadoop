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
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsDfsClient;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_HTTP_CONNECTION_TIMEOUT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_HTTP_READ_TIMEOUT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_MAX_IO_RETRIES;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_TOLERATE_CONCURRENT_APPEND;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_IS_HNS_ENABLED;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test end to end between ABFS client and ABFS server.
 */
public class ITestAzureBlobFileSystemE2E extends AbstractAbfsIntegrationTest {
  private static final int TEST_BYTE = 100;
  private static final int TEST_OFFSET = 100;
  private static final int TEST_DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;
  private static final int TEST_DEFAULT_READ_BUFFER_SIZE = 1023900;
  private static final int TEST_STABLE_DEFAULT_CONNECTION_TIMEOUT_MS = 500;
  private static final int TEST_STABLE_DEFAULT_READ_TIMEOUT_MS = 30000;
  private static final int TEST_UNSTABLE_READ_TIMEOUT_MS = 1;

  public ITestAzureBlobFileSystemE2E() throws Exception {
    super();
    AbfsConfiguration configuration = this.getConfiguration();
    configuration.set(ConfigurationKeys.FS_AZURE_READ_AHEAD_QUEUE_DEPTH, "0");
  }

  @Test
  public void testWriteOneByteToFile() throws Exception {
    final Path testFilePath = path(methodName.getMethodName());
    testWriteOneByteToFile(testFilePath);
  }

  @Test
  public void testReadWriteBytesToFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = path(methodName.getMethodName());
    testWriteOneByteToFile(testFilePath);
    try(FSDataInputStream inputStream = fs.open(testFilePath,
        TEST_DEFAULT_BUFFER_SIZE)) {
      assertEquals(TEST_BYTE, inputStream.read());
    }
  }

  @Test (expected = IOException.class)
  public void testOOBWritesAndReadFail() throws Exception {
    Configuration conf = this.getRawConfiguration();
    conf.setBoolean(AZURE_TOLERATE_CONCURRENT_APPEND, false);
    final AzureBlobFileSystem fs = getFileSystem();
    int readBufferSize = fs.getAbfsStore().getAbfsConfiguration().getReadBufferSize();

    byte[] bytesToRead = new byte[readBufferSize];
    final byte[] b = new byte[2 * readBufferSize];
    new Random().nextBytes(b);

    final Path testFilePath = path(methodName.getMethodName());
    try(FSDataOutputStream writeStream = fs.create(testFilePath)) {
      writeStream.write(b);
      writeStream.flush();
    }

    try (FSDataInputStream readStream = fs.open(testFilePath)) {
      assertEquals(readBufferSize,
          readStream.read(bytesToRead, 0, readBufferSize));
      try (FSDataOutputStream writeStream = fs.create(testFilePath)) {
        writeStream.write(b);
        writeStream.flush();
      }

      assertEquals(readBufferSize,
          readStream.read(bytesToRead, 0, readBufferSize));
    }
  }

  @Test
  public void testOOBWritesAndReadSucceed() throws Exception {
    Configuration conf = this.getRawConfiguration();
    conf.setBoolean(AZURE_TOLERATE_CONCURRENT_APPEND, true);
    final AzureBlobFileSystem fs = getFileSystem(conf);
    int readBufferSize = fs.getAbfsStore().getAbfsConfiguration().getReadBufferSize();

    byte[] bytesToRead = new byte[readBufferSize];
    final byte[] b = new byte[2 * readBufferSize];
    new Random().nextBytes(b);
    final Path testFilePath = path(methodName.getMethodName());

    try (FSDataOutputStream writeStream = fs.create(testFilePath)) {
      writeStream.write(b);
      writeStream.flush();
    }

    try (FSDataInputStream readStream = fs.open(testFilePath)) {
      // Read
      assertEquals(readBufferSize, readStream.read(bytesToRead, 0, readBufferSize));
      // Concurrent write
      try (FSDataOutputStream writeStream = fs.create(testFilePath)) {
        writeStream.write(b);
        writeStream.flush();
      }

      assertEquals(readBufferSize, readStream.read(bytesToRead, 0, readBufferSize));
    }
  }

  @Test
  public void testWriteWithBufferOffset() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = path(methodName.getMethodName());

    final byte[] b = new byte[1024 * 1000];
    new Random().nextBytes(b);
    try (FSDataOutputStream stream = fs.create(testFilePath)) {
      stream.write(b, TEST_OFFSET, b.length - TEST_OFFSET);
    }

    final byte[] r = new byte[TEST_DEFAULT_READ_BUFFER_SIZE];
    FSDataInputStream inputStream = fs.open(testFilePath, TEST_DEFAULT_BUFFER_SIZE);
    int result = inputStream.read(r);

    assertNotEquals(-1, result);
    assertArrayEquals(r, Arrays.copyOfRange(b, TEST_OFFSET, b.length));

    inputStream.close();
  }

  @Test
  public void testReadWriteHeavyBytesToFileWithSmallerChunks() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = path(methodName.getMethodName());

    final byte[] writeBuffer = new byte[5 * 1000 * 1024];
    new Random().nextBytes(writeBuffer);
    write(testFilePath, writeBuffer);

    final byte[] readBuffer = new byte[5 * 1000 * 1024];
    FSDataInputStream inputStream = fs.open(testFilePath, TEST_DEFAULT_BUFFER_SIZE);
    int offset = 0;
    while (inputStream.read(readBuffer, offset, TEST_OFFSET) > 0) {
      offset += TEST_OFFSET;
    }

    assertArrayEquals(readBuffer, writeBuffer);
    inputStream.close();
  }

  @Test
  public void testReadWithFileNotFoundException() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = path(methodName.getMethodName());
    testWriteOneByteToFile(testFilePath);

    try (FSDataInputStream inputStream = fs.open(testFilePath,
        TEST_DEFAULT_BUFFER_SIZE)) {
      fs.delete(testFilePath, true);
      assertPathDoesNotExist(fs, "This path should not exist", testFilePath);

      intercept(FileNotFoundException.class, () -> inputStream.read(new byte[1]));
    }
  }

  @Test
  public void testWriteWithFileNotFoundException() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = path(methodName.getMethodName());
    AbfsClient client = fs.getAbfsStore().getClientHandler().getIngressClient();

    try (FSDataOutputStream stream = fs.create(testFilePath)) {
      assertPathExists(fs, "Path should exist", testFilePath);
      stream.write(TEST_BYTE);

      fs.delete(testFilePath, true);
      assertPathDoesNotExist(fs, "This path should not exist", testFilePath);

      // trigger append call
      if (client instanceof AbfsDfsClient) {
        intercept(FileNotFoundException.class, stream::close);
      } else {
        intercept(IOException.class, stream::close);
      }
    }
  }

  @Test
  public void testFlushWithFileNotFoundException() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = path(methodName.getMethodName());
    AbfsClient client = fs.getAbfsStore().getClientHandler().getIngressClient();
    if (fs.getAbfsStore().isAppendBlobKey(fs.makeQualified(testFilePath).toString())) {
      return;
    }

    try (FSDataOutputStream stream = fs.create(testFilePath)) {
      assertPathExists(fs, "This path should exist", testFilePath);

      fs.delete(testFilePath, true);
      assertPathDoesNotExist(fs, "This path should not exist", testFilePath);

      if (client instanceof AbfsDfsClient) {
        intercept(FileNotFoundException.class, stream::close);
      } else {
        intercept(IOException.class, stream::close);
      }
    }
  }

  private void testWriteOneByteToFile(Path testFilePath) throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    try(FSDataOutputStream stream = fs.create(testFilePath)) {
      stream.write(TEST_BYTE);
    }

    FileStatus fileStatus = fs.getFileStatus(testFilePath);
    assertEquals(1, fileStatus.getLen());
  }

  @Test
  public void testHttpConnectionTimeout() throws Exception {
    // Not seeing connection failures while testing with 1 ms connection
    // timeout itself and on repeated TPCDS runs when cluster
    // and account are in same region, 10 ms is seen stable.
    // 500 ms is seen stable for cross region.
    testHttpTimeouts(TEST_STABLE_DEFAULT_CONNECTION_TIMEOUT_MS,
            TEST_STABLE_DEFAULT_READ_TIMEOUT_MS);
  }

  @Test(expected = InvalidAbfsRestOperationException.class)
  public void testHttpReadTimeout() throws Exception {
    // Small read timeout is bound to make the request fail.
    testHttpTimeouts(TEST_STABLE_DEFAULT_CONNECTION_TIMEOUT_MS,
            TEST_UNSTABLE_READ_TIMEOUT_MS);
  }

  public void testHttpTimeouts(int connectionTimeoutMs, int readTimeoutMs)
      throws Exception {
    // This is to make sure File System creation goes through before network calls start failing.
    assumeValidTestConfigPresent(this.getRawConfiguration(), FS_AZURE_ACCOUNT_IS_HNS_ENABLED);

    Configuration conf = this.getRawConfiguration();
    // set to small values that will cause timeouts
    conf.setInt(AZURE_HTTP_CONNECTION_TIMEOUT, connectionTimeoutMs);
    conf.setInt(AZURE_HTTP_READ_TIMEOUT, readTimeoutMs);
    conf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION,
        false);
    // Reduce retry count to reduce test run time
    conf.setInt(AZURE_MAX_IO_RETRIES, 1);
    final AzureBlobFileSystem fs = getFileSystem(conf);
    Assertions.assertThat(
            fs.getAbfsStore().getAbfsConfiguration().getHttpConnectionTimeout())
        .describedAs("HTTP connection time should be picked from config")
        .isEqualTo(connectionTimeoutMs);
    Assertions.assertThat(
            fs.getAbfsStore().getAbfsConfiguration().getHttpReadTimeout())
        .describedAs("HTTP Read time should be picked from config")
        .isEqualTo(readTimeoutMs);
    Path testPath = path(methodName.getMethodName());
    ContractTestUtils.createFile(fs, testPath, false, new byte[0]);
  }
}
