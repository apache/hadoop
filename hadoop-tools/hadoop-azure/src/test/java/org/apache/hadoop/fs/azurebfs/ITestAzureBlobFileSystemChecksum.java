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

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashSet;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.impl.OpenFileParameters;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_BUFFERED_PREAD_DISABLE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters.Mode.APPEND_MODE;

/**
 * Test For Verifying Checksum Related Operations
 */
public class ITestAzureBlobFileSystemChecksum extends AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemChecksum() throws Exception {
    super();
  }

  @Test
  public void testWriteReadWithChecksum() throws Exception {
    testWriteReadWithChecksumInternal(true);
    testWriteReadWithChecksumInternal(false);
  }

  @Test
  public void testAppendWithChecksumAtDifferentOffsets() throws Exception {
    AzureBlobFileSystem fs = getConfiguredFileSystem(4 * ONE_MB, 4 * ONE_MB, true);
    AbfsClient client = fs.getAbfsStore().getClient();
    Path path = path("testPath");
    fs.create(path);
    byte[] data= generateRandomBytes(16 * ONE_MB);

    appendWithOffsetHelper(client, path, data, fs, 0);
    appendWithOffsetHelper(client, path, data, fs, 1 * ONE_MB);
    appendWithOffsetHelper(client, path, data, fs, 4 * ONE_MB);
    appendWithOffsetHelper(client, path, data, fs, 8 * ONE_MB);
  }

  @Test
  public void testReadWithChecksumAtDifferentOffsets() throws Exception {
    AzureBlobFileSystem fs = getConfiguredFileSystem(4 * ONE_MB, 4 * ONE_MB, true);
    AbfsClient client = fs.getAbfsStore().getClient();
    Path path = path("testPath");
    fs.create(path);

    byte[] data = generateRandomBytes(16 * ONE_MB);
    FSDataOutputStream out = fs.create(path);
    out.write(data);
    out.hflush();
    out.close();

    readWithOffsetAndPositionHelper(client, path, data, fs, 0, 0);
    readWithOffsetAndPositionHelper(client, path, data, fs, 4 * ONE_MB, 0);
    readWithOffsetAndPositionHelper(client, path, data, fs, 4 * ONE_MB, 1 * ONE_MB);
    readWithOffsetAndPositionHelper(client, path, data, fs, 8 * ONE_MB, 2 * ONE_MB);
    readWithOffsetAndPositionHelper(client, path, data, fs, 15 * ONE_MB, 3 * ONE_MB);
  }

  @Test
  public void testWriteReadWithChecksumAndOptions() throws Exception {
    testWriteReadWithChecksumAndOptionsInternal(true);
    testWriteReadWithChecksumAndOptionsInternal(false);
  }

  private void readWithOffsetAndPositionHelper(AbfsClient client, Path path,
      byte[] data, AzureBlobFileSystem fs, final int position,
      final int offset) throws Exception {

    byte[] readBuffer = new byte[
        fs.getAbfsStore().getAbfsConfiguration().getReadBufferSize()];
    final int readLength = readBuffer.length - offset;
    client.read(path.toUri().getPath(), position, readBuffer, offset,
        readLength, "*", null, getTestTracingContext(fs, false));

    byte[] actual = Arrays.copyOfRange(readBuffer, offset, offset + readLength);
    byte[] expected = Arrays.copyOfRange(data, position, readLength + position);
    Assertions.assertThat(actual)
        .describedAs("")
        .containsExactly(expected);
  }

  private void appendWithOffsetHelper(AbfsClient client, Path path,
      byte[] data, AzureBlobFileSystem fs, final int offset) throws Exception {
    AppendRequestParameters reqParams = new AppendRequestParameters(
        0, offset, data.length - offset, APPEND_MODE, false, null, true);
    client.append(
        path.toUri().getPath(), data, reqParams, null, getTestTracingContext(fs, false));
  }

  private void testWriteReadWithChecksumInternal(final boolean readAheadEnabled)
      throws Exception {
    AzureBlobFileSystem fs = getConfiguredFileSystem(
        4 * ONE_MB, 4 * ONE_MB, readAheadEnabled);
    final int datasize = 16 * ONE_MB + 1000;

    Path testPath = path("testPath");
    byte[] bytesUploaded = generateRandomBytes(datasize);
    FSDataOutputStream out = fs.create(testPath);
    out.write(bytesUploaded);
    out.hflush();
    out.close();

    FSDataInputStream in = fs.open(testPath);
    byte[] bytesRead = new byte[bytesUploaded.length];
    in.read(bytesRead, 0, datasize);

    // Verify that the data read is same as data written
    Assertions.assertThat(bytesRead)
        .describedAs("Bytes read with checksum enabled are not as expected")
        .containsExactly(bytesUploaded);

    // Verify that reading from random position works
    in = fs.open(testPath);
    bytesRead = new byte[datasize];
    in.seek(ONE_MB);
    in.read(bytesRead, ONE_MB, datasize - 2 * ONE_MB);
  }

  private void testWriteReadWithChecksumAndOptionsInternal(
      final boolean readAheadEnabled) throws Exception {
    AzureBlobFileSystem fs = getConfiguredFileSystem(
        8 * ONE_MB, ONE_MB, readAheadEnabled);
    final int datasize = 16 * ONE_MB + 1000;

    Path testPath = path("testPath");
    byte[] bytesUploaded = generateRandomBytes(datasize);
    FSDataOutputStream out = fs.create(testPath);
    out.write(bytesUploaded);
    out.hflush();
    out.close();

    Configuration cpm1 = new Configuration();
    cpm1.setBoolean(FS_AZURE_BUFFERED_PREAD_DISABLE, true);
    FSDataInputStream in = fs.openFileWithOptions(testPath,
        new OpenFileParameters().withOptions(cpm1)
            .withMandatoryKeys(new HashSet<>())).get();
    byte[] bytesRead = new byte[datasize];
    in.read(1, bytesRead, 1, 4 * ONE_MB);

    // Verify that the data read is same as data written
    Assertions.assertThat(Arrays.copyOfRange(bytesRead, 1, 4 * ONE_MB))
        .describedAs("Bytes read with checksum enabled are not as expected")
        .containsExactly(Arrays.copyOfRange(bytesUploaded, 1, 4 * ONE_MB));
  }

  private AzureBlobFileSystem getConfiguredFileSystem(final int writeBuffer,
      final int readBuffer, final boolean readAheadEnabled) throws Exception {
    Configuration conf = getRawConfiguration();
    AzureBlobFileSystem fs =  (AzureBlobFileSystem) FileSystem.newInstance(conf);
    AbfsConfiguration abfsConf = fs.getAbfsStore().getAbfsConfiguration();
    abfsConf.setIsChecksumValidationEnabled(true);
    abfsConf.setWriteBufferSize(writeBuffer);
    abfsConf.setReadBufferSize(readBuffer);
    abfsConf.setReadAheadEnabled(readAheadEnabled);
    return fs;
  }

  public static byte[] generateRandomBytes(int numBytes) {
    SecureRandom secureRandom = new SecureRandom();
    byte[] randomBytes = new byte[numBytes];
    secureRandom.nextBytes(randomBytes);
    return randomBytes;
  }
}
