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


import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_DATA_READER_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_DATA_READER_CLIENT_SECRET;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Test Azure Oauth with Blob Data contributor role and Blob Data Reader role.
 * The Test AAD client need to be configured manually through Azure Portal, then save their properties in
 * configuration files.
 */
public class ITestAzureBlobFileSystemOauth extends AbstractAbfsIntegrationTest{

  private static final Path FILE_PATH = new Path("/testFile");
  private static final String EXISTED_FILE_PATH = "/existedFile";
  private static final String EXISTED_FOLDER_PATH = "/existedFolder";
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAbfsStreamStatistics.class);

  public ITestAzureBlobFileSystemOauth() throws Exception {
    assumeThat(this.getAuthType()).isEqualTo(AuthType.OAuth);
  }
  /*
  * BLOB DATA CONTRIBUTOR should have full access to the container and blobs in the container.
  * */
  @Test
  public void testBlobDataContributor() throws Exception {
    String clientId = this.getConfiguration().get(TestConfigurationKeys.FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_ID);
    assumeThat(clientId).as("Contributor client id not provided").isNotNull();
    String secret = this.getConfiguration().get(TestConfigurationKeys.FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_SECRET);
    assumeThat(secret).as("Contributor client secret not provided").isNotNull();

    Path existedFilePath = path(EXISTED_FILE_PATH);
    Path existedFolderPath = path(EXISTED_FOLDER_PATH);
    prepareFiles(existedFilePath, existedFolderPath);

    final AzureBlobFileSystem fs = getBlobConributor();

    // create and write into file in current container/fs
    try(FSDataOutputStream stream = fs.create(FILE_PATH)) {
      stream.write(0);
    }
    assertPathExists(fs, "This path should exist", FILE_PATH);
    FileStatus fileStatus = fs.getFileStatus(FILE_PATH);
    assertEquals(1, fileStatus.getLen());
    // delete file
    assertTrue(fs.delete(FILE_PATH, true));
    assertPathDoesNotExist(fs, "This path should not exist", FILE_PATH);

    // Verify Blob Data Contributor has full access to existed folder, file

    // READ FOLDER
    assertPathExists(fs, "This path should exist", existedFolderPath);

    //DELETE FOLDER
    fs.delete(existedFolderPath, true);
    assertPathDoesNotExist(fs, "This path should not exist", existedFolderPath);

    // READ FILE
    try (FSDataInputStream stream = fs.open(existedFilePath)) {
      assertTrue(stream.read() != 0);
    }

    assertEquals(0, fs.getFileStatus(existedFilePath).getLen());

    // WRITE FILE
    try (FSDataOutputStream stream = fs.append(existedFilePath)) {
      stream.write(0);
    }

    assertEquals(1, fs.getFileStatus(existedFilePath).getLen());

    // REMOVE FILE
    fs.delete(existedFilePath, true);
    assertPathDoesNotExist(fs, "This path should not exist", existedFilePath);
  }

  /*
   * BLOB DATA READER should have only READ access to the container and blobs in the container.
   * */
  @Test
  public void testBlobDataReader() throws Exception {
    String clientId = this.getConfiguration().get(TestConfigurationKeys.FS_AZURE_BLOB_DATA_READER_CLIENT_ID);
    assumeThat(clientId).as("Reader client id not provided").isNotNull();
    String secret = this.getConfiguration().get(TestConfigurationKeys.FS_AZURE_BLOB_DATA_READER_CLIENT_SECRET);
    assumeThat(secret).as("Reader client secret not provided").isNotNull();

    Path existedFilePath = path(EXISTED_FILE_PATH);
    Path existedFolderPath = path(EXISTED_FOLDER_PATH);
    prepareFiles(existedFilePath, existedFolderPath);
    final AzureBlobFileSystem fs = getBlobReader();

    // Use abfsStore in this test to verify the  ERROR code in AbfsRestOperationException
    AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();
    TracingContext tracingContext = getTestTracingContext(fs, true);
    // TEST READ FS
    Map<String, String> properties = abfsStore.getFilesystemProperties(tracingContext);
    // TEST READ FOLDER
    assertPathExists(fs, "This path should exist", existedFolderPath);

    // TEST DELETE FOLDER
    try {
      abfsStore.delete(existedFolderPath, true, tracingContext);
    } catch (AbfsRestOperationException e) {
      assertEquals(AzureServiceErrorCode.AUTHORIZATION_PERMISSION_MISS_MATCH, e.getErrorCode());
    }

    // TEST READ  FILE
    try (InputStream inputStream = abfsStore
        .openFileForRead(existedFilePath, null, tracingContext)) {
      assertTrue(inputStream.read() != 0);
    }

    // TEST WRITE FILE
    try {
      abfsStore.openFileForWrite(existedFilePath, fs.getFsStatistics(), true,
          tracingContext);
    } catch (AbfsRestOperationException e) {
      assertEquals(AzureServiceErrorCode.AUTHORIZATION_PERMISSION_MISS_MATCH, e.getErrorCode());
    } finally {
      IOUtils.cleanupWithLogger(LOG, abfsStore);
    }

  }

  /*
   * GetPathStatus with Blob Data Reader role should not throw an exception when marker creation fails due to permission issues.
   * */
  @Test
  public void testGetPathStatusWithReader() throws Exception {
    String clientId = this.getConfiguration().get(FS_AZURE_BLOB_DATA_READER_CLIENT_ID);
    assumeThat(clientId).as("Reader client id not provided").isNotNull();
    String secret = this.getConfiguration().get(FS_AZURE_BLOB_DATA_READER_CLIENT_SECRET);
    assumeThat(secret).as("Reader client secret not provided").isNotNull();

    Path existedFolderPath = path(EXISTED_FOLDER_PATH);
    createAzCopyFolder(existedFolderPath);
    final AzureBlobFileSystem fs = Mockito.spy(getBlobReader());

    // Use abfsStore in this test to verify the  ERROR code in AbfsRestOperationException
    AzureBlobFileSystemStore abfsStore = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(abfsStore).when(fs).getAbfsStore();
    AbfsBlobClient abfsClient = Mockito.spy(abfsStore.getClientHandler().getBlobClient());
    Mockito.doReturn(abfsClient).when(abfsStore).getClient();
    TracingContext tracingContext = getTestTracingContext(fs, true);

    // GETPATHSTATUS marker creation fail should not be propagated to the caller.
    assertThatCode(() -> abfsStore.getPathStatus(existedFolderPath, tracingContext))
        .as("Expected getPathStatus to complete without throwing an exception")
        .doesNotThrowAnyException();
    Mockito.verify(abfsClient, Mockito.times(1)).createMarkerAtPath(Mockito.anyString(), Mockito.nullable(String.class),
        Mockito.nullable(ContextEncryptionAdapter.class),
        Mockito.nullable(TracingContext.class));
  }

  private void prepareFiles(Path existedFilePath, Path existedFolderPath) throws IOException {
    // create test files/folders to verify access control diff between
    // Blob data contributor and Blob data reader
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.create(existedFilePath).close();
    assertPathExists(fs, "This path should exist", existedFilePath);
    fs.mkdirs(existedFolderPath);
    assertPathExists(fs, "This path should exist", existedFolderPath);
  }

  private AzureBlobFileSystem getBlobConributor() throws Exception {
    AbfsConfiguration abfsConfig = this.getConfiguration();
    abfsConfig.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID + "." + this.getAccountName(), abfsConfig.get(FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_ID));
    abfsConfig.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET + "." + this.getAccountName(), abfsConfig.get(FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_SECRET));
    Configuration rawConfig = abfsConfig.getRawConfiguration();
    return getFileSystem(rawConfig);
  }

  private AzureBlobFileSystem getBlobReader() throws Exception {
    AbfsConfiguration abfsConfig = this.getConfiguration();
    abfsConfig.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID + "." + this.getAccountName(), abfsConfig.get(FS_AZURE_BLOB_DATA_READER_CLIENT_ID));
    abfsConfig.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET + "." + this.getAccountName(), abfsConfig.get(FS_AZURE_BLOB_DATA_READER_CLIENT_SECRET));
    Configuration rawConfig = abfsConfig.getRawConfiguration();
    return getFileSystem(rawConfig);
  }
}
