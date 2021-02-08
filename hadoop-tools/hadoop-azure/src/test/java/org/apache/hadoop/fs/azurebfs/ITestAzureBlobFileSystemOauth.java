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

import org.junit.Assume;
import org.junit.Test;
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
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_DATA_READER_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_DATA_READER_CLIENT_SECRET;

/**
 * Test Azure Oauth with Blob Data contributor role and Blob Data Reader role.
 * The Test AAD client need to be configured manually through Azure Portal, then save their properties in
 * configuration files.
 */
public class ITestAzureBlobFileSystemOauth extends AbstractAbfsIntegrationTest{

  private static final Path FILE_PATH = new Path("/testFile");
  private static final Path EXISTED_FILE_PATH = new Path("/existedFile");
  private static final Path EXISTED_FOLDER_PATH = new Path("/existedFolder");
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAbfsStreamStatistics.class);

  public ITestAzureBlobFileSystemOauth() throws Exception {
    Assume.assumeTrue(this.getAuthType() == AuthType.OAuth);
  }
  /*
  * BLOB DATA CONTRIBUTOR should have full access to the container and blobs in the container.
  * */
  @Test
  public void testBlobDataContributor() throws Exception {
    String clientId = this.getConfiguration().get(TestConfigurationKeys.FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_ID);
    Assume.assumeTrue("Contributor client id not provided", clientId != null);
    String secret = this.getConfiguration().get(TestConfigurationKeys.FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_SECRET);
    Assume.assumeTrue("Contributor client secret not provided", secret != null);

    prepareFiles();

    final AzureBlobFileSystem fs = getBlobConributor();

    // create and write into file in current container/fs
    try(FSDataOutputStream stream = fs.create(FILE_PATH)) {
      stream.write(0);
    }
    assertTrue(fs.exists(FILE_PATH));
    FileStatus fileStatus = fs.getFileStatus(FILE_PATH);
    assertEquals(1, fileStatus.getLen());
    // delete file
    assertTrue(fs.delete(FILE_PATH, true));
    assertFalse(fs.exists(FILE_PATH));

    // Verify Blob Data Contributor has full access to existed folder, file

    // READ FOLDER
    assertTrue(fs.exists(EXISTED_FOLDER_PATH));

    //DELETE FOLDER
    fs.delete(EXISTED_FOLDER_PATH, true);
    assertFalse(fs.exists(EXISTED_FOLDER_PATH));

    // READ FILE
    try (FSDataInputStream stream = fs.open(EXISTED_FILE_PATH)) {
      assertTrue(stream.read() != 0);
    }

    assertEquals(0, fs.getFileStatus(EXISTED_FILE_PATH).getLen());

    // WRITE FILE
    try (FSDataOutputStream stream = fs.append(EXISTED_FILE_PATH)) {
      stream.write(0);
    }

    assertEquals(1, fs.getFileStatus(EXISTED_FILE_PATH).getLen());

    // REMOVE FILE
    fs.delete(EXISTED_FILE_PATH, true);
    assertFalse(fs.exists(EXISTED_FILE_PATH));
  }

  /*
   * BLOB DATA READER should have only READ access to the container and blobs in the container.
   * */
  @Test
  public void testBlobDataReader() throws Exception {
    String clientId = this.getConfiguration().get(TestConfigurationKeys.FS_AZURE_BLOB_DATA_READER_CLIENT_ID);
    Assume.assumeTrue("Reader client id not provided", clientId != null);
    String secret = this.getConfiguration().get(TestConfigurationKeys.FS_AZURE_BLOB_DATA_READER_CLIENT_SECRET);
    Assume.assumeTrue("Reader client secret not provided", secret != null);

    prepareFiles();
    final AzureBlobFileSystem fs = getBlobReader();

    // Use abfsStore in this test to verify the  ERROR code in AbfsRestOperationException
    AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();
    // TEST READ FS
    Map<String, String> properties = abfsStore.getFilesystemProperties();
    // TEST READ FOLDER
    assertTrue(fs.exists(EXISTED_FOLDER_PATH));

    // TEST DELETE FOLDER
    try {
      abfsStore.delete(EXISTED_FOLDER_PATH, true);
    } catch (AbfsRestOperationException e) {
      assertEquals(AzureServiceErrorCode.AUTHORIZATION_PERMISSION_MISS_MATCH, e.getErrorCode());
    }

    // TEST READ  FILE
    try (InputStream inputStream = abfsStore.openFileForRead(EXISTED_FILE_PATH, null)) {
      assertTrue(inputStream.read() != 0);
    }

    // TEST WRITE FILE
    try {
      abfsStore.openFileForWrite(EXISTED_FILE_PATH, fs.getFsStatistics(), true);
    } catch (AbfsRestOperationException e) {
      assertEquals(AzureServiceErrorCode.AUTHORIZATION_PERMISSION_MISS_MATCH, e.getErrorCode());
    } finally {
      IOUtils.cleanupWithLogger(LOG, abfsStore);
    }

  }

  private void prepareFiles() throws IOException {
    // create test files/folders to verify access control diff between
    // Blob data contributor and Blob data reader
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.create(EXISTED_FILE_PATH);
    assertTrue(fs.exists(EXISTED_FILE_PATH));
    fs.mkdirs(EXISTED_FOLDER_PATH);
    assertTrue(fs.exists(EXISTED_FOLDER_PATH));
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
