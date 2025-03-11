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

import java.net.URL;
import java.util.List;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientHandler;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_MKDIR_OVERWRITE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FS_AZURE_ENABLE_MKDIR_OVERWRITE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_METADATA_PREFIX;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;

/**
 * Test mkdir operation.
 */
public class ITestAzureBlobFileSystemMkDir extends AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemMkDir() throws Exception {
    super();
  }

  @Test
  public void testCreateDirWithExistingDir() throws Exception {
    Assume.assumeTrue(
        DEFAULT_FS_AZURE_ENABLE_MKDIR_OVERWRITE || !getIsNamespaceEnabled(
            getFileSystem()));
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = path("testFolder");
    assertMkdirs(fs, path);
    assertMkdirs(fs, path);
  }

  @Test
  public void testMkdirExistingDirOverwriteFalse() throws Exception {
    Assume.assumeFalse("Ignore test until default overwrite is set to false",
        DEFAULT_FS_AZURE_ENABLE_MKDIR_OVERWRITE);
    Assume.assumeTrue("Ignore test for Non-HNS accounts",
        getIsNamespaceEnabled(getFileSystem()));
    //execute test only for HNS account with default overwrite=false
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set(FS_AZURE_ENABLE_MKDIR_OVERWRITE, Boolean.toString(false));
    AzureBlobFileSystem fs = getFileSystem(config);
    Path path = path("testFolder");
    assertMkdirs(fs, path); //checks that mkdirs returns true
    long timeCreated = fs.getFileStatus(path).getModificationTime();
    assertMkdirs(fs, path); //call to existing dir should return success
    assertEquals("LMT should not be updated for existing dir", timeCreated,
        fs.getFileStatus(path).getModificationTime());
  }

  @Test
  public void createDirWithExistingFilename() throws Exception {
    Assume.assumeFalse("Ignore test until default overwrite is set to false",
        DEFAULT_FS_AZURE_ENABLE_MKDIR_OVERWRITE && getIsNamespaceEnabled(
            getFileSystem()));
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = path("testFilePath");
    fs.create(path).close();
    assertTrue(fs.getFileStatus(path).isFile());
    intercept(FileAlreadyExistsException.class, () -> fs.mkdirs(path));
  }

  @Test
  public void testCreateRoot() throws Exception {
    assertMkdirs(getFileSystem(), new Path("/"));
  }

  /**
   * Test mkdir for possible values of fs.azure.disable.default.create.overwrite
   * @throws Exception
   */
  @Test
  public void testDefaultCreateOverwriteDirTest() throws Throwable {
    // the config fs.azure.disable.default.create.overwrite should have no
    // effect on mkdirs
    testCreateDirOverwrite(true);
    testCreateDirOverwrite(false);
  }

  public void testCreateDirOverwrite(boolean enableConditionalCreateOverwrite)
      throws Throwable {
    final AzureBlobFileSystem currentFs = getFileSystem();
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set("fs.azure.enable.conditional.create.overwrite",
        Boolean.toString(enableConditionalCreateOverwrite));

    final AzureBlobFileSystem fs =
        (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
            config);

    long totalConnectionMadeBeforeTest = fs.getInstrumentationMap()
        .get(CONNECTIONS_MADE.getStatName());

    int mkdirRequestCount = 0;
    final Path dirPath = new Path("/DirPath_"
        + UUID.randomUUID().toString());

    // Case 1: Dir does not pre-exist
    fs.mkdirs(dirPath);

    // One request to server
    AbfsClient client = fs.getAbfsStore().getClientHandler().getIngressClient();
    if (client instanceof AbfsBlobClient && !getIsNamespaceEnabled(fs)) {
      // 1 GetBlobProperties + 1 ListBlobs + 1 PutBlob call.
      mkdirRequestCount +=3;
    } else {
      mkdirRequestCount++;
    }

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + mkdirRequestCount,
        fs.getInstrumentationMap());

    // Case 2: Dir pre-exists
    // Mkdir on existing Dir path will not lead to failure
    fs.mkdirs(dirPath);

    // One request to server
    if (client instanceof AbfsBlobClient && !getIsNamespaceEnabled(fs)) {
      // 1 ListBlobs + 1 GetBlobProperties
      mkdirRequestCount +=2;
    } else {
      mkdirRequestCount++;
    }

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + mkdirRequestCount,
        fs.getInstrumentationMap());
  }

  @Test
  public void testMkdirWithExistingFilename() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();

    fs.create(new Path("/testFilePath"));
    intercept(FileAlreadyExistsException.class, () -> fs.mkdirs(new Path("/testFilePath")));
    intercept(FileAlreadyExistsException.class, () -> fs.mkdirs(new Path("/testFilePath/newDir")));
  }

  /**
   * Test mkdirs with HDI folder configuration,
   * verifying the correct header and directory state.
   */
  @Test
  public void testMkdirsWithDifferentCaseHDIConfig() throws Exception {
    try (AzureBlobFileSystem fs = Mockito.spy(getFileSystem())) {
      assumeBlobServiceType();
      AbfsBlobClient abfsBlobClient = mockIngressClientHandler(fs);
      String configName = X_MS_METADATA_PREFIX + "Hdi_isfolder";
      // Mock the operation to modify the headers
      mockAbfsRestOperation(abfsBlobClient, configName);

      // Create the path and invoke mkdirs method
      Path path = new Path("/testPath");
      fs.mkdirs(path);

      // Assert that the response header has the updated value
      AbfsHttpOperation op = abfsBlobClient.getPathStatus(path.toUri().getPath(),
          true, getTestTracingContext(fs, true),
          null).getResult();

      // Verify the header and directory state
      Assertions.assertThat(op.getResponseHeader(configName))
          .describedAs("Header should be set to true")
          .isEqualTo(TRUE);
      Assertions.assertThat(abfsBlobClient.checkIsDir(op))
          .describedAs("Directory should be marked as true")
          .isTrue();
    }
  }

  /**
   * Test mkdirs with wrong HDI folder configuration,
   * verifying the correct header and directory state.
   */
  @Test
  public void testMkdirsWithWrongHDIConfig() throws Exception {
    try (AzureBlobFileSystem fs = Mockito.spy(getFileSystem())) {
      assumeBlobServiceType();
      AbfsBlobClient abfsBlobClient = mockIngressClientHandler(fs);
      String configName = X_MS_METADATA_PREFIX + "Hdi_isfolder1";

      // Mock the operation to modify the headers
      mockAbfsRestOperation(abfsBlobClient, configName);

      // Create the path and invoke mkdirs method
      Path path = new Path("/testPath");
      fs.mkdirs(path);

      // Assert the header and directory state
      AbfsHttpOperation op = abfsBlobClient.getPathStatus(path.toUri().getPath(),
          true, getTestTracingContext(fs, true),
          null).getResult();

      // Verify the header and directory state
      Assertions.assertThat(op.getResponseHeader(configName))
          .describedAs("Header should be set to TRUE")
          .isEqualTo(TRUE);
      Assertions.assertThat(abfsBlobClient.checkIsDir(op))
          .describedAs("No Directory config set, should be marked as false")
          .isFalse();
    }
  }

  /**
   * Helper method to mock the AbfsRestOperation and modify the request headers.
   *
   * @param abfsBlobClient the mocked AbfsBlobClient
   * @param newHeader the header to add in place of the old one
   */
  private void mockAbfsRestOperation(AbfsBlobClient abfsBlobClient, String newHeader) {
    Mockito.doAnswer(invocation -> {
      List<AbfsHttpHeader> requestHeaders = invocation.getArgument(3);

      // Remove the actual HDI config header and add the new one
      requestHeaders.removeIf(header ->
          HttpHeaderConfigurations.X_MS_META_HDI_ISFOLDER.equals(header.getName()));
      requestHeaders.add(new AbfsHttpHeader(newHeader, TRUE));

      // Call the real method
      return invocation.callRealMethod();
    }).when(abfsBlobClient).getAbfsRestOperation(eq(AbfsRestOperationType.PutBlob),
        eq(HTTP_METHOD_PUT), any(URL.class), anyList());
  }

  /**
   * Helper method to mock the AbfsBlobClient and set up the client handler.
   *
   * @param fs the AzureBlobFileSystem instance
   * @return the mocked AbfsBlobClient
   */
  private AbfsBlobClient mockIngressClientHandler(AzureBlobFileSystem fs) {
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClientHandler clientHandler = Mockito.spy(store.getClientHandler());
    AbfsBlobClient abfsBlobClient = (AbfsBlobClient) Mockito.spy(
        clientHandler.getClient());
    fs.getAbfsStore().setClient(abfsBlobClient);
    fs.getAbfsStore().setClientHandler(clientHandler);
    Mockito.doReturn(abfsBlobClient).when(clientHandler).getIngressClient();
    return abfsBlobClient;
  }
}
