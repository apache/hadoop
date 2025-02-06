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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientHandler;
import org.apache.hadoop.fs.azurebfs.services.AbfsDfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.TestAbfsClient;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_MKDIR_OVERWRITE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FS_AZURE_ENABLE_MKDIR_OVERWRITE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_TRANSACTION_ID;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

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
      // 1 GetBlobProperties + 1 PutBlob call.
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
   * Tests the idempotency of creating a path with retries by simulating
   * a conflict response (HTTP 409) from the Azure Blob File System client.
   * The method ensures that the path creation operation retries correctly
   * with the proper transaction ID headers, verifying idempotency during
   * failure recovery.
   *
   * @throws Exception if any error occurs during the operation.
   */
  @Test
  public void createPathRetryIdempotency() throws Exception {
    final AzureBlobFileSystem currentFs = getFileSystem();
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set("fs.azure.enable.client.transaction.id", "true");
    final AzureBlobFileSystem fs =
        (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
            config);
    assumeDfsServiceType();
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClientHandler clientHandler = Mockito.spy(store.getClientHandler());
    AbfsDfsClient abfsClient = (AbfsDfsClient) Mockito.spy(clientHandler.getClient());
    fs.getAbfsStore().setClient(abfsClient);
    fs.getAbfsStore().setClientHandler(clientHandler);
    Mockito.doReturn(abfsClient).when(clientHandler).getIngressClient();
    final Path nonOverwriteFile = new Path(
        "/NonOverwriteTest_FileName_" + UUID.randomUUID());
    final List<AbfsHttpHeader> headers = new ArrayList<>();
    TestAbfsClient.mockAbfsOperationCreation(abfsClient,
        new MockIntercept<AbfsRestOperation>() {
      private int count = 0;
      @Override
      public void answer(final AbfsRestOperation mockedObj,
          final InvocationOnMock answer) throws AbfsRestOperationException {
        if (count == 0) {
          count = 1;
          AbfsHttpOperation op = Mockito.mock(AbfsHttpOperation.class);
          Mockito.doReturn("PUT").when(op).getMethod();
          Mockito.doReturn("").when(op).getStorageErrorMessage();
          Mockito.doReturn(true).when(mockedObj).hasResult();
          Mockito.doReturn(op).when(mockedObj).getResult();
          Mockito.doReturn(HTTP_CONFLICT).when(op).getStatusCode();
          headers.addAll(mockedObj.getRequestHeaders());
          throw new AbfsRestOperationException(HTTP_CONFLICT,
              AzureServiceErrorCode.PATH_CONFLICT.getErrorCode(), "", null, op);
        }
      }
    });
    AbfsRestOperation getPathRestOp = Mockito.mock(AbfsRestOperation.class);
    AbfsHttpOperation op = Mockito.mock(AbfsHttpOperation.class);
    Mockito.doAnswer(answer -> {
      String requiredHeader = null;
      for (AbfsHttpHeader httpHeader : headers) {
        if (X_MS_CLIENT_TRANSACTION_ID.equalsIgnoreCase(httpHeader.getName())) {
          requiredHeader = httpHeader.getValue();
          break;
        }
      }
      return requiredHeader;
    }).when(op).getResponseHeader(X_MS_CLIENT_TRANSACTION_ID);
    Mockito.doReturn(true).when(getPathRestOp).hasResult();
    Mockito.doReturn(op).when(getPathRestOp).getResult();
    Mockito.doReturn(getPathRestOp).when(abfsClient).getPathStatus(
        Mockito.nullable(String.class), Mockito.nullable(Boolean.class),
        Mockito.nullable(TracingContext.class),
        Mockito.nullable(ContextEncryptionAdapter.class));
    fs.create(nonOverwriteFile, false);
  }
}
