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
import java.io.FilterOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ConcurrentWriteOperationDetectedException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientHandler;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil;
import org.apache.hadoop.fs.azurebfs.services.AbfsDfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.ITestAbfsClient;
import org.apache.hadoop.fs.azurebfs.services.RenameAtomicity;
import org.apache.hadoop.fs.azurebfs.services.TestAbfsClient;
import org.apache.hadoop.fs.azurebfs.services.VersionedFileStatus;
import org.apache.hadoop.fs.azurebfs.utils.DirectoryStateHelper;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.test.ReflectionUtils;

import static java.net.HttpURLConnection.HTTP_CLIENT_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_CLIENT_TRANSACTION_ID;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_MKDIR_OVERWRITE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_TRANSACTION_ID;
import static org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil.mockAddClientTransactionIdToHeader;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_CREATE_RECOVERY;
import static org.apache.hadoop.fs.azurebfs.services.RenameAtomicity.SUFFIX;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test create operation.
 */
public class ITestAzureBlobFileSystemCreate extends
    AbstractAbfsIntegrationTest {
  private static final Path TEST_FILE_PATH = new Path("testfile");
  private static final String TEST_FOLDER_PATH = "testFolder";
  private static final String TEST_CHILD_FILE = "childFile";

  public ITestAzureBlobFileSystemCreate() throws Exception {
    super();
  }

  @Test
  public void testEnsureFileCreatedImmediately() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    FSDataOutputStream out = fs.create(TEST_FILE_PATH);
    try {
      assertIsFile(fs, TEST_FILE_PATH);
    } finally {
      out.close();
    }
    assertIsFile(fs, TEST_FILE_PATH);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFolderPath = path(TEST_FOLDER_PATH);
    Path testFile = new Path(testFolderPath, TEST_CHILD_FILE);
    try {
      fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null);
      fail("Should've thrown");
    } catch (FileNotFoundException expected) {
    }
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.MKDIR, false, 0));
    fs.mkdirs(testFolderPath);
    fs.registerListener(null);

    fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null)
        .close();
    assertIsFile(fs, testFile);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive1() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFolderPath = path(TEST_FOLDER_PATH);
    Path testFile = new Path(testFolderPath, TEST_CHILD_FILE);
    try {
      fs.createNonRecursive(testFile, FsPermission.getDefault(),
          EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), 1024, (short) 1,
          1024, null);
      fail("Should've thrown");
    } catch (FileNotFoundException expected) {
    }
    fs.mkdirs(testFolderPath);
    fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null)
        .close();
    assertIsFile(fs, testFile);

  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive2() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    Path testFolderPath = path(TEST_FOLDER_PATH);
    Path testFile = new Path(testFolderPath, TEST_CHILD_FILE);
    try {
      fs.createNonRecursive(testFile, FsPermission.getDefault(), false, 1024,
          (short) 1, 1024, null);
      fail("Should've thrown");
    } catch (FileNotFoundException e) {
    }
    fs.mkdirs(testFolderPath);
    fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null)
        .close();
    assertIsFile(fs, testFile);
  }

  /**
   * Test createNonRecursive when parent exist.
   *
   * @throws Exception in case of failure
   */
  @Test
  public void testCreateNonRecursiveWhenParentExist() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeBlobServiceType();
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    Path createDirectoryPath = new Path("hbase/A");
    fs.mkdirs(createDirectoryPath);
    fs.createNonRecursive(new Path(createDirectoryPath, "B"), FsPermission
        .getDefault(), false, 1024,
        (short) 1, 1024, null);
    Assertions.assertThat(fs.exists(new Path(createDirectoryPath, "B")))
        .describedAs("File should be created").isTrue();
    fs.close();
  }

  /**
   * Test createNonRecursive when parent does not exist.
   *
   * @throws Exception in case of failure
   */
  @Test
  public void testCreateNonRecursiveWhenParentNotExist() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeBlobServiceType();
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    Path createDirectoryPath = new Path("A/");
    fs.mkdirs(createDirectoryPath);
    intercept(FileNotFoundException.class,
        () -> fs.createNonRecursive(new Path("A/B/C"), FsPermission
            .getDefault(), false, 1024, (short) 1, 1024, null));
    Assertions.assertThat(fs.exists(new Path("A/B/C")))
        .describedAs("New File should not be created.").isFalse();
    fs.close();
  }

  /**
   * Helper method to create a json file.
   * @param path parent path
   * @param renameJson rename json path
   *
   * @return file system
   * @throws IOException in case of failure
   */
  private AzureBlobFileSystem createJsonFile(Path path, Path renameJson) throws IOException {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeBlobServiceType();
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = Mockito.spy(store.getClient());
    doReturn(client).when(store).getClient();
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    fs.mkdirs(new Path(path, "test3"));
    VersionedFileStatus fileStatus
        = (VersionedFileStatus) fs.getFileStatus(path);
    new RenameAtomicity(path,
        new Path("/hbase/test4"), renameJson,
        getTestTracingContext(fs, true), fileStatus.getEtag(),
        client).preRename();
    Assertions.assertThat(fs.exists(renameJson))
        .describedAs("Rename Pending Json file should exist.")
        .isTrue();
    return fs;
  }

  /**
   * Test createNonRecursive when parent does not exist and rename pending exists.
   * Rename redo should fail.
   * Json file should be deleted.
   * No new File creation.
   *
   * @throws Exception in case of failure
   */
  @Test
  public void testCreateNonRecursiveWhenParentNotExistAndRenamePendingExist() throws Exception {
    AzureBlobFileSystem fs = null;
    try {
      Path path = new Path("/hbase/test1/test2");
      Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
      fs = createJsonFile(path, renameJson);
      fs.delete(path, true);
      Assertions.assertThat(fs.exists(renameJson)).isTrue();
      AzureBlobFileSystem finalFs = fs;
      intercept(FileNotFoundException.class,
          () -> finalFs.createNonRecursive(new Path(path, "test4"), FsPermission
              .getDefault(), false, 1024, (short) 1, 1024, null));
      Assertions.assertThat(finalFs.exists(new Path(path, "test4")))
          .describedAs("New File should not be created.")
          .isFalse();
      Assertions.assertThat(finalFs.exists(renameJson))
          .describedAs("Rename Pending Json file should be deleted.")
          .isFalse();
    } finally {
      if (fs != null) {
        fs.close();
      }
    }
  }

  /**
   * Test createNonRecursive when parent and rename pending exist.
   * Rename redo should be successful.
   * Json file should be deleted.
   * No file should be created.
   *
   * @throws Exception in case of failure
   */
  @Test
  public void testCreateNonRecursiveWhenParentAndRenamePendingExist() throws Exception {
    AzureBlobFileSystem fs = null;
    try {
      Path path = new Path("/hbase/test1/test2");
      Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
      fs = createJsonFile(path, renameJson);
      AzureBlobFileSystem finalFs = fs;
      intercept(FileNotFoundException.class,
          () -> finalFs.createNonRecursive(new Path(path, "test4"), FsPermission
              .getDefault(), false, 1024, (short) 1, 1024, null));
      Assertions.assertThat(finalFs.exists(path))
          .describedAs("Old path should be deleted.")
          .isFalse();
      Assertions.assertThat(finalFs.exists(new Path(path, "test4")))
          .describedAs("New File should not be created.")
          .isFalse();
      Assertions.assertThat(finalFs.exists(renameJson))
          .describedAs("Rename Pending Json file should be deleted.")
          .isFalse();
      Assertions.assertThat(finalFs.exists(new Path("/hbase/test4")))
          .describedAs("Rename should be successful.")
          .isTrue();
    } finally {
      if (fs != null) {
        fs.close();
      }
    }
  }

  @Test
  public void testCreateOnRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFile = path(AbfsHttpConstants.ROOT_PATH);
    AbfsRestOperationException ex = intercept(AbfsRestOperationException.class, () ->
        fs.create(testFile, true));
    if (ex.getStatusCode() != HTTP_CONFLICT) {
      // Request should fail with 409.
      throw ex;
    }

    ex = intercept(AbfsRestOperationException.class, () ->
        fs.createNonRecursive(testFile, FsPermission.getDefault(),
            false, 1024, (short) 1, 1024, null));
    if (ex.getStatusCode() != HTTP_CONFLICT) {
      // Request should fail with 409.
      throw ex;
    }
  }

  /**
   * Attempts to use to the ABFS stream after it is closed.
   */
  @Test
  public void testWriteAfterClose() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFolderPath = path(TEST_FOLDER_PATH);
    Path testPath = new Path(testFolderPath, TEST_CHILD_FILE);
    FSDataOutputStream out = fs.create(testPath);
    out.close();
    intercept(IOException.class, () -> out.write('a'));
    intercept(IOException.class, () -> out.write(new byte[]{'a'}));
    // hsync is not ignored on a closed stream
    // out.hsync();
    out.flush();
    out.close();
  }

  /**
   * Attempts to double close an ABFS output stream from within a
   * FilterOutputStream.
   * That class handles a double failure on close badly if the second
   * exception rethrows the first.
   */
  @Test
  public void testTryWithResources() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFolderPath = path(TEST_FOLDER_PATH);
    Path testPath = new Path(testFolderPath, TEST_CHILD_FILE);
    try (FSDataOutputStream out = fs.create(testPath)) {
      out.write('1');
      out.hsync();
      // this will cause the next write to failAll
      fs.delete(testPath, false);
      out.write('2');
      out.hsync();
      fail("Expected a failure");
    } catch (IOException fnfe) {
      //appendblob outputStream does not generate suppressed exception on close as it is
      //single threaded code
      if (!fs.getAbfsStore().isAppendBlobKey(fs.makeQualified(testPath).toString())) {
        // the exception raised in close() must be in the caught exception's
        // suppressed list
        Throwable[] suppressed = fnfe.getSuppressed();
        Assertions.assertThat(suppressed.length)
            .describedAs("suppressed count should be 1").isEqualTo(1);
        Throwable inner = suppressed[0];
        if (!(inner instanceof IOException)) {
          throw inner;
        }
        GenericTestUtils.assertExceptionContains(fnfe.getMessage(), inner);
      }
    }
  }

  /**
   * Attempts to write to the azure stream after it is closed will raise
   * an IOException.
   */
  @Test
  public void testFilterFSWriteAfterClose() throws Throwable {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      Path testPath = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
      FSDataOutputStream out = fs.create(testPath);
      intercept(IOException.class,
          () -> {
            try (FilterOutputStream fos = new FilterOutputStream(out)) {
              byte[] bytes = new byte[8 * ONE_MB];
              fos.write(bytes);
              fos.write(bytes);
              fos.flush();
              out.hsync();
              fs.delete(testPath, false);
              // trigger the first failure
              throw intercept(IOException.class,
                  () -> {
                    fos.write('b');
                    out.hsync();
                    return "hsync didn't raise an IOE";
                  });
            }
          });
    }
  }

  /**
   * Tests if the number of connections made for:
   * 1. create overwrite=false of a file that doesnt pre-exist
   * 2. create overwrite=false of a file that pre-exists
   * 3. create overwrite=true of a file that doesnt pre-exist
   * 4. create overwrite=true of a file that pre-exists
   * matches the expectation when run against both combinations of
   * fs.azure.enable.conditional.create.overwrite=true and
   * fs.azure.enable.conditional.create.overwrite=false
   * @throws Throwable
   */
  @Test
  public void testDefaultCreateOverwriteFileTest() throws Throwable {
    testCreateFileOverwrite(true);
    testCreateFileOverwrite(false);
  }

  public void testCreateFileOverwrite(boolean enableConditionalCreateOverwrite)
      throws Throwable {
    try (AzureBlobFileSystem currentFs = getFileSystem()) {
      Configuration config = new Configuration(this.getRawConfiguration());
      config.set("fs.azure.enable.conditional.create.overwrite",
          Boolean.toString(enableConditionalCreateOverwrite));
      AzureBlobFileSystemStore store = currentFs.getAbfsStore();
      AbfsClient client = store.getClientHandler().getIngressClient();

      try (AzureBlobFileSystem fs =
               (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
                   config)) {

        long totalConnectionMadeBeforeTest = fs.getInstrumentationMap()
            .get(CONNECTIONS_MADE.getStatName());

        int createRequestCount = 0;
        final Path nonOverwriteFile = new Path("/NonOverwriteTest_FileName_"
            + UUID.randomUUID().toString());

        // Case 1: Not Overwrite - File does not pre-exist
        // create should be successful
        fs.create(nonOverwriteFile, false);

        // One request to server to create path should be issued
        // two calls added for -
        // 1. getFileStatus on DFS endpoint : 1
        //    getFileStatus on Blob endpoint: 1 ListBlobcall
        // 2. actual create call: 1
        createRequestCount += (
            client instanceof AbfsBlobClient && !getIsNamespaceEnabled(fs)
                ? 2
                : 1);

        assertAbfsStatistics(
            CONNECTIONS_MADE,
            totalConnectionMadeBeforeTest + createRequestCount,
            fs.getInstrumentationMap());

        // Case 2: Not Overwrite - File pre-exists
        fs.registerListener(new TracingHeaderValidator(
            fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
            fs.getFileSystemId(), FSOperationType.CREATE, false, 0));
        intercept(FileAlreadyExistsException.class,
            () -> fs.create(nonOverwriteFile, false));
        fs.registerListener(null);

        // One request to server to create path should be issued
        // Only single tryGetFileStatus should happen
        // 1. getFileStatus on DFS endpoint : 1
        //    getFileStatus on Blob endpoint: 1 (No Additional List blob call as file exists)

        createRequestCount += (
            client instanceof AbfsBlobClient && !getIsNamespaceEnabled(fs)
                ? 2
                : 1);

        assertAbfsStatistics(
            CONNECTIONS_MADE,
            totalConnectionMadeBeforeTest + createRequestCount,
            fs.getInstrumentationMap());

        final Path overwriteFilePath = new Path("/OverwriteTest_FileName_"
            + UUID.randomUUID().toString());

        // Case 3: Overwrite - File does not pre-exist
        // create should be successful
        fs.create(overwriteFilePath, true);

        /// One request to server to create path should be issued
        // two calls added for -
        // 1. getFileStatus on DFS endpoint : 1
        //    getFileStatus on Blob endpoint: 1 ListBlobCall + 1 GPS
        // 2. actual create call: 1
        // 1 extra call when conditional overwrite is not enabled to check for empty directory
        createRequestCount += (client instanceof AbfsBlobClient
            && !getIsNamespaceEnabled(fs))
            ? (enableConditionalCreateOverwrite ? 2 : 3)
            : 1;

        assertAbfsStatistics(
            CONNECTIONS_MADE,
            totalConnectionMadeBeforeTest + createRequestCount,
            fs.getInstrumentationMap());

        // Case 4: Overwrite - File pre-exists
        fs.registerListener(new TracingHeaderValidator(
            fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
            fs.getFileSystemId(), FSOperationType.CREATE, true, 0));
        fs.create(overwriteFilePath, true);
        fs.registerListener(null);

        createRequestCount += (
            client instanceof AbfsBlobClient && !getIsNamespaceEnabled(fs)
                ? 1
                : 0);

        // Second actual create call will hap
        if (enableConditionalCreateOverwrite) {
          // Three requests will be sent to server to create path,
          // 1. create without overwrite
          // 2. GetFileStatus to get eTag
          // 3. create with overwrite
          createRequestCount += 3;
        } else {
          createRequestCount += (client instanceof AbfsBlobClient
              && !getIsNamespaceEnabled(fs)) ? 2 : 1;
        }

        assertAbfsStatistics(
            CONNECTIONS_MADE,
            totalConnectionMadeBeforeTest + createRequestCount,
            fs.getInstrumentationMap());
      }
    }
  }

  /**
   * Test negative scenarios with Create overwrite=false as default
   * With create overwrite=true ending in 3 calls:
   * A. Create overwrite=false
   * B. GFS
   * C. Create overwrite=true
   *
   * Scn1: A fails with HTTP409, leading to B which fails with HTTP404,
   *        detect parallel access
   * Scn2: A fails with HTTP409, leading to B which fails with HTTP500,
   *        fail create with HTTP500
   * Scn3: A fails with HTTP409, leading to B and then C,
   *        which fails with HTTP412, detect parallel access
   * Scn4: A fails with HTTP409, leading to B and then C,
   *        which fails with HTTP500, fail create with HTTP500
   * Scn5: A fails with HTTP500, fail create with HTTP500
   */
  @Test
  public void testNegativeScenariosForCreateOverwriteDisabled()
      throws Throwable {

    try (AzureBlobFileSystem currentFs = getFileSystem()) {
      Configuration config = new Configuration(this.getRawConfiguration());
      config.set("fs.azure.enable.conditional.create.overwrite",
          Boolean.toString(true));

      try (AzureBlobFileSystem fs =
               (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
                   config)) {

        // Get mock AbfsClient with current config
        AbfsClient
            mockClient
            = ITestAbfsClient.getMockAbfsClient(
            fs.getAbfsStore().getClient(),
            fs.getAbfsStore().getAbfsConfiguration());
        AbfsClientHandler clientHandler = Mockito.mock(AbfsClientHandler.class);
        when(clientHandler.getIngressClient()).thenReturn(mockClient);
        when(clientHandler.getClient(Mockito.any())).thenReturn(mockClient);

        AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();

        ReflectionUtils.setFinalField(AzureBlobFileSystemStore.class, abfsStore,
            "clientHandler", clientHandler);
        ReflectionUtils.setFinalField(AzureBlobFileSystemStore.class, abfsStore,
            "client", mockClient);

        AbfsRestOperation successOp = mock(
            AbfsRestOperation.class);
        AbfsHttpOperation http200Op = mock(
            AbfsHttpOperation.class);
        when(http200Op.getStatusCode()).thenReturn(HTTP_OK);
        when(successOp.getResult()).thenReturn(http200Op);

        AbfsRestOperationException conflictResponseEx
            = getMockAbfsRestOperationException(HTTP_CONFLICT);
        AbfsRestOperationException serverErrorResponseEx
            = getMockAbfsRestOperationException(HTTP_INTERNAL_ERROR);
        AbfsRestOperationException fileNotFoundResponseEx
            = getMockAbfsRestOperationException(HTTP_NOT_FOUND);
        AbfsRestOperationException preConditionResponseEx
            = getMockAbfsRestOperationException(HTTP_PRECON_FAILED);

        doCallRealMethod().when(mockClient)
            .conditionalCreateOverwriteFile(anyString(),
                Mockito.nullable(FileSystem.Statistics.class),
                Mockito.nullable(AzureBlobFileSystemStore.Permissions.class),
                anyBoolean(),
                Mockito.nullable(ContextEncryptionAdapter.class),
                Mockito.nullable(TracingContext.class));

        // mock for overwrite=false
        doThrow(conflictResponseEx) // Scn1: GFS fails with Http404
            .doThrow(conflictResponseEx) // Scn2: GFS fails with Http500
            .doThrow(
                conflictResponseEx) // Scn3: create overwrite=true fails with Http412
            .doThrow(
                conflictResponseEx) // Scn4: create overwrite=true fails with Http500
            .doThrow(
                serverErrorResponseEx) // Scn5: create overwrite=false fails with Http500
            .when(mockClient)
            .createPath(any(String.class), eq(true), eq(false),
                any(AzureBlobFileSystemStore.Permissions.class),
                any(boolean.class), eq(null), any(),
                any(TracingContext.class));

        doThrow(fileNotFoundResponseEx) // Scn1: GFS fails with Http404
            .doThrow(serverErrorResponseEx) // Scn2: GFS fails with Http500
            .doReturn(
                successOp) // Scn3: create overwrite=true fails with Http412
            .doReturn(
                successOp) // Scn4: create overwrite=true fails with Http500
            .when(mockClient)
            .getPathStatus(any(String.class), eq(false),
                any(TracingContext.class), nullable(
                    ContextEncryptionAdapter.class));

        // mock for overwrite=true
        doThrow(
            preConditionResponseEx) // Scn3: create overwrite=true fails with Http412
            .doThrow(
                serverErrorResponseEx) // Scn4: create overwrite=true fails with Http500
            .when(mockClient)
            .createPath(any(String.class), eq(true), eq(true),
                any(AzureBlobFileSystemStore.Permissions.class),
                any(boolean.class), eq(null), any(),
                any(TracingContext.class));

        if (mockClient instanceof AbfsBlobClient) {
          doReturn(false).when((AbfsBlobClient) mockClient)
              .isNonEmptyDirectory(anyString(),
                  Mockito.nullable(TracingContext.class));

          doNothing().when((AbfsBlobClient) mockClient)
              .tryMarkerCreation(anyString(),
                  anyBoolean(),
                  Mockito.nullable(String.class),
                  Mockito.nullable(ContextEncryptionAdapter.class),
                  Mockito.nullable(TracingContext.class));

          // mock for overwrite=true
          doThrow(
              preConditionResponseEx) // Scn3: create overwrite=true fails with Http412
              .doThrow(
                  serverErrorResponseEx) // Scn4: create overwrite=true fails with Http500
              .when((AbfsBlobClient) mockClient)
              .createPathRestOp(any(String.class), eq(true), eq(true),
                  any(boolean.class), eq(null), any(),
                  any(TracingContext.class));

          // mock for overwrite=false
          doThrow(conflictResponseEx) // Scn1: GFS fails with Http404
              .doThrow(conflictResponseEx) // Scn2: GFS fails with Http500
              .doThrow(
                  conflictResponseEx) // Scn3: create overwrite=true fails with Http412
              .doThrow(
                  conflictResponseEx) // Scn4: create overwrite=true fails with Http500
              .doThrow(
                  serverErrorResponseEx)
              // Scn5: create overwrite=false fails with Http500
              .when((AbfsBlobClient) mockClient)
              .createPathRestOp(any(String.class), eq(true), eq(false),
                  any(boolean.class), eq(null), any(),
                  any(TracingContext.class));

          doThrow(fileNotFoundResponseEx) // Scn1: GFS fails with Http404
              .doThrow(serverErrorResponseEx) // Scn2: GFS fails with Http500
              .doReturn(
                  successOp) // Scn3: create overwrite=true fails with Http412
              .doReturn(
                  successOp) // Scn4: create overwrite=true fails with Http500
              .when((AbfsBlobClient) mockClient)
              .getPathStatus(any(String.class), any(TracingContext.class),
                  nullable(
                      ContextEncryptionAdapter.class), eq(false));
        }

        // Scn1: GFS fails with Http404
        // Sequence of events expected:
        // 1. create overwrite=false - fail with conflict
        // 2. GFS - fail with File Not found
        // Create will fail with ConcurrentWriteOperationDetectedException
        validateCreateFileException(
            ConcurrentWriteOperationDetectedException.class,
            abfsStore);

        // Scn2: GFS fails with Http500
        // Sequence of events expected:
        // 1. create overwrite=false - fail with conflict
        // 2. GFS - fail with Server error
        // Create will fail with 500
        validateCreateFileException(AbfsRestOperationException.class,
            abfsStore);

        // Scn3: create overwrite=true fails with Http412
        // Sequence of events expected:
        // 1. create overwrite=false - fail with conflict
        // 2. GFS - pass
        // 3. create overwrite=true - fail with Pre-Condition
        // Create will fail with ConcurrentWriteOperationDetectedException
        validateCreateFileException(
            ConcurrentWriteOperationDetectedException.class,
            abfsStore);

        // Scn4: create overwrite=true fails with Http500
        // Sequence of events expected:
        // 1. create overwrite=false - fail with conflict
        // 2. GFS - pass
        // 3. create overwrite=true - fail with Server error
        // Create will fail with 500
        validateCreateFileException(AbfsRestOperationException.class,
            abfsStore);

        // Scn5: create overwrite=false fails with Http500
        // Sequence of events expected:
        // 1. create overwrite=false - fail with server error
        // Create will fail with 500
        validateCreateFileException(AbfsRestOperationException.class,
            abfsStore);
      }
    }
  }

  @Test
  public void testCreateMarkerFailExceptionIsSwallowed() throws Throwable {
    assumeBlobServiceType();
    try (AzureBlobFileSystem currentFs = getFileSystem()) {
      Configuration config = new Configuration(this.getRawConfiguration());
      config.set("fs.azure.enable.conditional.create.overwrite", Boolean.toString(true));

      try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(), config)) {
        AbfsClient mockClient = Mockito.spy(fs.getAbfsClient());
        AzureBlobFileSystemStore spiedStore = Mockito.spy(fs.getAbfsStore());
        spiedStore.setClient(mockClient);

        AbfsClientHandler clientHandler = Mockito.mock(AbfsClientHandler.class);
        when(clientHandler.getIngressClient()).thenReturn(mockClient);
        when(clientHandler.getClient(Mockito.any())).thenReturn(mockClient);
        Path testFolder = new Path("/dir1");
        createAzCopyFolder(testFolder);

        AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();
        ReflectionUtils.setFinalField(AzureBlobFileSystemStore.class, abfsStore, "clientHandler", clientHandler);
        ReflectionUtils.setFinalField(AzureBlobFileSystemStore.class, abfsStore, "client", mockClient);

        AbfsRestOperation successOp = mock(AbfsRestOperation.class);
        AbfsHttpOperation http200Op = mock(AbfsHttpOperation.class);
        when(http200Op.getStatusCode()).thenReturn(HTTP_OK);
        when(successOp.getResult()).thenReturn(http200Op);

        AbfsRestOperationException preConditionResponseEx = getMockAbfsRestOperationException(HTTP_PRECON_FAILED);

        doCallRealMethod().when(mockClient)
            .conditionalCreateOverwriteFile(anyString(),
                Mockito.nullable(FileSystem.Statistics.class),
                Mockito.nullable(AzureBlobFileSystemStore.Permissions.class),
                anyBoolean(),
                Mockito.nullable(ContextEncryptionAdapter.class),
                Mockito.nullable(TracingContext.class));

        doCallRealMethod().when((AbfsBlobClient) mockClient)
            .tryMarkerCreation(anyString(), anyBoolean(), Mockito.nullable(String.class),
                Mockito.nullable(ContextEncryptionAdapter.class),
                Mockito.nullable(TracingContext.class));

        Mockito.doReturn(new ArrayList<>(Collections.singletonList(testFolder)))
            .when((AbfsBlobClient) mockClient)
            .getMarkerPathsTobeCreated(any(Path.class), Mockito.nullable(TracingContext.class));

        doReturn(false).when((AbfsBlobClient) mockClient)
            .isNonEmptyDirectory(anyString(), Mockito.nullable(TracingContext.class));

        doAnswer(new Answer<Void>() {
          private boolean firstCall = true;

          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            if (firstCall) {
              firstCall = false;
              throw preConditionResponseEx;
            }
            return null;
          }
        }).doCallRealMethod()
            .when((AbfsBlobClient) mockClient)
            .createPathRestOp(anyString(), anyBoolean(), anyBoolean(),
                anyBoolean(), Mockito.nullable(String.class),
                Mockito.nullable(ContextEncryptionAdapter.class),
                Mockito.nullable(TracingContext.class));

        AbfsClientTestUtil.hookOnRestOpsForTracingContextSingularity(mockClient);

        doReturn(successOp)
            .when((AbfsBlobClient) mockClient)
            .getPathStatus(any(String.class), any(TracingContext.class),
                nullable(ContextEncryptionAdapter.class), eq(false));

        FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
        FsPermission umask = new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE);
        Path testPath = new Path("/dir1/testFile");
        abfsStore.createFile(testPath, null, true, permission, umask,
            getTestTracingContext(getFileSystem(), true));
        Assertions.assertThat(fs.exists(testPath))
            .describedAs("File not created when marker creation failed.")
            .isTrue();
      }
    }
  }

  private <E extends Throwable> void validateCreateFileException(final Class<E> exceptionClass, final AzureBlobFileSystemStore abfsStore)
      throws Exception {
    FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL,
        FsAction.ALL);
    FsPermission umask = new FsPermission(FsAction.NONE, FsAction.NONE,
        FsAction.NONE);
    Path testPath = new Path("/testFile");
    intercept(
        exceptionClass,
        () -> abfsStore.createFile(testPath, null, true, permission, umask,
            getTestTracingContext(getFileSystem(), true)));
  }

  private AbfsRestOperationException getMockAbfsRestOperationException(int status) {
    return new AbfsRestOperationException(status, "", "", new Exception());
  }

  /**
   * Attempts to test multiple flush calls.
   */
  @Test
  public void testMultipleFlush() throws Throwable {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      Path testPath = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
      try (FSDataOutputStream out = fs.create(testPath)) {
        out.write('1');
        out.hsync();
        out.write('2');
        out.hsync();
      }
    }
  }

  /**
   * Delete the blob before flush and verify that an exception should be thrown.
   */
  @Test
  public void testDeleteBeforeFlush() throws Throwable {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      Path testPath = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
      try (FSDataOutputStream out = fs.create(testPath)) {
        out.write('1');
        fs.delete(testPath, false);
        out.hsync();
        // this will cause the next write to failAll
      } catch (IOException fnfe) {
        //appendblob outputStream does not generate suppressed exception on close as it is
        //single threaded code
        if (!fs.getAbfsStore()
            .isAppendBlobKey(fs.makeQualified(testPath).toString())) {
          // the exception raised in close() must be in the caught exception's
          // suppressed list
          Throwable[] suppressed = fnfe.getSuppressed();
          assertEquals("suppressed count", 1, suppressed.length);
          Throwable inner = suppressed[0];
          if (!(inner instanceof IOException)) {
            throw inner;
          }
          GenericTestUtils.assertExceptionContains(fnfe.getMessage(),
              inner.getCause(), inner.getCause().getMessage());
        }
      }
    }
  }

  /**
   * Creating subdirectory on existing file path should fail.
   * @throws Exception
   */
  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.create(new Path("a/b/c"));
      fs.mkdirs(new Path("a/b/d"));
      intercept(IOException.class, () -> fs.mkdirs(new Path("a/b/c/d/e")));

      Assertions.assertThat(fs.exists(new Path("a/b/c"))).isTrue();
      Assertions.assertThat(fs.exists(new Path("a/b/d"))).isTrue();
      // Asserting directory created still exists as explicit.
      Assertions.assertThat(
              DirectoryStateHelper.isExplicitDirectory(new Path("a/b/d"), fs,
                  getTestTracingContext(fs, true)))
          .describedAs("Path is not an explicit directory")
          .isTrue();
    }
  }

  /**
   * Calling mkdir for existing implicit directory.
   * @throws Exception
   */
  @Test
  public void testMkdirSameFolder() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      createAzCopyFolder(new Path("a/b/d"));
      fs.mkdirs(new Path("a/b/d"));
    }
  }

  /**
   * Try creating file same as an existing directory.
   * @throws Exception
   */
  @Test
  public void testCreateDirectoryAndFile() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.mkdirs(new Path("a/b/c"));
      Assertions.assertThat(fs.exists(new Path("a/b/c"))).isTrue();
      intercept(IOException.class, () -> fs.create(new Path("a/b/c")));
      // Asserting that directory still exists as explicit
      Assertions.assertThat(
              DirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"),
                  fs, getTestTracingContext(fs, true)))
          .describedAs("Path is not an explicit directory")
          .isTrue();
    }
  }

  /**
   * Creating same file without specifying overwrite.
   * @throws Exception
   */
  @Test
  public void testCreateSameFile() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.create(new Path("a/b/c"));
      fs.create(new Path("a/b/c"));
      Assertions.assertThat(fs.exists(new Path("a/b/c")))
          .describedAs("Path does not exist")
          .isTrue();
    }
  }

  /**
   * Test the creation of a file without conditional overwrite.
   * This test sets the configuration `fs.azure.enable.conditional.create.overwrite` to false,
   * creates a directory, and then attempts to create a file at the same path with overwrite set to true.
   * It expects an IOException to be thrown.
   *
   * @throws Exception if any exception occurs during the test execution
   */
  @Test
  public void testCreationWithoutConditionalOverwrite()
      throws Exception {
    try (AzureBlobFileSystem currentFs = getFileSystem()) {
      Configuration config = new Configuration(this.getRawConfiguration());
      config.set("fs.azure.enable.conditional.create.overwrite",
          String.valueOf(false));

      try (AzureBlobFileSystem fs =
               (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
                   config)) {
        fs.mkdirs(new Path("a/b/c"));
        intercept(IOException.class,
            () -> fs.create(new Path("a/b/c"), true));
      }
    }
  }

  /**
   * Test the creation of a file with overwrite set to false without conditional overwrite.
   * This test sets the configuration `fs.azure.enable.conditional.create.overwrite` to false,
   * creates a directory, and then attempts to create a file at the same path with overwrite set to false.
   * It expects an IOException to be thrown.
   *
   * @throws Exception if any exception occurs during the test execution
   */
  @Test
  public void testCreationOverwriteFalseWithoutConditionalOverwrite() throws Exception {
    try (AzureBlobFileSystem currentFs = getFileSystem()) {
      Configuration config = new Configuration(this.getRawConfiguration());
      config.set("fs.azure.enable.conditional.create.overwrite",
          String.valueOf(false));

      try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
          currentFs.getUri(), config)) {
        fs.mkdirs(new Path("a/b/c"));
        intercept(IOException.class,
            () -> fs.create(new Path("a/b/c"), false));
      }
    }
  }

  /**
   * Creating same file with overwrite flag set to false.
   * @throws Exception
   */
  @Test
  public void testCreateSameFileWithOverwriteFalse() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.create(new Path("a/b/c"));
      Assertions.assertThat(fs.exists(new Path("a/b/c")))
          .describedAs("Path does not exist")
          .isTrue();
      intercept(IOException.class,
          () -> fs.create(new Path("a/b/c"), false));
    }
  }

  /**
   * Creation of already existing subpath should fail.
   * @throws Exception
   */
  @Test
  public void testCreateSubPath() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.create(new Path("a/b/c"));
      Assertions.assertThat(fs.exists(new Path("a/b/c")))
          .describedAs("Path does not exist")
          .isTrue();
      intercept(IOException.class,
          () -> fs.create(new Path("a/b")));
    }
  }

  /**
   * Test create path in parallel with overwrite false.
   **/
  @Test
  public void testParallelCreateOverwriteFalse()
      throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE, "false");
    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        configuration)) {
      ExecutorService executorService = Executors.newFixedThreadPool(5);
      List<Future<?>> futures = new ArrayList<>();

      final byte[] b = new byte[8 * ONE_MB];
      new Random().nextBytes(b);
      final Path filePath = path("/testPath");

      futures.add(executorService.submit(() -> {
        try {
          fs.create(filePath, false);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }));

      futures.add(executorService.submit(() -> {
        try {
          fs.create(filePath, false);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }));

      futures.add(executorService.submit(() -> {
        try {
          fs.create(filePath, false);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }));

      checkFuturesForExceptions(futures, 2);
    }
  }

  /**
   * Test create path in parallel with overwrite true.
   **/
  @Test
  public void testParallelCreateOverwriteTrue()
      throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE, "false");
    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        configuration)) {
      ExecutorService executorService = Executors.newFixedThreadPool(5);
      List<Future<?>> futures = new ArrayList<>();

      final byte[] b = new byte[8 * ONE_MB];
      new Random().nextBytes(b);
      final Path filePath = path("/testPath");

      futures.add(executorService.submit(() -> {
        try {
          fs.create(filePath);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }));

      futures.add(executorService.submit(() -> {
        try {
          fs.create(filePath);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }));

      futures.add(executorService.submit(() -> {
        try {
          fs.create(filePath);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }));

      checkFuturesForExceptions(futures, 0);
    }
  }

  /**
   * Creating path with parent explicit.
   */
  @Test
  public void testCreatePathParentExplicit() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.mkdirs(new Path("a/b/c"));
      Assertions.assertThat(fs.exists(new Path("a/b/c")))
          .describedAs("Path does not exist")
          .isTrue();
      fs.create(new Path("a/b/c/d"));
      Assertions.assertThat(fs.exists(new Path("a/b/c/d")))
          .describedAs("Path does not exist")
          .isTrue();

      // asserting that parent stays explicit
      Assertions.assertThat(
              DirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"),
                  fs, getTestTracingContext(fs, true)))
          .describedAs("Path is not an explicit directory")
          .isTrue();
    }
  }

  // Creation with append blob should succeed for blob endpoint
  @Test
  public void testCreateWithAppendBlobEnabled()
      throws IOException, NoSuchFieldException, IllegalAccessException {
    Configuration conf = getRawConfiguration();
    try (AzureBlobFileSystem fs = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.newInstance(conf))) {
      AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
      doReturn(true).when(store).isAppendBlobKey(anyString());

      // Set abfsStore as our mocked value.
      Field privateField = AzureBlobFileSystem.class.getDeclaredField(
          "abfsStore");
      privateField.setAccessible(true);
      privateField.set(fs, store);
      Path testPath = path("/testPath");
      AzureBlobFileSystemStore.Permissions permissions
          = new AzureBlobFileSystemStore.Permissions(false,
          FsPermission.getDefault(), FsPermission.getUMask(fs.getConf()));
      fs.getAbfsStore().getClientHandler().getBlobClient().
          createPath(makeQualified(testPath).toUri().getPath(), true, false,
              permissions, true, null,
              null, getTestTracingContext(fs, true));
    }
  }

  /**
   * Test create on implicit directory with explicit parent.
   * @throws Exception
   */
  @Test
  public void testParentExplicitPathImplicit() throws Exception {
    assumeBlobServiceType();
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.mkdirs(new Path("/explicitParent"));
      String sourcePathName = "/explicitParent/implicitDir";
      Path sourcePath = new Path(sourcePathName);
      createAzCopyFolder(sourcePath);

      intercept(IOException.class, () ->
          fs.create(sourcePath, true));
      intercept(IOException.class, () ->
          fs.create(sourcePath, false));

      Assertions.assertThat(
              DirectoryStateHelper.isExplicitDirectory(sourcePath.getParent(), fs,
                  getTestTracingContext(fs, true)))
          .describedAs("Parent directory should be explicit.")
          .isTrue();
      Assertions.assertThat(
              DirectoryStateHelper.isImplicitDirectory(sourcePath, fs,
                  getTestTracingContext(fs, true)))
          .describedAs("Path should be implicit.")
          .isTrue();
    }
  }

  /**
   * Test create on implicit directory with implicit parent
   * @throws Exception
   */
  @Test
  public void testParentImplicitPathImplicit() throws Exception {
    assumeBlobServiceType();
    try (AzureBlobFileSystem fs = getFileSystem()) {
      String parentPathName = "/implicitParent";
      Path parentPath = new Path(parentPathName);
      String sourcePathName = "/implicitParent/implicitDir";
      Path sourcePath = new Path(sourcePathName);

      createAzCopyFolder(parentPath);
      createAzCopyFolder(sourcePath);

      intercept(IOException.class, () ->
          fs.create(sourcePath, true));
      intercept(IOException.class, () ->
          fs.create(sourcePath, false));

      Assertions.assertThat(
              DirectoryStateHelper.isImplicitDirectory(parentPath, fs,
                  getTestTracingContext(fs, true)))
          .describedAs("Parent directory is implicit.")
          .isTrue();
      Assertions.assertThat(
              DirectoryStateHelper.isImplicitDirectory(sourcePath, fs,
                  getTestTracingContext(fs, true)))
          .describedAs("Path should also be implicit.")
          .isTrue();
    }
  }

  /**
   * Tests create file when file exists already and parent is implicit
   * Verifies using eTag for overwrite = true/false
   */
  @Test
  public void testCreateFileExistsImplicitParent() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      String parentPathName = "/implicitParent";
      Path parentPath = new Path(parentPathName);
      createAzCopyFolder(parentPath);

      String fileName = "/implicitParent/testFile";
      Path filePath = new Path(fileName);
      fs.create(filePath);
      String eTag = extractFileEtag(fileName);

      // testing createFile on already existing file path
      fs.create(filePath, true);

      String eTagAfterCreateOverwrite = extractFileEtag(fileName);

      Assertions.assertThat(eTag.equals(eTagAfterCreateOverwrite))
          .describedAs(
              "New file eTag after create overwrite should be different from old")
          .isFalse();

      intercept(IOException.class, () ->
          fs.create(filePath, false));

      String eTagAfterCreate = extractFileEtag(fileName);

      Assertions.assertThat(eTagAfterCreateOverwrite.equals(eTagAfterCreate))
          .describedAs("File eTag should not change as creation fails")
          .isTrue();

      Assertions.assertThat(
              DirectoryStateHelper.isExplicitDirectory(parentPath, fs,
                  getTestTracingContext(fs, true)))
          .describedAs("Parent path should also change to explicit.")
          .isTrue();
    }
  }

  /**
   * Tests create file when file exists already and parent is explicit
   * Verifies using eTag for overwrite = true/false
   */
  @Test
  public void testCreateFileExistsExplicitParent() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      String parentPathName = "/explicitParent";
      Path parentPath = new Path(parentPathName);
      fs.mkdirs(parentPath);

      String fileName = "/explicitParent/testFile";
      Path filePath = new Path(fileName);
      fs.create(filePath);
      String eTag = extractFileEtag(fileName);

      // testing createFile on already existing file path
      fs.create(filePath, true);

      String eTagAfterCreateOverwrite = extractFileEtag(fileName);

      Assertions.assertThat(eTag.equals(eTagAfterCreateOverwrite))
          .describedAs(
              "New file eTag after create overwrite should be different from old")
          .isFalse();

      intercept(IOException.class, () ->
          fs.create(filePath, false));

      String eTagAfterCreate = extractFileEtag(fileName);

      Assertions.assertThat(eTagAfterCreateOverwrite.equals(eTagAfterCreate))
          .describedAs("File eTag should not change as creation fails")
          .isTrue();

      Assertions.assertThat(
              DirectoryStateHelper.isExplicitDirectory(parentPath, fs,
                  getTestTracingContext(fs, true)))
          .describedAs("Parent path should also change to explicit.")
          .isTrue();
    }
  }

  /**
   * Tests create file when the parent is an existing file
   * should fail.
   * @throws Exception FileAlreadyExists for blob and IOException for dfs.
   */
  @Test
  public void testCreateFileParentFile() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      String parentName = "/testParentFile";
      Path parent = new Path(parentName);
      fs.create(parent);

      String childName = "/testParentFile/testChildFile";
      Path child = new Path(childName);
      IOException e = intercept(IOException.class, () ->
          fs.create(child, false));

      // asserting that parent stays explicit
      FileStatus status = fs.getAbfsStore()
          .getFileStatus(fs.makeQualified(new Path(parentName)),
              new TracingContext(getTestTracingContext(fs, true)));
      Assertions.assertThat(status.isDirectory())
          .describedAs("Path is not a file")
          .isFalse();
    }
  }

  /**
   * Creating directory on existing file path should fail.
   * @throws Exception
   */
  @Test
  public void testCreateMkdirs() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.create(new Path("a/b/c"));
      intercept(IOException.class,
            () -> fs.mkdirs(new Path("a/b/c/d")));
    }
  }

  /**
   * Test mkdirs.
   * @throws Exception
   */
  @Test
  public void testMkdirs() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.mkdirs(new Path("a/b"));
      fs.mkdirs(new Path("a/b/c/d"));
      fs.mkdirs(new Path("a/b/c/e"));

      Assertions.assertThat(fs.exists(new Path("a/b")))
          .describedAs("Path a/b does not exist")
          .isTrue();
      Assertions.assertThat(fs.exists(new Path("a/b/c/d")))
          .describedAs("Path a/b/c/d does not exist")
          .isTrue();
      Assertions.assertThat(fs.exists(new Path("a/b/c/e")))
          .describedAs("Path a/b/c/e does not exist")
          .isTrue();

      // Asserting that directories created as explicit
      FileStatus status = fs.getAbfsStore()
          .getFileStatus(fs.makeQualified(new Path("a/b")),
              new TracingContext(getTestTracingContext(fs, true)));
      Assertions.assertThat(status.isDirectory())
          .describedAs("Path a/b is not an explicit directory")
          .isTrue();
      FileStatus status1 = fs.getAbfsStore()
          .getFileStatus(fs.makeQualified(new Path("a/b/c/d")),
              new TracingContext(getTestTracingContext(fs, true)));
      Assertions.assertThat(status1.isDirectory())
          .describedAs("Path a/b/c/d is not an explicit directory")
          .isTrue();
      FileStatus status2 = fs.getAbfsStore()
          .getFileStatus(fs.makeQualified(new Path("a/b/c/e")),
              new TracingContext(getTestTracingContext(fs, true)));
      Assertions.assertThat(status2.isDirectory())
          .describedAs("Path a/b/c/e is not an explicit directory")
          .isTrue();
    }
  }

  /**
   * Creating subpath of directory path should fail.
   * @throws Exception
   */
  @Test
  public void testMkdirsCreateSubPath() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.mkdirs(new Path("a/b/c"));
      Assertions.assertThat(fs.exists(new Path("a/b/c")))
          .describedAs("Path a/b/c does not exist")
          .isTrue();
      intercept(IOException.class, () -> fs.create(new Path("a/b")));

      // Asserting that directories created as explicit
      FileStatus status2 = fs.getAbfsStore()
          .getFileStatus(fs.makeQualified(new Path("a/b/c")),
              new TracingContext(getTestTracingContext(fs, true)));
      Assertions.assertThat(status2.isDirectory())
          .describedAs("Path a/b/c is not an explicit directory")
          .isTrue();
    }
  }

  /**
   * Test creation of directory by level.
   * @throws Exception
   */
  @Test
  public void testMkdirsByLevel() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.mkdirs(new Path("a"));
      fs.mkdirs(new Path("a/b/c"));
      fs.mkdirs(new Path("a/b/c/d/e"));

      Assertions.assertThat(fs.exists(new Path("a")))
          .describedAs("Path a does not exist")
          .isTrue();
      Assertions.assertThat(fs.exists(new Path("a/b/c")))
          .describedAs("Path a/b/c does not exist")
          .isTrue();
      Assertions.assertThat(fs.exists(new Path("a/b/c/d/e")))
          .describedAs("Path a/b/c/d/e does not exist")
          .isTrue();

      // Asserting that directories created as explicit
      FileStatus status = fs.getAbfsStore()
          .getFileStatus(fs.makeQualified(new Path("a/")),
              new TracingContext(getTestTracingContext(fs, true)));
      Assertions.assertThat(status.isDirectory())
          .describedAs("Path a is not an explicit directory")
          .isTrue();
      FileStatus status1 = fs.getAbfsStore()
          .getFileStatus(fs.makeQualified(new Path("a/b/c")),
              new TracingContext(getTestTracingContext(fs, true)));
      Assertions.assertThat(status1.isDirectory())
          .describedAs("Path a/b/c is not an explicit directory")
          .isTrue();
      FileStatus status2 = fs.getAbfsStore()
          .getFileStatus(fs.makeQualified(new Path("a/b/c/d/e")),
              new TracingContext(getTestTracingContext(fs, true)));
      Assertions.assertThat(status2.isDirectory())
          .describedAs("Path a/b/c/d/e is not an explicit directory")
          .isTrue();
    }
  }

  /*
    Delete part of a path and validate sub path exists.
   */
  @Test
  public void testMkdirsWithDelete() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.mkdirs(new Path("a/b"));
      fs.mkdirs(new Path("a/b/c/d"));
      fs.delete(new Path("a/b/c/d"));
      fs.getFileStatus(new Path("a/b/c"));
      Assertions.assertThat(fs.exists(new Path("a/b/c")))
          .describedAs("Path a/b/c does not exist")
          .isTrue();
    }
  }

  /**
   * Verify mkdir and rename of parent.
   */
  @Test
  public void testMkdirsWithRename() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.mkdirs(new Path("a/b/c/d"));
      fs.create(new Path("e/file"));
      fs.delete(new Path("a/b/c/d"));
      Assertions.assertThat(fs.rename(new Path("e"), new Path("a/b/c/d")))
          .describedAs("Failed to rename path e to a/b/c/d")
          .isTrue();
      Assertions.assertThat(fs.exists(new Path("a/b/c/d/file")))
          .describedAs("Path a/b/c/d/file does not exist")
          .isTrue();
    }
  }

  /**
   * Create a file with name /dir1 and then mkdirs for /dir1/dir2 should fail.
   * @throws Exception
   */
  @Test
  public void testFileCreateMkdirsRoot() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.setWorkingDirectory(new Path("/"));
      final Path p1 = new Path("dir1");
      fs.create(p1);
      intercept(IOException.class,
          () -> fs.mkdirs(new Path("dir1/dir2")));
    }
  }

  /**
   * Create a file with name /dir1 and then mkdirs for /dir1/dir2 should fail.
   * @throws Exception
   */
  @Test
  public void testFileCreateMkdirsNonRoot() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      final Path p1 = new Path("dir1");
      fs.create(p1);
      intercept(IOException.class, () -> fs.mkdirs(new Path("dir1/dir2")));
    }
  }

  /**
   * Creation of same directory without overwrite flag should pass.
   * @throws Exception
   */
  @Test
  public void testCreateSameDirectory() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.mkdirs(new Path("a/b/c"));
      fs.mkdirs(new Path("a/b/c"));

      Assertions.assertThat(fs.exists(new Path("a/b/c")))
          .describedAs("Path a/b/c does not exist")
          .isTrue();
      // Asserting that directories created as explicit
      FileStatus status = fs.getAbfsStore()
          .getFileStatus(fs.makeQualified(new Path("a/b/c")),
              new TracingContext(getTestTracingContext(fs, true)));
      Assertions.assertThat(status.isDirectory())
          .describedAs("Path a/b/c is not an explicit directory")
          .isTrue();
    }
  }

  /**
   * Creation of same directory without overwrite flag should pass.
   * @throws Exception
   */
  @Test
  public void testCreateSamePathDirectory() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.create(new Path("a"));
      intercept(IOException.class, () -> fs.mkdirs(new Path("a")));
    }
  }

  /**
   * Creation of directory with root as parent
   */
  @Test
  public void testMkdirOnRootAsParent() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      final Path path = new Path("a");
      fs.setWorkingDirectory(new Path("/"));
      fs.mkdirs(path);

      // Asserting that the directory created by mkdir exists as explicit.
      FileStatus status = fs.getAbfsStore()
          .getFileStatus(fs.makeQualified(new Path("a")),
              new TracingContext(getTestTracingContext(fs, true)));
      Assertions.assertThat(status.isDirectory())
          .describedAs("Path a is not an explicit directory")
          .isTrue();
    }
  }

  /**
   * Creation of directory on root
   */
  @Test
  public void testMkdirOnRoot() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      final Path path = new Path("/");
      fs.setWorkingDirectory(new Path("/"));
      fs.mkdirs(path);

      FileStatus status = fs.getAbfsStore()
          .getFileStatus(fs.makeQualified(new Path("/")),
              new TracingContext(getTestTracingContext(fs, true)));
      Assertions.assertThat(status.isDirectory())
          .describedAs("Path is not an explicit directory")
          .isTrue();
    }
  }

  /**
   * Creation of file on path with unicode chars
   */
  @Test
  public void testCreateUnicode() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      final Path path = new Path("/file\u0031");
      fs.create(path);

      Assertions.assertThat(fs.exists(path))
          .describedAs("Path with unicode does not exist")
          .isTrue();
    }
  }

  /**
   * Creation of directory on path with unicode chars
   */
  @Test
  public void testMkdirUnicode() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      final Path path = new Path("/dir\u0031");
      fs.mkdirs(path);

      // Asserting that the directory created by mkdir exists as explicit.
      FileStatus status = fs.getAbfsStore()
          .getFileStatus(fs.makeQualified(path),
              new TracingContext(getTestTracingContext(fs, true)));
      Assertions.assertThat(status.isDirectory())
          .describedAs("Path is not an explicit directory")
          .isTrue();
    }
  }

  /**
   * Creation of directory on same path with parallel threads.
   */
  @Test
  public void testMkdirParallelRequests() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      final Path path = new Path("/dir1");

      ExecutorService es = Executors.newFixedThreadPool(3);

      List<CompletableFuture<Void>> tasks = new ArrayList<>();

      for (int i = 0; i < 3; i++) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          try {
            fs.mkdirs(path);
          } catch (IOException e) {
            throw new CompletionException(e);
          }
        }, es);
        tasks.add(future);
      }

      // Wait for all the tasks to complete
      CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();

      // Assert that the directory created by mkdir exists as explicit
      FileStatus status = fs.getAbfsStore()
          .getFileStatus(fs.makeQualified(path),
              new TracingContext(getTestTracingContext(fs, true)));
      Assertions.assertThat(status.isDirectory())
          .describedAs("Path is not an explicit directory")
          .isTrue();
    }
  }


  /**
   * Creation of directory with overwrite set to false should not fail according to DFS code.
   * @throws Exception
   */
  @Test
  public void testCreateSameDirectoryOverwriteFalse() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.setBoolean(FS_AZURE_ENABLE_MKDIR_OVERWRITE, false);
    try (AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(configuration)) {
      fs1.mkdirs(new Path("a/b/c"));
      fs1.mkdirs(new Path("a/b/c"));

      // Asserting that directories created as explicit
      FileStatus status = fs1.getAbfsStore()
          .getFileStatus(fs1.makeQualified(new Path("a/b/c")),
              new TracingContext(getTestTracingContext(fs1, true)));
      Assertions.assertThat(status.isDirectory())
          .describedAs("Path is not an explicit directory")
          .isTrue();
    }
  }

  /**
   * Try creating directory same as an existing file.
   */
  @Test
  public void testCreateDirectoryAndFileRecreation() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      fs.mkdirs(new Path("a/b/c"));
      fs.create(new Path("a/b/c/d"));
      Assertions.assertThat(fs.exists(new Path("a/b/c")))
          .describedAs("Directory a/b/c does not exist")
          .isTrue();
      Assertions.assertThat(fs.exists(new Path("a/b/c/d")))
          .describedAs("File a/b/c/d does not exist")
          .isTrue();
        intercept(IOException.class,
            () -> fs.mkdirs(new Path("a/b/c/d")));
    }
  }

  @Test
  public void testCreateNonRecursiveForAtomicDirectoryFile() throws Exception {
    try (AzureBlobFileSystem fileSystem = getFileSystem()) {
      fileSystem.setWorkingDirectory(new Path("/"));
      fileSystem.mkdirs(new Path("/hbase/dir"));
      fileSystem.createFile(new Path("/hbase/dir/file"))
          .overwrite(false)
          .replication((short) 1)
          .bufferSize(1024)
          .blockSize(1024)
          .build();
      Assertions.assertThat(fileSystem.exists(new Path("/hbase/dir/file")))
          .describedAs("File /hbase/dir/file does not exist")
          .isTrue();
    }
  }

  /**
   * Test creating a file on a non-existing path with an implicit parent directory.
   * This test creates an implicit directory, then creates a file
   * inside this implicit directory and asserts that it gets created.
   *
   * @throws Exception if any exception occurs during the test execution
   */
  @Test
  public void testCreateOnNonExistingPathWithImplicitParentDir() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      final Path implicitPath = new Path("dir1");
      final Path path = new Path("dir1/dir2");
      createAzCopyFolder(implicitPath);

      // Creating a directory on non-existing path inside an implicit directory
      fs.create(path);

      // Asserting that path created by azcopy becomes explicit.
      Assertions.assertThat(fs.exists(path))
          .describedAs("File dir1/dir2 does not exist")
          .isTrue();
    }
  }

  @Test
  public void testMkdirOnNonExistingPathWithImplicitParentDir() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      final Path implicitPath = new Path("dir1");
      final Path path = new Path("dir1/dir2");
      createAzCopyFolder(implicitPath);

      // Creating a directory on non-existing path inside an implicit directory
      fs.mkdirs(path);

      // Asserting that path created by azcopy becomes explicit.
      Assertions.assertThat(
              DirectoryStateHelper.isExplicitDirectory(implicitPath,
                  fs, getTestTracingContext(fs, true)))
          .describedAs("Path created by azcopy did not become explicit")
          .isTrue();

      // Asserting that the directory created by mkdir exists as explicit.
      Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(path,
              fs, getTestTracingContext(fs, true)))
          .describedAs("Directory created by mkdir does not exist as explicit")
          .isTrue();
    }
  }

  /**
   * Creation of directory with parent directory existing as implicit.
   * And the directory to be created existing as explicit directory
   * @throws Exception
   */
  @Test
  public void testMkdirOnExistingExplicitDirWithImplicitParentDir() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      final Path implicitPath = new Path("dir1");
      final Path path = new Path("dir1/dir2");

      // Creating implicit directory to be used as parent
      createAzCopyFolder(implicitPath);

      // Creating an explicit directory on the path first
      fs.mkdirs(path);

      // Creating a directory on existing explicit directory inside an implicit directory
      fs.mkdirs(path);

      // Asserting that path created by azcopy becomes explicit.
      Assertions.assertThat(
              DirectoryStateHelper.isExplicitDirectory(implicitPath,
                  fs, getTestTracingContext(fs, true)))
          .describedAs("Path created by azcopy did not become explicit")
          .isTrue();

      // Asserting that the directory created by mkdir exists as explicit.
      Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(path,
              fs, getTestTracingContext(fs, true)))
          .describedAs("Directory created by mkdir does not exist as explicit")
          .isTrue();
    }
  }

  /**
   * Creation of directory with parent directory existing as explicit.
   * And the directory to be created existing as implicit directory
   * @throws Exception
   */
  @Test
  public void testMkdirOnExistingImplicitDirWithExplicitParentDir() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      final Path explicitPath = new Path("dir1");
      final Path path = new Path("dir1/dir2");

      // Creating an explicit directory to be used a parent
      fs.mkdirs(explicitPath);

      createAzCopyFolder(path);

      // Creating a directory on existing implicit directory inside an explicit directory
      fs.mkdirs(path);

      // Asserting that the directory created by mkdir exists as explicit.
      Assertions.assertThat(
              DirectoryStateHelper.isExplicitDirectory(explicitPath,
                  fs, getTestTracingContext(fs, true)))
          .describedAs("Explicit parent directory does not exist as explicit")
          .isTrue();

      // Asserting that the directory created by mkdir exists as explicit.
      Assertions.assertThat(DirectoryStateHelper.isImplicitDirectory(path,
              fs, getTestTracingContext(fs, true)))
          .describedAs("Mkdir created explicit directory")
          .isTrue();
    }
  }

  /**
   * Creation of directory with parent directory existing as implicit.
   * And the directory to be created existing as implicit directory
   * @throws Exception
   */
  @Test
  public void testMkdirOnExistingImplicitDirWithImplicitParentDir() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      final Path implicitPath = new Path("dir3");
      final Path path = new Path("dir3/dir4");

      createAzCopyFolder(implicitPath);

      // Creating an implicit directory on path
      createAzCopyFolder(path);

      // Creating a directory on existing implicit directory inside an implicit directory
      fs.mkdirs(path);

      Assertions.assertThat(
              DirectoryStateHelper.isImplicitDirectory(implicitPath,
                  fs, getTestTracingContext(fs, true)))
          .describedAs("Marker is present for path created by azcopy")
          .isTrue();

      // Asserting that the mkdir didn't create markers for existing directory.
      Assertions.assertThat(DirectoryStateHelper.isImplicitDirectory(path,
              fs, getTestTracingContext(fs, true)))
          .describedAs("Marker is present for existing directory")
          .isTrue();
    }
  }

  /**
   * Creation of directory with parent directory existing as implicit.
   * And the directory to be created existing as file
   * @throws Exception
   */
  @Test
  public void testMkdirOnExistingFileWithImplicitParentDir() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      final Path implicitPath = new Path("dir1");
      final Path path = new Path("dir1/dir2");

      createAzCopyFolder(implicitPath);

      // Creating a file on path
      fs.create(path);

      // Creating a directory on existing file inside an implicit directory
      // Asserting that the mkdir fails
      LambdaTestUtils.intercept(FileAlreadyExistsException.class, () -> {
        fs.mkdirs(path);
      });

      // Asserting that the file still exists at path.
      Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(path,
              fs, getTestTracingContext(fs, true)))
          .describedAs("File still exists at path")
          .isFalse();
    }
  }

  /**
   * 1. a/b/c as implicit.
   * 2. Create marker for b.
   * 3. Do mkdir on a/b/c/d.
   * 4. Verify all b,c,d have marker but a is implicit.
   */
  @Test
  public void testImplicitExplicitFolder() throws Exception {
    Configuration configuration = Mockito.spy(getRawConfiguration());
    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration)) {
      final Path implicitPath = new Path("a/b/c");

      createAzCopyFolder(implicitPath);

      Path path = makeQualified(new Path("a/b"));
      AbfsBlobClient blobClient = (AbfsBlobClient) fs.getAbfsStore()
          .getClient(AbfsServiceType.BLOB);
      blobClient.createPathRestOp(path.toUri().getPath(), false, true,
          false, null, null, getTestTracingContext(fs, true));

      fs.mkdirs(new Path("a/b/c/d"));

      Assertions.assertThat(
              DirectoryStateHelper.isImplicitDirectory(new Path("a"),
                  fs, getTestTracingContext(fs, true)))
          .describedAs("Directory 'a' should be implicit")
          .isTrue();
      Assertions.assertThat(
              DirectoryStateHelper.isExplicitDirectory(new Path("a/b"),
                  fs, getTestTracingContext(fs, true)))
          .describedAs("Directory 'a/b' should be explicit")
          .isTrue();
      Assertions.assertThat(
              DirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"),
                  fs, getTestTracingContext(fs, true)))
          .describedAs("Directory 'a/b/c' should be explicit")
          .isTrue();
      Assertions.assertThat(
              DirectoryStateHelper.isExplicitDirectory(new Path("a/b/c/d"),
                  fs, getTestTracingContext(fs, true)))
          .describedAs("Directory 'a/b/c/d' should be explicit")
          .isTrue();
    }
  }

  /**
   * 1. a/b/c implicit.
   * 2. Marker for a and c.
   * 3. mkdir on a/b/c/d.
   * 4. Verify a,c,d are explicit but b is implicit.
   */
  @Test
  public void testImplicitExplicitFolder1() throws Exception {
    Configuration configuration = Mockito.spy(getRawConfiguration());
    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration)) {
      final Path implicitPath = new Path("a/b/c");

      createAzCopyFolder(implicitPath);

      Path path = makeQualified(new Path("a"));
      AbfsBlobClient blobClient = (AbfsBlobClient) fs.getAbfsStore()
          .getClient(AbfsServiceType.BLOB);
      blobClient.createPathRestOp(path.toUri().getPath(), false, true, false,
          null, null, getTestTracingContext(fs, true));

      Path newPath = makeQualified(new Path("a/b/c"));
      blobClient.createPathRestOp(newPath.toUri().getPath(), false, true,
          false, null, null, getTestTracingContext(fs, true));

      fs.mkdirs(new Path("a/b/c/d"));

      Assertions.assertThat(
              DirectoryStateHelper.isImplicitDirectory(new Path("a/b"),
                  fs, getTestTracingContext(fs, true)))
          .describedAs("Directory 'a/b' should be implicit")
          .isTrue();

      // Asserting that the directory created by mkdir exists as explicit.
      Assertions.assertThat(
              DirectoryStateHelper.isExplicitDirectory(new Path("a"),
                  fs, getTestTracingContext(fs, true)))
          .describedAs("Directory 'a' should be explicit")
          .isTrue();
      Assertions.assertThat(
              DirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"),
                  fs, getTestTracingContext(fs, true)))
          .describedAs("Directory 'a/b/c' should be explicit")
          .isTrue();
      Assertions.assertThat(
              DirectoryStateHelper.isExplicitDirectory(new Path("a/b/c/d"),
                  fs, getTestTracingContext(fs, true)))
          .describedAs("Directory 'a/b/c/d' should be explicit")
          .isTrue();
    }
  }

  /**
   * Extracts the eTag for an existing file
   * @param fileName file Path in String from container root
   * @return String etag for the file
   * @throws IOException
   */
  private String extractFileEtag(String fileName) throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsClient client = fs.getAbfsClient();
    final TracingContext testTracingContext = getTestTracingContext(fs, false);
    AbfsRestOperation op;
    op = client.getPathStatus(fileName, true, testTracingContext, null);
    return AzureBlobFileSystemStore.extractEtagHeader(op.getResult());
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
  public void testCreatePathRetryIdempotency() throws Exception {
    Configuration configuration = new Configuration(getRawConfiguration());
    configuration.set(FS_AZURE_ENABLE_CLIENT_TRANSACTION_ID, "true");
    try (AzureBlobFileSystem fs = getFileSystem(configuration)) {
      assumeRecoveryThroughClientTransactionID(true);
      AbfsDfsClient abfsClient = mockIngressClientHandler(fs);
      final Path nonOverwriteFile = new Path(
          "/NonOverwriteTest_FileName_" + UUID.randomUUID());
      final List<AbfsHttpHeader> headers = new ArrayList<>();
      mockRetriedRequest(abfsClient, headers);
      AbfsRestOperation getPathRestOp = Mockito.mock(AbfsRestOperation.class);
      AbfsHttpOperation op = Mockito.mock(AbfsHttpOperation.class);
      Mockito.doAnswer(answer -> {
        String requiredHeader = null;
        for (AbfsHttpHeader httpHeader : headers) {
          if (X_MS_CLIENT_TRANSACTION_ID.equalsIgnoreCase(
              httpHeader.getName())) {
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

  /**
   * Test to verify that the client transaction ID is included in the response header
   * during the creation of a new file in Azure Blob Storage.
   *
   * This test ensures that when a new file is created, the Azure Blob FileSystem client
   * correctly includes the client transaction ID in the response header for the created file.
   * The test uses a configuration where client transaction ID is enabled and verifies
   * its presence after the file creation operation.
   *
   * @throws Exception if any error occurs during test execution
   */
  @Test
  public void testGetClientTransactionIdAfterCreate() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      assumeRecoveryThroughClientTransactionID(true);
      final String[] clientTransactionId = new String[1];
      AbfsDfsClient abfsDfsClient = mockIngressClientHandler(fs);
      mockAddClientTransactionIdToHeader(abfsDfsClient, clientTransactionId);
      final Path nonOverwriteFile = new Path(
          "/NonOverwriteTest_FileName_" + UUID.randomUUID());
      fs.create(nonOverwriteFile, false);

      final AbfsHttpOperation getPathStatusOp =
          abfsDfsClient.getPathStatus(nonOverwriteFile.toUri().getPath(), false,
              getTestTracingContext(fs, true), null).getResult();
      Assertions.assertThat(
              getPathStatusOp.getResponseHeader(X_MS_CLIENT_TRANSACTION_ID))
          .describedAs("Client transaction ID should be set during create")
          .isNotNull();
      Assertions.assertThat(
              getPathStatusOp.getResponseHeader(X_MS_CLIENT_TRANSACTION_ID))
          .describedAs("Client transaction ID should be equal to the one set in the header")
          .isEqualTo(clientTransactionId[0]);
    }
  }

  /**
   * Test to verify that the client transaction ID is included in the response header
   * after two consecutive create operations on the same file in Azure Blob Storage.
   *
   * This test ensures that even after performing two create operations (with overwrite)
   * on the same file, the Azure Blob FileSystem client includes the client transaction ID
   * in the response header for the created file. The test checks for the presence of
   * the client transaction ID in the response after the second create call.
   *
   * @throws Exception if any error occurs during test execution
   */
  @Test
  public void testClientTransactionIdAfterTwoCreateCalls() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      assumeRecoveryThroughClientTransactionID(true);
      final String[] clientTransactionId = new String[1];
      AbfsDfsClient abfsDfsClient = mockIngressClientHandler(fs);
      mockAddClientTransactionIdToHeader(abfsDfsClient, clientTransactionId);
      Path testPath = path("testfile");
      AzureBlobFileSystemStore.Permissions permissions
          = new AzureBlobFileSystemStore.Permissions(false,
          FsPermission.getDefault(), FsPermission.getUMask(fs.getConf()));
      fs.create(testPath, false);
      fs.create(testPath, true);
      final AbfsHttpOperation getPathStatusOp =
          abfsDfsClient.getPathStatus(testPath.toUri().getPath(), false,
              getTestTracingContext(fs, true), null).getResult();
      Assertions.assertThat(
              getPathStatusOp.getResponseHeader(X_MS_CLIENT_TRANSACTION_ID))
          .describedAs("Client transaction ID should be set during create")
          .isNotNull();
      Assertions.assertThat(
              getPathStatusOp.getResponseHeader(X_MS_CLIENT_TRANSACTION_ID))
          .describedAs("Client transaction ID should be equal to the one set in the header")
          .isEqualTo(clientTransactionId[0]);
    }
  }

  /**
   * Test case to simulate a failure scenario during the recovery process while
   * creating a file in Azure Blob File System. This test verifies that when the
   * `getPathStatus` method encounters a timeout exception during recovery, it
   * triggers an appropriate failure response.
   *
   * The test mocks the `AbfsDfsClient` to simulate the failure behavior, including
   * a retry logic. It also verifies that an exception is correctly thrown and the
   * error message contains the expected details for recovery failure.
   *
   * @throws Exception If an error occurs during the test setup or execution.
   */
  @Test
  public void testFailureInGetPathStatusDuringCreateRecovery() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      assumeRecoveryThroughClientTransactionID(true);
      final String[] clientTransactionId = new String[1];
      AbfsDfsClient abfsDfsClient = mockIngressClientHandler(fs);
      mockAddClientTransactionIdToHeader(abfsDfsClient, clientTransactionId);
      mockRetriedRequest(abfsDfsClient, new ArrayList<>());
      boolean[] flag = new boolean[1];
      Mockito.doAnswer(getPathStatus -> {
        if (!flag[0]) {
          flag[0] = true;
          throw new AbfsRestOperationException(HTTP_CLIENT_TIMEOUT, "", "", new Exception());
        }
        return getPathStatus.callRealMethod();
      }).when(abfsDfsClient).getPathStatus(
          Mockito.nullable(String.class), Mockito.nullable(Boolean.class),
          Mockito.nullable(TracingContext.class),
          Mockito.nullable(ContextEncryptionAdapter.class));

      final Path nonOverwriteFile = new Path(
          "/NonOverwriteTest_FileName_" + UUID.randomUUID());
      String errorMessage = intercept(AbfsDriverException.class,
          () -> fs.create(nonOverwriteFile, false)).getErrorMessage();

      Assertions.assertThat(errorMessage)
          .describedAs("getPathStatus should fail while recovering")
          .contains(ERR_CREATE_RECOVERY);
    }
  }

  /**
   * Mocks and returns an instance of {@link AbfsDfsClient} for the given AzureBlobFileSystem.
   * This method sets up the necessary mock behavior for the client handler and ingress client.
   *
   * @param fs The {@link AzureBlobFileSystem} instance for which the client handler will be mocked.
   * @return A mocked {@link AbfsDfsClient} instance associated with the provided file system.
   */
  private AbfsDfsClient mockIngressClientHandler(AzureBlobFileSystem fs) {
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClientHandler clientHandler = Mockito.spy(store.getClientHandler());
    AbfsDfsClient abfsDfsClient = (AbfsDfsClient) Mockito.spy(
        clientHandler.getClient());
    fs.getAbfsStore().setClient(abfsDfsClient);
    fs.getAbfsStore().setClientHandler(clientHandler);
    Mockito.doReturn(abfsDfsClient).when(clientHandler).getIngressClient();
    return abfsDfsClient;
  }

  /**
   * Mocks the retry behavior for an AbfsDfsClient request. The method intercepts
   * the Abfs operation and simulates an HTTP conflict (HTTP 409) error on the
   * first invocation. It creates a mock HTTP operation with a PUT method and
   * specific status codes and error messages.
   *
   * @param abfsDfsClient The AbfsDfsClient to mock operations for.
   * @param headers The list of HTTP headers to which request headers will be added.
   *
   * @throws Exception If an error occurs during mock creation or operation execution.
   */
  private void mockRetriedRequest(AbfsDfsClient abfsDfsClient,
      final List<AbfsHttpHeader> headers) throws Exception {
    TestAbfsClient.mockAbfsOperationCreation(abfsDfsClient,
        new MockIntercept<AbfsRestOperation>() {
          private int count = 0;

          @Override
          public void answer(final AbfsRestOperation mockedObj,
              final InvocationOnMock answer)
              throws AbfsRestOperationException {
            if (count == 0) {
              count = 1;
              AbfsHttpOperation op = Mockito.mock(AbfsHttpOperation.class);
              Mockito.doReturn(HTTP_METHOD_PUT).when(op).getMethod();
              Mockito.doReturn(EMPTY_STRING).when(op).getStorageErrorMessage();
              Mockito.doReturn(true).when(mockedObj).hasResult();
              Mockito.doReturn(op).when(mockedObj).getResult();
              Mockito.doReturn(HTTP_CONFLICT).when(op).getStatusCode();
              headers.addAll(mockedObj.getRequestHeaders());
              throw new AbfsRestOperationException(HTTP_CONFLICT,
                  AzureServiceErrorCode.PATH_CONFLICT.getErrorCode(), EMPTY_STRING,
                  null, op);
            }
          }
        });
  }
}
