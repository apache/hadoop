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
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Stubber;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientHandler;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType;
import org.apache.hadoop.fs.azurebfs.services.ListResponseData;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil;
import org.apache.hadoop.fs.azurebfs.utils.DirectoryStateHelper;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_LIST_MAX_RESULTS;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_METADATA_PREFIX;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_BLOB_LIST_PARSING;
import static org.apache.hadoop.fs.azurebfs.services.RenameAtomicity.SUFFIX;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_RESET_MESSAGE;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_JDK_MESSAGE;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.fs.contract.ContractTestUtils.rename;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Test listStatus operation.
 */
public class ITestAzureBlobFileSystemListStatus extends
    AbstractAbfsIntegrationTest {
  private static final int TEST_FILES_NUMBER = 6000;
  public static final String TEST_CONTINUATION_TOKEN = "continuation";

  public ITestAzureBlobFileSystemListStatus() throws Exception {
    super();
  }

  @Test
  public void testListPath() throws Exception {
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set(AZURE_LIST_MAX_RESULTS, "5000");
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem
        .newInstance(getFileSystem().getUri(), config);
      final List<Future<Void>> tasks = new ArrayList<>();

      ExecutorService es = Executors.newFixedThreadPool(10);
      for (int i = 0; i < TEST_FILES_NUMBER; i++) {
        final Path fileName = new Path("/test" + i);
        Callable<Void> callable = new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            touch(fileName);
            return null;
          }
        };

        tasks.add(es.submit(callable));
      }

      for (Future<Void> task : tasks) {
        task.get();
      }

      es.shutdownNow();
      fs.registerListener(
              new TracingHeaderValidator(getConfiguration().getClientCorrelationId(),
                      fs.getFileSystemId(), FSOperationType.LISTSTATUS, true, 0));
      FileStatus[] files = fs.listStatus(new Path("/"));
      assertEquals(TEST_FILES_NUMBER, files.length /* user directory */);
    fs.registerListener(
            new TracingHeaderValidator(getConfiguration().getClientCorrelationId(),
                    fs.getFileSystemId(), FSOperationType.GET_ATTR, true, 0));
    fs.close();
  }

  /**
   * Test to verify that each paginated call to ListBlobs uses a new tracing context.
   * Test also verifies that the retry policy is called when a SocketTimeoutException
   * Test also verifies that empty list with valid continuation token is handled.
   * @throws Exception if there is an error or test assertions fails.
   */
  @Test
  public void testListPathTracingContext() throws Exception {
    final AzureBlobFileSystem spiedFs = Mockito.spy(getFileSystem());
    final AzureBlobFileSystemStore spiedStore = Mockito.spy(spiedFs.getAbfsStore());
    final AbfsClient spiedClient = Mockito.spy(spiedFs.getAbfsClient());
    final TracingContext spiedTracingContext = Mockito.spy(
        new TracingContext(
            spiedFs.getClientCorrelationId(), spiedFs.getFileSystemId(),
            FSOperationType.LISTSTATUS, true, TracingHeaderFormat.ALL_ID_FORMAT, null));

    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
    Mockito.doReturn(spiedClient).when(spiedStore).getClient();
    spiedFs.setWorkingDirectory(new Path("/"));

    AbfsClientTestUtil.setMockAbfsRestOperationForListOperation(spiedClient,
        (httpOperation) -> {
          Stubber stubber = Mockito.doThrow(
              new SocketTimeoutException(CONNECTION_TIMEOUT_JDK_MESSAGE));
          stubber.doNothing().when(httpOperation).processResponse(
              nullable(byte[].class), nullable(int.class), nullable(int.class));

          when(httpOperation.getStatusCode()).thenReturn(-1).thenReturn(HTTP_OK);
          return httpOperation;
        });

    List<FileStatus> fileStatuses = new ArrayList<>();
    spiedStore.listStatus(new Path("/"), "", fileStatuses, true, null, spiedTracingContext);

    // Assert that there were retries due to SocketTimeoutException
    Mockito.verify(spiedClient, Mockito.times(1))
        .getRetryPolicy(CONNECTION_TIMEOUT_ABBREVIATION);

    // Assert that there were 2 paginated ListPath calls were made 1 and 2.
    // 1. Without continuation token
    Mockito.verify(spiedClient, times(1)).listPath(
        "/", false,
        spiedFs.getAbfsStore().getAbfsConfiguration().getListMaxResults(),
        null, spiedTracingContext, spiedFs.getAbfsStore().getUri());
    // 2. With continuation token
    Mockito.verify(spiedClient, times(1)).listPath(
        "/", false,
        spiedFs.getAbfsStore().getAbfsConfiguration().getListMaxResults(),
        TEST_CONTINUATION_TOKEN, spiedTracingContext, spiedFs.getAbfsStore().getUri());

    // Assert that none of the API calls used the same tracing header.
    Mockito.verify(spiedTracingContext, times(0)).constructHeader(any(), any(), any());
  }

  @Test
  public void testListPathParsingFailure() throws Exception {
    assumeBlobServiceType();
    AzureBlobFileSystem spiedFs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore spiedStore = Mockito.spy(spiedFs.getAbfsStore());
    AbfsBlobClient spiedClient = Mockito.spy(spiedStore.getClientHandler()
        .getBlobClient());
    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
    Mockito.doReturn(spiedClient).when(spiedStore).getClient();

    Mockito.doThrow(new SocketException(CONNECTION_RESET_MESSAGE)).when(spiedClient).filterDuplicateEntriesAndRenamePendingFiles(any(), any());
    List<FileStatus> fileStatuses = new ArrayList<>();
    AbfsDriverException ex = intercept(AbfsDriverException.class,
      () -> {
        spiedStore.listStatus(new Path("/"), "", fileStatuses,
            true, null, getTestTracingContext(spiedFs, true));
      });
    Assertions.assertThat(ex.getStatusCode())
        .describedAs("Expecting Network Error status code")
        .isEqualTo(-1);
    Assertions.assertThat(ex.getErrorMessage())
        .describedAs("Expecting COPY_ABORTED error code")
        .contains(ERR_BLOB_LIST_PARSING);
  }

  /**
   * Creates a file, verifies that listStatus returns it,
   * even while the file is still open for writing.
   */
  @Test
  public void testListFileVsListDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = path("/testFile");
    try(FSDataOutputStream ignored = fs.create(path)) {
      FileStatus[] testFiles = fs.listStatus(path);
      assertEquals("length of test files", 1, testFiles.length);
      FileStatus status = testFiles[0];
      assertIsFileReference(status);
    }
  }

  @Test
  public void testListFileVsListDir2() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFolder = path("/testFolder");
    fs.mkdirs(testFolder);
    fs.mkdirs(new Path(testFolder + "/testFolder2"));
    fs.mkdirs(new Path(testFolder + "/testFolder2/testFolder3"));
    Path testFile0Path = new Path(
        testFolder + "/testFolder2/testFolder3/testFile");
    ContractTestUtils.touch(fs, testFile0Path);

    FileStatus[] testFiles = fs.listStatus(testFile0Path);
    assertEquals("Wrong listing size of file " + testFile0Path,
        1, testFiles.length);
    FileStatus file0 = testFiles[0];
    assertEquals("Wrong path for " + file0, new Path(getTestUrl(),
        testFolder + "/testFolder2/testFolder3/testFile"), file0.getPath());
    assertIsFileReference(file0);
  }

  @Test(expected = FileNotFoundException.class)
  public void testListNonExistentDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.listStatus(new Path("/testFile/"));
  }

  @Test
  public void testListFiles() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testDir = path("/test");
    fs.mkdirs(testDir);

    FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
    assertEquals(1, fileStatuses.length);

    fs.mkdirs(new Path(testDir + "/sub"));
    fileStatuses = fs.listStatus(testDir);
    assertEquals(1, fileStatuses.length);
    assertEquals("sub", fileStatuses[0].getPath().getName());
    assertIsDirectoryReference(fileStatuses[0]);
    Path childF = fs.makeQualified(new Path(testDir + "/f"));
    touch(childF);
    fileStatuses = fs.listStatus(testDir);
    assertEquals(2, fileStatuses.length);
    final FileStatus childStatus = fileStatuses[0];
    assertEquals(childF, childStatus.getPath());
    assertEquals("f", childStatus.getPath().getName());
    assertIsFileReference(childStatus);
    assertEquals(0, childStatus.getLen());
    final FileStatus status1 = fileStatuses[1];
    assertEquals("sub", status1.getPath().getName());
    assertIsDirectoryReference(status1);
    // look at the child through getFileStatus
    LocatedFileStatus locatedChildStatus = fs.listFiles(childF, false).next();
    assertIsFileReference(locatedChildStatus);

    fs.delete(testDir, true);
    intercept(FileNotFoundException.class,
        () -> fs.listFiles(childF, false).next());

    // do some final checks on the status (failing due to version checks)
    assertEquals("Path mismatch of " + locatedChildStatus,
        childF, locatedChildStatus.getPath());
    assertEquals("locatedstatus.equals(status)",
        locatedChildStatus, childStatus);
    assertEquals("status.equals(locatedstatus)",
        childStatus, locatedChildStatus);
  }

  private void assertIsDirectoryReference(FileStatus status) {
    assertTrue("Not a directory: " + status, status.isDirectory());
    assertFalse("Not a directory: " + status, status.isFile());
    assertEquals(0, status.getLen());
  }

  private void assertIsFileReference(FileStatus status) {
    assertFalse("Not a file: " + status, status.isDirectory());
    assertTrue("Not a file: " + status, status.isFile());
  }

  @Test
  public void testMkdirTrailingPeriodDirName() throws IOException {
    boolean exceptionThrown = false;
    final AzureBlobFileSystem fs = getFileSystem();

    Path nontrailingPeriodDir = path("testTrailingDir/dir");
    Path trailingPeriodDir = new Path("testMkdirTrailingDir/dir.");

    assertMkdirs(fs, nontrailingPeriodDir);

    try {
      fs.mkdirs(trailingPeriodDir);
    }
    catch(IllegalArgumentException e) {
      exceptionThrown = true;
    }
    assertTrue("Attempt to create file that ended with a dot should"
        + " throw IllegalArgumentException", exceptionThrown);
  }

  @Test
  public void testCreateTrailingPeriodFileName() throws IOException {
    boolean exceptionThrown = false;
    final AzureBlobFileSystem fs = getFileSystem();

    Path trailingPeriodFile = new Path("testTrailingDir/file.");
    Path nontrailingPeriodFile = path("testCreateTrailingDir/file");

    createFile(fs, nontrailingPeriodFile, false, new byte[0]);
    assertPathExists(fs, "Trailing period file does not exist",
        nontrailingPeriodFile);

    try {
      createFile(fs, trailingPeriodFile, false, new byte[0]);
    }
    catch(IllegalArgumentException e) {
      exceptionThrown = true;
    }
    assertTrue("Attempt to create file that ended with a dot should"
        + " throw IllegalArgumentException", exceptionThrown);
  }

  @Test
  public void testRenameTrailingPeriodFile() throws IOException {
    boolean exceptionThrown = false;
    final AzureBlobFileSystem fs = getFileSystem();

    Path nonTrailingPeriodFile = path("testTrailingDir/file");
    Path trailingPeriodFile = new Path("testRenameTrailingDir/file.");

    createFile(fs, nonTrailingPeriodFile, false, new byte[0]);
    try {
    rename(fs, nonTrailingPeriodFile, trailingPeriodFile);
    }
    catch(IllegalArgumentException e) {
      exceptionThrown = true;
    }
    assertTrue("Attempt to create file that ended with a dot should"
        + " throw IllegalArgumentException", exceptionThrown);
  }



  /**
   * Test to verify that listStatus returns the correct file status all types
   * of paths viz. implicit, explicit, file.
   * @throws Exception if there is an error or test assertions fails.
   */
  @Test
  public void testListStatusWithImplicitExplicitChildren() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    Path root = new Path(ROOT_PATH);

    // Create an implicit directory under root
    Path dir = new Path("a");
    Path fileInsideDir = new Path("a/file");
    createAzCopyFolder(dir);

    // Assert that implicit directory is returned
    FileStatus[] fileStatuses = fs.listStatus(root);
    Assertions.assertThat(fileStatuses.length)
        .describedAs("List size is not expected").isEqualTo(1);
    assertImplicitDirectoryFileStatus(fileStatuses[0], fs.makeQualified(dir));

    // Create a marker blob for the directory.
    fs.create(fileInsideDir);

    // Assert that only one entry of explicit directory is returned
    fileStatuses = fs.listStatus(root);
    Assertions.assertThat(fileStatuses.length)
        .describedAs("List size is not expected").isEqualTo(1);
    assertExplicitDirectoryFileStatus(fileStatuses[0], fs.makeQualified(dir));

    // Create a file under root
    Path file1 = new Path("b");
    fs.create(file1);

    // Assert that two entries are returned in alphabetic order.
    fileStatuses = fs.listStatus(root);
    Assertions.assertThat(fileStatuses.length)
        .describedAs("List size is not expected").isEqualTo(2);
    assertExplicitDirectoryFileStatus(fileStatuses[0], fs.makeQualified(dir));
    assertFilePathFileStatus(fileStatuses[1], fs.makeQualified(file1));

    // Create another implicit directory under root.
    Path dir2 = new Path("c");
    createAzCopyFolder(dir2);

    // Assert that three entries are returned in alphabetic order.
    fileStatuses = fs.listStatus(root);
    Assertions.assertThat(fileStatuses.length)
        .describedAs("List size is not expected").isEqualTo(3);
    assertExplicitDirectoryFileStatus(fileStatuses[0], fs.makeQualified(dir));
    assertFilePathFileStatus(fileStatuses[1], fs.makeQualified(file1));
    assertImplicitDirectoryFileStatus(fileStatuses[2], fs.makeQualified(dir2));
  }

  /**
   * Test to verify that listStatus returns the correct file status when called on an implicit path
   * @throws Exception if there is an error or test assertions fails.
   */
  @Test
  public void testListStatusOnImplicitDirectoryPath() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path implicitPath = new Path("/implicitDir");
    createAzCopyFolder(implicitPath);

    FileStatus[] statuses = fs.listStatus(implicitPath);
    Assertions.assertThat(statuses.length)
        .describedAs("List size is not expected").isGreaterThanOrEqualTo(1);
    assertImplicitDirectoryFileStatus(statuses[0], fs.makeQualified(statuses[0].getPath()));

    FileStatus[] statuses1 = fs.listStatus(new Path(statuses[0].getPath().toString()));
    Assertions.assertThat(statuses1.length)
        .describedAs("List size is not expected").isGreaterThanOrEqualTo(1);
    assertFilePathFileStatus(statuses1[0], fs.makeQualified(statuses1[0].getPath()));
  }

  @Test
  public void testListStatusOnEmptyDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path emptyDir = new Path("/emptyDir");
    fs.mkdirs(emptyDir);

    FileStatus[] statuses = fs.listStatus(emptyDir);
    Assertions.assertThat(statuses.length)
        .describedAs("List size is not expected").isEqualTo(0);
  }

  @Test
  public void testListStatusOnRenamePendingJsonFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path renamePendingJsonPath = new Path("/hbase/A/A-" + SUFFIX);
    fs.create(renamePendingJsonPath);

    FileStatus[] statuses = fs.listStatus(renamePendingJsonPath);
    Assertions.assertThat(statuses.length)
        .describedAs("List size is not expected").isEqualTo(1);
    assertFilePathFileStatus(statuses[0], fs.makeQualified(statuses[0].getPath()));
  }

  @Test
  public void testContinuationTokenAcrossListStatus() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/testContinuationToken");
    fs.mkdirs(path);
    fs.create(new Path(path + "/file1"));
    fs.create(new Path(path + "/file2"));

    fs.listStatus(path);

    ListResponseData listResponseData = fs.getAbfsStore().getClient().listPath(
        "/testContinuationToken", false, 1, null, getTestTracingContext(fs, true),
        fs.getAbfsStore().getUri());

    Assertions.assertThat(listResponseData.getContinuationToken())
        .describedAs("Continuation Token Should not be null").isNotNull();
    Assertions.assertThat(listResponseData.getFileStatusList())
        .describedAs("Listing Size Not as expected").hasSize(1);

    ListResponseData listResponseData1 =  fs.getAbfsStore().getClient().listPath(
        "/testContinuationToken", false, 1, listResponseData.getContinuationToken(), getTestTracingContext(fs, true),
        fs.getAbfsStore().getUri());

    Assertions.assertThat(listResponseData1.getContinuationToken())
        .describedAs("Continuation Token Should be null").isNull();
    Assertions.assertThat(listResponseData1.getFileStatusList())
        .describedAs("Listing Size Not as expected").hasSize(1);
  }

  @Test
  public void testInvalidContinuationToken() throws Exception {
    assumeHnsDisabled();
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/testInvalidContinuationToken");
    fs.mkdirs(path);
    fs.create(new Path(path + "/file1"));
    fs.create(new Path(path + "/file2"));

    intercept(AbfsRestOperationException.class,
        () -> fs.getAbfsStore().getClient().listPath(
            "/testInvalidContinuationToken", false, 1, "invalidToken",
            getTestTracingContext(fs, true), fs.getAbfsStore().getUri()));
  }

  @Test
  public void testEmptyContinuationToken() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/testInvalidContinuationToken");
    fs.mkdirs(path);
    fs.create(new Path(path + "/file1"));
    fs.create(new Path(path + "/file2"));

    ListResponseData listResponseData = fs.getAbfsStore().getClient().listPath(
        "/testInvalidContinuationToken", false, 1, "",
        getTestTracingContext(fs, true), fs.getAbfsStore().getUri());

    Assertions.assertThat(listResponseData.getContinuationToken())
        .describedAs("Continuation Token Should Not be null").isNotNull();
    Assertions.assertThat(listResponseData.getFileStatusList())
        .describedAs("Listing Size Not as expected").hasSize(1);
  }

  private void assertFilePathFileStatus(final FileStatus fileStatus,
      final Path qualifiedPath) {
    Assertions.assertThat(fileStatus.getPath())
        .describedAs("Path Not as expected").isEqualTo(qualifiedPath);
    Assertions.assertThat(fileStatus.isFile())
        .describedAs("Expecting a File Path").isEqualTo(true);
    Assertions.assertThat(fileStatus.isDirectory())
        .describedAs("Expecting a File Path").isEqualTo(false);
    Assertions.assertThat(fileStatus.getModificationTime()).isNotEqualTo(0);
  }

  private void assertImplicitDirectoryFileStatus(final FileStatus fileStatus,
      final Path qualifiedPath) throws Exception {
    assertDirectoryFileStatus(fileStatus, qualifiedPath);
    DirectoryStateHelper.isImplicitDirectory(qualifiedPath, getFileSystem(),
        getTestTracingContext(getFileSystem(), true));
    Assertions.assertThat(fileStatus.getModificationTime())
        .describedAs("Last Modified Time Not as Expected").isEqualTo(0);
  }

  private void assertExplicitDirectoryFileStatus(final FileStatus fileStatus,
      final Path qualifiedPath) throws Exception {
    assertDirectoryFileStatus(fileStatus, qualifiedPath);
    DirectoryStateHelper.isExplicitDirectory(qualifiedPath, getFileSystem(),
        getTestTracingContext(getFileSystem(), true));
    Assertions.assertThat(fileStatus.getModificationTime())
        .describedAs("Last Modified Time Not as Expected").isNotEqualTo(0);
  }

  private void assertDirectoryFileStatus(final FileStatus fileStatus,
      final Path qualifiedPath) {
    Assertions.assertThat(fileStatus.getPath())
        .describedAs("Path Not as Expected").isEqualTo(qualifiedPath);
    Assertions.assertThat(fileStatus.isDirectory())
        .describedAs("Expecting a Directory Path").isEqualTo(true);
    Assertions.assertThat(fileStatus.isFile())
        .describedAs("Expecting a Directory Path").isEqualTo(false);
    Assertions.assertThat(fileStatus.getLen())
        .describedAs("Content Length Not as Expected").isEqualTo(0);
  }

  /**
   * Helper method to mock the AbfsRestOperation and modify the request headers.
   *
   * @param abfsBlobClient the mocked AbfsBlobClient
   * @param newHeader the header to add in place of the old one
   */
  public static void mockAbfsRestOperation(AbfsBlobClient abfsBlobClient, String... newHeader) {
    Mockito.doAnswer(invocation -> {
      List<AbfsHttpHeader> requestHeaders = invocation.getArgument(3);

      // Remove the actual HDI config header and add the new one
      requestHeaders.removeIf(header ->
          HttpHeaderConfigurations.X_MS_META_HDI_ISFOLDER.equals(header.getName()));
      for (String header : newHeader) {
        requestHeaders.add(new AbfsHttpHeader(X_MS_METADATA_PREFIX + header, TRUE));
      }

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
  public static AbfsBlobClient mockIngressClientHandler(AzureBlobFileSystem fs) {
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClientHandler clientHandler = Mockito.spy(store.getClientHandler());
    AbfsBlobClient abfsBlobClient = (AbfsBlobClient) Mockito.spy(
        clientHandler.getClient());
    fs.getAbfsStore().setClient(abfsBlobClient);
    fs.getAbfsStore().setClientHandler(clientHandler);
    Mockito.doReturn(abfsBlobClient).when(clientHandler).getIngressClient();
    return abfsBlobClient;
  }

  /**
   * Test directory status with different HDI folder configuration,
   * verifying the correct header and directory state.
   */
  private void testIsDirectory(boolean expected, String... configName) throws Exception {
    try (AzureBlobFileSystem fs = Mockito.spy(getFileSystem())) {
      assumeBlobServiceType();
      AbfsBlobClient abfsBlobClient = mockIngressClientHandler(fs);
      // Mock the operation to modify the headers
      mockAbfsRestOperation(abfsBlobClient, configName);

      // Create the path and invoke mkdirs method
      Path path = new Path("/testPath");
      fs.mkdirs(path);

      // Assert that the response header has the updated value
      FileStatus[] fileStatus = fs.listStatus(path.getParent());

      AbfsHttpOperation op = abfsBlobClient.getPathStatus(
          path.toUri().getPath(),
          true, getTestTracingContext(fs, true),
          null).getResult();

      Assertions.assertThat(abfsBlobClient.checkIsDir(op))
          .describedAs("Directory should be marked as " + expected)
          .isEqualTo(expected);

      // Verify the header and directory state
      Assertions.assertThat(fileStatus.length)
          .describedAs("Expected directory state: " + expected)
          .isEqualTo(1);

      // Verify the header and directory state
      Assertions.assertThat(fileStatus[0].isDirectory())
          .describedAs("Expected directory state: " + expected)
          .isEqualTo(expected);

      fs.delete(path, true);
    }
  }

  /**
   * Test to verify the directory status with different HDI folder configurations.
   * Verifying the correct header and directory state.
   */
  @Test
  public void testIsDirectoryWithDifferentCases() throws Exception {
    testIsDirectory(true,  "HDI_ISFOLDER");

    testIsDirectory(true, "Hdi_ISFOLDER");

    testIsDirectory(true, "Hdi_isfolder");

    testIsDirectory(true, "hdi_isfolder");

    testIsDirectory(false, "Hdi_isfolder1");

    testIsDirectory(true, "HDI_ISFOLDER", "Hdi_ISFOLDER", "Hdi_isfolder");

    testIsDirectory(true, "HDI_ISFOLDER", "Hdi_ISFOLDER1", "Test");
  }
}
