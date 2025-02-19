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
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
import org.apache.hadoop.fs.azurebfs.contracts.services.DfsListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.DfsListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_LIST_MAX_RESULTS;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_JDK_MESSAGE;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.fs.contract.ContractTestUtils.rename;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Test listStatus operation.
 */
public class ITestAzureBlobFileSystemListStatus extends
    AbstractAbfsIntegrationTest {
  private static final int TEST_FILES_NUMBER = 6000;
  private static final String TEST_CONTINUATION_TOKEN = "continuation";

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
   * @throws Exception
   */
  @Test
  public void testListPathTracingContext() throws Exception {
    assumeDfsServiceType();
    final AzureBlobFileSystem fs = getFileSystem();
    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore spiedStore = Mockito.spy(fs.getAbfsStore());
    final AbfsClient spiedClient = Mockito.spy(fs.getAbfsClient());
    final TracingContext spiedTracingContext = Mockito.spy(
        new TracingContext(
            fs.getClientCorrelationId(), fs.getFileSystemId(),
            FSOperationType.LISTSTATUS, true, TracingHeaderFormat.ALL_ID_FORMAT, null));

    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
    spiedStore.setClient(spiedClient);
    spiedFs.setWorkingDirectory(new Path("/"));

    AbfsClientTestUtil.setMockAbfsRestOperationForListPathOperation(spiedClient,
        (httpOperation) -> {

          ListResultEntrySchema entry = new DfsListResultEntrySchema()
              .withName("a")
              .withIsDirectory(true);
          List<ListResultEntrySchema> paths = new ArrayList<>();
          paths.add(entry);
          paths.clear();
          entry = new DfsListResultEntrySchema()
              .withName("abc.txt")
              .withIsDirectory(false);
          paths.add(entry);
          ListResultSchema schema1 = new DfsListResultSchema().withPaths(paths);
          ListResultSchema schema2 = new DfsListResultSchema().withPaths(paths);

          when(httpOperation.getListResultSchema()).thenReturn(schema1)
              .thenReturn(schema2);
          when(httpOperation.getResponseHeader(
              HttpHeaderConfigurations.X_MS_CONTINUATION))
              .thenReturn(TEST_CONTINUATION_TOKEN)
              .thenReturn(EMPTY_STRING);

          Stubber stubber = Mockito.doThrow(
              new SocketTimeoutException(CONNECTION_TIMEOUT_JDK_MESSAGE));
          stubber.doNothing().when(httpOperation).processResponse(
              nullable(byte[].class), nullable(int.class), nullable(int.class));

          when(httpOperation.getStatusCode()).thenReturn(-1).thenReturn(HTTP_OK);
          return httpOperation;
        });

    List<FileStatus> fileStatuses = new ArrayList<>();
    spiedStore.listStatus(new Path("/"), "", fileStatuses, true, null, spiedTracingContext);

    // Assert that there were 2 paginated ListPath calls were made 1 and 2.
    // 1. Without continuation token
    Mockito.verify(spiedClient, times(1)).listPath(
        "/", false,
        spiedFs.getAbfsStore().getAbfsConfiguration().getListMaxResults(),
        null, spiedTracingContext);
    // 2. With continuation token
    Mockito.verify(spiedClient, times(1)).listPath(
        "/", false,
        spiedFs.getAbfsStore().getAbfsConfiguration().getListMaxResults(),
        TEST_CONTINUATION_TOKEN, spiedTracingContext);

    // Assert that none of the API calls used the same tracing header.
    Mockito.verify(spiedTracingContext, times(0)).constructHeader(any(), any(), any());
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
}
