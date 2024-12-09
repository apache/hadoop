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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticAssertions;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.TestAbfsClient;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.RENAME_PATH_ATTEMPTS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DIRECTORY;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_TRANSACTION_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_RESOURCE_TYPE;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertRenameOutcome;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.mockito.ArgumentMatchers.nullable;

/**
 * Test rename operation.
 */
public class ITestAzureBlobFileSystemRename extends
    AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemRename() throws Exception {
    super();
  }

  @Test
  public void testEnsureFileIsRenamed() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path src = path("testEnsureFileIsRenamed-src");
    touch(src);
    Path dest = path("testEnsureFileIsRenamed-dest");
    fs.delete(dest, true);
    assertRenameOutcome(fs, src, dest, true);

    assertIsFile(fs, dest);
    assertPathDoesNotExist(fs, "expected renamed", src);
  }

  @Test
  public void testRenameWithPreExistingDestination() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path src = path("renameSrc");
    touch(src);
    Path dest = path("renameDest");
    touch(dest);
    assertRenameOutcome(fs, src, dest, false);
  }

  @Test
  public void testRenameFileUnderDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path sourceDir = path("/testSrc");
    assertMkdirs(fs, sourceDir);
    String filename = "file1";
    Path file1 = new Path(sourceDir, filename);
    touch(file1);

    Path destDir = path("/testDst");
    assertRenameOutcome(fs, sourceDir, destDir, true);
    FileStatus[] fileStatus = fs.listStatus(destDir);
    assertNotNull("Null file status", fileStatus);
    FileStatus status = fileStatus[0];
    assertEquals("Wrong filename in " + status,
        filename, status.getPath().getName());
  }

  @Test
  public void testRenameDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testDir = path("testDir");
    fs.mkdirs(testDir);
    Path test1 = new Path(testDir + "/test1");
    fs.mkdirs(test1);
    fs.mkdirs(new Path(testDir + "/test1/test2"));
    fs.mkdirs(new Path(testDir + "/test1/test2/test3"));

    assertRenameOutcome(fs, test1,
        new Path(testDir + "/test10"), true);
    assertPathDoesNotExist(fs, "rename source dir", test1);
  }

  @Test
  public void testRenameFirstLevelDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final List<Future<Void>> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    Path source = path("/test");
    for (int i = 0; i < 1000; i++) {
      final Path fileName = new Path(source + "/" + i);
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
    Path dest = path("/renamedDir");
    assertRenameOutcome(fs, source, dest, true);

    FileStatus[] files = fs.listStatus(dest);
    assertEquals("Wrong number of files in listing", 1000, files.length);
    assertPathDoesNotExist(fs, "rename source dir", source);
  }

  @Test
  public void testRenameRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assertRenameOutcome(fs,
        new Path("/"),
        new Path("/testRenameRoot"),
        false);
    assertRenameOutcome(fs,
        new Path(fs.getUri().toString() + "/"),
        new Path(fs.getUri().toString() + "/s"),
        false);
  }

  @Test
  public void testPosixRenameDirectory() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path testDir2 = path("testDir2");
    fs.mkdirs(new Path(testDir2 + "/test1/test2/test3"));
    fs.mkdirs(new Path(testDir2 + "/test4"));
    Assert.assertTrue(fs.rename(new Path(testDir2 + "/test1/test2/test3"), new Path(testDir2 + "/test4")));
    assertPathExists(fs, "This path should exist", testDir2);
    assertPathExists(fs, "This path should exist",
        new Path(testDir2 + "/test1/test2"));
    assertPathExists(fs, "This path should exist",
        new Path(testDir2 + "/test4"));
    assertPathExists(fs, "This path should exist",
        new Path(testDir2 + "/test4/test3"));
    assertPathDoesNotExist(fs, "This path should not exist",
        new Path(testDir2 + "/test1/test2/test3"));
  }

  @Test
  public void testRenameWithNoDestinationParentDir() throws Exception {
    describe("Verifying the expected behaviour of ABFS rename when "
        + "destination parent Dir doesn't exist.");

    final AzureBlobFileSystem fs = getFileSystem();
    Path sourcePath = path(getMethodName());
    Path destPath = new Path("falseParent", "someChildFile");

    byte[] data = dataset(1024, 'a', 'z');
    writeDataset(fs, sourcePath, data, data.length, 1024, true);

    // Verify that renaming on a destination with no parent dir wasn't
    // successful.
    assertFalse("Rename result expected to be false with no Parent dir",
        fs.rename(sourcePath, destPath));

    // Verify that metadata was in an incomplete state after the rename
    // failure, and we retired the rename once more.
    IOStatistics ioStatistics = fs.getIOStatistics();
    IOStatisticAssertions.assertThatStatisticCounter(ioStatistics,
        RENAME_PATH_ATTEMPTS.getStatName())
        .describedAs("There should be 2 rename attempts if metadata "
            + "incomplete state failure is hit")
        .isEqualTo(2);
  }

  @Test
  public void renamePathRetryIdempotency() throws Exception {
    final AzureBlobFileSystem currentFs = getFileSystem();
    Configuration config = new Configuration(this.getRawConfiguration());
    final AzureBlobFileSystem fs =
            (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
                    config);
    AbfsClient abfsClient = Mockito.spy(fs.getAbfsClient());
    fs.getAbfsStore().setClient(abfsClient);
    Path sourceDir = path("/testSrc");
    assertMkdirs(fs, sourceDir);
    String filename = "file1";
    Path sourceFilePath = new Path(sourceDir, filename);
    touch(sourceFilePath);
    Path destFilePath = new Path(sourceDir, "file2");
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
                  Mockito.doReturn(SOURCE_PATH_NOT_FOUND.getErrorCode()).when(op)
                          .getStorageErrorCode();
                  Mockito.doReturn(true).when(mockedObj).hasResult();
                  Mockito.doReturn(op).when(mockedObj).getResult();
                  Mockito.doReturn(HTTP_NOT_FOUND).when(op).getStatusCode();
                  headers.addAll(mockedObj.getRequestHeaders());
                  throw new AbfsRestOperationException(HTTP_NOT_FOUND, SOURCE_PATH_NOT_FOUND.getErrorCode(),
                          "", null, op);
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
    Mockito.doReturn(DIRECTORY).when(op).getResponseHeader(X_MS_RESOURCE_TYPE);
    Mockito.doReturn(getPathRestOp).when(abfsClient).getPathStatus(
            nullable(String.class), nullable(Boolean.class),
            nullable(TracingContext.class), nullable(ContextEncryptionAdapter.class));
    fs.rename(sourceFilePath, destFilePath);
  }
}
