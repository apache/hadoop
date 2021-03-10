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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.TestAbfsClient;
import org.apache.hadoop.fs.azurebfs.services.TestAbfsPerfTracker;
import org.apache.hadoop.fs.azurebfs.utils.TestMockHelpers;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_DELETE_CONSIDERED_IDEMPOTENT;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.DeletePath;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertDeleted;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;


/**
 * Test delete operation.
 */
public class ITestAzureBlobFileSystemDelete extends
    AbstractAbfsIntegrationTest {

  private static final int REDUCED_RETRY_COUNT = 1;
  private static final int REDUCED_MAX_BACKOFF_INTERVALS_MS = 5000;

  public ITestAzureBlobFileSystemDelete() throws Exception {
    super();
  }

  @Test
  public void testDeleteRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    fs.mkdirs(new Path("/testFolder0"));
    fs.mkdirs(new Path("/testFolder1"));
    fs.mkdirs(new Path("/testFolder2"));
    touch(new Path("/testFolder1/testfile"));
    touch(new Path("/testFolder1/testfile2"));
    touch(new Path("/testFolder1/testfile3"));

    Path root = new Path("/");
    FileStatus[] ls = fs.listStatus(root);
    assertEquals(3, ls.length);

    fs.delete(root, true);
    ls = fs.listStatus(root);
    assertEquals("listing size", 0, ls.length);
  }

  @Test()
  public void testOpenFileAfterDelete() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testfile = new Path("/testFile");
    touch(testfile);
    assertDeleted(fs, testfile, false);

    intercept(FileNotFoundException.class,
        () -> fs.open(testfile));
  }

  @Test
  public void testEnsureFileIsDeleted() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testfile = new Path("testfile");
    touch(testfile);
    assertDeleted(fs, testfile, false);
    assertPathDoesNotExist(fs, "deleted", testfile);
  }

  @Test
  public void testDeleteDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path dir = new Path("testfile");
    fs.mkdirs(dir);
    fs.mkdirs(new Path("testfile/test1"));
    fs.mkdirs(new Path("testfile/test1/test2"));

    assertDeleted(fs, dir, true);
    assertPathDoesNotExist(fs, "deleted", dir);
  }

  @Test
  public void testDeleteFirstLevelDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final List<Future<Void>> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 1000; i++) {
      final Path fileName = new Path("/test/" + i);
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
    Path dir = new Path("/test");
    // first try a non-recursive delete, expect failure
    intercept(FileAlreadyExistsException.class,
        () -> fs.delete(dir, false));
    assertDeleted(fs, dir, true);
    assertPathDoesNotExist(fs, "deleted", dir);

  }

  @Test
  public void testDeleteIdempotency() throws Exception {
    Assume.assumeTrue(DEFAULT_DELETE_CONSIDERED_IDEMPOTENT);
    // Config to reduce the retry and maxBackoff time for test run
    AbfsConfiguration abfsConfig
        = TestAbfsConfigurationFieldsValidation.updateRetryConfigs(
        getConfiguration(),
        REDUCED_RETRY_COUNT, REDUCED_MAX_BACKOFF_INTERVALS_MS);

    final AzureBlobFileSystem fs = getFileSystem();
    AbfsClient abfsClient = fs.getAbfsStore().getClient();
    AbfsClient testClient = TestAbfsClient.createTestClientFromCurrentContext(
        abfsClient,
        abfsConfig);

    // Mock instance of AbfsRestOperation
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    // Set retryCount to non-zero
    when(op.isARetriedRequest()).thenReturn(true);

    // Case 1: Mock instance of Http Operation response. This will return
    // HTTP:Not Found
    AbfsHttpOperation http404Op = mock(AbfsHttpOperation.class);
    when(http404Op.getStatusCode()).thenReturn(HTTP_NOT_FOUND);

    // Mock delete response to 404
    when(op.getResult()).thenReturn(http404Op);

    Assertions.assertThat(testClient.deleteIdempotencyCheckOp(op)
        .getResult()
        .getStatusCode())
        .describedAs(
            "Delete is considered idempotent by default and should return success.")
        .isEqualTo(HTTP_OK);

    // Case 2: Mock instance of Http Operation response. This will return
    // HTTP:Bad Request
    AbfsHttpOperation http400Op = mock(AbfsHttpOperation.class);
    when(http400Op.getStatusCode()).thenReturn(HTTP_BAD_REQUEST);

    // Mock delete response to 400
    when(op.getResult()).thenReturn(http400Op);

    Assertions.assertThat(testClient.deleteIdempotencyCheckOp(op)
        .getResult()
        .getStatusCode())
        .describedAs(
            "Idempotency check to happen only for HTTP 404 response.")
        .isEqualTo(HTTP_BAD_REQUEST);

  }

  @Test
  public void testDeleteIdempotencyTriggerHttp404() throws Exception {

    final AzureBlobFileSystem fs = getFileSystem();
    AbfsClient client = TestAbfsClient.createTestClientFromCurrentContext(
        fs.getAbfsStore().getClient(),
        this.getConfiguration());

    // Case 1: Not a retried case should throw error back
    // Add asserts at AzureBlobFileSystemStore and AbfsClient levels
    intercept(AbfsRestOperationException.class,
        () -> fs.getAbfsStore().delete(
            new Path("/NonExistingPath"),
            false));

    intercept(AbfsRestOperationException.class,
        () -> client.deletePath(
        "/NonExistingPath",
        false,
        null));

    // mock idempotency check to mimic retried case
    AbfsClient mockClient = TestAbfsClient.getMockAbfsClient(
        fs.getAbfsStore().getClient(),
        this.getConfiguration());
    AzureBlobFileSystemStore mockStore = mock(AzureBlobFileSystemStore.class);
    mockStore = TestMockHelpers.setClassField(AzureBlobFileSystemStore.class, mockStore,
        "client", mockClient);
    mockStore = TestMockHelpers.setClassField(AzureBlobFileSystemStore.class,
        mockStore,
        "abfsPerfTracker",
        TestAbfsPerfTracker.getAPerfTrackerInstance(this.getConfiguration()));
    doCallRealMethod().when(mockStore).delete(new Path("/NonExistingPath"), false);

    // Case 2: Mimic retried case
    // Idempotency check on Delete always returns success
    AbfsRestOperation idempotencyRetOp = TestAbfsClient.getRestOp(
        DeletePath, mockClient, HTTP_METHOD_DELETE,
        TestAbfsClient.getTestUrl(mockClient, "/NonExistingPath"),
        TestAbfsClient.getTestRequestHeaders(mockClient));
    idempotencyRetOp.hardSetResult(HTTP_OK);

    doReturn(idempotencyRetOp).when(mockClient).deleteIdempotencyCheckOp(any());
    when(mockClient.deletePath("/NonExistingPath", false,
        null)).thenCallRealMethod();

    Assertions.assertThat(mockClient.deletePath(
        "/NonExistingPath",
        false,
        null)
        .getResult()
        .getStatusCode())
        .describedAs("Idempotency check reports successful "
            + "delete. 200OK should be returned")
        .isEqualTo(idempotencyRetOp.getResult().getStatusCode());

    // Call from AzureBlobFileSystemStore should not fail either
    mockStore.delete(new Path("/NonExistingPath"), false);
  }

}
