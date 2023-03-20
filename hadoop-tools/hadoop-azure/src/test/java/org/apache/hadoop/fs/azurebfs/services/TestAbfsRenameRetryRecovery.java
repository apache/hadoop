/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketException;
import java.net.URL;
import java.time.Duration;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.EtagSource;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.commit.ResilientCommitByRename;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.PATH_ALREADY_EXISTS;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.RENAME_DESTINATION_PARENT_PATH_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Testing Abfs Rename recovery using Mockito.
 */
public class TestAbfsRenameRetryRecovery extends AbstractAbfsIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestAbfsRenameRetryRecovery.class);

  public TestAbfsRenameRetryRecovery() throws Exception {
  }

  /**
   * Mock the AbfsClient to run a metadata incomplete scenario with recovery
   * rename.
   */
  @Test
  public void testRenameFailuresDueToIncompleteMetadata() throws Exception {
    String sourcePath = getMethodName() + "Source";
    String destNoParentPath = "/NoParent/Dest";
    AzureBlobFileSystem fs = getFileSystem();

    AbfsClient mockClient = TestAbfsClient.getMockAbfsClient(
        fs.getAbfsStore().getClient(),
        fs.getAbfsStore().getAbfsConfiguration());

    AbfsCounters abfsCounters = mock(AbfsCounters.class);
    when(mockClient.getAbfsCounters()).thenReturn(abfsCounters);
    // SuccessFul Result.
    AbfsRestOperation successOp =
        new AbfsRestOperation(AbfsRestOperationType.RenamePath, mockClient,
            HTTP_METHOD_PUT, null, null);
    AbfsClientRenameResult successResult = mock(AbfsClientRenameResult.class);
    doReturn(successOp).when(successResult).getOp();
    when(successResult.isIncompleteMetadataState()).thenReturn(false);

    // Failed Result.
    AbfsRestOperation failedOp = new AbfsRestOperation(AbfsRestOperationType.RenamePath, mockClient,
        HTTP_METHOD_PUT, null, null);
    AbfsClientRenameResult recoveredMetaDataIncompleteResult =
        mock(AbfsClientRenameResult.class);

    doReturn(failedOp).when(recoveredMetaDataIncompleteResult).getOp();
    when(recoveredMetaDataIncompleteResult.isIncompleteMetadataState()).thenReturn(true);

    // No destination Parent dir exception.
    AzureBlobFileSystemException destParentNotFound
        = getMockAbfsRestOperationException(
        RENAME_DESTINATION_PARENT_PATH_NOT_FOUND.getStatusCode(),
        RENAME_DESTINATION_PARENT_PATH_NOT_FOUND.getErrorCode());

    // We need to throw an exception once a rename is triggered with
    // destination having no parent, but after a retry it needs to succeed.
    when(mockClient.renamePath(sourcePath, destNoParentPath, null, null,
        null, false))
        .thenThrow(destParentNotFound)
        .thenReturn(recoveredMetaDataIncompleteResult);

    // Dest parent not found exc. to be raised.
    intercept(AzureBlobFileSystemException.class,
        () -> mockClient.renamePath(sourcePath,
        destNoParentPath, null, null,
        null, false));

    AbfsClientRenameResult resultOfSecondRenameCall =
        mockClient.renamePath(sourcePath,
        destNoParentPath, null, null,
        null, false);

    // the second rename call should be the recoveredResult due to
    // metaDataIncomplete
    Assertions.assertThat(resultOfSecondRenameCall)
        .describedAs("This result should be recovered result due to MetaData "
            + "being in incomplete state")
        .isSameAs(recoveredMetaDataIncompleteResult);
    // Verify Incomplete metadata state happened for our second rename call.
    assertTrue("Metadata incomplete state should be true if a rename is "
            + "retried after no Parent directory is found",
        resultOfSecondRenameCall.isIncompleteMetadataState());


    // Verify renamePath occurred two times implying a retry was attempted.
    verify(mockClient, times(2))
        .renamePath(sourcePath, destNoParentPath, null, null, null, false);

  }

  AbfsClient getMockAbfsClient() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();

    // adding mock objects to current AbfsClient
    AbfsClient spyClient = Mockito.spy(fs.getAbfsStore().getClient());

    Mockito.doAnswer(answer -> {
      AbfsRestOperation op = new AbfsRestOperation(AbfsRestOperationType.RenamePath,
          spyClient, HTTP_METHOD_PUT, answer.getArgument(0), answer.getArgument(1));
      AbfsRestOperation spiedOp = Mockito.spy(op);
      addSpyBehavior(spiedOp, op, spyClient);
      return spiedOp;
    }).when(spyClient).createRenameRestOperation(nullable(URL.class), nullable(List.class));

    return spyClient;

  }

  /**
   * Spies on a rest operation to inject transient failure.
   * the first createHttpOperation() invocation will return an abfs rest operation
   * which will fail.
   * @param spiedRestOp spied operation whose createHttpOperation() will fail first time
   * @param normalRestOp normal operation the good operation
   * @param client client.
   * @throws IOException failure
   */
  private void addSpyBehavior(final AbfsRestOperation spiedRestOp,
      final AbfsRestOperation normalRestOp,
      final AbfsClient client)
      throws IOException {
    AbfsHttpOperation failingOperation = Mockito.spy(normalRestOp.createHttpOperation());
    AbfsHttpOperation normalOp1 = normalRestOp.createHttpOperation();
    executeThenFail(client, normalRestOp, failingOperation, normalOp1);
    AbfsHttpOperation normalOp2 = normalRestOp.createHttpOperation();
    normalOp2.getConnection().setRequestProperty(HttpHeaderConfigurations.AUTHORIZATION,
        client.getAccessToken());

    when(spiedRestOp.createHttpOperation())
        .thenReturn(failingOperation)
        .thenReturn(normalOp2);
  }

  /**
   * Mock an idempotency failure by executing the normal operation, then
   * raising an IOE.
   * @param normalRestOp the rest operation used to sign the requests.
   * @param failingOperation failing operation
   * @param normalOp good operation
   * @throws IOException failure
   */
  private void executeThenFail(final AbfsClient client,
      final AbfsRestOperation normalRestOp,
      final AbfsHttpOperation failingOperation,
      final AbfsHttpOperation normalOp)
      throws IOException {
    Mockito.doAnswer(answer -> {
      LOG.info("Executing first attempt with post-operation fault injection");
      final byte[] buffer = answer.getArgument(0);
      final int offset = answer.getArgument(1);
      final int length = answer.getArgument(2);
      normalRestOp.signRequest(normalOp, length);
      normalOp.sendRequest(buffer, offset, length);
      normalOp.processResponse(buffer, offset, length);
      LOG.info("Actual outcome is {} \"{}\" \"{}\"; injecting failure",
          normalOp.getStatusCode(),
          normalOp.getStorageErrorCode(),
          normalOp.getStorageErrorMessage());
      throw new SocketException("connection-reset");
    }).when(failingOperation).sendRequest(Mockito.nullable(byte[].class),
        Mockito.nullable(int.class), Mockito.nullable(int.class));
  }

  /**
   * This is the good outcome: resilient rename.
   */
  @Test
  public void testRenameRecoverySrcDestEtagSame() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    TracingContext testTracingContext = getTestTracingContext(fs, false);

    Assume.assumeTrue(fs.getAbfsStore().getIsNamespaceEnabled(testTracingContext));

    AbfsClient mockClient = getMockAbfsClient();

    String base = "/" + getMethodName();
    String path1 = base + "/dummyFile1";
    String path2 = base + "/dummyFile2";

    touch(new Path(path1));

    // 404 and retry, send sourceEtag as null
    // source eTag matches -> rename should pass even when execute throws exception
    final AbfsClientRenameResult result =
        mockClient.renamePath(path1, path2, null, testTracingContext, null, false);
    Assertions.assertThat(result.isRenameRecovered())
        .describedAs("rename result recovered flag of %s", result)
        .isTrue();
  }

  /**
   * execute a failing rename but have the file at the far end not match.
   * This is done by explicitly passing in a made up etag for the source
   * etag and creating a file at the far end.
   * The first rename will actually fail with a path exists exception,
   * but as that is swallowed, it's not a problem.
   */
  @Test
  public void testRenameRecoverySourceDestEtagDifferent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    TracingContext testTracingContext = getTestTracingContext(fs, false);

    Assume.assumeTrue(fs.getAbfsStore().getIsNamespaceEnabled(testTracingContext));

    AbfsClient spyClient = getMockAbfsClient();

    String base = "/" + getMethodName();
    String path1 = base + "/dummyFile1";
    String path2 = base + "/dummyFile2";

    touch(new Path(path2));

    // source eTag does not match -> throw exception
    expectErrorCode(SOURCE_PATH_NOT_FOUND, intercept(AbfsRestOperationException.class, () ->
            spyClient.renamePath(path1, path2, null, testTracingContext, "source", false))
    );
  }

  /**
   * Assert that an exception failed with a specific error code.
   * @param code code
   * @param e exception
   * @throws AbfsRestOperationException if there is a mismatch
   */
  private static void expectErrorCode(final AzureServiceErrorCode code,
      final AbfsRestOperationException e) throws AbfsRestOperationException {
    if (e.getErrorCode() != code) {
      throw e;
    }
  }

  /**
   * Directory rename failure is unrecoverable.
   */
  @Test
  public void testDirRenameRecoveryUnsupported() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    TracingContext testTracingContext = getTestTracingContext(fs, false);

    Assume.assumeTrue(fs.getAbfsStore().getIsNamespaceEnabled(testTracingContext));

    AbfsClient spyClient = getMockAbfsClient();

    String base = "/" + getMethodName();
    String path1 = base + "/dummyDir1";
    String path2 = base + "/dummyDir2";

    fs.mkdirs(new Path(path1));

    // source eTag does not match -> throw exception
    expectErrorCode(SOURCE_PATH_NOT_FOUND, intercept(AbfsRestOperationException.class, () ->
            spyClient.renamePath(path1, path2, null, testTracingContext, null, false)));
  }

  /**
   * Even with failures, having
   */
  @Test
  public void testExistingPathCorrectlyRejected() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    TracingContext testTracingContext = getTestTracingContext(fs, false);

    Assume.assumeTrue(fs.getAbfsStore().getIsNamespaceEnabled(testTracingContext));

    AbfsClient spyClient = getMockAbfsClient();

    String base = "/" + getMethodName();
    String path1 = base + "/dummyDir1";
    String path2 = base + "/dummyDir2";


    touch(new Path(path1));
    touch(new Path(path2));

    // source eTag does not match -> throw exception
    expectErrorCode(PATH_ALREADY_EXISTS, intercept(AbfsRestOperationException.class, () ->
            spyClient.renamePath(path1, path2, null, testTracingContext, null, false)));
  }

  /**
   * Test the resilient commit code works through fault injection, including
   * reporting recovery.
   */
  @Test
  public void testResilientCommitOperation() throws Throwable {
    AzureBlobFileSystem fs = getFileSystem();
    TracingContext testTracingContext = getTestTracingContext(fs, false);

    final AzureBlobFileSystemStore store = fs.getAbfsStore();
    Assume.assumeTrue(store.getIsNamespaceEnabled(testTracingContext));

    // patch in the mock abfs client to the filesystem, for the resilient
    // commit API to pick up.
    setAbfsClient(store, getMockAbfsClient());

    String base = "/" + getMethodName();
    String path1 = base + "/dummyDir1";
    String path2 = base + "/dummyDir2";


    final Path source = new Path(path1);
    touch(source);
    final String sourceTag = ((EtagSource) fs.getFileStatus(source)).getEtag();

    final ResilientCommitByRename commit = fs.createResilientCommitSupport(source);
    final Pair<Boolean, Duration> outcome =
        commit.commitSingleFileByRename(source, new Path(path2), sourceTag);
    Assertions.assertThat(outcome.getKey())
        .describedAs("recovery flag")
        .isTrue();
  }
  /**
   * Test the resilient commit code works through fault injection, including
   * reporting recovery.
   */
  @Test
  public void testResilientCommitOperationTagMismatch() throws Throwable {
    AzureBlobFileSystem fs = getFileSystem();
    TracingContext testTracingContext = getTestTracingContext(fs, false);

    final AzureBlobFileSystemStore store = fs.getAbfsStore();
    Assume.assumeTrue(store.getIsNamespaceEnabled(testTracingContext));

    // patch in the mock abfs client to the filesystem, for the resilient
    // commit API to pick up.
    setAbfsClient(store, getMockAbfsClient());

    String base = "/" + getMethodName();
    String path1 = base + "/dummyDir1";
    String path2 = base + "/dummyDir2";


    final Path source = new Path(path1);
    touch(source);
    final String sourceTag = ((EtagSource) fs.getFileStatus(source)).getEtag();

    final ResilientCommitByRename commit = fs.createResilientCommitSupport(source);
    intercept(FileNotFoundException.class, () ->
        commit.commitSingleFileByRename(source, new Path(path2), "not the right tag"));
  }

  /**
   * Method to create an AbfsRestOperationException.
   * @param statusCode status code to be used.
   * @param errorCode error code to be used.
   * @return the exception.
   */
  private AbfsRestOperationException getMockAbfsRestOperationException(
      int statusCode, String errorCode) {
    return new AbfsRestOperationException(statusCode, errorCode,
        "No Parent found for the Destination file",
        new Exception());
  }

}
