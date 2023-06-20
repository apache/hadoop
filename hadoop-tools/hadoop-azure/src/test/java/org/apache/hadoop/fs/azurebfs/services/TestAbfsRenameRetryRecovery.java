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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.EtagSource;
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
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.PATH_ALREADY_EXISTS;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.RENAME_DESTINATION_PARENT_PATH_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.RENAME_PATH_ATTEMPTS;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupCounterStatistic;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.ArgumentMatchers.anyList;
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

  private boolean isNamespaceEnabled;

  public TestAbfsRenameRetryRecovery() throws Exception {
    isNamespaceEnabled = getConfiguration()
            .getBoolean(TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
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

    AbfsClient mockClient = ITestAbfsClient.getMockAbfsClient(
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
        null, false, isNamespaceEnabled))
        .thenThrow(destParentNotFound)
        .thenReturn(recoveredMetaDataIncompleteResult);

    // Dest parent not found exc. to be raised.
    intercept(AzureBlobFileSystemException.class,
        () -> mockClient.renamePath(sourcePath,
        destNoParentPath, null, null,
        null, false, isNamespaceEnabled));

    AbfsClientRenameResult resultOfSecondRenameCall =
        mockClient.renamePath(sourcePath,
        destNoParentPath, null, null,
        null, false, isNamespaceEnabled);

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
        .renamePath(sourcePath, destNoParentPath, null, null, null, false,
                isNamespaceEnabled);

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
    }).when(spyClient).createRenameRestOperation(Mockito.any(URL.class), anyList());

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
  public void testRenameRecoveryEtagMatchFsLevel() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();
    TracingContext testTracingContext = getTestTracingContext(fs, false);

    Assume.assumeTrue(fs.getAbfsStore().getIsNamespaceEnabled(testTracingContext));

    AbfsClient mockClient = getMockAbfsClient();

    String base = "/" + getMethodName();
    String path1 = base + "/dummyFile1";
    String path2 = base + "/dummyFile2";

    touch(new Path(path1));

    setAbfsClient(abfsStore, mockClient);

    // checking correct count in AbfsCounters
    AbfsCounters counter = mockClient.getAbfsCounters();
    IOStatistics ioStats = counter.getIOStatistics();

    Long connMadeBeforeRename = lookupCounterStatistic(ioStats, CONNECTIONS_MADE.getStatName());
    Long renamePathAttemptsBeforeRename = lookupCounterStatistic(ioStats, RENAME_PATH_ATTEMPTS.getStatName());

    // 404 and retry, send sourceEtag as null
    // source eTag matches -> rename should pass even when execute throws exception
    fs.rename(new Path(path1), new Path(path2));

    // validating stat counters after rename
    // 4 calls should have happened in total for rename
    // 1 -> original rename rest call, 2 -> first retry,
    // +2 for getPathStatus calls
    assertThatStatisticCounter(ioStats,
            CONNECTIONS_MADE.getStatName())
            .isEqualTo(4 + connMadeBeforeRename);
    // the RENAME_PATH_ATTEMPTS stat should be incremented by 1
    // retries happen internally within AbfsRestOperation execute()
    // the stat for RENAME_PATH_ATTEMPTS is updated only once before execute() is called
    assertThatStatisticCounter(ioStats,
            RENAME_PATH_ATTEMPTS.getStatName())
            .isEqualTo(1 + renamePathAttemptsBeforeRename);

  }

  /**
   * execute a failing rename but have the file at the far end not match.
   * This is done by explicitly passing in a made up etag for the source
   * etag and creating a file at the far end.
   * The first rename will actually fail with a path exists exception,
   * but as that is swallowed, it's not a problem.
   */
  @Test
  public void testRenameRecoveryEtagMismatchFsLevel() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();
    TracingContext testTracingContext = getTestTracingContext(fs, false);

    Assume.assumeTrue(fs.getAbfsStore().getIsNamespaceEnabled(testTracingContext));

    AbfsClient mockClient = getMockAbfsClient();

    String base = "/" + getMethodName();
    String path1 = base + "/dummyFile1";
    String path2 = base + "/dummyFile2";

    fs.create(new Path(path2));

    setAbfsClient(abfsStore, mockClient);

    // source eTag does not match -> rename should be a failure
    assertEquals(false, fs.rename(new Path(path1), new Path(path2)));

  }

  @Test
  public void testRenameRecoveryFailsForDirFsLevel() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();
    TracingContext testTracingContext = getTestTracingContext(fs, false);

    Assume.assumeTrue(fs.getAbfsStore().getIsNamespaceEnabled(testTracingContext));

    AbfsClient mockClient = getMockAbfsClient();

    String dir1 = "/dummyDir1";
    String dir2 = "/dummyDir2";

    Path path1 = new Path(dir1);
    Path path2 = new Path(dir2);

    fs.mkdirs(path1);

    setAbfsClient(abfsStore, mockClient);

    // checking correct count in AbfsCounters
    AbfsCounters counter = mockClient.getAbfsCounters();
    IOStatistics ioStats = counter.getIOStatistics();

    Long connMadeBeforeRename = lookupCounterStatistic(ioStats, CONNECTIONS_MADE.getStatName());
    Long renamePathAttemptsBeforeRename = lookupCounterStatistic(ioStats, RENAME_PATH_ATTEMPTS.getStatName());

    // source eTag does not match -> rename should be a failure
    boolean renameResult = fs.rename(path1, path2);
    assertEquals(false, renameResult);

    // validating stat counters after rename
    // 3 calls should have happened in total for rename
    // 1 -> original rename rest call, 2 -> first retry,
    // +1 for getPathStatus calls
    // last getPathStatus call should be skipped
    assertThatStatisticCounter(ioStats,
            CONNECTIONS_MADE.getStatName())
            .isEqualTo(3 + connMadeBeforeRename);

    // the RENAME_PATH_ATTEMPTS stat should be incremented by 1
    // retries happen internally within AbfsRestOperation execute()
    // the stat for RENAME_PATH_ATTEMPTS is updated only once before execute() is called
    assertThatStatisticCounter(ioStats,
            RENAME_PATH_ATTEMPTS.getStatName())
            .isEqualTo(1 + renamePathAttemptsBeforeRename);
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
            spyClient.renamePath(path1, path2, null, testTracingContext, null, false,
                    isNamespaceEnabled)));
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
            spyClient.renamePath(path1, path2, null, testTracingContext, null, false,
                    isNamespaceEnabled)));
  }

  /**
   * Test that rename recovery remains unsupported for
   * FNS configurations.
   */
  @Test
  public void testRenameRecoveryUnsupportedForFlatNamespace() throws Exception {
    Assume.assumeTrue(!isNamespaceEnabled);
    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();
    TracingContext testTracingContext = getTestTracingContext(fs, false);

    AbfsClient mockClient = getMockAbfsClient();

    String base = "/" + getMethodName();
    String path1 = base + "/dummyFile1";
    String path2 = base + "/dummyFile2";

    touch(new Path(path1));

    setAbfsClient(abfsStore, mockClient);

    // checking correct count in AbfsCounters
    AbfsCounters counter = mockClient.getAbfsCounters();
    IOStatistics ioStats = counter.getIOStatistics();

    Long connMadeBeforeRename = lookupCounterStatistic(ioStats, CONNECTIONS_MADE.getStatName());
    Long renamePathAttemptsBeforeRename = lookupCounterStatistic(ioStats, RENAME_PATH_ATTEMPTS.getStatName());

    expectErrorCode(SOURCE_PATH_NOT_FOUND, intercept(AbfsRestOperationException.class, () ->
            mockClient.renamePath(path1, path2, null, testTracingContext, null, false,
                    isNamespaceEnabled)));

    // validating stat counters after rename

    // only 2 calls should have happened in total for rename
    // 1 -> original rename rest call, 2 -> first retry,
    // no getPathStatus calls
    // last getPathStatus call should be skipped
    assertThatStatisticCounter(ioStats,
            CONNECTIONS_MADE.getStatName())
            .isEqualTo(2 + connMadeBeforeRename);

    // the RENAME_PATH_ATTEMPTS stat should be incremented by 1
    // retries happen internally within AbfsRestOperation execute()
    // the stat for RENAME_PATH_ATTEMPTS is updated only once before execute() is called
    assertThatStatisticCounter(ioStats,
            RENAME_PATH_ATTEMPTS.getStatName())
            .isEqualTo(1 + renamePathAttemptsBeforeRename);
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
