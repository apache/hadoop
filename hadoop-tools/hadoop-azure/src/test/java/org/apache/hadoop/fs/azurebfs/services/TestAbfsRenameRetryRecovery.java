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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import javax.net.ssl.HttpsURLConnection;

import org.apache.hadoop.fs.azurebfs.AbfsStatistic;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.RENAME_DESTINATION_PARENT_PATH_NOT_FOUND;
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

    // specifying AbfsHttpOperation mock behavior

    // mock object representing the 404 path not found result
    AbfsHttpOperation mockHttp404Op = Mockito.mock(AbfsHttpOperation.class);
    Mockito.doReturn(404).when(mockHttp404Op).getStatusCode();
    Mockito.doNothing().when(mockHttp404Op).processResponse(nullable(byte[].class), Mockito.any(int.class), Mockito.any(int.class));
    Mockito.doNothing().when(mockHttp404Op).setRequestProperty(nullable(String.class), nullable(String.class));
    Mockito.doNothing().when(mockHttp404Op).sendRequest(nullable(byte[].class), Mockito.any(int.class), Mockito.any(int.class));
    Mockito.doReturn("PUT").when(mockHttp404Op).getMethod();
    Mockito.doReturn("Source Path not found").when(mockHttp404Op).getStorageErrorMessage();
    Mockito.doReturn("SourcePathNotFound").when(mockHttp404Op).getStorageErrorCode();


    // // mock object representing the 500 timeout result for first try of rename
    AbfsHttpOperation mockHttp500Op = Mockito.mock(AbfsHttpOperation.class);
    Mockito.doReturn(500).when(mockHttp500Op).getStatusCode();
    Mockito.doThrow(IOException.class)
            .when(mockHttp500Op).processResponse(nullable(byte[].class), Mockito.any(int.class), Mockito.any(int.class));
    Mockito.doNothing().when(mockHttp500Op).setRequestProperty(nullable(String.class), nullable(String.class));
    Mockito.doNothing().when(mockHttp500Op).sendRequest(nullable(byte[].class), Mockito.any(int.class), Mockito.any(int.class));
    Mockito.doReturn("PUT").when(mockHttp500Op).getMethod();
    Mockito.doReturn("ClientTimeoutError").when(mockHttp500Op).getStorageErrorCode();

    // creating mock HttpUrlConnection object
    HttpURLConnection mockUrlConn = Mockito.mock(HttpsURLConnection.class);

    // tying all mocks together
    Mockito.doReturn(mockUrlConn).when(mockHttp404Op).getConnection();
    Mockito.doReturn(mockUrlConn).when(mockHttp500Op).getConnection();

    // adding mock objects to current AbfsClient
    AbfsClient spyClient = Mockito.spy(fs.getAbfsStore().getClient());
    // Rest Operation is spied as it needs to have spyclient instance as a param to the constructor
    // directly returning a mock for this would make the client instance null
    AbfsRestOperation mockRestOp = Mockito.spy(new AbfsRestOperation(
                    AbfsRestOperationType.RenamePath,
                    spyClient,
                    HTTP_METHOD_PUT,
                    null,
            null)
    );
    Mockito.doReturn(mockRestOp).when(spyClient).createRenameRestOperation(nullable(URL.class), nullable(List.class));

    Mockito.doReturn(mockHttp500Op).doReturn(mockHttp404Op).when(mockRestOp).createHttpOperation();
    Mockito.doReturn(mockHttp500Op).doReturn(mockHttp404Op).when(mockRestOp).getResult();

    Mockito.doReturn(true).when(mockRestOp).hasResult();


    SharedKeyCredentials mockSharedKeyCreds = mock(SharedKeyCredentials.class);
    Mockito.doNothing().when(mockSharedKeyCreds).signRequest(Mockito.any(HttpURLConnection.class), Mockito.any(long.class));
    // real method calls made once at start and once at end
    // for the two getPathStatus calls that actually have to be made
    Mockito.doCallRealMethod().doReturn(mockSharedKeyCreds).doReturn(mockSharedKeyCreds).doCallRealMethod().when(spyClient).getSharedKeyCredentials();

    return spyClient;

  }

  @Test
  public void testRenameRecoverySrcDestEtagSame() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();
    TracingContext testTracingContext = getTestTracingContext(fs, false);

    Assume.assumeTrue(fs.getAbfsStore().getIsNamespaceEnabled(testTracingContext));

    AbfsClient mockClient = getMockAbfsClient();

    // simulating eTag check returns true
    // source and destination eTag equal
    //
    Mockito.doReturn(true).when(mockClient).isSourceDestEtagEqual(
            nullable(String.class), nullable(AbfsHttpOperation.class)
    );

    String path1 = "/dummyFile1";
    String path2 = "dummyFile2";
    fs.create(new Path(path1));
    fs.create(new Path(path2));

    abfsStore.setClient(mockClient);

    // checking correct count in AbfsCounters
    AbfsCounters counter = mockClient.getAbfsCounters();
    Long connMadeBeforeRename = counter.getIOStatistics().counters().
            get(AbfsStatistic.CONNECTIONS_MADE.getStatName());
    Long renamePathAttemptsBeforeRename = counter.getIOStatistics().counters().
            get(AbfsStatistic.RENAME_PATH_ATTEMPTS.getStatName());

    // 404 and retry, send sourceEtag as null
    // source eTag matches -> rename should pass even when execute throws exception
    fs.rename(new Path(path1), new Path(path2));

    // validating stat counters after rename
    Long connMadeAfterRename = counter.getIOStatistics().counters().
            get(AbfsStatistic.CONNECTIONS_MADE.getStatName());
    Long renamePathAttemptsAfterRename = counter.getIOStatistics().counters().
            get(AbfsStatistic.RENAME_PATH_ATTEMPTS.getStatName());

    // 4 calls should have happened in total for rename
    // 1 -> original rename rest call, 2 -> first retry,
    // +2 for getPathStatus calls
    assertEquals(Long.valueOf(connMadeBeforeRename+4), connMadeAfterRename);

    // the RENAME_PATH_ATTEMPTS stat should be incremented by 1
    // retries happen internally within AbfsRestOperation execute()
    // the stat for RENAME_PATH_ATTEMPTS is updated only once before execute() is called
    assertEquals(Long.valueOf(renamePathAttemptsBeforeRename+1), renamePathAttemptsAfterRename);

  }

  @Test
  public void testRenameRecoverySrcDestEtagDifferent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();
    TracingContext testTracingContext = getTestTracingContext(fs, false);

    Assume.assumeTrue(fs.getAbfsStore().getIsNamespaceEnabled(testTracingContext));

    AbfsClient mockClient = getMockAbfsClient();

    String path1 = "/dummyFile1";
    String path2 = "/dummyFile2";

    fs.create(new Path(path1));
    fs.create(new Path(path2));

    abfsStore.setClient(mockClient);

    // checking correct count in AbfsCounters
    AbfsCounters counter = mockClient.getAbfsCounters();
    Long connMadeBeforeRename = counter.getIOStatistics().counters().
            get(AbfsStatistic.CONNECTIONS_MADE.getStatName());
    Long renamePathAttemptsBeforeRename = counter.getIOStatistics().counters().
            get(AbfsStatistic.RENAME_PATH_ATTEMPTS.getStatName());

    // source eTag does not match -> rename should be a failure
    boolean renameResult = fs.rename(new Path(path1), new Path(path2));
    assertEquals(false, renameResult);

    // validating stat counters after rename
    Long connMadeAfterRename = counter.getIOStatistics().counters().
            get(AbfsStatistic.CONNECTIONS_MADE.getStatName());
    Long renamePathAttemptsAfterRename = counter.getIOStatistics().counters().
            get(AbfsStatistic.RENAME_PATH_ATTEMPTS.getStatName());

    // 4 calls should have happened in total for rename
    // 1 -> original rename rest call, 2 -> first retry,
    // +2 for getPathStatus calls
    assertEquals(Long.valueOf(connMadeBeforeRename+4), connMadeAfterRename);

    // the RENAME_PATH_ATTEMPTS stat should be incremented by 1
    // retries happen internally within AbfsRestOperation execute()
    // the stat for RENAME_PATH_ATTEMPTS is updated only once before execute() is called
    assertEquals(Long.valueOf(renamePathAttemptsBeforeRename+1), renamePathAttemptsAfterRename);
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
