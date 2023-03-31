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

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.RENAME_DESTINATION_PARENT_PATH_NOT_FOUND;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
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
