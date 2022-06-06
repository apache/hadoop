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

package org.apache.hadoop.fs.azurebfs;

import java.lang.reflect.Field;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientResult;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.TestAbfsClient;
import org.apache.hadoop.fs.statistics.IOStatistics;

import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.RENAME_DESTINATION_PARENT_PATH_NOT_FOUND;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_RENAME;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    boolean doesDestParentDirExist = false;
    AzureBlobFileSystem fs = getFileSystem();

    AbfsClient mockClient = TestAbfsClient.getMockAbfsClient(
        fs.getAbfsStore().getClient(),
        fs.getAbfsStore().getAbfsConfiguration());

    AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();
    abfsStore = setAzureBlobSystemStoreField(abfsStore, "client", mockClient);

    // SuccessFul Result.
    AbfsRestOperation successOp = mock(AbfsRestOperation.class);
    AbfsClientResult successResult = mock(AbfsClientResult.class);
    when(successResult.getOp()).thenReturn(successOp);
    when(successResult.isIncompleteMetadataState()).thenReturn(false);

    // Failed Result.
    AbfsRestOperation failedOp = mock(AbfsRestOperation.class);
    AbfsClientResult recoveredMetaDataIncompleteResult = mock(AbfsClientResult.class);
    when(recoveredMetaDataIncompleteResult.getOp()).thenReturn(failedOp);
    when(recoveredMetaDataIncompleteResult.isIncompleteMetadataState()).thenReturn(true);

    // No destination Parent dir exception.
    AzureBlobFileSystemException destParentNotFound
        = getMockAbfsRestOperationException(
        RENAME_DESTINATION_PARENT_PATH_NOT_FOUND.getStatusCode(),
        RENAME_DESTINATION_PARENT_PATH_NOT_FOUND.getErrorCode());

    // We need to throw an exception once a rename is triggered with
    // destination having no parent, but after a retry it needs to succeed.
    when(mockClient.renamePath(sourcePath, destNoParentPath, null, null,
        null)).thenAnswer(

        new Answer<AbfsClientResult>() {
          @Override
          public AbfsClientResult answer(InvocationOnMock invocationOnMock)
              throws Throwable {
            IOStatistics ioStatistics = fs.getIOStatistics();
            long renameCalls = ioStatistics.counters().get(OP_RENAME);
            LOG.info("Rename Calls from IOstatistics: {}", renameCalls);
            if (renameCalls == 0) {
              throw destParentNotFound;
            }
            return recoveredMetaDataIncompleteResult;
          }
        }).thenReturn(recoveredMetaDataIncompleteResult);

    // Dest parent not found exc. to be raised.
    intercept(AzureBlobFileSystemException.class,
        () -> mockClient.renamePath(sourcePath,
        destNoParentPath, null, null,
        null));

    AbfsClientResult resultOfSecondRenameCall =
        mockClient.renamePath(sourcePath,
        destNoParentPath, null, null,
        null);

    // the second rename call should be the recoveredResult due to
    // metaDataIncomplete
    Assertions.assertThat(resultOfSecondRenameCall)
        .describedAs("This result should be recovered result due to MetaData "
            + "being in incomplete state")
        .isEqualTo(recoveredMetaDataIncompleteResult);
    // Verify Incomplete metadata state happened for our second rename call.
    assertTrue(resultOfSecondRenameCall.isIncompleteMetadataState());


    // Verify renamePath occurred two times implying a retry was attempted.
    verify(mockClient, times(2))
        .renamePath(sourcePath, destNoParentPath, null, null, null);

  }

  private AbfsRestOperationException getMockAbfsRestOperationException(
      int statusCode, String errorCode) {
    return new AbfsRestOperationException(statusCode, errorCode,
        "No Parent found for the Destination file",
        new Exception());
  }

  private AzureBlobFileSystemStore setAzureBlobSystemStoreField(
      final AzureBlobFileSystemStore abfsStore,
      final String fieldName,
      Object fieldObject) throws Exception {

    Field abfsClientField = AzureBlobFileSystemStore.class.getDeclaredField(
        fieldName);
    abfsClientField.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(abfsClientField,
        abfsClientField.getModifiers() & ~java.lang.reflect.Modifier.FINAL);
    abfsClientField.set(abfsStore, fieldObject);
    return abfsStore;
  }

}
