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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsTestWithTimeout;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_AZURE_LIST_MAX_RESULTS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FS_AZURE_LISTING_ACTION_THREADS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FS_AZURE_PRODUCER_QUEUE_MAX_SIZE;

public class TestListActionTaker extends AbstractAbfsTestWithTimeout {

  public TestListActionTaker() throws Exception {
  }

  /**
   * This test method verifies the behavior of the producer-consumer pattern implemented in the ListActionTaker class.
   * The producer (ListActionTaker) should only resume producing (listing and enqueuing blobs) when the consumer lag becomes tolerable.
   * The test method mocks the necessary components and checks the behavior of the ListActionTaker under these conditions.
   *
   * @throws IOException if an I/O error occurs
   */
  @Test
  public void testProducerResumeOnlyOnConsumerLagBecomesTolerable() throws
      IOException {
    Path path = new Path("test");
    AbfsConfiguration abfsConfiguration = Mockito.mock(AbfsConfiguration.class);
    AbfsBlobClient client = Mockito.mock(AbfsBlobClient.class);
    Mockito.doReturn(abfsConfiguration).when(client).getAbfsConfiguration();
    Mockito.doReturn(DEFAULT_AZURE_LIST_MAX_RESULTS)
        .when(abfsConfiguration)
        .getListingMaxConsumptionLag();
    Mockito.doReturn(DEFAULT_FS_AZURE_PRODUCER_QUEUE_MAX_SIZE)
        .when(abfsConfiguration)
        .getProducerQueueMaxSize();
    ListResponseData listResponseData = Mockito.mock(ListResponseData.class);
    AbfsRestOperation op = Mockito.mock(AbfsRestOperation.class);
    AbfsHttpOperation httpOperation = Mockito.mock(AbfsHttpOperation.class);
    Mockito.doReturn(httpOperation).when(op).getResult();
    Mockito.doReturn(op).when(listResponseData).getOp();
    BlobListResultSchema listResultSchema = Mockito.mock(
        BlobListResultSchema.class);
    Mockito.doReturn(listResultSchema)
        .when(httpOperation)
        .getListResultSchema();
    Mockito.doReturn("a")
        .doReturn("b")
        .doReturn("c")
        .doReturn(null)
        .when(listResultSchema).getNextMarker();
    TracingContext tracingContext = Mockito.mock(TracingContext.class);
    ListActionTaker listActionTaker = new ListActionTaker(path, client,
        tracingContext) {
      private ListBlobQueue listBlobQueue;
      private boolean isListAndEnqueueInProgress;
      private boolean completed;

      @Override
      protected ListBlobQueue createListBlobQueue(final AbfsConfiguration configuration)
          throws InvalidConfigurationValueException {
        listBlobQueue = super.createListBlobQueue(configuration);
        return listBlobQueue;
      }

      @Override
      int getMaxConsumptionParallelism() {
        return DEFAULT_FS_AZURE_LISTING_ACTION_THREADS;
      }

      @Override
      boolean takeAction(final Path path) throws AzureBlobFileSystemException {
        while (!isListAndEnqueueInProgress
            && listBlobQueue.size() < DEFAULT_AZURE_LIST_MAX_RESULTS
            && !completed) {
          // wait for the producer to produce more items
        }
        return true;
      }


      @Override
      protected String listAndEnqueue(final ListBlobQueue listBlobQueue,
          final String continuationToken) throws AzureBlobFileSystemException {
        isListAndEnqueueInProgress = true;
        String contToken = super.listAndEnqueue(listBlobQueue,
            continuationToken);
        isListAndEnqueueInProgress = false;
        if (contToken == null) {
          completed = true;
        }
        return contToken;
      }

      @Override
      protected void addPaths(final List<Path> paths,
          final ListResultSchema retrievedSchema) {
        for (int i = 0; i < DEFAULT_AZURE_LIST_MAX_RESULTS; i++) {
          paths.add(new Path("test" + i));
        }
      }
    };
    final int[] occurrences = {0};
    Mockito.doAnswer(answer -> {
          occurrences[0]++;
          Assertions.assertThat((int) answer.getArgument(2))
              .isEqualTo(DEFAULT_AZURE_LIST_MAX_RESULTS);
          return listResponseData;
        }).when(client)
        .listPath(Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyInt(),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class), Mockito.nullable(URI.class));

    listActionTaker.listRecursiveAndTakeAction();
  }
}
