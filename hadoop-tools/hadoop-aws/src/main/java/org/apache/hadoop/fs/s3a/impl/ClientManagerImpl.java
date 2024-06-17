/*
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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.net.URI;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_CLIENT_CREATION;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;

public class ClientManagerImpl implements ClientManager {

  private final S3ClientFactory clientFactory;

  /**
   * Parameters to create sync/async clients.
   */
  private final S3ClientFactory.S3ClientCreationParameters clientCreationParameters;

  /**
   * Duration tracker factory for creation.
   */
  private final DurationTrackerFactory durationTrackerFactory;

  /**
   * Core S3 client.
   */
  private S3Client s3Client;

  /** Async client is used for transfer manager. */
  private S3AsyncClient s3AsyncClient;

  /** Transfer manager. */
  private S3TransferManager transferManager;

  public ClientManagerImpl(
      final S3ClientFactory clientFactory,
      final S3ClientFactory.S3ClientCreationParameters clientCreationParameters,
      final DurationTrackerFactory durationTrackerFactory) throws IOException {
    this.clientFactory = requireNonNull(clientFactory);
    this.clientCreationParameters = requireNonNull(clientCreationParameters);
    this.durationTrackerFactory = requireNonNull(durationTrackerFactory);
  }

  @Override
  public synchronized S3Client getOrCreateS3Client() throws IOException {
    if (s3Client == null) {
      // demand create the S3 client.
      s3Client = trackDuration(durationTrackerFactory,
          STORE_CLIENT_CREATION.getSymbol(), () ->
              clientFactory.createS3Client(getUri(), clientCreationParameters));
    }
    return s3Client;
  }

  @Override
  public synchronized S3AsyncClient getOrCreateAsyncClient() throws IOException {
    if (s3AsyncClient == null) {
      // demand create the Async S3 client.
      s3AsyncClient = trackDuration(durationTrackerFactory,
          STORE_CLIENT_CREATION.getSymbol(), () ->
              clientFactory.createS3AsyncClient(getUri(), clientCreationParameters));
    }
    return s3AsyncClient;
  }

  @Override
  public synchronized S3TransferManager getOrCreateTransferManager() throws IOException {
    if (transferManager == null) {

      final S3AsyncClient asyncClient = getOrCreateAsyncClient();

      transferManager = trackDuration(durationTrackerFactory,
          STORE_CLIENT_CREATION.getSymbol(), () ->
              clientFactory.createS3TransferManager(asyncClient));
    }
    return transferManager;
  }


  @Override
  public synchronized void close() throws Exception {
    if (transferManager != null) {
      transferManager.close();
      transferManager = null;
    }
    if (s3AsyncClient != null) {
      s3AsyncClient.close();
      s3AsyncClient = null;
    }
    if (s3Client != null) {
      s3Client.close();
      s3Client = null;
    }
  }

  /**
   * Get the URI of the filesystem.
   * @return URI to use when creating clients.
   */
  public URI getUri() {
    return clientCreationParameters.getPathUri();
  }
}
