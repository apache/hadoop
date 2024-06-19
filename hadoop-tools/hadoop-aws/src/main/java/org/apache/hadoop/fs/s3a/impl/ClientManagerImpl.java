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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_CLIENT_CREATION;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;
import static org.apache.hadoop.util.Preconditions.checkState;

/**
 * Client manager for on-demand creation of S3 clients,
 * with parallelized close of them in {@link #close()}.
 * Updates {@link org.apache.hadoop.fs.s3a.Statistic#STORE_CLIENT_CREATION}
 * to track count and duration of client creation.
 */
public class ClientManagerImpl implements ClientManager {

  public static final Logger LOG = LoggerFactory.getLogger(ClientManagerImpl.class);
  /**
   * Client factory to invoke.
   */
  private final S3ClientFactory clientFactory;

  /**
   * Closed flag.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

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
  private AtomicReference<S3Client> s3Client = new AtomicReference<>();

  /** Async client is used for transfer manager. */
  private AtomicReference<S3AsyncClient> s3AsyncClient = new AtomicReference<>();

  /** Transfer manager. */
  private AtomicReference<S3TransferManager> transferManager = new AtomicReference<>();

  /**
   * Constructor.
   * This does not create any clients.
   * @param clientFactory client factory to invoke
   * @param clientCreationParameters creation parameters.
   * @param durationTrackerFactory duration tracker.
   */
  public ClientManagerImpl(
      final S3ClientFactory clientFactory,
      final S3ClientFactory.S3ClientCreationParameters clientCreationParameters,
      final DurationTrackerFactory durationTrackerFactory) {
    this.clientFactory = requireNonNull(clientFactory);
    this.clientCreationParameters = requireNonNull(clientCreationParameters);
    this.durationTrackerFactory = requireNonNull(durationTrackerFactory);
  }

  @Override
  public synchronized S3Client getOrCreateS3Client() throws IOException {
    checkNotClosed();
    if (s3Client.get()== null) {
      LOG.debug("Creating S3 client for {}", getUri());
      s3Client.set(trackDuration(durationTrackerFactory,
          STORE_CLIENT_CREATION.getSymbol(), () ->
              clientFactory.createS3Client(getUri(), clientCreationParameters)));
    }
    return s3Client.get();
  }

  @Override
  public synchronized S3AsyncClient getOrCreateAsyncClient() throws IOException {

    checkNotClosed();
    if (s3AsyncClient == null) {
      LOG.debug("Creating Async S3 client for {}", getUri());
      s3AsyncClient.set(trackDuration(durationTrackerFactory,
          STORE_CLIENT_CREATION.getSymbol(), () ->
              clientFactory.createS3AsyncClient(
                  getUri(),
                  clientCreationParameters)));
    }
    return s3AsyncClient.get();
  }

  @Override
  public synchronized S3TransferManager getOrCreateTransferManager() throws IOException {
    checkNotClosed();
    if (transferManager.get() == null) {
      // get the async client, which is likely to be demand-created.
      final S3AsyncClient asyncClient = getOrCreateAsyncClient();
      // then create the transfer manager.
      LOG.debug("Creating S3 transfer manager for {}", getUri());
      transferManager.set(trackDuration(durationTrackerFactory,
          STORE_CLIENT_CREATION.getSymbol(), () ->
              clientFactory.createS3TransferManager(asyncClient)));
    }
    return transferManager.get();
  }

  /**
   * Check that the client manager is not closed.
   * @throws IllegalStateException if it is closed.
   */
  private void checkNotClosed() {
    checkState(!closed.get(), "Client manager is closed");
  }

  /**
   * Close() is synchronized to avoid race conditions between
   * slow client creation and this close operation.
   */
  @Override
  public synchronized void close() {
    if (closed.getAndSet(true)) {
      // re-entrant close.
      return;
    }
    close(transferManager.get());
    close(s3AsyncClient.get());
    close(s3Client.get());
  }

  /**
   * Get the URI of the filesystem.
   * @return URI to use when creating clients.
   */
  public URI getUri() {
    return clientCreationParameters.getPathUri();
  }

  /**
   * Queue closing a closeable, logging any exception, and returning null
   * to use in assigning the field.
   * @param closeable closeable.
   * @param <T> type of closeable
   * @return null
   */
  private <T extends AutoCloseable> CompletableFuture<T> close(T closeable) {
    if (closeable == null) {
      // no-op
      return completedFuture(null);
    }
    return supplyAsync(() -> {
      try {
        closeable.close();
      } catch (Exception e) {
        LOG.warn("Failed to close {}", closeable, e);
      }
      return null;
    });
  }

  @Override
  public String toString() {
    return "ClientManagerImpl{" +
        "closed=" + closed.get() +
        ", s3Client=" + s3Client +
        ", s3AsyncClient=" + s3AsyncClient +
        ", transferManager=" + transferManager +
        '}';
  }
}
