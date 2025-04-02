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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.FileSystemOperationUnhandledException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_AZURE_LIST_MAX_RESULTS;

/**
 * ListActionTaker is an abstract class that provides a way to list the paths
 * recursively and take action on each path. The implementations of this class
 * should provide the action to be taken on each listed path.
 */
public abstract class ListActionTaker {

  private static final Logger LOG = LoggerFactory.getLogger(
      ListActionTaker.class);

  private final Path path;

  private final AbfsBlobClient abfsClient;

  private final TracingContext tracingContext;

  private final ExecutorService executorService;

  private final AtomicBoolean producerThreadToBeStopped = new AtomicBoolean(
      false);

  /** Constructor.
   *
   * @param path the path to list recursively.
   * @param abfsClient the AbfsBlobClient to use for listing.
   * @param tracingContext the tracing context to use for listing.
   */
  public ListActionTaker(Path path,
      AbfsBlobClient abfsClient,
      TracingContext tracingContext) {
    this.path = path;
    this.abfsClient = abfsClient;
    this.tracingContext = tracingContext;
    executorService = Executors.newFixedThreadPool(
        getMaxConsumptionParallelism());
  }

  public AbfsBlobClient getAbfsClient() {
    return abfsClient;
  }

  /** Get the maximum number of parallelism for consumption.
   *
   * @return the maximum number of parallelism for consumption.
   */
  abstract int getMaxConsumptionParallelism();

  /** Take action on a path.
   *
   * @param path the path to take action on.
   * @return true if the action is successful.
   * @throws AzureBlobFileSystemException if the action fails.
   */
  abstract boolean takeAction(Path path) throws AzureBlobFileSystemException;

  private boolean takeAction(List<Path> paths)
      throws AzureBlobFileSystemException {
    List<Future<Boolean>> futureList = new ArrayList<>();
    for (Path path : paths) {
      Future<Boolean> future = executorService.submit(() -> takeAction(path));
      futureList.add(future);
    }

    AzureBlobFileSystemException executionException = null;
    boolean actionResult = true;
    for (Future<Boolean> future : futureList) {
      try {
        Boolean result = future.get();
        if (!result) {
          actionResult = false;
        }
      } catch (InterruptedException e) {
        LOG.debug("Thread interrupted while taking action on path: {}",
            path.toUri().getPath());
      } catch (ExecutionException e) {
        LOG.debug("Execution exception while taking action on path: {}",
            path.toUri().getPath());
        if (e.getCause() instanceof AzureBlobFileSystemException) {
          executionException = (AzureBlobFileSystemException) e.getCause();
        } else {
          executionException =
              new FileSystemOperationUnhandledException(executionException);
        }
      }
    }
    if (executionException != null) {
      throw executionException;
    }
    return actionResult;
  }

  /**
   * Spawns a producer thread that list the children of the path recursively and queue
   * them in into {@link ListBlobQueue}. On the main thread, it dequeues the
   * path and supply them to parallel thread for relevant action which is defined
   * in {@link #takeAction(Path)}.
   *
   * @return true if the action is successful.
   * @throws AzureBlobFileSystemException if the action fails.
   */
  public boolean listRecursiveAndTakeAction()
      throws AzureBlobFileSystemException {
    AbfsConfiguration configuration = getAbfsClient().getAbfsConfiguration();
    Thread producerThread = null;
    try {
      ListBlobQueue listBlobQueue = createListBlobQueue(configuration);
      producerThread = new Thread(() -> {
        try {
          produceConsumableList(listBlobQueue);
        } catch (AzureBlobFileSystemException e) {
          listBlobQueue.markProducerFailure(e);
        }
      });
      producerThread.start();

      while (!listBlobQueue.getIsCompleted()) {
        List<Path> paths = listBlobQueue.consume();
        if (paths == null) {
          continue;
        }
        try {
          boolean resultOnPartAction = takeAction(paths);
          if (!resultOnPartAction) {
            return false;
          }
        } catch (AzureBlobFileSystemException parallelConsumptionException) {
          listBlobQueue.markConsumptionFailed();
          throw parallelConsumptionException;
        }
      }
      return true;
    } finally {
      if (producerThread != null) {
        producerThreadToBeStopped.set(true);
      }
      executorService.shutdownNow();
    }
  }

  /**
   * Create a {@link ListBlobQueue} instance.
   *
   * @param configuration the configuration to use.
   * @return the created {@link ListBlobQueue} instance.
   * @throws InvalidConfigurationValueException if the configuration is invalid.
   */
  @VisibleForTesting
  protected ListBlobQueue createListBlobQueue(final AbfsConfiguration configuration)
      throws InvalidConfigurationValueException {
    return new ListBlobQueue(
        configuration.getProducerQueueMaxSize(),
        getMaxConsumptionParallelism(),
        configuration.getListingMaxConsumptionLag()
    );
  }

  /**
   * Produce the consumable list of paths.
   *
   * @param listBlobQueue the {@link ListBlobQueue} to enqueue the paths.
   * @throws AzureBlobFileSystemException if the listing fails.
   */
  private void produceConsumableList(final ListBlobQueue listBlobQueue)
      throws AzureBlobFileSystemException {
    String continuationToken = null;
    do {
      continuationToken = listAndEnqueue(listBlobQueue, continuationToken);
    } while (!producerThreadToBeStopped.get() && continuationToken != null
        && !listBlobQueue.getConsumptionFailed());
    listBlobQueue.complete();
  }

  /**
   * List the paths and enqueue them into the {@link ListBlobQueue}.
   *
   * @param listBlobQueue the {@link ListBlobQueue} to enqueue the paths.
   * @param continuationToken the continuation token to use for listing.
   * @return the continuation token for the next listing.
   * @throws AzureBlobFileSystemException if the listing fails.
   */
  @VisibleForTesting
  protected String listAndEnqueue(final ListBlobQueue listBlobQueue,
      String continuationToken) throws AzureBlobFileSystemException {
    final int queueAvailableSizeForProduction = Math.min(
        DEFAULT_AZURE_LIST_MAX_RESULTS,
        listBlobQueue.availableSizeForProduction());
    if (queueAvailableSizeForProduction == 0) {
      return null;
    }
    final AbfsRestOperation op;
    try {
      op = getAbfsClient().listPath(path.toUri().getPath(),
          true,
          queueAvailableSizeForProduction, continuationToken,
          tracingContext, null).getOp();
    } catch (AzureBlobFileSystemException ex) {
      throw ex;
    } catch (IOException ex) {
      throw new AbfsRestOperationException(-1, null,
          "Unknown exception from listing: " + ex.getMessage(), ex);
    }

    ListResultSchema retrievedSchema = op.getResult().getListResultSchema();
    if (retrievedSchema == null) {
      return continuationToken;
    }
    continuationToken
        = ((BlobListResultSchema) retrievedSchema).getNextMarker();
    List<Path> paths = new ArrayList<>();
    addPaths(paths, retrievedSchema);
    listBlobQueue.enqueue(paths);
    return continuationToken;
  }

  /**
   * Add the paths from the retrieved schema to the list of paths.
   *
   * @param paths the list of paths to add to.
   * @param retrievedSchema the retrieved schema.
   */
  @VisibleForTesting
  protected void addPaths(final List<Path> paths,
      final ListResultSchema retrievedSchema) {
    for (ListResultEntrySchema entry : retrievedSchema.paths()) {
      Path entryPath = new Path(ROOT_PATH + entry.name());
      if (!entryPath.equals(this.path)) {
        paths.add(entryPath);
      }
    }
  }
}
