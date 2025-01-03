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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;

/**
 * ListActionTaker is an abstract class that provides a way to list the paths
 * recursively and take action on each path. The implementations of this class
 * should provide the action to be taken on each listed path.
 */
public abstract class ListActionTaker {

    private static final Logger LOG = LoggerFactory.getLogger(ListActionTaker.class);

    private final Path path;

    private final AbfsBlobClient abfsBlobClient;

    private final TracingContext tracingContext;

    private final ExecutorService executorService;

    private final AtomicBoolean producerThreadToBeStopped = new AtomicBoolean(
            false);

    /** Constructor.
     *
     * @param path the path to list recursively.
     * @param abfsBlobClient the AbfsBlobClient to use for listing.
     * @param tracingContext the tracing context to use for listing.
     */
    public ListActionTaker(Path path,
                           AbfsBlobClient abfsBlobClient,
                           TracingContext tracingContext) {
        this.path = path;
        this.abfsBlobClient = abfsBlobClient;
        this.tracingContext = tracingContext;
        executorService = Executors.newFixedThreadPool(
                getMaxConsumptionParallelism());
    }

    /** Get the AbfsBlobClient.
     *
     * @return the AbfsBlobClient.
     */
    public AbfsBlobClient getAbfsBlobClient() {
        return abfsBlobClient;
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

    /** Take action on a list of paths in parallel.
     *
     * @param paths the list of paths to take action on.
     * @return true if the action is successful.
     * @throws AzureBlobFileSystemException if the action fails.
     */
    private boolean takeAction(List<Path> paths) throws AzureBlobFileSystemException {
        List<Future<Boolean>> futureList = new ArrayList<>();
        for (Path path : paths) {
            Future<Boolean> future = executorService.submit(() -> {
                return takeAction(path);
            });
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
                executionException = (AzureBlobFileSystemException) e.getCause();
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
    public boolean listRecursiveAndTakeAction() throws AzureBlobFileSystemException {
        AbfsConfiguration configuration = abfsBlobClient.getAbfsConfiguration();
        Thread producerThread = null;
        try {
            ListBlobQueue listBlobQueue = new ListBlobQueue(
                    configuration.getProducerQueueMaxSize(), getMaxConsumptionParallelism());
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

    /** List the children of the path recursively and queue them in into
     * {@link ListBlobQueue}.
     *
     * @param listBlobQueue the queue to which the paths are enqueued.
     * @throws AzureBlobFileSystemException if the listing fails.
     */
    private void produceConsumableList(final ListBlobQueue listBlobQueue)
            throws AzureBlobFileSystemException {
        String continuationToken = null;
        do {
            List<Path> paths = new ArrayList<>();
            final int queueAvailableSize = listBlobQueue.availableSize();
            if (queueAvailableSize == 0) {
                break;
            }
            final AbfsRestOperation op;
            try {
                op = abfsBlobClient.listPath(path.toUri().getPath(),
                        true,
                        queueAvailableSize, continuationToken,
                        tracingContext);
            } catch (AzureBlobFileSystemException ex) {
                throw ex;
            } catch (IOException ex) {
                throw new AbfsRestOperationException(-1, null,
                        "Unknown exception from listing: " + ex.getMessage(), ex);
            }

            ListResultSchema retrievedSchema = op.getResult().getListResultSchema();
            if (retrievedSchema == null) {
                continue;
            }
            continuationToken
                    = ((BlobListResultSchema) retrievedSchema).getNextMarker();
            for (ListResultEntrySchema entry : retrievedSchema.paths()) {
                Path entryPath = new Path(ROOT_PATH, entry.name());
                if (!entryPath.equals(this.path)) {
                    paths.add(entryPath);
                }
            }
            listBlobQueue.enqueue(paths);
        } while (!producerThreadToBeStopped.get() && continuationToken != null
                && !listBlobQueue.getConsumptionFailed());
        listBlobQueue.complete();
    }
}