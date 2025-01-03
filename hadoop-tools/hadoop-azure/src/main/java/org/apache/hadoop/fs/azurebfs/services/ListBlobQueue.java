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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

/**
 * Data-structure to hold the list of paths to be processed. The paths are
 * enqueued by the producer and dequeued by the consumer. The producer can
 * enqueue the paths until the queue is full. The consumer can consume the paths
 * until the queue is empty. The producer can mark the queue as completed once
 * all the paths are enqueued and there is no more paths that can be returned from
 * server. The consumer can mark the queue as failed if it encounters any exception
 * while consuming the paths.
 */
public class ListBlobQueue {

    private final Queue<Path> pathQueue = new ArrayDeque<>();

    private final int maxSize;

    private final int consumeSetSize;

    private volatile boolean isCompleted = false;

    private volatile boolean isConsumptionFailed = false;

    private volatile AzureBlobFileSystemException failureFromProducer;

    /** Constructor.
     *
     * @param maxSize maximum size of the queue
     * @param consumeSetSize number of paths to be consumed at a time
     */
    ListBlobQueue(int maxSize, int consumeSetSize) {
        this.maxSize = maxSize;
        this.consumeSetSize = consumeSetSize;
    }

    /** Mark the queue as failed.*/
    void markProducerFailure(AzureBlobFileSystemException failure) {
        failureFromProducer = failure;
    }

    /** Mark the queue as completed.*/
    void complete() {
        isCompleted = true;
    }

    /** Mark the consumption as failed.*/
    synchronized void markConsumptionFailed() {
        isConsumptionFailed = true;
        notify();
    }

    /** Check if the consumption has failed.
     *
     * @return true if the consumption has failed
     */
    boolean getConsumptionFailed() {
        return isConsumptionFailed;
    }

    /** Check if the queue is completed.
     *
     * @return true if the queue is completed
     */
    boolean getIsCompleted() {
        return isCompleted && size() == 0;
    }

    /** Get the exception from producer.
     *
     * @return exception from producer
     */
    private AzureBlobFileSystemException getException() {
        return failureFromProducer;
    }

    /** Enqueue the paths.
     *
     * @param pathList list of paths to be enqueued
     */
    synchronized void enqueue(List<Path> pathList) {
        if (isCompleted) {
            throw new IllegalStateException(
                    "Cannot enqueue paths as the queue is already marked as completed");
        }
        pathQueue.addAll(pathList);
    }

    /** Consume the paths.
     *
     * @return list of paths to be consumed
     * @throws AzureBlobFileSystemException if the consumption fails
     */
    synchronized List<Path> consume() throws AzureBlobFileSystemException {
        AzureBlobFileSystemException exception = getException();
        if (exception != null) {
            throw exception;
        }
        return dequeue();
    }

    /** Dequeue the paths.
     *
     * @return list of paths to be consumed
     */
    private List<Path> dequeue() {
        List<Path> pathListForConsumption = new ArrayList<>();
        int counter = 0;
        while (counter < consumeSetSize && pathQueue.size() > 0) {
            pathListForConsumption.add(pathQueue.poll());
            counter++;
        }
        if (counter > 0) {
            notify();
        }
        return pathListForConsumption;
    }

    private synchronized int size() {
        return pathQueue.size();
    }

    /**
     * Returns the available size of the queue. This is calculated by subtracting the current size of the queue
     * from its maximum size. If the queue is full, this method will wait until some elements are consumed and
     * space becomes available. If consumption has failed, it immediately returns zero. This method is synchronized
     * to prevent concurrent modifications of the queue.
     *
     * @return the available size of the queue
     */
    synchronized int availableSize() {
        while (maxSize - size() <= 0) {
            if (isConsumptionFailed) {
                return 0;
            }
            try {
                wait();
            } catch (InterruptedException ignored) {
            }
        }
        return maxSize - size();
    }
}