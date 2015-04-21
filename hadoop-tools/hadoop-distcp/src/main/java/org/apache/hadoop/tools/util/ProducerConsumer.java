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

package org.apache.hadoop.tools.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.util.WorkReport;
import org.apache.hadoop.tools.util.WorkRequest;
import org.apache.hadoop.tools.util.WorkRequestProcessor;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * ProducerConsumer class encapsulates input and output queues and a
 * thread-pool of Workers that loop on WorkRequest<T> inputQueue and for each
 * consumed WorkRequest Workers invoke WorkRequestProcessor.processItem()
 * and output resulting WorkReport<R> to the outputQueue.
 */
public class ProducerConsumer<T, R> {
  private Log LOG = LogFactory.getLog(ProducerConsumer.class);
  private LinkedBlockingQueue<WorkRequest<T>> inputQueue;
  private LinkedBlockingQueue<WorkReport<R>> outputQueue;
  private ExecutorService executor;
  private AtomicInteger workCnt;

  /**
   *  ProducerConsumer maintains input and output queues and a thread-pool of
   *  workers.
   *
   *  @param numThreads   Size of thread-pool to execute Workers.
   */
  public ProducerConsumer(int numThreads) {
    this.inputQueue = new LinkedBlockingQueue<WorkRequest<T>>();
    this.outputQueue = new LinkedBlockingQueue<WorkReport<R>>();
    executor = Executors.newFixedThreadPool(numThreads);
    workCnt = new AtomicInteger(0);
  }

  /**
   *  Add another worker that will consume WorkRequest<T> items from input
   *  queue, process each item using supplied processor, and for every
   *  processed item output WorkReport<R> to output queue.
   *
   *  @param processor  Processor implementing WorkRequestProcessor interface.
   *
   */
  public void addWorker(WorkRequestProcessor<T, R> processor) {
    executor.execute(new Worker(processor));
  }

  /**
   *  Shutdown ProducerConsumer worker thread-pool without waiting for
   *  completion of any pending work.
   */
  public void shutdown() {
    executor.shutdown();
  }

  /**
   *  Returns number of pending ProducerConsumer items (submitted to input
   *  queue for processing via put() method but not yet consumed by take()
   *  or blockingTake().
   *
   *  @return  Number of items in ProducerConsumer (either pending for
   *           processing or waiting to be consumed).
   */
  public int getWorkCnt() {
    return workCnt.get();
  }

  /**
   *  Returns true if there are items in ProducerConsumer that are either
   *  pending for processing or waiting to be consumed.
   *
   *  @return  True if there were more items put() to ProducerConsumer than
   *           consumed by take() or blockingTake().
   */
  public boolean hasWork() {
    return workCnt.get() > 0;
  }

  /**
   *  Blocking put workRequest to ProducerConsumer input queue.
   *
   *  @param  WorkRequest<T> item to be processed.
   */
  public void put(WorkRequest<T> workRequest) {
    boolean isDone = false;
    while (!isDone) {
      try {
        inputQueue.put(workRequest);
        workCnt.incrementAndGet();
        isDone = true;
      } catch (InterruptedException ie) {
        LOG.error("Could not put workRequest into inputQueue. Retrying...");
      }
    }
  }

  /**
   *  Blocking take from ProducerConsumer output queue that can be interrupted.
   *
   *  @return  WorkReport<R> item returned by processor's processItem().
   */
  public WorkReport<R> take() throws InterruptedException {
    WorkReport<R> report = outputQueue.take();
    workCnt.decrementAndGet();
    return report;
  }

  /**
   *  Blocking take from ProducerConsumer output queue (catches exceptions and
   *  retries forever).
   *
   *  @return  WorkReport<R> item returned by processor's processItem().
   */
  public WorkReport<R> blockingTake() {
    while (true) {
      try {
        WorkReport<R> report = outputQueue.take();
        workCnt.decrementAndGet();
        return report;
      } catch (InterruptedException ie) {
        LOG.debug("Retrying in blockingTake...");
      }
    }
  }

  private class Worker implements Runnable {
    private WorkRequestProcessor<T, R> processor;

    public Worker(WorkRequestProcessor<T, R> processor) {
      this.processor = processor;
    }

    public void run() {
      while (true) {
        try {
          WorkRequest<T> work = inputQueue.take();
          WorkReport<R> result = processor.processItem(work);

          boolean isDone = false;
          while (!isDone) {
            try {
              outputQueue.put(result);
              isDone = true;
            } catch (InterruptedException ie) {
              LOG.debug("Could not put report into outputQueue. Retrying...");
            }
          }
        } catch (InterruptedException ie) {
          LOG.debug("Interrupted while waiting for request from inputQueue.");
        }
      }
    }
  }
}
