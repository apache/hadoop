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

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_THREAD_COUNT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_THREAD_COUNT_KEY;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.jooq.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

/**
 * Implementation of ReconTaskController.
 */
public class ReconTaskControllerImpl implements ReconTaskController {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconTaskControllerImpl.class);

  private Map<String, ReconDBUpdateTask> reconDBUpdateTasks;
  private ExecutorService executorService;
  private int threadCount = 1;
  private final Semaphore taskSemaphore = new Semaphore(1);
  private final ReconOMMetadataManager omMetadataManager;
  private Map<String, AtomicInteger> taskFailureCounter = new HashMap<>();
  private static final int TASK_FAILURE_THRESHOLD = 2;
  private ReconTaskStatusDao reconTaskStatusDao;

  @Inject
  public ReconTaskControllerImpl(OzoneConfiguration configuration,
                                 ReconOMMetadataManager omMetadataManager,
                                 Configuration sqlConfiguration) {
    this.omMetadataManager = omMetadataManager;
    reconDBUpdateTasks = new HashMap<>();
    threadCount = configuration.getInt(OZONE_RECON_TASK_THREAD_COUNT_KEY,
        OZONE_RECON_TASK_THREAD_COUNT_DEFAULT);
    executorService = Executors.newFixedThreadPool(threadCount);
    reconTaskStatusDao = new ReconTaskStatusDao(sqlConfiguration);
  }

  @Override
  public void registerTask(ReconDBUpdateTask task) {
    String taskName = task.getTaskName();
    LOG.info("Registered task " + taskName + " with controller.");

    // Store task in Task Map.
    reconDBUpdateTasks.put(taskName, task);
    // Store Task in Task failure tracker.
    taskFailureCounter.put(taskName, new AtomicInteger(0));
    // Create DB record for the task.
    ReconTaskStatus reconTaskStatusRecord = new ReconTaskStatus(taskName,
        0L, 0L);
    reconTaskStatusDao.insert(reconTaskStatusRecord);
  }

  /**
   * For every registered task, we try process step twice and then reprocess
   * once (if process failed twice) to absorb the events. If a task has failed
   * reprocess call more than 2 times across events, it is unregistered
   * (blacklisted).
   * @param events set of events
   * @throws InterruptedException
   */
  @Override
  public void consumeOMEvents(OMUpdateEventBatch events)
      throws InterruptedException {
    taskSemaphore.acquire();

    try {
      Collection<Callable<Pair>> tasks = new ArrayList<>();
      for (Map.Entry<String, ReconDBUpdateTask> taskEntry :
          reconDBUpdateTasks.entrySet()) {
        ReconDBUpdateTask task = taskEntry.getValue();
        tasks.add(() -> task.process(events));
      }

      List<Future<Pair>> results = executorService.invokeAll(tasks);
      List<String> failedTasks = processTaskResults(results, events);

      //Retry
      List<String> retryFailedTasks = new ArrayList<>();
      if (!failedTasks.isEmpty()) {
        tasks.clear();
        for (String taskName : failedTasks) {
          ReconDBUpdateTask task = reconDBUpdateTasks.get(taskName);
          tasks.add(() -> task.process(events));
        }
        results = executorService.invokeAll(tasks);
        retryFailedTasks = processTaskResults(results, events);
      }

      //Reprocess
      //TODO Move to a separate task queue since reprocess may be a heavy
      //operation for large OM DB instances
      if (!retryFailedTasks.isEmpty()) {
        tasks.clear();
        for (String taskName : failedTasks) {
          ReconDBUpdateTask task = reconDBUpdateTasks.get(taskName);
          tasks.add(() -> task.reprocess(omMetadataManager));
        }
        results = executorService.invokeAll(tasks);
        List<String> reprocessFailedTasks = processTaskResults(results, events);
        for (String taskName : reprocessFailedTasks) {
          LOG.info("Reprocess step failed for task : " + taskName);
          if (taskFailureCounter.get(taskName).incrementAndGet() >
              TASK_FAILURE_THRESHOLD) {
            LOG.info("Blacklisting Task since it failed retry and " +
                "reprocess more than " + TASK_FAILURE_THRESHOLD + " times.");
            reconDBUpdateTasks.remove(taskName);
          }
        }
      }
    } catch (ExecutionException e) {
      LOG.error("Unexpected error : ", e);
    } finally {
      taskSemaphore.release();
    }
  }

  /**
   * Store the last completed event sequence number and timestamp to the DB
   * for that task.
   * @param taskName taskname to be updated.
   * @param eventInfo contains the new sequence number and timestamp.
   */
  private void storeLastCompletedTransaction(
      String taskName, OMDBUpdateEvent.EventInfo eventInfo) {
    ReconTaskStatus reconTaskStatusRecord = new ReconTaskStatus(taskName,
        eventInfo.getEventTimestampMillis(), eventInfo.getSequenceNumber());
    reconTaskStatusDao.update(reconTaskStatusRecord);
  }

  @Override
  public Map<String, ReconDBUpdateTask> getRegisteredTasks() {
    return reconDBUpdateTasks;
  }

  /**
   * Wait on results of all tasks.
   * @param results Set of Futures.
   * @param events Events.
   * @return List of failed task names
   * @throws ExecutionException execution Exception
   * @throws InterruptedException Interrupted Exception
   */
  private List<String> processTaskResults(List<Future<Pair>> results,
                                          OMUpdateEventBatch events)
      throws ExecutionException, InterruptedException {
    List<String> failedTasks = new ArrayList<>();
    for (Future<Pair> f : results) {
      String taskName = f.get().getLeft().toString();
      if (!(Boolean)f.get().getRight()) {
        LOG.info("Failed task : " + taskName);
        failedTasks.add(f.get().getLeft().toString());
      } else {
        taskFailureCounter.get(taskName).set(0);
        storeLastCompletedTransaction(taskName, events.getLastEventInfo());
      }
    }
    return failedTasks;
  }
}
