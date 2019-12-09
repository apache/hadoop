/*
 *
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
 *
 */

package org.apache.hadoop.resourceestimator.skylinestore.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.skylinestore.api.SkylineStore;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.DuplicateRecurrenceIdException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.EmptyResourceSkylineException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.RecurrenceIdNotFoundException;
import org.apache.hadoop.resourceestimator.skylinestore.exceptions.SkylineStoreException;
import org.apache.hadoop.resourceestimator.skylinestore.validator.SkylineStoreValidator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An in-memory implementation of {@link SkylineStore}.
 */
public class InMemoryStore implements SkylineStore {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(InMemoryStore.class);
  private final ReentrantReadWriteLock readWriteLock =
      new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();
  private final SkylineStoreValidator inputValidator =
      new SkylineStoreValidator();
  /**
   * A pipeline job's history {@link ResourceSkyline}s. TODO: we may flatten it
   * out for quick access.
   */
  private final Map<RecurrenceId, List<ResourceSkyline>> skylineStore =
      new HashMap<>(); // pipelineId, resource skyline
  // Recurring pipeline's predicted {@link ResourceSkyline}s.
  private final Map<String, RLESparseResourceAllocation> estimationStore =
      new HashMap<>(); // pipelineId, ResourceSkyline

  private List<ResourceSkyline> eliminateNull(
      final List<ResourceSkyline> resourceSkylines) {
    final List<ResourceSkyline> result = new ArrayList<>();
    for (final ResourceSkyline resourceSkyline : resourceSkylines) {
      if (resourceSkyline != null) {
        result.add(resourceSkyline);
      }
    }
    return result;
  }

  @Override public final void addHistory(final RecurrenceId recurrenceId,
      final List<ResourceSkyline> resourceSkylines)
      throws SkylineStoreException {
    inputValidator.validate(recurrenceId, resourceSkylines);
    writeLock.lock();
    try {
      // remove the null elements in the resourceSkylines
      final List<ResourceSkyline> filteredInput =
          eliminateNull(resourceSkylines);
      if (filteredInput.size() > 0) {
        if (skylineStore.containsKey(recurrenceId)) {
          // if filteredInput has duplicate jobIds with existing skylines in the
          // store,
          // throw out an exception
          final List<ResourceSkyline> jobHistory =
              skylineStore.get(recurrenceId);
          final List<String> oldJobIds = new ArrayList<>();
          for (final ResourceSkyline resourceSkyline : jobHistory) {
            oldJobIds.add(resourceSkyline.getJobId());
          }
          if (!oldJobIds.isEmpty()) {
            for (ResourceSkyline elem : filteredInput) {
              if (oldJobIds.contains(elem.getJobId())) {
                StringBuilder errMsg = new StringBuilder();
                errMsg.append(
                    "Trying to addHistory duplicate resource skylines for "
                        + recurrenceId
                        + ". Use updateHistory function instead.");
                LOGGER.error(errMsg.toString());
                throw new DuplicateRecurrenceIdException(errMsg.toString());
              }
            }
          }
          skylineStore.get(recurrenceId).addAll(filteredInput);
          LOGGER.info("Successfully addHistory new resource skylines for {}.",
              recurrenceId);
        } else {
          skylineStore.put(recurrenceId, filteredInput);
          LOGGER.info("Successfully addHistory new resource skylines for {}.",
              recurrenceId);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override public void addEstimation(String pipelineId,
      RLESparseResourceAllocation resourceSkyline)
      throws SkylineStoreException {
    inputValidator.validate(pipelineId, resourceSkyline);
    writeLock.lock();
    try {
      estimationStore.put(pipelineId, resourceSkyline);
      LOGGER.info("Successfully add estimated resource allocation for {}.",
          pipelineId);
    } finally {
      writeLock.unlock();
    }
  }

  @Override public final void deleteHistory(final RecurrenceId recurrenceId)
      throws SkylineStoreException {
    inputValidator.validate(recurrenceId);
    writeLock.lock();
    try {
      if (skylineStore.containsKey(recurrenceId)) {
        skylineStore.remove(recurrenceId);
        LOGGER.warn("Delete resource skylines for {}.", recurrenceId);
      } else {
        StringBuilder errMsg = new StringBuilder();
        errMsg.append(
            "Trying to deleteHistory non-existing recurring pipeline  "
                + recurrenceId + "\'s resource skylines");
        LOGGER.error(errMsg.toString());
        throw new RecurrenceIdNotFoundException(errMsg.toString());
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override public final void updateHistory(final RecurrenceId recurrenceId,
      final List<ResourceSkyline> resourceSkylines)
      throws SkylineStoreException {
    inputValidator.validate(recurrenceId, resourceSkylines);
    writeLock.lock();
    try {
      if (skylineStore.containsKey(recurrenceId)) {
        // remove the null elements in the resourceSkylines
        List<ResourceSkyline> filteredInput = eliminateNull(resourceSkylines);
        if (filteredInput.size() > 0) {
          skylineStore.put(recurrenceId, filteredInput);
          LOGGER.info("Successfully updateHistory resource skylines for {}.",
              recurrenceId);
        } else {
          StringBuilder errMsg = new StringBuilder();
          errMsg.append("Trying to updateHistory " + recurrenceId
              + " with empty resource skyline");
          LOGGER.error(errMsg.toString());
          throw new EmptyResourceSkylineException(errMsg.toString());
        }
      } else {
        StringBuilder errMsg = new StringBuilder();
        errMsg.append(
            "Trying to updateHistory non-existing resource skylines for "
                + recurrenceId);
        LOGGER.error(errMsg.toString());
        throw new RecurrenceIdNotFoundException(errMsg.toString());
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override public final Map<RecurrenceId, List<ResourceSkyline>> getHistory(
      final RecurrenceId recurrenceId) throws SkylineStoreException {
    inputValidator.validate(recurrenceId);
    readLock.lock();
    try {
      String pipelineId = recurrenceId.getPipelineId();
      // User tries to getHistory all resource skylines in the skylineStore
      if (pipelineId.equals("*")) {
        LOGGER
            .info("Successfully query resource skylines for {}.", recurrenceId);
        return Collections.unmodifiableMap(skylineStore);
      }
      String runId = recurrenceId.getRunId();
      Map<RecurrenceId, List<ResourceSkyline>> result =
          new HashMap<RecurrenceId, List<ResourceSkyline>>();
      // User tries to getHistory pipelineId's all resource skylines in the
      // skylineStore
      if (runId.equals("*")) {
        // TODO: this for loop is expensive, so we may change the type of
        // skylineStore to
        // speed up this loop.
        for (Map.Entry<RecurrenceId, List<ResourceSkyline>> entry : skylineStore
            .entrySet()) {
          RecurrenceId index = entry.getKey();
          if (index.getPipelineId().equals(pipelineId)) {
            result.put(index, entry.getValue());
          }
        }
        if (result.size() > 0) {
          LOGGER.info("Successfully query resource skylines for {}.",
              recurrenceId);
          return Collections.unmodifiableMap(result);
        } else {
          LOGGER.warn(
              "Trying to getHistory non-existing resource skylines for {}.",
              recurrenceId);
          return null;
        }
      }
      // User tries to getHistory {pipelineId, runId}'s resource skylines
      if (skylineStore.containsKey(recurrenceId)) {
        result.put(recurrenceId, skylineStore.get(recurrenceId));
      } else {
        LOGGER
            .warn("Trying to getHistory non-existing resource skylines for {}.",
                recurrenceId);
        return null;
      }
      LOGGER.info("Successfully query resource skylines for {}.", recurrenceId);
      return Collections.unmodifiableMap(result);
    } finally {
      readLock.unlock();
    }
  }

  @Override public final RLESparseResourceAllocation getEstimation(
      String pipelineId) throws SkylineStoreException {
    inputValidator.validate(pipelineId);
    readLock.lock();
    try {
      return estimationStore.get(pipelineId);
    } finally {
      readLock.unlock();
    }
  }
}
