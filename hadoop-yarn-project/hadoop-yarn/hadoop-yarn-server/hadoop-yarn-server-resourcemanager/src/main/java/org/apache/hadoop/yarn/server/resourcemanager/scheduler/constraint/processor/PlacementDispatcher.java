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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithm;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmOutput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmOutputCollector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.PlacedSchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.SchedulingRequestWithPlacementAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class initializes the Constraint Placement Algorithm. It dispatches
 * input to the algorithm and collects output from it.
 */
class PlacementDispatcher implements
    ConstraintPlacementAlgorithmOutputCollector {

  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementDispatcher.class);
  private ConstraintPlacementAlgorithm algorithm;
  private ExecutorService algorithmThreadPool;

  private Map<ApplicationId, List<PlacedSchedulingRequest>>
      placedRequests = new ConcurrentHashMap<>();
  private Map<ApplicationId, List<SchedulingRequestWithPlacementAttempt>>
      rejectedRequests = new ConcurrentHashMap<>();

  public void init(RMContext rmContext,
      ConstraintPlacementAlgorithm placementAlgorithm, int poolSize) {
    LOG.info("Initializing Constraint Placement Planner:");
    this.algorithm = placementAlgorithm;
    this.algorithm.init(rmContext);
    this.algorithmThreadPool = Executors.newFixedThreadPool(poolSize);
  }

  void dispatch(final BatchedRequests batchedRequests) {
    final ConstraintPlacementAlgorithmOutputCollector collector = this;
    Runnable placingTask = () -> {
      LOG.debug("Got [{}] requests to place from application [{}].. " +
              "Attempt count [{}]",
          batchedRequests.getSchedulingRequests().size(),
          batchedRequests.getApplicationId(),
          batchedRequests.getPlacementAttempt());
      algorithm.place(batchedRequests, collector);
    };
    this.algorithmThreadPool.submit(placingTask);
  }

  public List<PlacedSchedulingRequest> pullPlacedRequests(
      ApplicationId applicationId) {
    List<PlacedSchedulingRequest> placedReqs =
        this.placedRequests.get(applicationId);
    if (placedReqs != null && !placedReqs.isEmpty()) {
      List<PlacedSchedulingRequest> retList = new ArrayList<>();
      synchronized (placedReqs) {
        if (placedReqs.size() > 0) {
          retList.addAll(placedReqs);
          placedReqs.clear();
        }
      }
      return retList;
    }
    return Collections.emptyList();
  }

  public List<SchedulingRequestWithPlacementAttempt> pullRejectedRequests(
      ApplicationId applicationId) {
    List<SchedulingRequestWithPlacementAttempt> rejectedReqs =
        this.rejectedRequests.get(applicationId);
    if (rejectedReqs != null && !rejectedReqs.isEmpty()) {
      List<SchedulingRequestWithPlacementAttempt> retList = new ArrayList<>();
      synchronized (rejectedReqs) {
        if (rejectedReqs.size() > 0) {
          retList.addAll(rejectedReqs);
          rejectedReqs.clear();
        }
      }
      return retList;
    }
    return Collections.emptyList();
  }

  void clearApplicationState(ApplicationId applicationId) {
    placedRequests.remove(applicationId);
    rejectedRequests.remove(applicationId);
  }

  @Override
  public void collect(ConstraintPlacementAlgorithmOutput placement) {
    if (!placement.getPlacedRequests().isEmpty()) {
      List<PlacedSchedulingRequest> processed =
          placedRequests.computeIfAbsent(
              placement.getApplicationId(), k -> new ArrayList<>());
      synchronized (processed) {
        LOG.debug(
            "Planning Algorithm has placed for application [{}]" +
                " the following [{}]", placement.getApplicationId(),
            placement.getPlacedRequests());
        for (PlacedSchedulingRequest esr :
            placement.getPlacedRequests()) {
          processed.add(esr);
        }
      }
    }
    if (!placement.getRejectedRequests().isEmpty()) {
      List<SchedulingRequestWithPlacementAttempt> rejected =
          rejectedRequests.computeIfAbsent(
              placement.getApplicationId(), k -> new ArrayList());
      LOG.warn(
          "Planning Algorithm has rejected for application [{}]" +
              " the following [{}]", placement.getApplicationId(),
          placement.getRejectedRequests());
      synchronized (rejected) {
        rejected.addAll(placement.getRejectedRequests());
      }
    }
  }
}
