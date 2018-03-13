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

import com.google.common.collect.Lists;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.RejectedSchedulingRequest;
import org.apache.hadoop.yarn.api.records.RejectionReason;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm.DefaultPlacementAlgorithm;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithm;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.PlacedSchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.SchedulingRequestWithPlacementAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.SchedulingResponse;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * An ApplicationMasterServiceProcessor that performs Constrained placement of
 * Scheduling Requests. It does the following:
 * 1. All initialization.
 * 2. Intercepts placement constraints from the register call and adds it to
 *    the placement constraint manager.
 * 3. Dispatches Scheduling Requests to the Planner.
 */
public class PlacementConstraintProcessor extends AbstractPlacementProcessor {

  /**
   * Wrapper over the SchedulingResponse that wires in the placement attempt
   * and last attempted Node.
   */
  static final class Response extends SchedulingResponse {

    private final int placementAttempt;
    private final SchedulerNode attemptedNode;

    private Response(boolean isSuccess, ApplicationId applicationId,
        SchedulingRequest schedulingRequest, int placementAttempt,
        SchedulerNode attemptedNode) {
      super(isSuccess, applicationId, schedulingRequest);
      this.placementAttempt = placementAttempt;
      this.attemptedNode = attemptedNode;
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementConstraintProcessor.class);

  private ExecutorService schedulingThreadPool;
  private int retryAttempts;
  private Map<ApplicationId, List<BatchedRequests>> requestsToRetry =
      new ConcurrentHashMap<>();
  private Map<ApplicationId, List<SchedulingRequest>> requestsToReject =
      new ConcurrentHashMap<>();

  private BatchedRequests.IteratorType iteratorType;
  private PlacementDispatcher placementDispatcher;


  @Override
  public void init(ApplicationMasterServiceContext amsContext,
      ApplicationMasterServiceProcessor nextProcessor) {
    LOG.info("Initializing Constraint Placement Processor:");
    super.init(amsContext, nextProcessor);

    // Only the first class is considered - even if a comma separated
    // list is provided. (This is for simplicity, since getInstances does a
    // lot of good things by handling things correctly)
    List<ConstraintPlacementAlgorithm> instances =
        ((RMContextImpl) amsContext).getYarnConfiguration().getInstances(
            YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_ALGORITHM_CLASS,
            ConstraintPlacementAlgorithm.class);
    ConstraintPlacementAlgorithm algorithm = null;
    if (instances != null && !instances.isEmpty()) {
      algorithm = instances.get(0);
    } else {
      algorithm = new DefaultPlacementAlgorithm();
    }
    LOG.info("Placement Algorithm [{}]", algorithm.getClass().getName());

    String iteratorName = ((RMContextImpl) amsContext).getYarnConfiguration()
        .get(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_ALGORITHM_ITERATOR,
            BatchedRequests.IteratorType.SERIAL.name());
    LOG.info("Placement Algorithm Iterator[{}]", iteratorName);
    try {
      iteratorType = BatchedRequests.IteratorType.valueOf(iteratorName);
    } catch (IllegalArgumentException e) {
      throw new YarnRuntimeException(
          "Could not instantiate Placement Algorithm Iterator: ", e);
    }

    int algoPSize = ((RMContextImpl) amsContext).getYarnConfiguration().getInt(
        YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_ALGORITHM_POOL_SIZE,
        YarnConfiguration.DEFAULT_RM_PLACEMENT_CONSTRAINTS_ALGORITHM_POOL_SIZE);
    this.placementDispatcher = new PlacementDispatcher();
    this.placementDispatcher.init(
        ((RMContextImpl)amsContext), algorithm, algoPSize);
    LOG.info("Planning Algorithm pool size [{}]", algoPSize);

    int schedPSize = ((RMContextImpl) amsContext).getYarnConfiguration().getInt(
        YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_SCHEDULER_POOL_SIZE,
        YarnConfiguration.DEFAULT_RM_PLACEMENT_CONSTRAINTS_SCHEDULER_POOL_SIZE);
    this.schedulingThreadPool = Executors.newFixedThreadPool(schedPSize);
    LOG.info("Scheduler pool size [{}]", schedPSize);

    // Number of times a request that is not satisfied by the scheduler
    // can be retried.
    this.retryAttempts =
        ((RMContextImpl) amsContext).getYarnConfiguration().getInt(
            YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_RETRY_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_PLACEMENT_CONSTRAINTS_RETRY_ATTEMPTS);
    LOG.info("Num retry attempts [{}]", this.retryAttempts);
  }

  @Override
  public void allocate(ApplicationAttemptId appAttemptId,
      AllocateRequest request, AllocateResponse response) throws YarnException {
    // Copy the scheduling request since we will clear it later after sending
    // to dispatcher
    List<SchedulingRequest> schedulingRequests =
        new ArrayList<>(request.getSchedulingRequests());
    dispatchRequestsForPlacement(appAttemptId, schedulingRequests);
    reDispatchRetryableRequests(appAttemptId);
    schedulePlacedRequests(appAttemptId);

    // Remove SchedulingRequest from AllocateRequest to avoid SchedulingRequest
    // added to scheduler.
    request.setSchedulingRequests(Collections.emptyList());

    nextAMSProcessor.allocate(appAttemptId, request, response);

    handleRejectedRequests(appAttemptId, response);
  }

  private void dispatchRequestsForPlacement(ApplicationAttemptId appAttemptId,
      List<SchedulingRequest> schedulingRequests) {
    if (schedulingRequests != null && !schedulingRequests.isEmpty()) {
      // Normalize the Requests before dispatching
      schedulingRequests.forEach(req -> {
        Resource reqResource = req.getResourceSizing().getResources();
        req.getResourceSizing()
            .setResources(this.scheduler.getNormalizedResource(reqResource));
      });
      this.placementDispatcher.dispatch(new BatchedRequests(iteratorType,
          appAttemptId.getApplicationId(), schedulingRequests, 1));
    }
  }

  private void reDispatchRetryableRequests(ApplicationAttemptId appAttId) {
    List<BatchedRequests> reqsToRetry =
        this.requestsToRetry.get(appAttId.getApplicationId());
    if (reqsToRetry != null && !reqsToRetry.isEmpty()) {
      synchronized (reqsToRetry) {
        for (BatchedRequests bReq: reqsToRetry) {
          this.placementDispatcher.dispatch(bReq);
        }
        reqsToRetry.clear();
      }
    }
  }

  private void schedulePlacedRequests(ApplicationAttemptId appAttemptId) {
    ApplicationId applicationId = appAttemptId.getApplicationId();
    List<PlacedSchedulingRequest> placedSchedulingRequests =
        this.placementDispatcher.pullPlacedRequests(applicationId);
    for (PlacedSchedulingRequest placedReq : placedSchedulingRequests) {
      SchedulingRequest sReq = placedReq.getSchedulingRequest();
      for (SchedulerNode node : placedReq.getNodes()) {
        final SchedulingRequest sReqClone =
            SchedulingRequest.newInstance(sReq.getAllocationRequestId(),
                sReq.getPriority(), sReq.getExecutionType(),
                sReq.getAllocationTags(),
                ResourceSizing.newInstance(
                    sReq.getResourceSizing().getResources()),
                sReq.getPlacementConstraint());
        SchedulerApplicationAttempt applicationAttempt =
            this.scheduler.getApplicationAttempt(appAttemptId);
        Runnable task = () -> {
          boolean success =
              scheduler.attemptAllocationOnNode(
                  applicationAttempt, sReqClone, node);
          if (!success) {
            LOG.warn("Unsuccessful allocation attempt [{}] for [{}]",
                placedReq.getPlacementAttempt(), sReqClone);
          }
          handleSchedulingResponse(
              new Response(success, applicationId, sReqClone,
              placedReq.getPlacementAttempt(), node));
        };
        this.schedulingThreadPool.submit(task);
      }
    }
  }

  private void handleRejectedRequests(ApplicationAttemptId appAttemptId,
      AllocateResponse response) {
    List<SchedulingRequestWithPlacementAttempt> rejectedAlgoRequests =
        this.placementDispatcher.pullRejectedRequests(
            appAttemptId.getApplicationId());
    if (rejectedAlgoRequests != null && !rejectedAlgoRequests.isEmpty()) {
      LOG.warn("Following requests of [{}] were rejected by" +
              " the PlacementAlgorithmOutput Algorithm: {}",
          appAttemptId.getApplicationId(), rejectedAlgoRequests);
      rejectedAlgoRequests.stream()
          .filter(req -> req.getPlacementAttempt() < retryAttempts)
          .forEach(req -> handleSchedulingResponse(
              new Response(false, appAttemptId.getApplicationId(),
                  req.getSchedulingRequest(), req.getPlacementAttempt(),
                  null)));
      ApplicationMasterServiceUtils.addToRejectedSchedulingRequests(response,
          rejectedAlgoRequests.stream()
              .filter(req -> req.getPlacementAttempt() >= retryAttempts)
              .map(sr -> RejectedSchedulingRequest.newInstance(
                  RejectionReason.COULD_NOT_PLACE_ON_NODE,
                  sr.getSchedulingRequest()))
              .collect(Collectors.toList()));
    }
    List<SchedulingRequest> rejectedRequests =
        this.requestsToReject.get(appAttemptId.getApplicationId());
    if (rejectedRequests != null && !rejectedRequests.isEmpty()) {
      synchronized (rejectedRequests) {
        LOG.warn("Following requests of [{}] exhausted all retry attempts " +
                "trying to schedule on placed node: {}",
            appAttemptId.getApplicationId(), rejectedRequests);
        ApplicationMasterServiceUtils.addToRejectedSchedulingRequests(response,
            rejectedRequests.stream()
                .map(sr -> RejectedSchedulingRequest.newInstance(
                    RejectionReason.COULD_NOT_SCHEDULE_ON_NODE, sr))
                .collect(Collectors.toList()));
        rejectedRequests.clear();
      }
    }
  }

  @Override
  public void finishApplicationMaster(ApplicationAttemptId appAttemptId,
      FinishApplicationMasterRequest request,
      FinishApplicationMasterResponse response) {
    placementDispatcher.clearApplicationState(appAttemptId.getApplicationId());
    requestsToReject.remove(appAttemptId.getApplicationId());
    requestsToRetry.remove(appAttemptId.getApplicationId());
    super.finishApplicationMaster(appAttemptId, request, response);
  }

  private void handleSchedulingResponse(SchedulingResponse schedulerResponse) {
    int placementAttempt = ((Response)schedulerResponse).placementAttempt;
    // Retry this placement as it is not successful and we are still
    // under max retry. The req is batched with other unsuccessful
    // requests from the same app
    if (!schedulerResponse.isSuccess() && placementAttempt < retryAttempts) {
      List<BatchedRequests> reqsToRetry =
          requestsToRetry.computeIfAbsent(
              schedulerResponse.getApplicationId(),
              k -> new ArrayList<>());
      synchronized (reqsToRetry) {
        addToRetryList(schedulerResponse, placementAttempt, reqsToRetry);
      }
      LOG.warn("Going to retry request for application [{}] after [{}]" +
              " attempts: [{}]", schedulerResponse.getApplicationId(),
          placementAttempt, schedulerResponse.getSchedulingRequest());
    } else {
      if (!schedulerResponse.isSuccess()) {
        LOG.warn("Not retrying request for application [{}] after [{}]" +
                " attempts: [{}]", schedulerResponse.getApplicationId(),
            placementAttempt, schedulerResponse.getSchedulingRequest());
        List<SchedulingRequest> reqsToReject =
            requestsToReject.computeIfAbsent(
                schedulerResponse.getApplicationId(),
                k -> new ArrayList<>());
        synchronized (reqsToReject) {
          reqsToReject.add(schedulerResponse.getSchedulingRequest());
        }
      }
    }
  }

  private void addToRetryList(SchedulingResponse schedulerResponse,
      int placementAttempt, List<BatchedRequests> reqsToRetry) {
    boolean isAdded = false;
    for (BatchedRequests br : reqsToRetry) {
      if (br.getPlacementAttempt() == placementAttempt + 1) {
        br.addToBatch(schedulerResponse.getSchedulingRequest());
        br.addToBlacklist(
            schedulerResponse.getSchedulingRequest().getAllocationTags(),
            ((Response) schedulerResponse).attemptedNode);
        isAdded = true;
        break;
      }
    }
    if (!isAdded) {
      BatchedRequests br = new BatchedRequests(iteratorType,
          schedulerResponse.getApplicationId(),
          Lists.newArrayList(schedulerResponse.getSchedulingRequest()),
          placementAttempt + 1);
      reqsToRetry.add(br);
      br.addToBlacklist(
          schedulerResponse.getSchedulingRequest().getAllocationTags(),
          ((Response) schedulerResponse).attemptedNode);
    }
  }
}
