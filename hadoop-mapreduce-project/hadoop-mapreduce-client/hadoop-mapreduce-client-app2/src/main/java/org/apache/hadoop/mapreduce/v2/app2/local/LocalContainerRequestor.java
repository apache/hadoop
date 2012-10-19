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

package org.apache.hadoop.mapreduce.v2.app2.local;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.ContainerRequestor;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMCommunicator;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMCommunicatorEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMContainerRequestor.ContainerRequest;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.BuilderUtils;

/**
 * For UberAM, the LocalContainerRequestor is responsible for sending keep-alive
 * heartbeats to the RM, along with sending over job progress. Also provides any
 * additional information to the rest of the AM - ApplicationACLs etc.
 */
public class LocalContainerRequestor extends RMCommunicator implements
    ContainerRequestor {

  private static final Log LOG =
      LogFactory.getLog(LocalContainerRequestor.class);
  
  private long retrystartTime;
  private long retryInterval;
  
  public LocalContainerRequestor(ClientService clientService, AppContext context) {
    super(clientService, context);
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    retryInterval =
        getConfig().getLong(MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS,
            MRJobConfig.DEFAULT_MR_AM_TO_RM_WAIT_INTERVAL_MS);
    // Init startTime to current time. If all goes well, it will be reset after
    // first attempt to contact RM.
    retrystartTime = System.currentTimeMillis();
  }
  
  @SuppressWarnings("unchecked")
  @Override
  protected void heartbeat() throws Exception {
    AllocateRequest allocateRequest = BuilderUtils.newAllocateRequest(
        this.applicationAttemptId, this.lastResponseID, super
            .getApplicationProgress(), new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>());
    AllocateResponse allocateResponse = scheduler.allocate(allocateRequest);
    AMResponse response;
    try {
      response = allocateResponse.getAMResponse();
      // Reset retry count if no exception occurred.
      retrystartTime = System.currentTimeMillis();
    } catch (Exception e) {
      // This can happen when the connection to the RM has gone down. Keep
      // re-trying until the retryInterval has expired.
      if (System.currentTimeMillis() - retrystartTime >= retryInterval) {
        LOG.error("Could not contact RM after " + retryInterval + " milliseconds.");
        eventHandler.handle(new JobEvent(this.getJob().getID(),
                                         JobEventType.INTERNAL_ERROR));
        throw new YarnException("Could not contact RM after " +
                                retryInterval + " milliseconds.");
      }
      // Throw this up to the caller, which may decide to ignore it and
      // continue to attempt to contact the RM.
      throw e;
    }
    if (response.getReboot()) {
      LOG.info("Event from RM: shutting down Application Master");
      // This can happen if the RM has been restarted. If it is in that state,
      // this application must clean itself up.
      eventHandler.handle(new JobEvent(this.getJob().getID(),
                                       JobEventType.INTERNAL_ERROR));
      throw new YarnException("Resource Manager doesn't recognize AttemptId: " +
                               this.getContext().getApplicationID());
    }
  }

  @Override
  public void handle(RMCommunicatorEvent rawEvent) {
    switch (rawEvent.getType()) {
    case CONTAINER_DEALLOCATE:
      LOG.warn("Unexpected eventType: " + rawEvent.getType() + ", Event: "
          + rawEvent);
      break;
    case CONTAINER_FAILED:
      LOG.warn("Unexpected eventType: " + rawEvent.getType() + ", Event: "
          + rawEvent);
      break;
    case CONTAINER_REQ:
      LOG.warn("Unexpected eventType: " + rawEvent.getType() + ", Event: "
          + rawEvent);
      break;
    default:
      break;
    }
  }

  @Override
  public Resource getAvailableResources() {
    throw new YarnException("Unexpected call to getAvailableResource");
  }

  @Override
  public void addContainerReq(ContainerRequest req) {
    throw new YarnException("Unexpected call to addContainerReq");
  }

  @Override
  public void decContainerReq(ContainerRequest req) {
    throw new YarnException("Unexpected call to decContainerReq");
  }
}
