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

package org.apache.hadoop.yarn.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.apache.hadoop.yarn.util.AsyncCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Extends Thread and provides an implementation that is used for processing the
 * AM heart beat request asynchronously and sending back the response using the
 * callback method registered with the system.
 */
public class AMHeartbeatRequestHandler extends Thread {
  public static final Logger LOG =
      LoggerFactory.getLogger(AMHeartbeatRequestHandler.class);

  // Indication flag for the thread to keep running
  private volatile boolean keepRunning;

  // For unit test draining
  private volatile boolean isThreadWaiting;

  private Configuration conf;
  private ApplicationId applicationId;

  private BlockingQueue<AsyncAllocateRequestInfo> requestQueue;
  private AMRMClientRelayer rmProxyRelayer;
  private UserGroupInformation userUgi;
  private int lastResponseId;

  public AMHeartbeatRequestHandler(Configuration conf,
      ApplicationId applicationId) {
    super("AMHeartbeatRequestHandler Heartbeat Handler Thread");
    this.setUncaughtExceptionHandler(
        new HeartBeatThreadUncaughtExceptionHandler());
    this.keepRunning = true;
    this.isThreadWaiting = false;

    this.conf = conf;
    this.applicationId = applicationId;
    this.requestQueue = new LinkedBlockingQueue<>();

    resetLastResponseId();
  }

  /**
   * Shutdown the thread.
   */
  public void shutdown() {
    this.keepRunning = false;
    this.interrupt();
  }

  @Override
  public void run() {
    while (keepRunning) {
      AsyncAllocateRequestInfo requestInfo;
      try {
        this.isThreadWaiting = true;
        requestInfo = this.requestQueue.take();
        this.isThreadWaiting = false;

        if (requestInfo == null) {
          throw new YarnException(
              "Null requestInfo taken from request queue");
        }
        if (!this.keepRunning) {
          break;
        }

        // change the response id before forwarding the allocate request as we
        // could have different values for each UAM
        AllocateRequest request = requestInfo.getRequest();
        if (request == null) {
          throw new YarnException("Null allocateRequest from requestInfo");
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Sending Heartbeat to RM. AskList:"
              + ((request.getAskList() == null) ? " empty"
                  : request.getAskList().size()));
        }

        request.setResponseId(lastResponseId);
        AllocateResponse response = rmProxyRelayer.allocate(request);
        if (response == null) {
          throw new YarnException("Null allocateResponse from allocate");
        }

        lastResponseId = response.getResponseId();
        // update token if RM has reissued/renewed
        if (response.getAMRMToken() != null) {
          LOG.debug("Received new AMRMToken");
          YarnServerSecurityUtils.updateAMRMToken(response.getAMRMToken(),
              userUgi, conf);
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Received Heartbeat reply from RM. Allocated Containers:"
              + ((response.getAllocatedContainers() == null) ? " empty"
                  : response.getAllocatedContainers().size()));
        }

        if (requestInfo.getCallback() == null) {
          throw new YarnException("Null callback from requestInfo");
        }
        requestInfo.getCallback().callback(response);
      } catch (InterruptedException ex) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Interrupted while waiting for queue", ex);
        }
      } catch (Throwable ex) {
        LOG.warn(
            "Error occurred while processing heart beat for " + applicationId,
            ex);
      }
    }

    LOG.info("AMHeartbeatRequestHandler thread for {} is exiting",
        applicationId);
  }

  /**
   * Reset the lastResponseId to zero.
   */
  public void resetLastResponseId() {
    this.lastResponseId = 0;
  }

  /**
   * Set the AMRMClientRelayer for RM connection.
   */
  public void setAMRMClientRelayer(AMRMClientRelayer relayer) {
    this.rmProxyRelayer = relayer;
  }

  /**
   * Set the UGI for RM connection.
   */
  public void setUGI(UserGroupInformation ugi) {
    this.userUgi = ugi;
  }

  /**
   * Sends the specified heart beat request to the resource manager and invokes
   * the callback asynchronously with the response.
   *
   * @param request the allocate request
   * @param callback the callback method for the request
   * @throws YarnException if registerAM is not called yet
   */
  public void allocateAsync(AllocateRequest request,
      AsyncCallback<AllocateResponse> callback) throws YarnException {
    try {
      this.requestQueue.put(new AsyncAllocateRequestInfo(request, callback));
    } catch (InterruptedException ex) {
      // Should not happen as we have MAX_INT queue length
      LOG.debug("Interrupted while waiting to put on response queue", ex);
    }
  }

  @VisibleForTesting
  public void drainHeartbeatThread() {
    while (!this.isThreadWaiting || this.requestQueue.size() > 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
      }
    }
  }

  @VisibleForTesting
  public int getRequestQueueSize() {
    return this.requestQueue.size();
  }

  /**
   * Data structure that encapsulates AllocateRequest and AsyncCallback
   * instance.
   */
  public static class AsyncAllocateRequestInfo {
    private AllocateRequest request;
    private AsyncCallback<AllocateResponse> callback;

    public AsyncAllocateRequestInfo(AllocateRequest request,
        AsyncCallback<AllocateResponse> callback) {
      Preconditions.checkArgument(request != null,
          "AllocateRequest cannot be null");
      Preconditions.checkArgument(callback != null, "Callback cannot be null");

      this.request = request;
      this.callback = callback;
    }

    public AsyncCallback<AllocateResponse> getCallback() {
      return this.callback;
    }

    public AllocateRequest getRequest() {
      return this.request;
    }
  }

  /**
   * Uncaught exception handler for the background heartbeat thread.
   */
  public class HeartBeatThreadUncaughtExceptionHandler
      implements UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread t, Throwable e) {
      LOG.error("Heartbeat thread {} for application {} crashed!", t.getName(),
          applicationId, e);
    }
  }
}
