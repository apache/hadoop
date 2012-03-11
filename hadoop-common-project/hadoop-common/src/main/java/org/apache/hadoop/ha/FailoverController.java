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
package org.apache.hadoop.ha;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;

import com.google.common.base.Preconditions;

/**
 * The FailOverController is responsible for electing an active service
 * on startup or when the current active is changing (eg due to failure),
 * monitoring the health of a service, and performing a fail-over when a
 * new active service is either manually selected by a user or elected.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FailoverController {

  private static final Log LOG = LogFactory.getLog(FailoverController.class);

  /**
   * Perform pre-failover checks on the given service we plan to
   * failover to, eg to prevent failing over to a service (eg due
   * to it being inaccessible, already active, not healthy, etc).
   *
   * An option to ignore toSvc if it claims it is not ready to
   * become active is provided in case performing a failover will
   * allow it to become active, eg because it triggers a log roll
   * so the standby can learn about new blocks and leave safemode.
   *
   * @param toSvc service to make active
   * @param toSvcName name of service to make active
   * @param forceActive ignore toSvc if it reports that it is not ready
   * @throws FailoverFailedException if we should avoid failover
   */
  private static void preFailoverChecks(HAServiceProtocol toSvc,
                                        InetSocketAddress toSvcAddr,
                                        boolean forceActive)
      throws FailoverFailedException {
    HAServiceState toSvcState;

    try {
      toSvcState = toSvc.getServiceState();
    } catch (IOException e) {
      String msg = "Unable to get service state for " + toSvcAddr;
      LOG.error(msg, e);
      throw new FailoverFailedException(msg, e);
    }

    if (!toSvcState.equals(HAServiceState.STANDBY)) {
      throw new FailoverFailedException(
          "Can't failover to an active service");
    }

    try {
      HAServiceProtocolHelper.monitorHealth(toSvc);
    } catch (HealthCheckFailedException hce) {
      throw new FailoverFailedException(
          "Can't failover to an unhealthy service", hce);
    } catch (IOException e) {
      throw new FailoverFailedException(
          "Got an IO exception", e);
    }

    try {
      if (!toSvc.readyToBecomeActive()) {
        if (!forceActive) {
          throw new FailoverFailedException(
              toSvcAddr + " is not ready to become active");
        }
      }
    } catch (IOException e) {
      throw new FailoverFailedException(
          "Got an IO exception", e);
    }
  }

  /**
   * Failover from service 1 to service 2. If the failover fails
   * then try to failback.
   *
   * @param fromSvc currently active service
   * @param fromSvcAddr addr of the currently active service
   * @param toSvc service to make active
   * @param toSvcAddr addr of the service to make active
   * @param fencer for fencing fromSvc
   * @param forceFence to fence fromSvc even if not strictly necessary
   * @param forceActive try to make toSvc active even if it is not ready
   * @throws FailoverFailedException if the failover fails
   */
  public static void failover(HAServiceProtocol fromSvc,
                              InetSocketAddress fromSvcAddr,
                              HAServiceProtocol toSvc,
                              InetSocketAddress toSvcAddr,
                              NodeFencer fencer,
                              boolean forceFence,
                              boolean forceActive)
      throws FailoverFailedException {
    Preconditions.checkArgument(fencer != null, "failover requires a fencer");
    preFailoverChecks(toSvc, toSvcAddr, forceActive);

    // Try to make fromSvc standby
    boolean tryFence = true;
    try {
      HAServiceProtocolHelper.transitionToStandby(fromSvc);
      // We should try to fence if we failed or it was forced
      tryFence = forceFence ? true : false;
    } catch (ServiceFailedException sfe) {
      LOG.warn("Unable to make " + fromSvcAddr + " standby (" +
          sfe.getMessage() + ")");
    } catch (IOException ioe) {
      LOG.warn("Unable to make " + fromSvcAddr +
          " standby (unable to connect)", ioe);
    }

    // Fence fromSvc if it's required or forced by the user
    if (tryFence) {
      if (!fencer.fence(fromSvcAddr)) {
        throw new FailoverFailedException("Unable to fence " +
            fromSvcAddr + ". Fencing failed.");
      }
    }

    // Try to make toSvc active
    boolean failed = false;
    Throwable cause = null;
    try {
      HAServiceProtocolHelper.transitionToActive(toSvc);
    } catch (ServiceFailedException sfe) {
      LOG.error("Unable to make " + toSvcAddr + " active (" +
          sfe.getMessage() + "). Failing back.");
      failed = true;
      cause = sfe;
    } catch (IOException ioe) {
      LOG.error("Unable to make " + toSvcAddr +
          " active (unable to connect). Failing back.", ioe);
      failed = true;
      cause = ioe;
    }

    // We failed to make toSvc active
    if (failed) {
      String msg = "Unable to failover to " + toSvcAddr;
      // Only try to failback if we didn't fence fromSvc
      if (!tryFence) {
        try {
          // Unconditionally fence toSvc in case it is still trying to
          // become active, eg we timed out waiting for its response.
          // Unconditionally force fromSvc to become active since it
          // was previously active when we initiated failover.
          failover(toSvc, toSvcAddr, fromSvc, fromSvcAddr, fencer, true, true);
        } catch (FailoverFailedException ffe) {
          msg += ". Failback to " + fromSvcAddr +
            " failed (" + ffe.getMessage() + ")";
          LOG.fatal(msg);
        }
      }
      throw new FailoverFailedException(msg, cause);
    }
  }
}
