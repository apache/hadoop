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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;

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
   * @param toSvc service to make active
   * @param toSvcName name of service to make active
   * @throws FailoverFailedException if we should avoid failover
   */
  private static void preFailoverChecks(HAServiceProtocol toSvc,
                                        String toSvcName)
      throws FailoverFailedException {
    HAServiceState toSvcState;
    try {
      toSvcState = toSvc.getServiceState();
    } catch (Exception e) {
      String msg = "Unable to get service state for " + toSvcName;
      LOG.error(msg, e);
      throw new FailoverFailedException(msg, e);
    }
    if (!toSvcState.equals(HAServiceState.STANDBY)) {
      throw new FailoverFailedException(
          "Can't failover to an active service");
    }
    try {
      toSvc.monitorHealth();
    } catch (HealthCheckFailedException hce) {
      throw new FailoverFailedException(
          "Can't failover to an unhealthy service", hce);
    }
    // TODO(HA): ask toSvc if it's capable. Eg not in SM.
  }

  /**
   * Failover from service 1 to service 2. If the failover fails
   * then try to failback.
   *
   * @param fromSvc currently active service
   * @param fromSvcName name of currently active service
   * @param toSvc service to make active
   * @param toSvcName name of service to make active
   * @throws FailoverFailedException if the failover fails
   */
  public static void failover(HAServiceProtocol fromSvc, String fromSvcName,
                              HAServiceProtocol toSvc, String toSvcName)
      throws FailoverFailedException {
    preFailoverChecks(toSvc, toSvcName);

    // Try to make fromSvc standby
    try {
      fromSvc.transitionToStandby();
    } catch (ServiceFailedException sfe) {
      LOG.warn("Unable to make " + fromSvcName + " standby (" +
          sfe.getMessage() + ")");
    } catch (Exception e) {
      LOG.warn("Unable to make " + fromSvcName +
          " standby (unable to connect)", e);
      // TODO(HA): fence fromSvc and unfence on failback
    }

    // Try to make toSvc active
    boolean failed = false;
    Throwable cause = null;
    try {
      toSvc.transitionToActive();
    } catch (ServiceFailedException sfe) {
      LOG.error("Unable to make " + toSvcName + " active (" +
          sfe.getMessage() + "). Failing back");
      failed = true;
      cause = sfe;
    } catch (Exception e) {
      LOG.error("Unable to make " + toSvcName +
          " active (unable to connect). Failing back", e);
      failed = true;
      cause = e;
    }

    // Try to failback if we failed to make toSvc active
    if (failed) {
      String msg = "Unable to failover to " + toSvcName;
      try {
        fromSvc.transitionToActive();
      } catch (ServiceFailedException sfe) {
        msg = "Failback to " + fromSvcName + " failed (" +
              sfe.getMessage() + ")";
        LOG.fatal(msg);
      } catch (Exception e) {
        msg = "Failback to " + fromSvcName + " failed (unable to connect)";
        LOG.fatal(msg);
      }
      throw new FailoverFailedException(msg, cause);
    }
  }
}
