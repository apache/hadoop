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

package org.apache.hadoop.hdfs.server.federation.fairness;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;

/**
 * Base fairness policy that implements @RouterRpcFairnessPolicyController.
 * Internally a map of nameservice to Semaphore is used to control permits.
 */
public class AbstractRouterRpcFairnessPolicyController
    implements RouterRpcFairnessPolicyController {

  public static final Logger LOG =
      LoggerFactory.getLogger(AbstractRouterRpcFairnessPolicyController.class);

  /** Hash table to hold semaphore for each configured name service. */
  private Map<String, AbstractPermitManager> permits;

  /** Version to support dynamically change permits. **/
  private final int version;

  public static final String ERROR_MSG = "Configured handlers "
      + DFS_ROUTER_HANDLER_COUNT_KEY + '='
      + " %d is less than the minimum required handlers %d";

  AbstractRouterRpcFairnessPolicyController(int version) {
    permits = new HashMap<>();
    this.version = version;
  }

  /**
   * Init the permits.
   */
  public void initPermits(Map<String, AbstractPermitManager> newPermits) {
    this.permits = newPermits;
  }

  @Override
  public int getVersion() {
    return this.version;
  }

  @Override
  public Permit acquirePermit(String nsId) {
    LOG.debug("Taking lock for nameservice {}", nsId);
    AbstractPermitManager permitManager = permits.get(nsId);
    if (permitManager != null) {
      return permitManager.acquirePermit();
    } else {
      // TODO Add one metric to monitor this abnormal case.
      LOG.warn("Can't find NSPermit for {}.", nsId, new Throwable());
      return Permit.getNoPermit();
    }
  }

  @Override
  public void releasePermit(String nsId, Permit permitInstance) {
    if (permitInstance == null || permitInstance.isDontNeedPermit()) {
      return;
    }
    AbstractPermitManager permitManager =
        this.permits.get(nsId);
    if (permitManager != null) {
      permitManager.releasePermit(permitInstance);
    }
  }

  @Override
  public void shutdown() {
    LOG.debug("Shutting down router fairness policy controller");
    // drain all semaphores
    for (AbstractPermitManager permitManager: this.permits.values()) {
      permitManager.drainPermits();
    }
  }

  @Override
  public String getAvailableHandlerOnPerNs() {
    JSONObject json = new JSONObject();
    for (Map.Entry<String, AbstractPermitManager> entry : permits.entrySet()) {
      try {
        String nsId = entry.getKey();
        int availableHandler = entry.getValue().availablePermits();
        json.put(nsId, availableHandler);
      } catch (JSONException e) {
        LOG.warn("Cannot put {} into JSONObject", entry.getKey(), e);
      }
    }
    return json.toString();
  }

  protected int getDedicatedHandlers(Configuration conf, String nsId) {
    return conf.getInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + nsId, 1);
  }

  /**
   * Validate all configured dedicated handlers for the nameservices.
   * @return sum of dedicated handlers of all nameservices
   * @throws IllegalArgumentException
   *         if total dedicated handlers more than handler count.
   */
  protected int validateHandlersCount(Configuration conf,
      int handlerCount, Set<String> allConfiguredNS) {
    int totalDedicatedHandlers = 0;
    for (String nsId : allConfiguredNS) {
      int dedicatedHandlers = getDedicatedHandlers(conf, nsId);
      totalDedicatedHandlers += dedicatedHandlers;
    }
    if (totalDedicatedHandlers > handlerCount) {
      String msg = String.format(ERROR_MSG, handlerCount,
          totalDedicatedHandlers);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    return totalDedicatedHandlers;
  }
}
