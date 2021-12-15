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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base fairness policy that implements @RouterRpcFairnessPolicyController.
 * Internally a map of nameservice to Semaphore is used to control permits.
 */
public class AbstractRouterRpcFairnessPolicyController
    implements RouterRpcFairnessPolicyController {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractRouterRpcFairnessPolicyController.class);

  /** Hash table to hold semaphore for each configured name service. */
  private Map<String, Semaphore> permits;

  public void init(Configuration conf) {
    this.permits = new HashMap<>();
  }

  @Override
  public boolean acquirePermit(String nsId) {
    try {
      LOG.debug("Taking lock for nameservice {}", nsId);
      return this.permits.get(nsId).tryAcquire(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.debug("Cannot get a permit for nameservice {}", nsId);
    }
    return false;
  }

  @Override
  public void releasePermit(String nsId) {
    this.permits.get(nsId).release();
  }

  @Override
  public void shutdown() {
    // drain all semaphores
    for (Semaphore sema: this.permits.values()) {
      sema.drainPermits();
    }
  }

  protected void insertNameServiceWithPermits(String nsId, int maxPermits) {
    this.permits.put(nsId, new Semaphore(maxPermits));
  }

  protected int getAvailablePermits(String nsId) {
    return this.permits.get(nsId).availablePermits();
  }

  @Override
  public String getAvailableHandlerOnPerNs() {
    JSONObject json = new JSONObject();
    for (Map.Entry<String, Semaphore> entry : permits.entrySet()) {
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
}
