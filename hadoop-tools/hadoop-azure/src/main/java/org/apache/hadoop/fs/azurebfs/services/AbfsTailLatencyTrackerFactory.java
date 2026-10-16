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

package org.apache.hadoop.fs.azurebfs.services;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.util.WeakReferenceMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to get an instance of tail latency tracker class per account.
 */
final class AbfsTailLatencyTrackerFactory {

  private AbfsTailLatencyTrackerFactory() {
  }

  private static AbfsConfiguration abfsConfig;

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsTailLatencyTrackerFactory.class);

  /**
   * List of references notified of loss.
   */
  private static List<String> lostReferences = new ArrayList<>();

  /**
   * Map which stores instance of tail latency tracker class per account.
   */
  private static WeakReferenceMap<String, AbfsTailLatencyTracker>
      trackerMap = new WeakReferenceMap<>(
      AbfsTailLatencyTrackerFactory::factory,
      AbfsTailLatencyTrackerFactory::referenceLost);

  /**
   * Returns instance of tail latency tracker.
   * @param accountName Account name.
   * @return instance of tail latency tracker.
   */
  private static AbfsTailLatencyTracker factory(final String accountName) {
    return new AbfsTailLatencyTracker(abfsConfig);
  }

  /**
   * Reference lost callback.
   * @param accountName key lost.
   */
  private static void referenceLost(String accountName) {
    lostReferences.add(accountName);
  }

  /**
   * Returns an instance of {@link AbfsTailLatencyTracker}.
   *
   * @param accountName The account for which we need instance of throttling intercept.
   * @param abfsConfiguration The object of {@link AbfsConfiguration} class.
   * @return Instance of {@link AbfsTailLatencyTracker}.
   */
  static synchronized AbfsTailLatencyTracker getInstance(String accountName,
      AbfsConfiguration abfsConfiguration) {
    if (abfsConfiguration.isTailLatencyTrackerEnabled()) {
      abfsConfig = abfsConfiguration;
      AbfsTailLatencyTracker latencyTracker;
      // If singleton is enabled use a static instance of the intercept class for all accounts
      if (accountName.isEmpty()) {
        LOG.debug("Using singleton AbfsTailLatencyTracker for account agnostic tracker");
        latencyTracker = AbfsTailLatencyTracker.initializeSingleton(
            abfsConfiguration);
      } else {
        // Return the instance from the map
        latencyTracker = trackerMap.get(accountName);
        if (latencyTracker == null) {
          LOG.debug("Creating new account specific AbfsTailLatencyTracker for account: {}",
              accountName);
          latencyTracker = new AbfsTailLatencyTracker(abfsConfiguration);
          trackerMap.put(accountName, latencyTracker);
        }
      }
      return latencyTracker;
    }
    return null;
  }
}
