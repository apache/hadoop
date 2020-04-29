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
package org.apache.hadoop.hdfs.server.federation.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.PeriodicService;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to periodically monitor the connection of the StateStore
 * {@link StateStoreService} data store and to re-open the connection
 * to the data store if required.
 */
public class StateStoreConnectionMonitorService extends PeriodicService {

  private static final Logger LOG =
      LoggerFactory.getLogger(StateStoreConnectionMonitorService.class);

  /** Service that maintains the State Store connection. */
  private final StateStoreService stateStore;


  /**
   * Create a new service to monitor the connectivity of the state store driver.
   *
   * @param store Instance of the state store to be monitored.
   */
  public StateStoreConnectionMonitorService(StateStoreService store) {
    super(StateStoreConnectionMonitorService.class.getSimpleName());
    this.stateStore = store;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.setIntervalMs(conf.getLong(
        RBFConfigKeys.FEDERATION_STORE_CONNECTION_TEST_MS,
        RBFConfigKeys.FEDERATION_STORE_CONNECTION_TEST_MS_DEFAULT));

    super.serviceInit(conf);
  }

  @Override
  public void periodicInvoke() {
    LOG.debug("Checking state store connection");
    if (!stateStore.isDriverReady()) {
      LOG.info("Attempting to open state store driver.");
      stateStore.loadDriver();
    }
  }
}