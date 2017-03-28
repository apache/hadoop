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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.CompositeService;

/**
 * A service to initialize a
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver
 * StateStoreDriver} and maintain the connection to the data store. There
 * are multiple state store driver connections supported:
 * <ul>
 * <li>File {@link org.apache.hadoop.hdfs.server.federation.store.driver.impl.
 * StateStoreFileImpl StateStoreFileImpl}
 * <li>ZooKeeper {@link org.apache.hadoop.hdfs.server.federation.store.driver.
 * impl.StateStoreZooKeeperImpl StateStoreZooKeeperImpl}
 * </ul>
 * <p>
 * The service also supports the dynamic registration of data interfaces such as
 * the following:
 * <ul>
 * <li>{@link MembershipStateStore}: state of the Namenodes in the
 * federation.
 * <li>{@link MountTableStore}: Mount table between to subclusters.
 * See {@link org.apache.hadoop.fs.viewfs.ViewFs ViewFs}.
 * <li>{@link RouterStateStore}: State of the routers in the federation.
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class StateStoreService extends CompositeService {

  /** Identifier for the service. */
  private String identifier;

  // Stub class
  public StateStoreService(String name) {
    super(name);
  }

  /**
   * Fetch a unique identifier for this state store instance. Typically it is
   * the address of the router.
   *
   * @return Unique identifier for this store.
   */
  public String getIdentifier() {
    return this.identifier;
  }

  /**
   * Set a unique synchronization identifier for this store.
   *
   * @param id Unique identifier, typically the router's RPC address.
   */
  public void setIdentifier(String id) {
    this.identifier = id;
  }
}
