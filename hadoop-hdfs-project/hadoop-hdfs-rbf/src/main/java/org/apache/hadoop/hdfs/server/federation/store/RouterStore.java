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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RouterHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RouterHeartbeatResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;

/**
 * Management API for
 * {@link org.apache.hadoop.hdfs.server.federation.store.records.RouterState
 *  RouterState} records in the state store. Accesses the data store via the
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.
 * StateStoreDriver StateStoreDriver} interface. No data is cached.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class RouterStore extends CachedRecordStore<RouterState> {

  public RouterStore(StateStoreDriver driver) {
    super(RouterState.class, driver, true);
  }

  /**
   * Fetches the current router state object.
   *
   * @param request Fully populated request object.
   * @return The matching router record or null if none exists.
   * @throws IOException Throws exception if unable to query the data store or
   *           if more than one matching record is found.
   */
  public abstract GetRouterRegistrationResponse getRouterRegistration(
      GetRouterRegistrationRequest request) throws IOException;

  /**
   * Fetches all router status objects.
   *
   * @param request Fully populated request object.
   * @return List of Router records present in the data store.
   * @throws IOException Throws exception if unable to query the data store
   */
  public abstract GetRouterRegistrationsResponse getRouterRegistrations(
      GetRouterRegistrationsRequest request) throws IOException;

  /**
   * Update the state of this router in the State Store.
   *
   * @param request Fully populated request object.
   * @return True if the update was successfully recorded, false otherwise.
   * @throws IOException Throws exception if unable to query the data store
   */
  public abstract RouterHeartbeatResponse routerHeartbeat(
      RouterHeartbeatRequest request) throws IOException;
}
