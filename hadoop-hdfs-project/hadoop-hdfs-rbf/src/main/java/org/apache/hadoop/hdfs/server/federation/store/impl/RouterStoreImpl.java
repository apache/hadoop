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
package org.apache.hadoop.hdfs.server.federation.store.impl;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.RouterStore;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RouterHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RouterHeartbeatResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.Query;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;

/**
 * Implementation of the {@link RouterStore} state store API.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RouterStoreImpl extends RouterStore {

  public RouterStoreImpl(StateStoreDriver driver) {
    super(driver);
  }

  @Override
  public GetRouterRegistrationResponse getRouterRegistration(
      GetRouterRegistrationRequest request) throws IOException {

    final RouterState partial = RouterState.newInstance();
    partial.setAddress(request.getRouterId());
    final Query<RouterState> query = new Query<RouterState>(partial);
    RouterState record = getDriver().get(getRecordClass(), query);
    if (record != null) {
      overrideExpiredRecord(record);
    }
    GetRouterRegistrationResponse response =
        GetRouterRegistrationResponse.newInstance();
    response.setRouter(record);
    return response;
  }

  @Override
  public GetRouterRegistrationsResponse getRouterRegistrations(
      GetRouterRegistrationsRequest request) throws IOException {

    // Get all values from the cache
    QueryResult<RouterState> recordsAndTimeStamp =
        getCachedRecordsAndTimeStamp();
    List<RouterState> records = recordsAndTimeStamp.getRecords();
    long timestamp = recordsAndTimeStamp.getTimestamp();

    // Generate response
    GetRouterRegistrationsResponse response =
        GetRouterRegistrationsResponse.newInstance();
    response.setRouters(records);
    response.setTimestamp(timestamp);
    return response;
  }

  @Override
  public RouterHeartbeatResponse routerHeartbeat(RouterHeartbeatRequest request)
      throws IOException {

    RouterState record = request.getRouter();
    boolean status = getDriver().put(record, true, false);
    RouterHeartbeatResponse response =
        RouterHeartbeatResponse.newInstance(status);
    return response;
  }
}
