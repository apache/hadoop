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
package org.apache.hadoop.hdfs.server.federation.store.protocol;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;

/**
 * API request for retrieving a single router registration present in the state
 * store.
 */
public abstract class GetRouterRegistrationRequest {

  public static GetRouterRegistrationRequest newInstance() {
    return StateStoreSerializer.newRecord(GetRouterRegistrationRequest.class);
  }

  public static GetRouterRegistrationRequest newInstance(String routerId) {
    GetRouterRegistrationRequest request = newInstance();
    request.setRouterId(routerId);
    return request;
  }

  @Public
  @Unstable
  public abstract String getRouterId();

  @Public
  @Unstable
  public abstract void setRouterId(String routerId);
}