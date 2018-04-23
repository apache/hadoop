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
 * API request for disabling a name service and updating its state in the
 * State Store.
 */
public abstract class DisableNameserviceRequest {

  public static DisableNameserviceRequest newInstance() {
    return StateStoreSerializer.newRecord(DisableNameserviceRequest.class);
  }

  public static DisableNameserviceRequest newInstance(String nsId) {
    DisableNameserviceRequest request = newInstance();
    request.setNameServiceId(nsId);
    return request;
  }

  @Public
  @Unstable
  public abstract String getNameServiceId();

  @Public
  @Unstable
  public abstract void setNameServiceId(String nsId);
}
