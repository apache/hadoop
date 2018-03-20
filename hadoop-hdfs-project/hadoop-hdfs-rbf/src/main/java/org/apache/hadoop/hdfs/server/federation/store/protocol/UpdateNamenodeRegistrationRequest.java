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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;

/**
 * API request for overriding an existing namenode registration in the state
 * store.
 */
public abstract class UpdateNamenodeRegistrationRequest {

  public static UpdateNamenodeRegistrationRequest newInstance()
      throws IOException {
    return StateStoreSerializer.newRecord(
        UpdateNamenodeRegistrationRequest.class);
  }

  public static UpdateNamenodeRegistrationRequest newInstance(
      String nameserviceId, String namenodeId,
      FederationNamenodeServiceState state) throws IOException {
    UpdateNamenodeRegistrationRequest request = newInstance();
    request.setNameserviceId(nameserviceId);
    request.setNamenodeId(namenodeId);
    request.setState(state);
    return request;
  }

  @Private
  @Unstable
  public abstract String getNameserviceId();

  @Private
  @Unstable
  public abstract String getNamenodeId();

  @Private
  @Unstable
  public abstract FederationNamenodeServiceState getState();

  @Private
  @Unstable
  public abstract void setNameserviceId(String nsId);

  @Private
  @Unstable
  public abstract void setNamenodeId(String nnId);

  @Private
  @Unstable
  public abstract void setState(FederationNamenodeServiceState state);
}