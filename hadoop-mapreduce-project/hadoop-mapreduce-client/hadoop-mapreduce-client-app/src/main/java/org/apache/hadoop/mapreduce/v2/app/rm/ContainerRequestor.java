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
package org.apache.hadoop.mapreduce.v2.app.rm;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerRequestor.ContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * Interface for ContainerReqestor.
 */
public interface ContainerRequestor {

  AllocateResponse makeRemoteRequest()
      throws YarnRuntimeException, YarnException, IOException;

  void addContainerReq(ContainerRequest request);

  void decContainerReq(ContainerRequest request);

  void containerAssigned(Container allocated, ContainerRequest assigned,
      Map<ApplicationAccessType, String> acls);

  void release(ContainerId containerId);

  boolean isNodeBlacklisted(String hostname);

  Resource getAvailableResources();

  void containerFailedOnHost(String hostName);

  ContainerRequest filterRequest(ContainerRequest orig);
}