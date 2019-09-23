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

package org.apache.hadoop.yarn.service;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CancelUpgradeRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CancelUpgradeResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CompInstancesUpgradeResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CompInstancesUpgradeRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.DecommissionCompInstancesRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.DecommissionCompInstancesResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.FlexComponentsRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.FlexComponentsResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetCompInstancesRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetCompInstancesResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetStatusResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetStatusRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.RestartServiceRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.RestartServiceResponseProto;

import org.apache.hadoop.yarn.proto.ClientAMProtocol.StopResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.StopRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.UpgradeServiceRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.UpgradeServiceResponseProto;


import java.io.IOException;

public interface ClientAMProtocol {
  FlexComponentsResponseProto flexComponents(FlexComponentsRequestProto request)
      throws IOException, YarnException;

  GetStatusResponseProto getStatus(GetStatusRequestProto requestProto)
      throws IOException, YarnException;

  StopResponseProto stop(StopRequestProto requestProto)
      throws IOException, YarnException;

  UpgradeServiceResponseProto upgrade(UpgradeServiceRequestProto request)
      throws IOException, YarnException;

  RestartServiceResponseProto restart(RestartServiceRequestProto request)
      throws IOException, YarnException;

  CompInstancesUpgradeResponseProto upgrade(
      CompInstancesUpgradeRequestProto request) throws IOException,
      YarnException;

  GetCompInstancesResponseProto getCompInstances(
      GetCompInstancesRequestProto request) throws IOException, YarnException;

  CancelUpgradeResponseProto cancelUpgrade(
      CancelUpgradeRequestProto request) throws IOException, YarnException;

  DecommissionCompInstancesResponseProto decommissionCompInstances(
      DecommissionCompInstancesRequestProto request) throws IOException,
      YarnException;
}
