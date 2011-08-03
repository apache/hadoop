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

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;

public class LocalRMInterface implements ResourceTracker {

  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  
  @Override
  public RegisterNodeManagerResponse registerNodeManager(RegisterNodeManagerRequest request) throws YarnRemoteException {
    RegistrationResponse registrationResponse = recordFactory.newRecordInstance(RegistrationResponse.class);
    RegisterNodeManagerResponse response = recordFactory.newRecordInstance(RegisterNodeManagerResponse.class);
    response.setRegistrationResponse(registrationResponse);
    return response;
  }

  @Override
  public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request) throws YarnRemoteException {
    NodeHeartbeatResponse response = recordFactory.newRecordInstance(NodeHeartbeatResponse.class);
    return response;
  }
}