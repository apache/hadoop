/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.api.impl.pb.service;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.yarn.api.MaintenanceTracker;
import org.apache.hadoop.yarn.api.MaintenanceTrackerPB;
import org.apache.hadoop.yarn.api.protocolrecords.SetMaintenanceModeResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SetMaintenanceModeRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SetMaintenanceModeResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;

import java.io.IOException;

public class MaintenanceTrackerPBServiceImpl implements MaintenanceTrackerPB {

  private MaintenanceTracker real;

  public MaintenanceTrackerPBServiceImpl(MaintenanceTracker impl) {
    this.real = impl;
  }

  @Override
  public YarnServiceProtos.SetMaintenanceModeResponseProto setMaintenanceMode(
      RpcController controller,
      YarnServiceProtos.SetMaintenanceModeRequestProto proto)
      throws ServiceException {
    SetMaintenanceModeRequestPBImpl request =
        new SetMaintenanceModeRequestPBImpl(proto);
    SetMaintenanceModeResponse response;
    try {
      response = real.setMaintenanceMode(request);
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return ((SetMaintenanceModeResponsePBImpl) response).getProto();
  }
}
