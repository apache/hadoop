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

package org.apache.hadoop.yarn.api.impl.pb.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.MaintenanceTracker;
import org.apache.hadoop.yarn.api.MaintenanceTrackerPB;
import org.apache.hadoop.yarn.api.protocolrecords.SetMaintenanceModeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SetMaintenanceModeResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SetMaintenanceModeRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SetMaintenanceModeResponsePBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SetMaintenanceModeRequestProto;

import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * Client to set a node into Decommissioning mode.
 */
public class MaintenanceTrackerPBClientImpl
    implements MaintenanceTracker, Closeable {
  private MaintenanceTrackerPB proxy;

  public MaintenanceTrackerPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, MaintenanceTrackerPB.class,
        ProtobufRpcEngine2.class);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    this.proxy = (MaintenanceTrackerPB) RPC
        .getProxy(MaintenanceTrackerPB.class, clientVersion, addr, ugi, conf,
            NetUtils.getDefaultSocketFactory(conf),
            conf.getInt(YarnConfiguration.MAINTENANCE_COMMAND_TIMEOUT,
                YarnConfiguration.DEFAULT_MAINTENANCE_COMMAND_TIMEOUT_MS));
  }

  @Override public void close() throws IOException {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override public SetMaintenanceModeResponse setMaintenanceMode(
      SetMaintenanceModeRequest request) throws YarnException, IOException {
    SetMaintenanceModeRequestProto protoRequest =
        ((SetMaintenanceModeRequestPBImpl) request).getProto();
    try {
      return new SetMaintenanceModeResponsePBImpl(
          this.proxy.setMaintenanceMode(null, protoRequest));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
