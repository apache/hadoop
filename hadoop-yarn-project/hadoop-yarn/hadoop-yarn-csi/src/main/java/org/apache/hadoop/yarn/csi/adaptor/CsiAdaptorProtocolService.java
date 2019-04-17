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
package org.apache.hadoop.yarn.csi.adaptor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.api.CsiAdaptorPlugin;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NodePublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NodePublishVolumeResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NodeUnpublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NodeUnpublishVolumeResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.csi.CsiConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * This is a Hadoop RPC server, we uses the Hadoop RPC framework here
 * because we need to stick to the security model current Hadoop supports.
 */
public class CsiAdaptorProtocolService extends AbstractService
    implements CsiAdaptorProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(CsiAdaptorProtocolService.class);

  private Server server;
  private InetSocketAddress adaptorServiceAddress;
  private CsiAdaptorPlugin serverImpl;

  public CsiAdaptorProtocolService(CsiAdaptorPlugin adaptorImpl) {
    super(CsiAdaptorProtocolService.class.getName());
    this.serverImpl = adaptorImpl;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    adaptorServiceAddress = CsiConfigUtils
        .getCsiAdaptorAddressForDriver(serverImpl.getDriverName(), conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    this.server = rpc.getServer(
        CsiAdaptorProtocol.class,
        serverImpl, adaptorServiceAddress, conf, null, 1);
    this.server.start();
    LOG.info("{} started, listening on address: {}",
        CsiAdaptorProtocolService.class.getName(),
        adaptorServiceAddress.toString());
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }
    super.serviceStop();
  }

  @Override
  public GetPluginInfoResponse getPluginInfo(
      GetPluginInfoRequest request) throws YarnException, IOException {
    return serverImpl.getPluginInfo(request);
  }

  @Override
  public ValidateVolumeCapabilitiesResponse validateVolumeCapacity(
      ValidateVolumeCapabilitiesRequest request) throws YarnException,
      IOException {
    return serverImpl.validateVolumeCapacity(request);
  }

  @Override
  public NodePublishVolumeResponse nodePublishVolume(
      NodePublishVolumeRequest request) throws YarnException, IOException {
    return serverImpl.nodePublishVolume(request);
  }

  @Override
  public NodeUnpublishVolumeResponse nodeUnpublishVolume(
      NodeUnpublishVolumeRequest request) throws YarnException, IOException {
    return serverImpl.nodeUnpublishVolume(request);
  }
}
