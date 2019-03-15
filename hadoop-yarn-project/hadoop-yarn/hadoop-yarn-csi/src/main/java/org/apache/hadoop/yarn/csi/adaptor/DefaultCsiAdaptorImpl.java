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

import csi.v0.Csi;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.CsiAdaptorPlugin;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NodePublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NodePublishVolumeResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NodeUnpublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NodeUnpublishVolumeResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesResponse;
import org.apache.hadoop.yarn.csi.client.CsiClient;
import org.apache.hadoop.yarn.csi.client.CsiClientImpl;
import org.apache.hadoop.yarn.csi.translator.ProtoTranslatorFactory;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.csi.CsiConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The default implementation of csi-driver-adaptor service.
 */
public class DefaultCsiAdaptorImpl implements CsiAdaptorPlugin {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultCsiAdaptorImpl.class);

  private String driverName;
  private CsiClient csiClient;

  public DefaultCsiAdaptorImpl() {
    // use default constructor for reflection
  }

  @Override
  public void init(String driverName, Configuration conf)
      throws YarnException {
    // if the driver end point is invalid, following code will fail.
    String driverEndpoint = CsiConfigUtils
        .getCsiDriverEndpoint(driverName, conf);
    LOG.info("This csi-adaptor is configured to contact with"
            + " the csi-driver {} via gRPC endpoint: {}",
        driverName, driverEndpoint);
    this.csiClient = new CsiClientImpl(driverEndpoint);
    this.driverName = driverName;
  }

  @Override
  public String getDriverName() {
    return driverName;
  }

  @Override
  public GetPluginInfoResponse getPluginInfo(
      GetPluginInfoRequest request) throws YarnException, IOException {
    Csi.GetPluginInfoResponse response = csiClient.getPluginInfo();
    return ProtoTranslatorFactory.getTranslator(
        GetPluginInfoResponse.class, Csi.GetPluginInfoResponse.class)
        .convertFrom(response);
  }

  @Override
  public ValidateVolumeCapabilitiesResponse validateVolumeCapacity(
      ValidateVolumeCapabilitiesRequest request) throws YarnException,
      IOException {
    Csi.ValidateVolumeCapabilitiesRequest req = ProtoTranslatorFactory
        .getTranslator(ValidateVolumeCapabilitiesRequest.class,
            Csi.ValidateVolumeCapabilitiesRequest.class)
        .convertTo(request);
    Csi.ValidateVolumeCapabilitiesResponse response =
        csiClient.validateVolumeCapabilities(req);
    return ProtoTranslatorFactory.getTranslator(
        ValidateVolumeCapabilitiesResponse.class,
        Csi.ValidateVolumeCapabilitiesResponse.class)
        .convertFrom(response);
  }

  @Override
  public NodePublishVolumeResponse nodePublishVolume(
      NodePublishVolumeRequest request) throws YarnException, IOException {
    LOG.debug("Received nodePublishVolume call, request: {}",
        request);
    Csi.NodePublishVolumeRequest req = ProtoTranslatorFactory
        .getTranslator(NodePublishVolumeRequest.class,
            Csi.NodePublishVolumeRequest.class).convertTo(request);
    LOG.debug("Translate to CSI proto message: {}", req);
    csiClient.nodePublishVolume(req);
    return NodePublishVolumeResponse.newInstance();
  }

  @Override
  public NodeUnpublishVolumeResponse nodeUnpublishVolume(
      NodeUnpublishVolumeRequest request) throws YarnException, IOException {
    LOG.debug("Received nodeUnpublishVolume call, request: {}",
        request);
    Csi.NodeUnpublishVolumeRequest req = ProtoTranslatorFactory
        .getTranslator(NodeUnpublishVolumeRequest.class,
            Csi.NodeUnpublishVolumeRequest.class).convertTo(request);
    LOG.debug("Translate to CSI proto message: {}", req);
    csiClient.nodeUnpublishVolume(req);
    return NodeUnpublishVolumeResponse.newInstance();
  }
}
