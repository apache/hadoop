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

import java.io.IOException;

/**
 * This class is used by {@link TestCsiAdaptorService} for testing.
 * It gives some dummy implementation for a adaptor plugin, and used to
 * verify the plugin can be properly loaded by NM and execution logic is
 * as expected.
 *
 * This is created as a separated class instead of an inner class, because
 * {@link CsiAdaptorServices} is loading classes using conf.getClass(),
 * the utility class is unable to resolve inner classes.
 */
public class MockCsiAdaptor implements CsiAdaptorPlugin {

  private String driverName;

  @Override
  public void init(String driverName, Configuration conf)
      throws YarnException {
    this.driverName = driverName;
  }

  @Override
  public String getDriverName() {
    return this.driverName;
  }

  @Override
  public GetPluginInfoResponse getPluginInfo(
      GetPluginInfoRequest request) throws YarnException, IOException {
    return GetPluginInfoResponse.newInstance(driverName,
        "1.0");
  }

  @Override
  public ValidateVolumeCapabilitiesResponse validateVolumeCapacity(
      ValidateVolumeCapabilitiesRequest request)
      throws YarnException, IOException {
    return ValidateVolumeCapabilitiesResponse.newInstance(true,
        "verified via MockCsiAdaptor");
  }

  @Override
  public NodePublishVolumeResponse nodePublishVolume(
      NodePublishVolumeRequest request) throws YarnException, IOException {
    return null;
  }

  @Override
  public NodeUnpublishVolumeResponse nodeUnpublishVolume(
      NodeUnpublishVolumeRequest request) throws YarnException, IOException {
    return null;
  }
}
