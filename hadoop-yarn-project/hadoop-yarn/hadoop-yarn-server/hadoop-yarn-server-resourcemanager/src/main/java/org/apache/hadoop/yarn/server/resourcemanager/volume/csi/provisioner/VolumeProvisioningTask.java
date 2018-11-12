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
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner;

import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.event.ControllerPublishVolumeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.event.ValidateVolumeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.Volume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A provisioning task encapsulates all the logic required by a storage system
 * to provision a volume. This class is the common implementation, it might
 * be override if the provisioning behavior of a certain storage system
 * is not completely align with this implementation.
 */
public class VolumeProvisioningTask implements VolumeProvisioner {

  private static final Logger LOG =  LoggerFactory
      .getLogger(VolumeProvisioningTask.class);

  private List<Volume> volumes;

  public VolumeProvisioningTask(List<Volume> volumes) {
    this.volumes = volumes;
  }

  public List<Volume> getVolumes() {
    return this.volumes;
  }

  @Override
  public VolumeProvisioningResults call() throws Exception {
    VolumeProvisioningResults vpr = new VolumeProvisioningResults();

    // Wait all volumes are reaching expected state
    for (Volume vs : volumes) {
      LOG.info("Provisioning volume : {}", vs.getVolumeId().toString());
      vs.handle(new ValidateVolumeEvent(vs));
      vs.handle(new ControllerPublishVolumeEvent(vs));
    }

    // collect results
    volumes.stream().forEach(v ->
        vpr.addResult(v.getVolumeId(), v.getVolumeState()));

    return vpr;
  }
}
