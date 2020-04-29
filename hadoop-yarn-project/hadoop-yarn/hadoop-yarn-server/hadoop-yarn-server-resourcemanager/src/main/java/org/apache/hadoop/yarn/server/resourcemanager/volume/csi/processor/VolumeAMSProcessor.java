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
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi.processor;

import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.VolumeManager;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.Volume;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.VolumeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner.VolumeProvisioningResults;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner.VolumeProvisioningTask;
import org.apache.hadoop.yarn.server.volume.csi.VolumeMetaData;
import org.apache.hadoop.yarn.server.volume.csi.exception.InvalidVolumeException;
import org.apache.hadoop.yarn.server.volume.csi.exception.VolumeException;
import org.apache.hadoop.yarn.server.volume.csi.exception.VolumeProvisioningException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * AMS processor that handles volume resource requests.
 *
 */
public class VolumeAMSProcessor implements ApplicationMasterServiceProcessor {

  private static final Logger LOG =  LoggerFactory
      .getLogger(VolumeAMSProcessor.class);

  private ApplicationMasterServiceProcessor nextAMSProcessor;
  private VolumeManager volumeManager;

  @Override
  public void init(ApplicationMasterServiceContext amsContext,
      ApplicationMasterServiceProcessor nextProcessor) {
    LOG.info("Initializing CSI volume processor");
    this.nextAMSProcessor = nextProcessor;
    this.volumeManager = ((RMContext) amsContext).getVolumeManager();
  }

  @Override
  public void registerApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      RegisterApplicationMasterRequest request,
      RegisterApplicationMasterResponse response)
      throws IOException, YarnException {
    this.nextAMSProcessor.registerApplicationMaster(applicationAttemptId,
        request, response);
  }

  @Override
  public void allocate(ApplicationAttemptId appAttemptId,
      AllocateRequest request, AllocateResponse response) throws YarnException {
    List<Volume> volumes = aggregateVolumesFrom(request);
    if (volumes != null && volumes.size() > 0) {
      ScheduledFuture<VolumeProvisioningResults> result =
          this.volumeManager.schedule(new VolumeProvisioningTask(volumes), 0);
      try {
        VolumeProvisioningResults volumeResult =
            result.get(3, TimeUnit.SECONDS);
        if (!volumeResult.isSuccess()) {
          throw new VolumeProvisioningException("Volume provisioning failed,"
              + " result details: " + volumeResult.getBriefMessage());
        }
      } catch (TimeoutException | InterruptedException | ExecutionException e) {
        LOG.warn("Volume provisioning task failed", e);
        throw new VolumeException("Volume provisioning task failed", e);
      }
    }

    // Go to next processor
    this.nextAMSProcessor.allocate(appAttemptId, request, response);
  }

  // Currently only scheduling request is supported.
  private List<Volume> aggregateVolumesFrom(AllocateRequest request)
      throws VolumeException {
    List<Volume> volumeList = new ArrayList<>();
    List<SchedulingRequest> requests = request.getSchedulingRequests();
    if (requests != null) {
      for (SchedulingRequest req : requests) {
        Resource totalResource = req.getResourceSizing().getResources();
        List<ResourceInformation> resourceList =
            totalResource.getAllResourcesListCopy();
        for (ResourceInformation resourceInformation : resourceList) {
          List<VolumeMetaData> volumes =
              VolumeMetaData.fromResource(resourceInformation);
          for (VolumeMetaData vs : volumes) {
            if (vs.getVolumeCapabilityRange().getMinCapacity() <= 0) {
              // capacity not specified, ignore
              continue;
            } else if (vs.isProvisionedVolume()) {
              volumeList.add(checkAndGetVolume(vs));
            } else {
              throw new InvalidVolumeException("Only pre-provisioned volume"
                  + " is supported now, volumeID must exist.");
            }
          }
        }
      }
    }
    return volumeList;
  }

  /**
   * If given volume ID already exists in the volume manager,
   * it returns the existing volume. Otherwise, it creates a new
   * volume and add that to volume manager.
   * @param metaData
   * @return volume
   */
  private Volume checkAndGetVolume(VolumeMetaData metaData)
      throws InvalidVolumeException {
    Volume toAdd = new VolumeImpl(metaData);
    CsiAdaptorProtocol adaptor = volumeManager
        .getAdaptorByDriverName(metaData.getDriverName());
    if (adaptor == null) {
      throw new InvalidVolumeException("It seems for the driver name"
          + " specified in the volume " + metaData.getDriverName()
          + " ,there is no matched driver-adaptor can be found. "
          + "Is the driver probably registered? Please check if"
          + " adaptors service addresses defined in "
          + YarnConfiguration.NM_CSI_ADAPTOR_ADDRESSES
          + " are correct and services are started.");
    }
    toAdd.setClient(adaptor);
    return this.volumeManager.addOrGetVolume(toAdd);
  }

  @Override
  public void finishApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      FinishApplicationMasterRequest request,
      FinishApplicationMasterResponse response) {
    this.nextAMSProcessor.finishApplicationMaster(applicationAttemptId,
        request, response);
  }
}
