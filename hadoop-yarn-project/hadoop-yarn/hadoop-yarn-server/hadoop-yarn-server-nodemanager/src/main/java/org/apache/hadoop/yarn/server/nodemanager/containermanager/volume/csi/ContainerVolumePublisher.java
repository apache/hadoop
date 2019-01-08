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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.volume.csi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.NodePublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NodeUnpublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.volume.csi.CsiConstants;
import org.apache.hadoop.yarn.server.volume.csi.VolumeMetaData;
import org.apache.hadoop.yarn.server.volume.csi.exception.InvalidVolumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Publish/un-publish CSI volumes on node manager.
 */
public class ContainerVolumePublisher {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerVolumePublisher.class);

  private final Container container;
  private final String localMountRoot;
  private final DockerLinuxContainerRuntime runtime;

  public ContainerVolumePublisher(Container container, String localMountRoot,
      DockerLinuxContainerRuntime runtime) {
    LOG.info("Initiate container volume publisher, containerID={},"
            + " volume local mount rootDir={}",
        container.getContainerId().toString(), localMountRoot);
    this.container = container;
    this.localMountRoot = localMountRoot;
    this.runtime = runtime;
  }

  /**
   * It first discovers the volume info from container resource;
   * then negotiates with CSI driver adaptor to publish the volume on this
   * node manager, on a specific directory under container's work dir;
   * and then map the local mounted directory to volume target mount in
   * the docker container.
   *
   * CSI volume publish is a two phase work, by reaching up here
   * we can assume the 1st phase is done on the RM side, which means
   * YARN is already called the controller service of csi-driver
   * to publish the volume; here we only need to call the node service of
   * csi-driver to publish the volume on this local node manager.
   *
   * @return a map where each key is the local mounted path on current node,
   *   and value is the remote mount path on the container.
   * @throws YarnException
   * @throws IOException
   */
  public Map<String, String> publishVolumes() throws YarnException,
      IOException {
    LOG.info("publishing volumes");
    Map<String, String> volumeMounts = new HashMap<>();
    List<VolumeMetaData> volumes = getVolumes();
    LOG.info("Found {} volumes to be published on this node", volumes.size());
    for (VolumeMetaData volume : volumes) {
      Map<String, String> bindings = publishVolume(volume);
      if (bindings != null && !bindings.isEmpty()) {
        volumeMounts.putAll(bindings);
      }
    }
    return volumeMounts;
  }

  public void unpublishVolumes() throws YarnException, IOException {
    LOG.info("Un-publishing Volumes");
    List<VolumeMetaData> volumes = getVolumes();
    LOG.info("Volumes to un-publish {}", volumes.size());
    for (VolumeMetaData volume : volumes) {
      this.unpublishVolume(volume);
    }
  }

  private File getLocalVolumeMountPath(
      String containerWorkDir, String volumeId) {
    return new File(containerWorkDir, volumeId + "_mount");
  }

  private File getLocalVolumeStagingPath(
      String containerWorkDir, String volumeId) {
    return new File(containerWorkDir, volumeId + "_staging");
  }

  private List<VolumeMetaData> getVolumes() throws InvalidVolumeException {
    List<VolumeMetaData> volumes = new ArrayList<>();
    Resource containerResource = container.getResource();
    if (containerResource != null) {
      for (ResourceInformation resourceInformation :
          containerResource.getAllResourcesListCopy()) {
        if (resourceInformation.getTags()
            .contains(CsiConstants.CSI_VOLUME_RESOURCE_TAG)) {
          volumes.addAll(VolumeMetaData.fromResource(resourceInformation));
        }
      }
    }
    if (volumes.size() > 0) {
      LOG.info("Total number of volumes require provisioning is {}",
          volumes.size());
    }
    return volumes;
  }

  private Map<String, String> publishVolume(VolumeMetaData volume)
      throws IOException, YarnException {
    Map<String, String> bindVolumes = new HashMap<>();
    // compose a local mount for CSI volume with the container ID
    File localMount = getLocalVolumeMountPath(
        localMountRoot, volume.getVolumeId().toString());
    File localStaging = getLocalVolumeStagingPath(
        localMountRoot, volume.getVolumeId().toString());
    LOG.info("Volume {}, local mount path: {}, local staging path {}",
        volume.getVolumeId().toString(), localMount, localStaging);

    NodePublishVolumeRequest publishRequest = NodePublishVolumeRequest
        .newInstance(volume.getVolumeId().getId(), // volume Id
            false, // read only flag
            localMount.getAbsolutePath(), // target path
            localStaging.getAbsolutePath(), // staging path
            new ValidateVolumeCapabilitiesRequest.VolumeCapability(
                ValidateVolumeCapabilitiesRequest
                    .AccessMode.SINGLE_NODE_WRITER,
                ValidateVolumeCapabilitiesRequest.VolumeType.FILE_SYSTEM,
                ImmutableList.of()), // capability
            ImmutableMap.of(), // publish context
            ImmutableMap.of());  // secrets

    // make sure the volume is a known type
    if (runtime.getCsiClients().get(volume.getDriverName()) == null) {
      throw new YarnException("No csi-adaptor is found that can talk"
          + " to csi-driver " + volume.getDriverName());
    }

    // publish volume to node
    LOG.info("Publish volume on NM, request {}",
        publishRequest.toString());
    runtime.getCsiClients().get(volume.getDriverName())
        .nodePublishVolume(publishRequest);
    // once succeed, bind the container to this mount
    String containerMountPath = volume.getMountPoint();
    bindVolumes.put(localMount.getAbsolutePath(), containerMountPath);
    return bindVolumes;
  }

  private void unpublishVolume(VolumeMetaData volume)
      throws YarnException, IOException {
    CsiAdaptorProtocol csiClient =
        runtime.getCsiClients().get(volume.getDriverName());
    if (csiClient == null) {
      throw new YarnException(
          "No csi-adaptor is found that can talk"
              + " to csi-driver " + volume.getDriverName());
    }

    // When container is launched, the container work dir is memorized,
    // and that is also the dir we mount the volume to.
    File localMount = getLocalVolumeMountPath(container.getCsiVolumesRootDir(),
        volume.getVolumeId().toString());
    if (!localMount.exists()) {
      LOG.info("Local mount {} no longer exist, skipping cleaning"
          + " up the volume", localMount.getAbsolutePath());
      return;
    }
    NodeUnpublishVolumeRequest unpublishRequest =
        NodeUnpublishVolumeRequest.newInstance(
            volume.getVolumeId().getId(), // volume id
            localMount.getAbsolutePath());  // target path

    // un-publish volume from node
    LOG.info("Un-publish volume {}, request {}",
        volume.getVolumeId().toString(), unpublishRequest.toString());
    csiClient.nodeUnpublishVolume(unpublishRequest);
  }
}
