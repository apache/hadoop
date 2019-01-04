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
package org.apache.hadoop.yarn.csi.translator;

import csi.v0.Csi;
import org.apache.hadoop.yarn.api.protocolrecords.NodePublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * This class helps to transform a YARN side NodePublishVolumeRequest
 * to corresponding CSI protocol message.
 * @param <A> YARN NodePublishVolumeRequest
 * @param <B> CSI NodePublishVolumeRequest
 */
public class NodePublishVolumeRequestProtoTranslator<A, B> implements
    ProtoTranslator<NodePublishVolumeRequest,
        Csi.NodePublishVolumeRequest> {

  @Override
  public Csi.NodePublishVolumeRequest convertTo(
      NodePublishVolumeRequest messageA) throws YarnException {
    Csi.NodePublishVolumeRequest.Builder builder =
        Csi.NodePublishVolumeRequest.newBuilder();
    ValidateVolumeCapabilitiesRequest.VolumeCapability cap =
        messageA.getVolumeCapability();
    Csi.VolumeCapability csiVolumeCap = Csi.VolumeCapability.newBuilder()
        .setAccessMode(Csi.VolumeCapability.AccessMode.newBuilder()
            .setModeValue(cap.getAccessMode().ordinal())) // access mode
        // TODO support block
        .setMount(Csi.VolumeCapability.MountVolume.newBuilder()
            // TODO support fsType
            .setFsType("xfs") // fs type
            .addAllMountFlags(cap.getMountFlags())) // mount flags
        .build();
    builder.setVolumeCapability(csiVolumeCap);
    builder.setVolumeId(messageA.getVolumeId());
    builder.setTargetPath(messageA.getTargetPath());
    builder.setReadonly(messageA.getReadOnly());
    builder.putAllNodePublishSecrets(messageA.getSecrets());
    builder.putAllPublishInfo(messageA.getPublishContext());
    builder.setStagingTargetPath(messageA.getStagingPath());
    return builder.build();
  }

  @Override
  public NodePublishVolumeRequest convertFrom(
      Csi.NodePublishVolumeRequest messageB) throws YarnException {
    Csi.VolumeCapability cap0 = messageB.getVolumeCapability();
    ValidateVolumeCapabilitiesRequest.VolumeCapability cap =
        new ValidateVolumeCapabilitiesRequest.VolumeCapability(
            ValidateVolumeCapabilitiesRequest.AccessMode
                .valueOf(cap0.getAccessMode().getMode().name()),
            ValidateVolumeCapabilitiesRequest.VolumeType.FILE_SYSTEM,
            cap0.getMount().getMountFlagsList());
    return NodePublishVolumeRequest.newInstance(
        messageB.getVolumeId(), messageB.getReadonly(),
        messageB.getTargetPath(), messageB.getStagingTargetPath(),
        cap, messageB.getPublishInfoMap(),
        messageB.getNodePublishSecretsMap());
  }
}
