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
import org.apache.hadoop.yarn.api.protocolrecords.NodeUnpublishVolumeRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * This class helps to transform a YARN side NodeUnpublishVolumeRequest
 * to corresponding CSI protocol message.
 * @param <A> YARN NodeUnpublishVolumeRequest
 * @param <B> CSI NodeUnpublishVolumeRequest
 */
public class NodeUnpublishVolumeRequestProtoTranslator<A, B> implements
    ProtoTranslator<NodeUnpublishVolumeRequest,
        Csi.NodeUnpublishVolumeRequest> {

  @Override
  public Csi.NodeUnpublishVolumeRequest convertTo(
      NodeUnpublishVolumeRequest messageA) throws YarnException {
    return Csi.NodeUnpublishVolumeRequest.newBuilder()
        .setVolumeId(messageA.getVolumeId())
        .setTargetPath(messageA.getTargetPath())
        .build();
  }

  @Override
  public NodeUnpublishVolumeRequest convertFrom(
      Csi.NodeUnpublishVolumeRequest messageB) throws YarnException {
    return NodeUnpublishVolumeRequest
        .newInstance(messageB.getVolumeId(), messageB.getTargetPath());
  }
}
