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
import org.apache.hadoop.yarn.api.protocolrecords.NodeUnpublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoResponse;

/**
 * Factory class to get desired proto transformer instance.
 */
public final class ProtoTranslatorFactory {

  private ProtoTranslatorFactory() {
    // hide constructor for the factory class
  }

  /**
   * Get a {@link ProtoTranslator} based on the given input message
   * types. If the type is not supported, a IllegalArgumentException
   * will be thrown. When adding more transformers to this factory class,
   * note each transformer works exactly for one message to another
   * (and vice versa). For each type of the message, make sure there is
   * a corresponding unit test added, such as
   * TestValidateVolumeCapabilitiesRequest.
   *
   * @param yarnProto yarn proto message
   * @param csiProto CSI proto message
   * @param <A> yarn proto message
   * @param <B> CSI proto message
   * @throws IllegalArgumentException
   *   when given types are not supported
   * @return
   *   a proto message transformer that transforms
   *   YARN internal proto message to CSI
   */
  public static <A, B> ProtoTranslator<A, B> getTranslator(
      Class<A> yarnProto, Class<B> csiProto) {
    if (yarnProto == ValidateVolumeCapabilitiesRequest.class
        && csiProto == Csi.ValidateVolumeCapabilitiesRequest.class) {
      return new ValidateVolumeCapabilitiesRequestProtoTranslator();
    } else if (yarnProto == ValidateVolumeCapabilitiesResponse.class
        && csiProto == Csi.ValidateVolumeCapabilitiesResponse.class) {
      return new ValidationVolumeCapabilitiesResponseProtoTranslator();
    } else if (yarnProto == NodePublishVolumeRequest.class
        && csiProto == Csi.NodePublishVolumeRequest.class) {
      return new NodePublishVolumeRequestProtoTranslator();
    } else if (yarnProto == GetPluginInfoResponse.class
        && csiProto == Csi.GetPluginInfoResponse.class) {
      return new GetPluginInfoResponseProtoTranslator();
    } else if (yarnProto == NodeUnpublishVolumeRequest.class
        && csiProto == Csi.NodeUnpublishVolumeRequest.class) {
      return new NodeUnpublishVolumeRequestProtoTranslator();
    }
    throw new IllegalArgumentException("A problem is found while processing"
        + " proto message translating. Unexpected message types,"
        + " no transformer is found can handle the transformation from type "
        + yarnProto.getName() + " <-> " + csiProto.getName());
  }
}
