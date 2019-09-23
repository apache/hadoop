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
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * Proto message translator for ValidateVolumeCapabilitiesResponse.
 * @param <A> ValidateVolumeCapabilitiesResponse
 * @param <B> Csi.ValidateVolumeCapabilitiesResponse
 */
public class ValidationVolumeCapabilitiesResponseProtoTranslator<A, B>
    implements ProtoTranslator<ValidateVolumeCapabilitiesResponse,
            Csi.ValidateVolumeCapabilitiesResponse> {

  @Override
  public Csi.ValidateVolumeCapabilitiesResponse convertTo(
      ValidateVolumeCapabilitiesResponse response) throws YarnException {
    return Csi.ValidateVolumeCapabilitiesResponse.newBuilder()
        .setSupported(response.isSupported())
        .setMessage(response.getResponseMessage())
        .build();
  }

  @Override
  public ValidateVolumeCapabilitiesResponse convertFrom(
      Csi.ValidateVolumeCapabilitiesResponse response) throws YarnException {
    return ValidateVolumeCapabilitiesResponse.newInstance(
        response.getSupported(), response.getMessage());
  }
}
