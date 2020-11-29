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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest.VolumeCapability;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ValidateVolumeCapabilitiesRequestPBImpl;
import org.apache.hadoop.yarn.proto.CsiAdaptorProtos;
import org.apache.hadoop.yarn.proto.CsiAdaptorProtos.VolumeCapability.AccessMode;
import org.apache.hadoop.yarn.proto.CsiAdaptorProtos.VolumeCapability.VolumeType;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest.AccessMode.MULTI_NODE_MULTI_WRITER;
import static org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest.VolumeType.FILE_SYSTEM;

/**
 * UT for message exchanges.
 */
public class TestValidateVolumeCapabilityRequest {

  @Test
  public void testPBRecord() {
    CsiAdaptorProtos.VolumeCapability vcProto =
        CsiAdaptorProtos.VolumeCapability.newBuilder()
            .setAccessMode(AccessMode.MULTI_NODE_MULTI_WRITER)
            .setVolumeType(VolumeType.FILE_SYSTEM)
            .addMountFlags("flag0")
            .addMountFlags("flag1")
            .build();

    CsiAdaptorProtos.ValidateVolumeCapabilitiesRequest requestProto =
        CsiAdaptorProtos.ValidateVolumeCapabilitiesRequest.newBuilder()
            .setVolumeId("volume-id-0000001")
            .addVolumeCapabilities(vcProto)
            .addVolumeAttributes(YarnProtos.StringStringMapProto
                .newBuilder().setKey("attr0")
                .setValue("value0")
                .build())
            .addVolumeAttributes(YarnProtos.StringStringMapProto
                .newBuilder().setKey("attr1")
                .setValue("value1")
                .build())
            .build();

    ValidateVolumeCapabilitiesRequestPBImpl request =
        new ValidateVolumeCapabilitiesRequestPBImpl(requestProto);

    Assert.assertEquals("volume-id-0000001", request.getVolumeId());
    Assert.assertEquals(2, request.getVolumeAttributes().size());
    Assert.assertEquals("value0", request.getVolumeAttributes().get("attr0"));
    Assert.assertEquals("value1", request.getVolumeAttributes().get("attr1"));
    Assert.assertEquals(1, request.getVolumeCapabilities().size());
    VolumeCapability vc =
        request.getVolumeCapabilities().get(0);
    Assert.assertEquals(MULTI_NODE_MULTI_WRITER, vc.getAccessMode());
    Assert.assertEquals(FILE_SYSTEM, vc.getVolumeType());
    Assert.assertEquals(2, vc.getMountFlags().size());

    Assert.assertEquals(requestProto, request.getProto());
  }

  @Test
  public void testNewInstance() {
    ValidateVolumeCapabilitiesRequest pbImpl =
        ValidateVolumeCapabilitiesRequestPBImpl
            .newInstance("volume-id-0000123",
                ImmutableList.of(
                    new VolumeCapability(
                        MULTI_NODE_MULTI_WRITER, FILE_SYSTEM,
                        ImmutableList.of("mountFlag1", "mountFlag2"))),
                ImmutableMap.of("k1", "v1", "k2", "v2"));

    Assert.assertEquals("volume-id-0000123", pbImpl.getVolumeId());
    Assert.assertEquals(1, pbImpl.getVolumeCapabilities().size());
    Assert.assertEquals(FILE_SYSTEM,
        pbImpl.getVolumeCapabilities().get(0).getVolumeType());
    Assert.assertEquals(MULTI_NODE_MULTI_WRITER,
        pbImpl.getVolumeCapabilities().get(0).getAccessMode());
    Assert.assertEquals(2, pbImpl.getVolumeAttributes().size());

    CsiAdaptorProtos.ValidateVolumeCapabilitiesRequest proto =
        ((ValidateVolumeCapabilitiesRequestPBImpl) pbImpl).getProto();
    Assert.assertEquals("volume-id-0000123", proto.getVolumeId());
    Assert.assertEquals(1, proto.getVolumeCapabilitiesCount());
    Assert.assertEquals(AccessMode.MULTI_NODE_MULTI_WRITER,
        proto.getVolumeCapabilities(0).getAccessMode());
    Assert.assertEquals(VolumeType.FILE_SYSTEM,
        proto.getVolumeCapabilities(0).getVolumeType());
    Assert.assertEquals(2, proto.getVolumeCapabilities(0)
        .getMountFlagsCount());
    Assert.assertEquals(2, proto.getVolumeCapabilities(0)
        .getMountFlagsList().size());
  }
}
