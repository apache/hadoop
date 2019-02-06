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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateResponseProto;
import org.junit.Assert;
import org.junit.Test;

/**
 * Simple test to verify the protos generated are valid
 */
public class TestProtos {

  @Test
  public void testProtoCanBePrinted() throws Exception {
    EpochProto proto = EpochProto.newBuilder().setEpoch(100).build();
    String protoString = proto.toString();
    Assert.assertNotNull(protoString);
  }

  @Test
  public void testProtoAllocateResponse() {
    AllocateResponseProto proto = AllocateResponseProto.getDefaultInstance();
    AllocateResponsePBImpl alloc = new AllocateResponsePBImpl(proto);
    List<NMToken> nmTokens = new ArrayList<NMToken>();
    try {
      alloc.setNMTokens(nmTokens);
    } catch (Exception ex) {
      fail();
    }
  }
}
