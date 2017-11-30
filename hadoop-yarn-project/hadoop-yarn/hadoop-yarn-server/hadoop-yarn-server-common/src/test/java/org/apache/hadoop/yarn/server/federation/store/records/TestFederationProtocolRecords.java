/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.yarn.api.BasePBImplRecordsTest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.AddApplicationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.AddApplicationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.DeleteApplicationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.DeleteApplicationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationsHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationsHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterInfoRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterInfoResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPoliciesConfigurationsRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPoliciesConfigurationsResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPolicyConfigurationRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPolicyConfigurationResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClustersInfoRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClustersInfoResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SetSubClusterPolicyConfigurationRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SetSubClusterPolicyConfigurationResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterDeregisterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterDeregisterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterHeartbeatRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterHeartbeatResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterInfoProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterRegisterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterRegisterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.UpdateApplicationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.UpdateApplicationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.AddApplicationHomeSubClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.AddApplicationHomeSubClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.DeleteApplicationHomeSubClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.DeleteApplicationHomeSubClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetApplicationHomeSubClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetApplicationHomeSubClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetApplicationsHomeSubClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetApplicationsHomeSubClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClusterInfoRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClusterInfoResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClusterPoliciesConfigurationsRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClusterPoliciesConfigurationsResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClusterPolicyConfigurationRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClusterPolicyConfigurationResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClustersInfoRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.GetSubClustersInfoResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SetSubClusterPolicyConfigurationRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SetSubClusterPolicyConfigurationResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterDeregisterRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterDeregisterResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterHeartbeatRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterIdPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterInfoPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterRegisterRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterRegisterResponsePBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.UpdateApplicationHomeSubClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.UpdateApplicationHomeSubClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.records.Version;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class for federation protocol records.
 */
public class TestFederationProtocolRecords extends BasePBImplRecordsTest {

  @BeforeClass
  public static void setup() throws Exception {
    generateByNewInstance(ApplicationId.class);
    generateByNewInstance(Version.class);
    generateByNewInstance(SubClusterId.class);
    generateByNewInstance(SubClusterInfo.class);
    generateByNewInstance(ApplicationHomeSubCluster.class);
    generateByNewInstance(SubClusterPolicyConfiguration.class);
  }

  @Test
  public void testSubClusterId() throws Exception {
    validatePBImplRecord(SubClusterIdPBImpl.class, SubClusterIdProto.class);
  }

  @Test
  public void testSubClusterInfo() throws Exception {
    validatePBImplRecord(SubClusterInfoPBImpl.class, SubClusterInfoProto.class);
  }

  @Test
  public void testSubClusterRegisterRequest() throws Exception {
    validatePBImplRecord(SubClusterRegisterRequestPBImpl.class,
        SubClusterRegisterRequestProto.class);
  }

  @Test
  public void testSubClusterRegisterResponse() throws Exception {
    validatePBImplRecord(SubClusterRegisterResponsePBImpl.class,
        SubClusterRegisterResponseProto.class);
  }

  @Test
  public void testSubClusterDeregisterRequest() throws Exception {
    validatePBImplRecord(SubClusterDeregisterRequestPBImpl.class,
        SubClusterDeregisterRequestProto.class);
  }

  @Test
  public void testSubClusterDeregisterResponse() throws Exception {
    validatePBImplRecord(SubClusterDeregisterResponsePBImpl.class,
        SubClusterDeregisterResponseProto.class);
  }

  @Test
  public void testSubClusterHeartbeatRequest() throws Exception {
    validatePBImplRecord(SubClusterHeartbeatRequestPBImpl.class,
        SubClusterHeartbeatRequestProto.class);
  }

  @Test
  public void testSubClusterHeartbeatResponse() throws Exception {
    validatePBImplRecord(SubClusterHeartbeatResponsePBImpl.class,
        SubClusterHeartbeatResponseProto.class);
  }

  @Test
  public void testGetSubClusterRequest() throws Exception {
    validatePBImplRecord(GetSubClusterInfoRequestPBImpl.class,
        GetSubClusterInfoRequestProto.class);
  }

  @Test
  public void testGetSubClusterResponse() throws Exception {
    validatePBImplRecord(GetSubClusterInfoResponsePBImpl.class,
        GetSubClusterInfoResponseProto.class);
  }

  @Test
  public void testGetSubClustersInfoRequest() throws Exception {
    validatePBImplRecord(GetSubClustersInfoRequestPBImpl.class,
        GetSubClustersInfoRequestProto.class);
  }

  @Test
  public void testGetSubClustersInfoResponse() throws Exception {
    validatePBImplRecord(GetSubClustersInfoResponsePBImpl.class,
        GetSubClustersInfoResponseProto.class);
  }

  @Test
  public void testAddApplicationHomeSubClusterRequest() throws Exception {
    validatePBImplRecord(AddApplicationHomeSubClusterRequestPBImpl.class,
        AddApplicationHomeSubClusterRequestProto.class);
  }

  @Test
  public void testAddApplicationHomeSubClusterResponse() throws Exception {
    validatePBImplRecord(AddApplicationHomeSubClusterResponsePBImpl.class,
        AddApplicationHomeSubClusterResponseProto.class);
  }

  @Test
  public void testUpdateApplicationHomeSubClusterRequest() throws Exception {
    validatePBImplRecord(UpdateApplicationHomeSubClusterRequestPBImpl.class,
        UpdateApplicationHomeSubClusterRequestProto.class);
  }

  @Test
  public void testUpdateApplicationHomeSubClusterResponse() throws Exception {
    validatePBImplRecord(UpdateApplicationHomeSubClusterResponsePBImpl.class,
        UpdateApplicationHomeSubClusterResponseProto.class);
  }

  @Test
  public void testGetApplicationHomeSubClusterRequest() throws Exception {
    validatePBImplRecord(GetApplicationHomeSubClusterRequestPBImpl.class,
        GetApplicationHomeSubClusterRequestProto.class);
  }

  @Test
  public void testGetApplicationHomeSubClusterResponse() throws Exception {
    validatePBImplRecord(GetApplicationHomeSubClusterResponsePBImpl.class,
        GetApplicationHomeSubClusterResponseProto.class);
  }

  @Test
  public void testGetApplicationsHomeSubClusterRequest() throws Exception {
    validatePBImplRecord(GetApplicationsHomeSubClusterRequestPBImpl.class,
        GetApplicationsHomeSubClusterRequestProto.class);
  }

  @Test
  public void testGetApplicationsHomeSubClusterResponse() throws Exception {
    validatePBImplRecord(GetApplicationsHomeSubClusterResponsePBImpl.class,
        GetApplicationsHomeSubClusterResponseProto.class);
  }

  @Test
  public void testDeleteApplicationHomeSubClusterRequest() throws Exception {
    validatePBImplRecord(DeleteApplicationHomeSubClusterRequestPBImpl.class,
        DeleteApplicationHomeSubClusterRequestProto.class);
  }

  @Test
  public void testDeleteApplicationHomeSubClusterResponse() throws Exception {
    validatePBImplRecord(DeleteApplicationHomeSubClusterResponsePBImpl.class,
        DeleteApplicationHomeSubClusterResponseProto.class);
  }

  @Test
  public void testGetSubClusterPolicyConfigurationRequest() throws Exception {
    validatePBImplRecord(GetSubClusterPolicyConfigurationRequestPBImpl.class,
        GetSubClusterPolicyConfigurationRequestProto.class);
  }

  @Test
  public void testGetSubClusterPolicyConfigurationResponse() throws Exception {
    validatePBImplRecord(GetSubClusterPolicyConfigurationResponsePBImpl.class,
        GetSubClusterPolicyConfigurationResponseProto.class);
  }

  @Test
  public void testSetSubClusterPolicyConfigurationRequest() throws Exception {
    validatePBImplRecord(SetSubClusterPolicyConfigurationRequestPBImpl.class,
        SetSubClusterPolicyConfigurationRequestProto.class);
  }

  @Test
  public void testSetSubClusterPolicyConfigurationResponse() throws Exception {
    validatePBImplRecord(SetSubClusterPolicyConfigurationResponsePBImpl.class,
        SetSubClusterPolicyConfigurationResponseProto.class);
  }

  @Test
  public void testGetSubClusterPoliciesConfigurationsRequest()
      throws Exception {
    validatePBImplRecord(GetSubClusterPoliciesConfigurationsRequestPBImpl.class,
        GetSubClusterPoliciesConfigurationsRequestProto.class);
  }

  @Test
  public void testGetSubClusterPoliciesConfigurationsResponse()
      throws Exception {
    validatePBImplRecord(
        GetSubClusterPoliciesConfigurationsResponsePBImpl.class,
        GetSubClusterPoliciesConfigurationsResponseProto.class);
  }
}
