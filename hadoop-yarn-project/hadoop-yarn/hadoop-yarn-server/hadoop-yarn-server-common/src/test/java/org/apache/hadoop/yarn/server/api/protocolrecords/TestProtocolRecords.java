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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NMContainerStatusPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Test;

public class TestProtocolRecords {

  @Test
  public void testNMContainerStatus() {
    ApplicationId appId = ApplicationId.newInstance(123456789, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newInstance(attemptId, 1);
    Resource resource = Resource.newInstance(1000, 200);

    NMContainerStatus report =
        NMContainerStatus.newInstance(containerId,
          ContainerState.COMPLETE, resource, "diagnostics",
          ContainerExitStatus.ABORTED, Priority.newInstance(10), 1234);
    NMContainerStatus reportProto =
        new NMContainerStatusPBImpl(
          ((NMContainerStatusPBImpl) report).getProto());
    Assert.assertEquals("diagnostics", reportProto.getDiagnostics());
    Assert.assertEquals(resource, reportProto.getAllocatedResource());
    Assert.assertEquals(ContainerExitStatus.ABORTED,
      reportProto.getContainerExitStatus());
    Assert.assertEquals(ContainerState.COMPLETE,
      reportProto.getContainerState());
    Assert.assertEquals(containerId, reportProto.getContainerId());
    Assert.assertEquals(Priority.newInstance(10), reportProto.getPriority());
    Assert.assertEquals(1234, reportProto.getCreationTime());
  }

  @Test
  public void testRegisterNodeManagerRequest() {
    ApplicationId appId = ApplicationId.newInstance(123456789, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newInstance(attemptId, 1);

    NMContainerStatus containerReport =
        NMContainerStatus.newInstance(containerId,
          ContainerState.RUNNING, Resource.newInstance(1024, 1), "diagnostics",
          0, Priority.newInstance(10), 1234);
    List<NMContainerStatus> reports = Arrays.asList(containerReport);
    RegisterNodeManagerRequest request =
        RegisterNodeManagerRequest.newInstance(
          NodeId.newInstance("1.1.1.1", 1000), 8080,
            Resource.newInstance(1024, 1), "NM-version-id", reports,
            Arrays.asList(appId));
    RegisterNodeManagerRequest requestProto =
        new RegisterNodeManagerRequestPBImpl(
          ((RegisterNodeManagerRequestPBImpl) request).getProto());
    Assert.assertEquals(containerReport, requestProto
      .getNMContainerStatuses().get(0));
    Assert.assertEquals(8080, requestProto.getHttpPort());
    Assert.assertEquals("NM-version-id", requestProto.getNMVersion());
    Assert.assertEquals(NodeId.newInstance("1.1.1.1", 1000),
      requestProto.getNodeId());
    Assert.assertEquals(Resource.newInstance(1024, 1),
      requestProto.getResource());
    Assert.assertEquals(1, requestProto.getRunningApplications().size());
    Assert.assertEquals(appId, requestProto.getRunningApplications().get(0)); 
  }

  @Test
  public void testNodeHeartBeatResponse() throws IOException {
    NodeHeartbeatResponse record =
        Records.newRecord(NodeHeartbeatResponse.class);
    Map<ApplicationId, ByteBuffer> appCredentials =
        new HashMap<ApplicationId, ByteBuffer>();
    Credentials app1Cred = new Credentials();

    Token<DelegationTokenIdentifier> token1 =
        new Token<DelegationTokenIdentifier>();
    token1.setKind(new Text("kind1"));
    app1Cred.addToken(new Text("token1"), token1);
    Token<DelegationTokenIdentifier> token2 =
        new Token<DelegationTokenIdentifier>();
    token2.setKind(new Text("kind2"));
    app1Cred.addToken(new Text("token2"), token2);

    DataOutputBuffer dob = new DataOutputBuffer();
    app1Cred.writeTokenStorageToStream(dob);
    ByteBuffer byteBuffer1 = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    appCredentials.put(ApplicationId.newInstance(1234, 1), byteBuffer1);
    record.setSystemCredentialsForApps(appCredentials);

    NodeHeartbeatResponse proto =
        new NodeHeartbeatResponsePBImpl(
          ((NodeHeartbeatResponsePBImpl) record).getProto());
    Assert.assertEquals(appCredentials, proto.getSystemCredentialsForApps());
  }
}
