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
package org.apache.hadoop.yarn.security;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.LogAggregationContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.junit.Assert;
import org.junit.Test;

public class TestYARNTokenIdentifier {

  @Test
  public void testNMTokenIdentifier() throws IOException {
    testNMTokenIdentifier(false);
  }

  @Test
  public void testNMTokenIdentifierOldFormat() throws IOException {
    testNMTokenIdentifier(true);
  }

  public void testNMTokenIdentifier(boolean oldFormat) throws IOException {
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(1, 1), 1);
    NodeId nodeId = NodeId.newInstance("host0", 0);
    String applicationSubmitter = "usr0";
    int masterKeyId = 1;
    
    NMTokenIdentifier token = new NMTokenIdentifier(
        appAttemptId, nodeId, applicationSubmitter, masterKeyId);
    
    NMTokenIdentifier anotherToken = new NMTokenIdentifier();

    byte[] tokenContent;
    if (oldFormat) {
      tokenContent = writeInOldFormat(token);
    } else {
      tokenContent = token.getBytes();
    }
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(tokenContent, tokenContent.length);
    anotherToken.readFields(dib);
    
    // verify the whole record equals with original record
    Assert.assertEquals("Token is not the same after serialization " +
        "and deserialization.", token, anotherToken);
    
    // verify all properties are the same as original
    Assert.assertEquals(
        "appAttemptId from proto is not the same with original token",
        anotherToken.getApplicationAttemptId(), appAttemptId);
    
    Assert.assertEquals(
        "NodeId from proto is not the same with original token",
        anotherToken.getNodeId(), nodeId);
    
    Assert.assertEquals(
        "applicationSubmitter from proto is not the same with original token",
        anotherToken.getApplicationSubmitter(), applicationSubmitter);
    
    Assert.assertEquals(
        "masterKeyId from proto is not the same with original token",
        anotherToken.getKeyId(), masterKeyId);
  }

  @Test
  public void testAMRMTokenIdentifier() throws IOException {
    testAMRMTokenIdentifier(false);
  }

  @Test
  public void testAMRMTokenIdentifierOldFormat() throws IOException {
    testAMRMTokenIdentifier(true);
  }

  public void testAMRMTokenIdentifier(boolean oldFormat) throws IOException {
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(1, 1), 1);
    int masterKeyId = 1;
  
    AMRMTokenIdentifier token = new AMRMTokenIdentifier(appAttemptId, masterKeyId);
    
    AMRMTokenIdentifier anotherToken = new AMRMTokenIdentifier();

    byte[] tokenContent;
    if (oldFormat) {
      tokenContent = writeInOldFormat(token);
    } else {
      tokenContent = token.getBytes();
    }
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(tokenContent, tokenContent.length);
    anotherToken.readFields(dib);
        
    // verify the whole record equals with original record
    Assert.assertEquals("Token is not the same after serialization " +
        "and deserialization.", token, anotherToken);
        
    Assert.assertEquals("ApplicationAttemptId from proto is not the same with original token",
        anotherToken.getApplicationAttemptId(), appAttemptId);
    
    Assert.assertEquals("masterKeyId from proto is not the same with original token",
        anotherToken.getKeyId(), masterKeyId);
  }
  
  @Test
  public void testClientToAMTokenIdentifier() throws IOException {
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(1, 1), 1);
    
    String clientName = "user";
  
    ClientToAMTokenIdentifier token = new ClientToAMTokenIdentifier(
        appAttemptId, clientName);
    
    ClientToAMTokenIdentifier anotherToken = new ClientToAMTokenIdentifier();
    
    byte[] tokenContent = token.getBytes();
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(tokenContent, tokenContent.length);
    anotherToken.readFields(dib);
        
    // verify the whole record equals with original record
    Assert.assertEquals("Token is not the same after serialization " +
        "and deserialization.", token, anotherToken);
        
    Assert.assertEquals("ApplicationAttemptId from proto is not the same with original token",
        anotherToken.getApplicationAttemptID(), appAttemptId);
    
    Assert.assertEquals("clientName from proto is not the same with original token",
        anotherToken.getClientName(), clientName);
  }

  @Test
  public void testContainerTokenIdentifier() throws IOException {
    testContainerTokenIdentifier(false, false);
  }

  @Test
  public void testContainerTokenIdentifierOldFormat() throws IOException {
    testContainerTokenIdentifier(true, true);
    testContainerTokenIdentifier(true, false);
  }

  public void testContainerTokenIdentifier(boolean oldFormat,
      boolean withLogAggregation) throws IOException {
    ContainerId containerID = ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(
            1, 1), 1), 1);
    String hostName = "host0";
    String appSubmitter = "usr0";
    Resource r = Resource.newInstance(1024, 1);
    long expiryTimeStamp = 1000;
    int masterKeyId = 1;
    long rmIdentifier = 1;
    Priority priority = Priority.newInstance(1);
    long creationTime = 1000;
    
    ContainerTokenIdentifier token = new ContainerTokenIdentifier(
        containerID, hostName, appSubmitter, r, expiryTimeStamp,
        masterKeyId, rmIdentifier, priority, creationTime);
    
    ContainerTokenIdentifier anotherToken = new ContainerTokenIdentifier();

    byte[] tokenContent;
    if (oldFormat) {
      tokenContent = writeInOldFormat(token, withLogAggregation);
    } else {
      tokenContent = token.getBytes();
    }
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(tokenContent, tokenContent.length);
    anotherToken.readFields(dib);
    
    // verify the whole record equals with original record
    Assert.assertEquals("Token is not the same after serialization " +
        "and deserialization.", token, anotherToken);
    
    Assert.assertEquals(
        "ContainerID from proto is not the same with original token",
        anotherToken.getContainerID(), containerID);
    
    Assert.assertEquals(
        "Hostname from proto is not the same with original token",
        anotherToken.getNmHostAddress(), hostName);
    
    Assert.assertEquals(
        "ApplicationSubmitter from proto is not the same with original token",
        anotherToken.getApplicationSubmitter(), appSubmitter);
    
    Assert.assertEquals(
        "Resource from proto is not the same with original token",
        anotherToken.getResource(), r);
    
    Assert.assertEquals(
        "expiryTimeStamp from proto is not the same with original token",
        anotherToken.getExpiryTimeStamp(), expiryTimeStamp);
    
    Assert.assertEquals(
        "KeyId from proto is not the same with original token",
        anotherToken.getMasterKeyId(), masterKeyId);
    
    Assert.assertEquals(
        "RMIdentifier from proto is not the same with original token",
        anotherToken.getRMIdentifier(), rmIdentifier);
    
    Assert.assertEquals(
        "Priority from proto is not the same with original token",
        anotherToken.getPriority(), priority);
    
    Assert.assertEquals(
        "CreationTime from proto is not the same with original token",
        anotherToken.getCreationTime(), creationTime);
    
    Assert.assertNull(anotherToken.getLogAggregationContext());

    Assert.assertEquals(CommonNodeLabelsManager.NO_LABEL,
        anotherToken.getNodeLabelExpression());

    Assert.assertEquals(ContainerType.TASK,
        anotherToken.getContainerType());

    Assert.assertEquals(ExecutionType.GUARANTEED,
        anotherToken.getExecutionType());
  }

  @Test
  public void testRMDelegationTokenIdentifier() throws IOException {
    testRMDelegationTokenIdentifier(false);
  }

  @Test
  public void testRMDelegationTokenIdentifierOldFormat() throws IOException {
    testRMDelegationTokenIdentifier(true);
  }

  public void testRMDelegationTokenIdentifier(boolean oldFormat)
      throws IOException {

    Text owner = new Text("user1");
    Text renewer = new Text("user2");
    Text realUser = new Text("user3");
    long issueDate = 1;
    long maxDate = 2;
    int sequenceNumber = 3;
    int masterKeyId = 4;

    RMDelegationTokenIdentifier originalToken =
        new RMDelegationTokenIdentifier(owner, renewer, realUser);
    originalToken.setIssueDate(issueDate);
    originalToken.setMaxDate(maxDate);
    originalToken.setSequenceNumber(sequenceNumber);
    originalToken.setMasterKeyId(masterKeyId);

    RMDelegationTokenIdentifier anotherToken
        = new RMDelegationTokenIdentifier();

    if (oldFormat) {
      DataInputBuffer inBuf = new DataInputBuffer();
      DataOutputBuffer outBuf = new DataOutputBuffer();
      originalToken.writeInOldFormat(outBuf);
      inBuf.reset(outBuf.getData(), 0, outBuf.getLength());
      anotherToken.readFieldsInOldFormat(inBuf);
      inBuf.close();
    } else {
      byte[] tokenContent = originalToken.getBytes();
      DataInputBuffer dib = new DataInputBuffer();
      dib.reset(tokenContent, tokenContent.length);
      anotherToken.readFields(dib);
      dib.close();
    }
    // verify the whole record equals with original record
    Assert.assertEquals(
        "Token is not the same after serialization and deserialization.",
        originalToken, anotherToken);
    Assert.assertEquals(
        "owner from proto is not the same with original token",
        owner, anotherToken.getOwner());
    Assert.assertEquals(
        "renewer from proto is not the same with original token",
        renewer, anotherToken.getRenewer());
    Assert.assertEquals(
        "realUser from proto is not the same with original token",
        realUser, anotherToken.getRealUser());
    Assert.assertEquals(
        "issueDate from proto is not the same with original token",
        issueDate, anotherToken.getIssueDate());
    Assert.assertEquals(
        "maxDate from proto is not the same with original token",
        maxDate, anotherToken.getMaxDate());
    Assert.assertEquals(
        "sequenceNumber from proto is not the same with original token",
        sequenceNumber, anotherToken.getSequenceNumber());
    Assert.assertEquals(
        "masterKeyId from proto is not the same with original token",
        masterKeyId, anotherToken.getMasterKeyId());

    // Test getProto
    YARNDelegationTokenIdentifierProto tokenProto = originalToken.getProto();
    // Write token proto to stream
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    tokenProto.writeTo(out);
    // Read token
    byte[] tokenData = baos.toByteArray();
    RMDelegationTokenIdentifier readToken = new RMDelegationTokenIdentifier();
    DataInputBuffer db = new DataInputBuffer();
    db.reset(tokenData, tokenData.length);
    readToken.readFields(db);

    // Verify if read token equals with original token
    Assert.assertEquals("Token from getProto is not the same after " +
        "serialization and deserialization.", originalToken, readToken);
    db.close();
    out.close();
  }
  
  @Test
  public void testTimelineDelegationTokenIdentifier() throws IOException {
    
    Text owner = new Text("user1");
    Text renewer = new Text("user2");
    Text realUser = new Text("user3");
    long issueDate = 1;
    long maxDate = 2;
    int sequenceNumber = 3;
    int masterKeyId = 4;
    
    TimelineDelegationTokenIdentifier token = 
        new TimelineDelegationTokenIdentifier(owner, renewer, realUser);
    token.setIssueDate(issueDate);
    token.setMaxDate(maxDate);
    token.setSequenceNumber(sequenceNumber);
    token.setMasterKeyId(masterKeyId);
    
    TimelineDelegationTokenIdentifier anotherToken = 
        new TimelineDelegationTokenIdentifier();
    
    byte[] tokenContent = token.getBytes();
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(tokenContent, tokenContent.length);
    anotherToken.readFields(dib);
        
    // verify the whole record equals with original record
    Assert.assertEquals("Token is not the same after serialization " +
        "and deserialization.", token, anotherToken);
    
    Assert.assertEquals("owner from proto is not the same with original token",
        anotherToken.getOwner(), owner);
    
    Assert.assertEquals("renewer from proto is not the same with original token",
        anotherToken.getRenewer(), renewer);
    
    Assert.assertEquals("realUser from proto is not the same with original token",
        anotherToken.getRealUser(), realUser);
    
    Assert.assertEquals("issueDate from proto is not the same with original token",
        anotherToken.getIssueDate(), issueDate);
    
    Assert.assertEquals("maxDate from proto is not the same with original token",
        anotherToken.getMaxDate(), maxDate);
    
    Assert.assertEquals("sequenceNumber from proto is not the same with original token",
        anotherToken.getSequenceNumber(), sequenceNumber);
    
    Assert.assertEquals("masterKeyId from proto is not the same with original token",
        anotherToken.getMasterKeyId(), masterKeyId);
  }

  @Test
  public void testParseTimelineDelegationTokenIdentifierRenewer() throws IOException {
    // Server side when generation a timeline DT
    Configuration conf = new YarnConfiguration();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL,
        "RULE:[2:$1@$0]([nr]m@.*EXAMPLE.COM)s/.*/yarn/");
    HadoopKerberosName.setConfiguration(conf);
    Text owner = new Text("owner");
    Text renewer = new Text("rm/localhost@EXAMPLE.COM");
    Text realUser = new Text("realUser");
    TimelineDelegationTokenIdentifier token =
        new TimelineDelegationTokenIdentifier(owner, renewer, realUser);
    Assert.assertEquals(new Text("yarn"), token.getRenewer());
  }

  @Test
  public void testAMContainerTokenIdentifier() throws IOException {
    ContainerId containerID = ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(
            1, 1), 1), 1);
    String hostName = "host0";
    String appSubmitter = "usr0";
    Resource r = Resource.newInstance(1024, 1);
    long expiryTimeStamp = 1000;
    int masterKeyId = 1;
    long rmIdentifier = 1;
    Priority priority = Priority.newInstance(1);
    long creationTime = 1000;

    ContainerTokenIdentifier token =
        new ContainerTokenIdentifier(containerID, hostName, appSubmitter, r,
            expiryTimeStamp, masterKeyId, rmIdentifier, priority, creationTime,
            null, CommonNodeLabelsManager.NO_LABEL, ContainerType.APPLICATION_MASTER);

    ContainerTokenIdentifier anotherToken = new ContainerTokenIdentifier();

    byte[] tokenContent = token.getBytes();
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(tokenContent, tokenContent.length);
    anotherToken.readFields(dib);

    Assert.assertEquals(ContainerType.APPLICATION_MASTER,
        anotherToken.getContainerType());

    Assert.assertEquals(ExecutionType.GUARANTEED,
        anotherToken.getExecutionType());

    token =
        new ContainerTokenIdentifier(containerID, 0, hostName, appSubmitter, r,
            expiryTimeStamp, masterKeyId, rmIdentifier, priority, creationTime,
            null, CommonNodeLabelsManager.NO_LABEL, ContainerType.TASK,
            ExecutionType.OPPORTUNISTIC);

    anotherToken = new ContainerTokenIdentifier();

    tokenContent = token.getBytes();
    dib = new DataInputBuffer();
    dib.reset(tokenContent, tokenContent.length);
    anotherToken.readFields(dib);

    Assert.assertEquals(ContainerType.TASK,
        anotherToken.getContainerType());

    Assert.assertEquals(ExecutionType.OPPORTUNISTIC,
        anotherToken.getExecutionType());
  }

  @SuppressWarnings("deprecation")
  private byte[] writeInOldFormat(ContainerTokenIdentifier token,
      boolean withLogAggregation) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    ApplicationAttemptId applicationAttemptId = token.getContainerID()
        .getApplicationAttemptId();
    ApplicationId applicationId = applicationAttemptId.getApplicationId();
    out.writeLong(applicationId.getClusterTimestamp());
    out.writeInt(applicationId.getId());
    out.writeInt(applicationAttemptId.getAttemptId());
    out.writeLong(token.getContainerID().getContainerId());
    out.writeUTF(token.getNmHostAddress());
    out.writeUTF(token.getApplicationSubmitter());
    out.writeInt(token.getResource().getMemory());
    out.writeInt(token.getResource().getVirtualCores());
    out.writeLong(token.getExpiryTimeStamp());
    out.writeInt(token.getMasterKeyId());
    out.writeLong(token.getRMIdentifier());
    out.writeInt(token.getPriority().getPriority());
    out.writeLong(token.getCreationTime());
    if (withLogAggregation) {
      if (token.getLogAggregationContext() == null) {
        out.writeInt(-1);
      } else {
        byte[] logAggregationContext = ((LogAggregationContextPBImpl)
            token.getLogAggregationContext()).getProto().toByteArray();
        out.writeInt(logAggregationContext.length);
        out.write(logAggregationContext);
      }
    }
    out.close();
    return baos.toByteArray();
  }

  private byte[] writeInOldFormat(NMTokenIdentifier token) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    ApplicationId applicationId = token.getApplicationAttemptId()
        .getApplicationId();
    out.writeLong(applicationId.getClusterTimestamp());
    out.writeInt(applicationId.getId());
    out.writeInt(token.getApplicationAttemptId().getAttemptId());
    out.writeUTF(token.getNodeId().toString());
    out.writeUTF(token.getApplicationSubmitter());
    out.writeInt(token.getKeyId());
    out.close();
    return baos.toByteArray();
  }

  private byte[] writeInOldFormat(AMRMTokenIdentifier token)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    ApplicationId applicationId = token.getApplicationAttemptId()
        .getApplicationId();
    out.writeLong(applicationId.getClusterTimestamp());
    out.writeInt(applicationId.getId());
    out.writeInt(token.getApplicationAttemptId().getAttemptId());
    out.writeInt(token.getKeyId());
    out.close();
    return baos.toByteArray();
  }
}
