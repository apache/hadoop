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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Assert;
import org.junit.Test;

public class TestYARNTokenIdentifier {

  @Test
  public void testNMTokenIdentifier() {
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(1, 1), 1);
    NodeId nodeId = NodeId.newInstance("host0", 0);
    String applicationSubmitter = "usr0";
    int masterKeyId = 1;
    
    NMTokenIdentifier token = new NMTokenIdentifier(
        appAttemptId, nodeId, applicationSubmitter, masterKeyId);
    
    NMTokenIdentifier anotherToken = new NMTokenIdentifier(
        appAttemptId, nodeId, applicationSubmitter, masterKeyId);
    
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
  public void testAMRMTokenIdentifier() {
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(1, 1), 1);
    int masterKeyId = 1;
  
    AMRMTokenIdentifier token = new AMRMTokenIdentifier(appAttemptId, masterKeyId);
    
    AMRMTokenIdentifier anotherToken = new AMRMTokenIdentifier(
        appAttemptId, masterKeyId);
        
    // verify the whole record equals with original record
    Assert.assertEquals("Token is not the same after serialization " +
        "and deserialization.", token, anotherToken);
        
    Assert.assertEquals("ApplicationAttemptId from proto is not the same with original token",
        anotherToken.getApplicationAttemptId(), appAttemptId);
    
    Assert.assertEquals("masterKeyId from proto is not the same with original token",
        anotherToken.getKeyId(), masterKeyId);
  }
  
  @Test
  public void testContainerTokenIdentifier() {
    ContainerId containerID = ContainerId.newInstance(
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
    
    ContainerTokenIdentifier anotherToken = new ContainerTokenIdentifier(
        containerID, hostName, appSubmitter, r, expiryTimeStamp, 
        masterKeyId, rmIdentifier, priority, creationTime);
    
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
  }

}
