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
package org.apache.hadoop.yarn.api;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationsRequestPBImpl;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.Assert;
import org.junit.Test;

public class TestGetApplicationsRequest {

  @Test
  public void testGetApplicationsRequest(){
    GetApplicationsRequest request = GetApplicationsRequest.newInstance();
    
    EnumSet<YarnApplicationState> appStates = 
      EnumSet.of(YarnApplicationState.ACCEPTED);
    request.setApplicationStates(appStates);
    
    Set<String> tags = new HashSet<String>();
    tags.add("tag1");
    request.setApplicationTags(tags);
    
    Set<String> types = new HashSet<String>();
    types.add("type1");
    request.setApplicationTypes(types);
    
    long startBegin = System.currentTimeMillis();
    long startEnd = System.currentTimeMillis() + 1;
    request.setStartRange(startBegin, startEnd);
    long finishBegin = System.currentTimeMillis() + 2;
    long finishEnd = System.currentTimeMillis() + 3;
    request.setFinishRange(finishBegin, finishEnd);
    
    long limit = 100L;
    request.setLimit(limit);
    
    Set<String> queues = new HashSet<String>();
    queues.add("queue1");
    request.setQueues(queues);
    
    
    Set<String> users = new HashSet<String>();
    users.add("user1");
    request.setUsers(users);
    
    ApplicationsRequestScope scope = ApplicationsRequestScope.ALL;
    request.setScope(scope);
    
    GetApplicationsRequest requestFromProto = new GetApplicationsRequestPBImpl(
        ((GetApplicationsRequestPBImpl)request).getProto());
    
    // verify the whole record equals with original record
    Assert.assertEquals(requestFromProto, request);

    // verify all properties are the same as original request
    Assert.assertEquals(
        "ApplicationStates from proto is not the same with original request",
        requestFromProto.getApplicationStates(), appStates);
    
    Assert.assertEquals(
        "ApplicationTags from proto is not the same with original request",
        requestFromProto.getApplicationTags(), tags);
    
    Assert.assertEquals(
        "ApplicationTypes from proto is not the same with original request",
        requestFromProto.getApplicationTypes(), types);
    
    Assert.assertEquals(
        "StartRange from proto is not the same with original request",
        requestFromProto.getStartRange(), new LongRange(startBegin, startEnd));
    
    Assert.assertEquals(
        "FinishRange from proto is not the same with original request",
        requestFromProto.getFinishRange(), new LongRange(finishBegin, finishEnd));
    
    Assert.assertEquals(
        "Limit from proto is not the same with original request",
        requestFromProto.getLimit(), limit);
    
    Assert.assertEquals(
        "Queues from proto is not the same with original request",
        requestFromProto.getQueues(), queues);
    
    Assert.assertEquals(
        "Users from proto is not the same with original request",
        requestFromProto.getUsers(), users);
  }

}
