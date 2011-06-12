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

package org.apache.hadoop.yarn.server.resourcemanager.resource;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

public class ResourceRequest {
  
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  
  public static org.apache.hadoop.yarn.api.records.ResourceRequest create(
      Priority priority, String hostName, 
      org.apache.hadoop.yarn.api.records.Resource capability, int numContainers) {
    org.apache.hadoop.yarn.api.records.ResourceRequest request = recordFactory.newRecordInstance(org.apache.hadoop.yarn.api.records.ResourceRequest.class);
    request.setPriority(priority);
    request.setHostName(hostName);
    request.setCapability(capability);
    request.setNumContainers(numContainers);
    return request;
  }
  
  public static org.apache.hadoop.yarn.api.records.ResourceRequest create(
      org.apache.hadoop.yarn.api.records.ResourceRequest r) {
    org.apache.hadoop.yarn.api.records.ResourceRequest request = recordFactory.newRecordInstance(org.apache.hadoop.yarn.api.records.ResourceRequest.class);
    request.setPriority(r.getPriority());
    request.setHostName(r.getHostName());
    request.setCapability(r.getCapability());
    request.setNumContainers(r.getNumContainers());
    return request;
  }
  
  public static class Comparator 
  implements java.util.Comparator<org.apache.hadoop.yarn.api.records.ResourceRequest> {
    @Override
    public int compare(org.apache.hadoop.yarn.api.records.ResourceRequest r1,
        org.apache.hadoop.yarn.api.records.ResourceRequest r2) {
      
      // Compare priority, host and capability
      int ret = r1.getPriority().compareTo(r2.getPriority());
      if (ret == 0) {
        String h1 = r1.getHostName();
        String h2 = r2.getHostName();
        ret = h1.compareTo(h2);
      }
      if (ret == 0) {
        ret = r1.getCapability().compareTo(r2.getCapability());
      }
      return ret;
    }
  }
}
