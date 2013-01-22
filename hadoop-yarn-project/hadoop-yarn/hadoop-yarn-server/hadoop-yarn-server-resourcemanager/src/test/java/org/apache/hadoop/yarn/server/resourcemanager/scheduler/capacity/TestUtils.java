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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;

public class TestUtils {
  private static final Log LOG = LogFactory.getLog(TestUtils.class);

  /**
   * Get a mock {@link RMContext} for use in test cases.
   * @return a mock {@link RMContext} for use in test cases
   */
  @SuppressWarnings("rawtypes") 
  public static RMContext getMockRMContext() {
    // Null dispatcher
    Dispatcher nullDispatcher = new Dispatcher() {
      private final EventHandler handler =
          new EventHandler() {
            @Override
            public void handle(Event event) {
            }
          };
      @Override
      public void register(Class<? extends Enum> eventType, 
          EventHandler handler) {
      }
      @Override
      public EventHandler getEventHandler() {
        return handler; 
      }
    };
    
    // No op 
    ContainerAllocationExpirer cae = 
        new ContainerAllocationExpirer(nullDispatcher);
    
    Configuration conf = new Configuration();
    RMContext rmContext =
        new RMContextImpl(nullDispatcher, cae, null, null, null,
          new ApplicationTokenSecretManager(conf),
          new RMContainerTokenSecretManager(conf),
          new ClientToAMTokenSecretManagerInRM());
    
    return rmContext;
  }
  
  /**
   * Hook to spy on queues.
   */
  static class SpyHook extends CapacityScheduler.QueueHook {
    @Override
    public CSQueue hook(CSQueue queue) {
      return spy(queue);
    }
  }
  public static SpyHook spyHook = new SpyHook();
  
  private static final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);

  public static Priority createMockPriority( int priority) {
//    Priority p = mock(Priority.class);
//    when(p.getPriority()).thenReturn(priority);
    Priority p = recordFactory.newRecordInstance(Priority.class);
    p.setPriority(priority);
    return p;
  }
  
  public static ResourceRequest createResourceRequest(
      String hostName, int memory, int numContainers, Priority priority,
      RecordFactory recordFactory) {
    ResourceRequest request = 
        recordFactory.newRecordInstance(ResourceRequest.class);
    Resource capability = Resources.createResource(memory, 1);
    
    request.setNumContainers(numContainers);
    request.setHostName(hostName);
    request.setCapability(capability);
    request.setPriority(priority);
    return request;
  }
  
  public static ApplicationId getMockApplicationId(int appId) {
    ApplicationId applicationId = mock(ApplicationId.class);
    when(applicationId.getClusterTimestamp()).thenReturn(0L);
    when(applicationId.getId()).thenReturn(appId);
    return applicationId;
  }
  
  public static ApplicationAttemptId 
  getMockApplicationAttemptId(int appId, int attemptId) {
    ApplicationId applicationId = mock(ApplicationId.class);
    when(applicationId.getClusterTimestamp()).thenReturn(0L);
    when(applicationId.getId()).thenReturn(appId);
    ApplicationAttemptId applicationAttemptId = mock(ApplicationAttemptId.class);  
    when(applicationAttemptId.getApplicationId()).thenReturn(applicationId);
    when(applicationAttemptId.getAttemptId()).thenReturn(attemptId);
    return applicationAttemptId;
  }
  
  public static FiCaSchedulerNode getMockNode(
      String host, String rack, int port, int capability) {
    NodeId nodeId = mock(NodeId.class);
    when(nodeId.getHost()).thenReturn(host);
    when(nodeId.getPort()).thenReturn(port);
    RMNode rmNode = mock(RMNode.class);
    when(rmNode.getNodeID()).thenReturn(nodeId);
    when(rmNode.getTotalCapability()).thenReturn(
        Resources.createResource(capability, 1));
    when(rmNode.getNodeAddress()).thenReturn(host+":"+port);
    when(rmNode.getHostName()).thenReturn(host);
    when(rmNode.getRackName()).thenReturn(rack);
    
    FiCaSchedulerNode node = spy(new FiCaSchedulerNode(rmNode));
    LOG.info("node = " + host + " avail=" + node.getAvailableResource());
    return node;
  }
  
  public static ContainerId getMockContainerId(FiCaSchedulerApp application) {
    ContainerId containerId = mock(ContainerId.class);
    doReturn(application.getApplicationAttemptId()).
    when(containerId).getApplicationAttemptId();
    doReturn(application.getNewContainerId()).when(containerId).getId();
    return containerId;
  }
  
  public static Container getMockContainer(
      ContainerId containerId, NodeId nodeId, 
      Resource resource, Priority priority) {
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(containerId);
    when(container.getNodeId()).thenReturn(nodeId);
    when(container.getResource()).thenReturn(resource);
    when(container.getPriority()).thenReturn(priority);
    return container;
  }
}
