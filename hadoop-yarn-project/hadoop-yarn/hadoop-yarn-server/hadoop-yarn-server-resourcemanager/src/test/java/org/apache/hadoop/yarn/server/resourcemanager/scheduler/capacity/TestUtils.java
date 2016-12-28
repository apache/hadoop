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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Set;

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
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestUtils {
  private static final Log LOG = LogFactory.getLog(TestUtils.class);

  /**
   * Get a mock {@link RMContext} for use in test cases.
   * @return a mock {@link RMContext} for use in test cases
   */
  @SuppressWarnings({ "rawtypes", "unchecked" }) 
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
    RMApplicationHistoryWriter writer =  mock(RMApplicationHistoryWriter.class);
    RMContextImpl rmContext =
        new RMContextImpl(nullDispatcher, cae, null, null, null,
          new AMRMTokenSecretManager(conf, null),
          new RMContainerTokenSecretManager(conf),
          new NMTokenSecretManagerInRM(conf),
          new ClientToAMTokenSecretManagerInRM());
    RMNodeLabelsManager nlm = mock(RMNodeLabelsManager.class);
    when(
        nlm.getQueueResource(any(String.class), any(Set.class),
            any(Resource.class))).thenAnswer(new Answer<Resource>() {
      @Override
      public Resource answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        return (Resource) args[2];
      }
    });
    
    when(nlm.getResourceByLabel(any(String.class), any(Resource.class)))
        .thenAnswer(new Answer<Resource>() {
          @Override public Resource answer(InvocationOnMock invocation)
              throws Throwable {
            Object[] args = invocation.getArguments();
            return (Resource) args[1];
          }
        });
    
    rmContext.setNodeLabelManager(nlm);
    rmContext.setSystemMetricsPublisher(mock(SystemMetricsPublisher.class));
    rmContext.setRMApplicationHistoryWriter(mock(RMApplicationHistoryWriter.class));
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
      String resourceName, int memory, int numContainers, boolean relaxLocality,
      Priority priority, RecordFactory recordFactory) {
    ResourceRequest request = 
        recordFactory.newRecordInstance(ResourceRequest.class);
    Resource capability = Resources.createResource(memory, 1);
    
    request.setNumContainers(numContainers);
    request.setResourceName(resourceName);
    request.setCapability(capability);
    request.setRelaxLocality(relaxLocality);
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
    ApplicationId applicationId = BuilderUtils.newApplicationId(0l, appId);
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
    
    FiCaSchedulerNode node = spy(new FiCaSchedulerNode(rmNode, false));
    LOG.info("node = " + host + " avail=" + node.getAvailableResource());
    return node;
  }

  @SuppressWarnings("deprecation")
  public static ContainerId getMockContainerId(FiCaSchedulerApp application) {
    ContainerId containerId = mock(ContainerId.class);
    doReturn(application.getApplicationAttemptId()).
    when(containerId).getApplicationAttemptId();
    long id = application.getNewContainerId();
    doReturn((int)id).when(containerId).getId();
    doReturn(id).when(containerId).getContainerId();
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

  /**
   * Get a queue structure:
   * <pre>
   *             Root
   *            /  |  \
   *           a   b   c
   *          10   20  70
   * </pre>
   */
  public static Configuration
  getConfigurationWithMultipleQueues(Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "a", "b", "c" });

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 100);
    conf.setUserLimitFactor(A, 100);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 20);
    conf.setMaximumCapacity(B, 100);
    conf.setUserLimitFactor(B, 100);

    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    conf.setCapacity(C, 70);
    conf.setMaximumCapacity(C, 100);
    conf.setUserLimitFactor(C, 100);

    return conf;
  }
}
