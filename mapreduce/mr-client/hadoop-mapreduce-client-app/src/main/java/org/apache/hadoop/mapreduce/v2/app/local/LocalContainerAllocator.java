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

package org.apache.hadoop.mapreduce.v2.app.local;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMCommunicator;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

/**
 * Allocates containers locally. Doesn't allocate a real container;
 * instead sends an allocated event for all requests.
 */
public class LocalContainerAllocator extends RMCommunicator
    implements ContainerAllocator {

  private static final Log LOG =
      LogFactory.getLog(LocalContainerAllocator.class);

  private final EventHandler eventHandler;
  private final ApplicationId appID;
  private AtomicInteger containerCount = new AtomicInteger();

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  public LocalContainerAllocator(ClientService clientService,
                                 AppContext context) {
    super(clientService, context);
    this.eventHandler = context.getEventHandler();
    this.appID = context.getApplicationID();
  }

  @Override
  public void handle(ContainerAllocatorEvent event) {
    if (event.getType() == ContainerAllocator.EventType.CONTAINER_REQ) {
      LOG.info("Processing the event " + event.toString());
      ContainerId cID = recordFactory.newRecordInstance(ContainerId.class);
      cID.setAppId(appID);
      // use negative ids to denote that these are local. Need a better way ??
      cID.setId((-1) * containerCount.getAndIncrement());
      
      Container container = recordFactory.newRecordInstance(Container.class);
      container.setId(cID);
      container.setContainerManagerAddress("localhost");
      container.setContainerToken(null);
      container.setNodeHttpAddress("localhost:9999");
      // send the container-assigned event to task attempt
      eventHandler.handle(new TaskAttemptContainerAssignedEvent(
          event.getAttemptID(), container));
    }
  }

}
