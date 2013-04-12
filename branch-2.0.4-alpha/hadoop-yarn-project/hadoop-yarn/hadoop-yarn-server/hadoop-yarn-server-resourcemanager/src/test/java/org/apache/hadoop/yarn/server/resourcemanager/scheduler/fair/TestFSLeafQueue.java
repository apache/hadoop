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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFSLeafQueue {
  private FSLeafQueue schedulable = null;
  private Resource maxResource = Resources.createResource(10);

  @Before
  public void setup() throws IOException {
    FairScheduler scheduler = new FairScheduler();
    Configuration conf = createConfiguration();
    // All tests assume only one assignment per node update
    conf.set(FairSchedulerConfiguration.ASSIGN_MULTIPLE, "false");
    ResourceManager resourceManager = new ResourceManager();
    resourceManager.init(conf);
    ((AsyncDispatcher)resourceManager.getRMContext().getDispatcher()).start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    
    String queueName = "root.queue1";
    QueueManager mockMgr = mock(QueueManager.class);
    when(mockMgr.getMaxResources(queueName)).thenReturn(maxResource);

    schedulable = new FSLeafQueue(queueName, mockMgr, scheduler, null);
  }

  @Test
  public void testUpdateDemand() {
    AppSchedulable app = mock(AppSchedulable.class);
    Mockito.when(app.getDemand()).thenReturn(maxResource);

    schedulable.addAppSchedulable(app);
    schedulable.addAppSchedulable(app);

    schedulable.updateDemand();

    assertTrue("Demand is greater than max allowed ",
        Resources.equals(schedulable.getDemand(), maxResource));
  }
  
  private Configuration createConfiguration() {
    Configuration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
        ResourceScheduler.class);
    return conf;
  }
}
