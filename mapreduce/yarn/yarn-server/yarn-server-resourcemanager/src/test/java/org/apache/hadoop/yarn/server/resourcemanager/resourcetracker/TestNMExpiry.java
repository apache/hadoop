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

package org.apache.hadoop.yarn.server.resourcemanager.resourcetracker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;
import org.apache.hadoop.yarn.server.resourcemanager.RMConfig;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemStore;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.RMResourceTrackerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceListener;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.junit.Before;
import org.junit.Test;

public class TestNMExpiry extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestNMExpiry.class);
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  
  RMResourceTrackerImpl resourceTracker;
  ContainerTokenSecretManager containerTokenSecretManager = 
    new ContainerTokenSecretManager();
  AtomicInteger test = new AtomicInteger();
  AtomicInteger notify = new AtomicInteger();

  private static class VoidResourceListener implements ResourceListener {
   
    @Override
    public void removeNode(NodeInfo node) {
    }
    @Override
    public void nodeUpdate(NodeInfo nodeInfo,
        Map<String, List<Container>> containers) {
     
    }
    @Override
    public void addNode(NodeInfo nodeInfo) {
        
    }
  }

  private class TestRMResourceTrackerImpl extends RMResourceTrackerImpl {
    public TestRMResourceTrackerImpl(
        ContainerTokenSecretManager containerTokenSecretManager, RMContext context) {
      super(containerTokenSecretManager, context);
    }

    @Override
    protected void expireNMs(List<NodeId> ids) {
      for (NodeId id: ids) {
        LOG.info("Expired  " + id);
        if (test.addAndGet(1) == 2) {
          try {
            /* delay atleast 2 seconds to make sure the 3rd one does not expire
             * 
             */
            Thread.sleep(2000);
          } catch(InterruptedException ie){}
          synchronized(notify) {
            notify.addAndGet(1);
            notify.notifyAll();
          }
        }
      }
    }
  }

  @Before
  public void setUp() {
    Configuration conf = new Configuration();
    RMContext context = new ResourceManager.RMContextImpl(new MemStore());
    resourceTracker = new TestRMResourceTrackerImpl(containerTokenSecretManager, 
       context);
    resourceTracker.addListener(new VoidResourceListener());
    
    conf.setLong(RMConfig.NM_EXPIRY_INTERVAL, 1000);
    resourceTracker.init(conf);
    resourceTracker.start();
  }

  private class ThirdNodeHeartBeatThread extends Thread {
    public void run() {
      int lastResponseID = 0;
      while (!stopT) {
        try {
          org.apache.hadoop.yarn.server.api.records.NodeStatus nodeStatus =
              recordFactory
                  .newRecordInstance(org.apache.hadoop.yarn.server.api.records.NodeStatus.class);
          nodeStatus.setNodeId(thirdNodeRegResponse.getNodeId());
          nodeStatus.setResponseId(lastResponseID);
          nodeStatus.setNodeHealthStatus(recordFactory.newRecordInstance(NodeHealthStatus.class));
          nodeStatus.getNodeHealthStatus().setIsNodeHealthy(true);
          lastResponseID =
              resourceTracker.nodeHeartbeat(nodeStatus)
                  .getResponseId();
        } catch(Exception e) {
          LOG.info("failed to heartbeat ", e);
        }
      }
    } 
  }

  boolean stopT = false;
  RegistrationResponse thirdNodeRegResponse;

  @Test
  public void testNMExpiry() throws Exception {
    String hostname1 = "localhost1";
    String hostname2 = "localhost2";
    String hostname3 = "localhost3";
    Resource capability = recordFactory.newRecordInstance(Resource.class);
    resourceTracker.registerNodeManager(hostname1, 0, 0, capability);
    resourceTracker.registerNodeManager(hostname2, 0, 0, capability);
    thirdNodeRegResponse =
        resourceTracker.registerNodeManager(hostname3, 0, 0, capability);
    /* test to see if hostanme 3 does not expire */
    stopT = false;
    new ThirdNodeHeartBeatThread().start();
    int timeOut = 0;
    synchronized (notify) {
      while (notify.get() == 0 && timeOut++ < 30) {
        notify.wait(1000);
      }
    }
    if (test.get() != 2) 
      assertTrue(false);

    stopT = true;
  }
}
