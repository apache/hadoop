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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.SNEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * Negotiates with the scheduler for allocation of application master container.
 *
 */
class SchedulerNegotiator extends AbstractService implements EventHandler<ASMEvent<SNEventType>> {

  private static final Log LOG = LogFactory.getLog(SchedulerNegotiator.class);
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  final static Priority AM_CONTAINER_PRIORITY = recordFactory.newRecordInstance(Priority.class);
  static {
    AM_CONTAINER_PRIORITY.setPriority(0);
  }
  static final List<ResourceRequest> EMPTY_ASK =
    new ArrayList<ResourceRequest>();
  static final List<Container> EMPTY_RELEASE = 
    new ArrayList<Container>();

  private final EventHandler handler;

  private final SchedulerThread schedulerThread;
  private final YarnScheduler scheduler;
  private final List<AppContext> pendingApplications = 
    new ArrayList<AppContext>();

  @SuppressWarnings("unchecked")
  public SchedulerNegotiator(RMContext context, 
      YarnScheduler scheduler) {
    super("SchedulerNegotiator");
    this.handler = context.getDispatcher().getEventHandler();
    context.getDispatcher().register(SNEventType.class, this);
    this.scheduler = scheduler;
    this.schedulerThread = new SchedulerThread();
  }

  @Override
  public synchronized void start() {
    super.start();
    schedulerThread.start();
  }


  @Override
  public synchronized void stop() {
    schedulerThread.shutdown();
    try {
      schedulerThread.join(1000);
    } catch (InterruptedException ie) {
      LOG.info(schedulerThread.getName() + " interrupted during join ", ie);
    }

    super.stop();
  }

  private class SchedulerThread extends Thread {
    private volatile boolean shutdown = false;

    public SchedulerThread() {
      setName("ApplicationsManager:SchedulerThread");
      setDaemon(true);
    }
    
      @Override
    public void run() {
      List<AppContext> toSubmit = 
        new ArrayList<AppContext>();
      List<AppContext> submittedApplications =
        new ArrayList<AppContext>();
      Map<ApplicationId, List<Container>> firstAllocate = new HashMap<ApplicationId, List<Container>>();
      while (!shutdown && !isInterrupted()) {
        try {
          toSubmit.addAll(getPendingApplications());
          if (toSubmit.size() > 0) {
            LOG.info("Got " + toSubmit.size() + " applications to submit");

            submittedApplications.addAll(toSubmit);

            for (AppContext masterInfo: toSubmit) {
              // Register request for the ApplicationMaster container
              ResourceRequest request = 
                org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceRequest.create(
                    AM_CONTAINER_PRIORITY, "*", masterInfo.getResource(), 1);
              /*
               * it is not ok to ignore the first allocate since we might 
               * get containers on the first allocate.
               */
              LOG.debug("About to request resources for AM of " + 
                  masterInfo.getMaster() + " required " + request);
              Allocation allocation = scheduler.allocate(masterInfo.getMaster().getApplicationId(), 
                  Collections.singletonList(request), 
                  EMPTY_RELEASE);
              if (!allocation.getContainers().isEmpty()) {
                firstAllocate.put(masterInfo.getApplicationID(), allocation.getContainers());
              }
            }
            toSubmit.clear();
          }

          List<Container> containers = null;

          for (Iterator<AppContext> it=submittedApplications.iterator(); 
          it.hasNext();) {
            AppContext masterInfo = it.next();
            ApplicationId appId = masterInfo.getMaster().getApplicationId();
            containers = scheduler.allocate(appId, 
                EMPTY_ASK, EMPTY_RELEASE).getContainers();
            if (firstAllocate.containsKey(appId)) {
              containers = firstAllocate.get(appId);
              firstAllocate.remove(appId);
            }
            if (!containers.isEmpty()) {
              // there should be only one container for an application
              assert(containers.size() == 1);
              it.remove();
              Container container = containers.get(0);
              
              LOG.info("Found container " + container + " for AM of " + 
                  masterInfo.getMaster());
              SNAppContext snAppContext = new SNAppContext(masterInfo.getApplicationID(),
                container);
              handler.handle(new ASMEvent<ApplicationEventType>(
                ApplicationEventType.ALLOCATED, snAppContext));
            }
          }

          Thread.sleep(1000);
        } catch(Exception e) {
          LOG.info("Exception in submitting applications ", e);
        }
      }
    }
    
    /**
     * shutdown the thread.
     */
    public void shutdown() {
      shutdown = true;
      interrupt();
    }
  }

  private Collection<? extends AppContext> getPendingApplications() {
    List<AppContext> pending = new ArrayList<AppContext>();
    synchronized (pendingApplications) {
      pending.addAll(pendingApplications);
      pendingApplications.clear();
    }
    return pending;
  }

  private void addPending(AppContext masterInfo) {
    LOG.info("Adding to pending " + masterInfo.getMaster());
    synchronized(pendingApplications) {
      pendingApplications.add(masterInfo);
    }
  }

  @Override
  public void handle(ASMEvent<SNEventType> appEvent)  {
    SNEventType event = appEvent.getType();
    AppContext appContext = appEvent.getAppContext();
    switch (event) {
    case SCHEDULE:
      addPending(appContext);
      break;
    case RELEASE:
      try {
      scheduler.allocate(appContext.getApplicationID(), 
          EMPTY_ASK, Collections.singletonList(appContext.getMasterContainer()));
      } catch(IOException ie) {
        //TODO remove IOException from the scheduler.
        LOG.error("Error while releasing container for AM " + appContext.getApplicationID());
      }
      handler.handle(new ASMEvent<ApplicationEventType>(ApplicationEventType.RELEASED, 
          appContext));
      break;
    case CLEANUP:
      try {
        finishApplication(appContext);
      } catch (IOException ie) {
        LOG.info("Error finishing application", ie);
      }
      break;
    default:
      break;
    }
  }
  
  private void finishApplication(AppContext masterInfo)  
  throws IOException {
    LOG.info("Finishing application: cleaning up container " +
        masterInfo.getMasterContainer());
    //TODO we should release the container but looks like we just 
    // wait for update from NodeManager
    Container[] containers = new Container[] {masterInfo.getMasterContainer()};
    scheduler.allocate(masterInfo.getMaster().getApplicationId(), 
        EMPTY_ASK, Arrays.asList(containers));
  }
  
  private static class SNAppContext implements AppContext {
    private final ApplicationId appID;
    private final Container container;
    private final UnsupportedOperationException notImplementedException;
    
    public SNAppContext(ApplicationId appID, Container container) {
      this.appID = appID;
      this.container = container;
      this.notImplementedException = new UnsupportedOperationException("Not Implemented");
    }
    
    @Override
    public ApplicationSubmissionContext getSubmissionContext() {
      throw notImplementedException;
    }

    @Override
    public Resource getResource() {
     throw notImplementedException;
    }

    @Override
    public ApplicationId getApplicationID() {
      return appID;
    }

    @Override
    public ApplicationStatus getStatus() {
      throw notImplementedException;
    }
   
    @Override
    public ApplicationMaster getMaster() {
      throw notImplementedException;
    }

    @Override
    public Container getMasterContainer() {
      return container;
    }

    @Override
    public String getUser() {
      throw notImplementedException;
    }

    @Override
    public String getName() {
      throw notImplementedException;
    }

    @Override
    public String getQueue() {
      throw notImplementedException;
    }

    @Override
    public int getFailedCount() {
      throw notImplementedException;
    }

    @Override
    public ApplicationStore getStore() {
      throw notImplementedException;
    }

    @Override
    public long getStartTime() {
      throw notImplementedException;
    }

    @Override
    public long getFinishTime() {
      throw notImplementedException;
    }
  }
}