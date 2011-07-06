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
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.AMAllocatedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.SNEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * Negotiates with the scheduler for allocation of application master container.
 *
 */
public class SchedulerNegotiator extends AbstractService implements EventHandler<ASMEvent<SNEventType>> {

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
  private final List<Application> pendingApplications = 
    new ArrayList<Application>();

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
      List<Application> toSubmit = new ArrayList<Application>();
      List<Application> submittedApplications = new ArrayList<Application>();
      Map<ApplicationId, List<Container>> firstAllocate 
              = new HashMap<ApplicationId, List<Container>>();
      while (!shutdown && !isInterrupted()) {
        try {
          toSubmit.addAll(getPendingApplications());
          if (toSubmit.size() > 0) {
            LOG.info("Got " + toSubmit.size() + " applications to submit");

            submittedApplications.addAll(toSubmit);

            for (Application masterInfo: toSubmit) {
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

          for (Iterator<Application> it=submittedApplications.iterator(); 
          it.hasNext();) {
            Application application = it.next();
            ApplicationId appId = application.getMaster().getApplicationId();
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
              
              LOG.info("Found container " + container + " for AM of "
                  + application.getMaster());
              handler.handle(new AMAllocatedEvent(application
                  .getApplicationID(), container));
            }
          }

          // TODO: This can be detrimental. Use wait/notify.
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

  private Collection<? extends Application> getPendingApplications() {
    List<Application> pending = new ArrayList<Application>();
    synchronized (pendingApplications) {
      pending.addAll(pendingApplications);
      pendingApplications.clear();
    }
    return pending;
  }

  private void addPending(Application application) {
    LOG.info("Adding to pending " + application.getApplicationID());
    synchronized(pendingApplications) {
      pendingApplications.add(application);
    }
  }

  private void finishApplication(Application application)  
  throws IOException {
    LOG.info("Finishing application: cleaning up container " +
        application.getMasterContainer());
    //TODO we should release the container but looks like we just 
    // wait for update from NodeManager
    Container[] containers = new Container[] {application.getMasterContainer()};
    scheduler.allocate(application.getApplicationID(), 
        EMPTY_ASK, Arrays.asList(containers));
  }

  @Override
  public void handle(ASMEvent<SNEventType> appEvent)  {
    SNEventType eventType = appEvent.getType();
    Application application = appEvent.getApplication();
    switch (eventType) {
    case SCHEDULE:
      addPending(application);
      break;
    case RELEASE:
      try {
        finishApplication(application);
      } catch(IOException ie) {
        //TODO remove IOException from the scheduler.
        LOG.error("Error while releasing container for AM " + application.getApplicationID());
      }
      handler.handle(new ApplicationEvent(
          ApplicationEventType.RELEASED, application.getApplicationID()));
      break;
    default:
      LOG.warn("Unknown event " + eventType + " received. Ignoring.");
      break;
    }
  }
}