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

package org.apache.hadoop.service.workflow;

import com.google.common.base.Preconditions;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceParent;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.service.ServiceStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This resembles the YARN CompositeService, except that it
 * starts one service after another
 * <p>
 * Workflow
 * <ol>
 *   <li>When the <code>WorkflowSequenceService</code> instance is
 *   initialized, it only initializes itself.</li>
 *   
 *   <li>When the <code>WorkflowSequenceService</code> instance is
 *   started, it initializes then starts the first of its children.
 *   If there are no children, it immediately stops.</li>
 *   
 *   <li>When the active child stops, it did not fail, and the parent has not
 *   stopped -then the next service is initialized and started. If there is no
 *   remaining child the parent service stops.</li>
 *   
 *   <li>If the active child did fail, the parent service notes the exception
 *   and stops -effectively propagating up the failure.
 *   </li>
 * </ol>
 * 
 * New service instances MAY be added to a running instance -but no guarantees
 * can be made as to whether or not they will be run.
 */

public class WorkflowSequenceService extends AbstractService implements
    ServiceParent, ServiceStateChangeListener {

  private static final Logger LOG =
      LoggerFactory.getLogger(WorkflowSequenceService.class);

  /**
   * list of services.
   */
  private final List<Service> serviceList = new ArrayList<>();

  /**
   * The currently active service.
   * Volatile -may change & so should be read into a 
   * local variable before working with
   */
  private volatile Service activeService;

  /**
   * The previous service to the one (if any) that is running.
   *  <code>null</code> if none have finished yet; this implicitly
   *  holds before any service has been started.
   */
  private volatile Service previousService;

  /**
   * Construct an instance.
   * @param name service name
   */
  public WorkflowSequenceService(String name) {
    super(name);
  }

  /**
   * Construct an instance with the default name.
   */
  public WorkflowSequenceService() {
    this("WorkflowSequenceService");
  }

  /**
   * Create a service sequence with the given list of services.
   * @param name service name
   * @param children initial sequence
   */
  public WorkflowSequenceService(String name, Service... children) {
    super(name);
    for (Service service : children) {
      addService(service);
    }
  }

  /**
   * Create a service sequence with the given list of services.
   * @param name service name
   * @param children initial sequence
   */
  public WorkflowSequenceService(String name, List<Service> children) {
    super(name);
    for (Service service : children) {
      addService(service);
    }
  }

  /**
   * Get the current service -which may be null.
   * @return service running
   */
  public Service getActiveService() {
    return activeService;
  }

  /**
   * Get the previously active service.
   * @return the service last run, or null if there is none.
   */
  public Service getPreviousService() {
    return previousService;
  }

  /**
   * start the first service.
   *
   * If there are none to start, <code>stop()</code> this service.
   * @throws Exception
   */
  @Override
  protected void serviceStart() throws Exception {
    if (!startNextService()) {
      //nothing to start -so stop
      stop();
    }
  }

  /**
   * Stop the service.
   *
   * This stops any currently running child service.
   * @throws Exception on any failure
   */
  @Override
  protected void serviceStop() throws Exception {
    //stop current service.
    //this triggers a callback that is caught and ignored
    Service current = activeService;
    previousService = current;
    activeService = null;
    if (current != null) {
      current.stop();
    }
  }

  /**
   * Start the next service in the list.
   *
   * Return false if there are no more services to run, or this
   * service has stopped
   * @return true if a service was started
   * @throws RuntimeException from any init or start failure
   * @throws ServiceStateException if this call is made before
   * the service is started
   */
  public synchronized boolean startNextService() {
    LOG.debug("Starting next child service");
    if (isInState(STATE.STOPPED)) {
      //downgrade to a failed
      LOG.debug("Not starting next service -{} is stopped", this);
      return false;
    }
    if (!isInState(STATE.STARTED)) {
      //reject attempts to start a service too early
      throw new ServiceStateException(
        "Cannot start a child service when not started");
    }
    if (serviceList.isEmpty()) {
      //nothing left to run
      LOG.debug("No services left to start");
      return false;
    }
    if (activeService != null && activeService.getFailureCause() != null) {
      //did the last service fail? Is this caused by some premature callback?
      LOG.debug("Not starting next service due to a failure of {}",
          activeService);
      return false;
    }

    //bear in mind that init & start can fail, which
    //can trigger re-entrant calls into the state change listener.
    //by setting the current service to null
    //the start-next-service logic is skipped.
    //now, what does that mean w.r.t exit states?

    activeService = null;
    Service head = serviceList.remove(0);
    LOG.debug("Starting {}", head);
    try {
      head.init(getConfig());
      head.registerServiceListener(this);
      head.start();
    } catch (RuntimeException e) {
      noteFailure(e);
      throw e;
    }
    //at this point the service must have explicitly started & not failed,
    //else an exception would have been raised
    activeService = head;

    return true;
  }

  /**
   * State change event relays service stop events to
   * {@link #onServiceCompleted(Service)}.
   *
   * Subclasses can extend that with extra logic
   * @param service the service that has changed.
   */
  @Override
  public void stateChanged(Service service) {
    // only react to the state change when it is the current service
    // and it has entered the STOPPED state
    if (service == activeService && service.isInState(STATE.STOPPED)) {
      onServiceCompleted(service);
    }
  }

  /**
   * Handler for service completion: base class starts the next service.
   * @param service service that has completed
   */
  protected synchronized void onServiceCompleted(Service service) {
    LOG.info("Running service stopped: {}", service);
    previousService = activeService;
    //start the next service if we are not stopped ourselves
    if (isInState(STATE.STARTED)) {

      //did the service fail? if so: propagate
      Throwable failureCause = service.getFailureCause();
      if (failureCause != null) {
        Exception e = (failureCause instanceof Exception)
            ? (Exception) failureCause
            : new Exception(failureCause);
        noteFailure(e);
        stop();
      }
      
      //start the next service
      boolean started;
      try {
        started = startNextService();
      } catch (Exception e) {
        //something went wrong here
        noteFailure(e);
        started = false;
      }
      if (!started) {
        //no start because list is empty
        //stop and expect the notification to go upstream
        stop();
      }
    } else {
      //not started, so just note that the current service
      //has gone away
      activeService = null;
    }
  }

  /**
   * Add the passed {@link Service} to the list of services managed by this.
   * {@link WorkflowSequenceService}
   * @param service the {@link Service} to be added
   */
  @Override
  public synchronized void addService(Service service) {
    Preconditions.checkArgument(service != null, "null service argument");
    LOG.debug("Adding service {} ", service.getName());
    synchronized (serviceList) {
      serviceList.add(service);
    }
  }

  /**
   * Get an unmodifiable list of services.
   * @return a list of child services at the time of invocation -
   * added services will not be picked up.
   */
  @Override //Parent
  public synchronized List<Service> getServices() {
    return Collections.unmodifiableList(serviceList);
  }

  @Override // Object
  public synchronized String toString() {
    return super.toString() + "; current service " + activeService
           + "; queued service count=" + serviceList.size();
  }

}
