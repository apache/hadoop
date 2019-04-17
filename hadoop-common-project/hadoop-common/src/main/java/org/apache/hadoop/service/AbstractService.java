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

package org.apache.hadoop.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the base implementation class for services.
 */
@Public
@Evolving
public abstract class AbstractService implements Service {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractService.class);

  /**
   * Service name.
   */
  private final String name;

  /** service state */
  private final ServiceStateModel stateModel;

  /**
   * Service start time. Will be zero until the service is started.
   */
  private long startTime;

  /**
   * The configuration. Will be null until the service is initialized.
   */
  private volatile Configuration config;

  /**
   * List of state change listeners; it is final to ensure
   * that it will never be null.
   */
  private final ServiceOperations.ServiceListeners listeners
    = new ServiceOperations.ServiceListeners();
  /**
   * Static listeners to all events across all services
   */
  private static ServiceOperations.ServiceListeners globalListeners
    = new ServiceOperations.ServiceListeners();

  /**
   * The cause of any failure -will be null.
   * if a service did not stop due to a failure.
   */
  private Exception failureCause;

  /**
   * the state in which the service was when it failed.
   * Only valid when the service is stopped due to a failure
   */
  private STATE failureState = null;

  /**
   * object used to co-ordinate {@link #waitForServiceToStop(long)}
   * across threads.
   */
  private final AtomicBoolean terminationNotification =
    new AtomicBoolean(false);

  /**
   * History of lifecycle transitions
   */
  private final List<LifecycleEvent> lifecycleHistory
    = new ArrayList<LifecycleEvent>(5);

  /**
   * Map of blocking dependencies
   */
  private final Map<String,String> blockerMap = new HashMap<String, String>();

  private final Object stateChangeLock = new Object();
 
  /**
   * Construct the service.
   * @param name service name
   */
  public AbstractService(String name) {
    this.name = name;
    stateModel = new ServiceStateModel(name);
  }

  @Override
  public final STATE getServiceState() {
    return stateModel.getState();
  }

  @Override
  public final synchronized Throwable getFailureCause() {
    return failureCause;
  }

  @Override
  public synchronized STATE getFailureState() {
    return failureState;
  }

  /**
   * Set the configuration for this service.
   * This method is called during {@link #init(Configuration)}
   * and should only be needed if for some reason a service implementation
   * needs to override that initial setting -for example replacing
   * it with a new subclass of {@link Configuration}
   * @param conf new configuration.
   */
  protected void setConfig(Configuration conf) {
    this.config = conf;
  }

  /**
   * {@inheritDoc}
   * This invokes {@link #serviceInit}
   * @param conf the configuration of the service. This must not be null
   * @throws ServiceStateException if the configuration was null,
   * the state change not permitted, or something else went wrong
   */
  @Override
  public void init(Configuration conf) {
    if (conf == null) {
      throw new ServiceStateException("Cannot initialize service "
                                      + getName() + ": null configuration");
    }
    if (isInState(STATE.INITED)) {
      return;
    }
    synchronized (stateChangeLock) {
      if (enterState(STATE.INITED) != STATE.INITED) {
        setConfig(conf);
        try {
          serviceInit(config);
          if (isInState(STATE.INITED)) {
            //if the service ended up here during init,
            //notify the listeners
            notifyListeners();
          }
        } catch (Exception e) {
          noteFailure(e);
          ServiceOperations.stopQuietly(LOG, this);
          throw ServiceStateException.convert(e);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   * @throws ServiceStateException if the current service state does not permit
   * this action
   */
  @Override
  public void start() {
    if (isInState(STATE.STARTED)) {
      return;
    }
    //enter the started state
    synchronized (stateChangeLock) {
      if (stateModel.enterState(STATE.STARTED) != STATE.STARTED) {
        try {
          startTime = System.currentTimeMillis();
          serviceStart();
          if (isInState(STATE.STARTED)) {
            //if the service started (and isn't now in a later state), notify
            LOG.debug("Service {} is started", getName());
            notifyListeners();
          }
        } catch (Exception e) {
          noteFailure(e);
          ServiceOperations.stopQuietly(LOG, this);
          throw ServiceStateException.convert(e);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop() {
    if (isInState(STATE.STOPPED)) {
      return;
    }
    synchronized (stateChangeLock) {
      if (enterState(STATE.STOPPED) != STATE.STOPPED) {
        try {
          serviceStop();
        } catch (Exception e) {
          //stop-time exceptions are logged if they are the first one,
          noteFailure(e);
          throw ServiceStateException.convert(e);
        } finally {
          //report that the service has terminated
          terminationNotification.set(true);
          synchronized (terminationNotification) {
            terminationNotification.notifyAll();
          }
          //notify anything listening for events
          notifyListeners();
        }
      } else {
        //already stopped: note it
        LOG.debug("Ignoring re-entrant call to stop()");
      }
    }
  }

  /**
   * Relay to {@link #stop()}
   * @throws IOException
   */
  @Override
  public final void close() throws IOException {
    stop();
  }

  /**
   * Failure handling: record the exception
   * that triggered it -if there was not one already.
   * Services are free to call this themselves.
   * @param exception the exception
   */
  protected final void noteFailure(Exception exception) {
    LOG.debug("noteFailure", exception);
    if (exception == null) {
      //make sure failure logic doesn't itself cause problems
      return;
    }
    //record the failure details, and log it
    synchronized (this) {
      if (failureCause == null) {
        failureCause = exception;
        failureState = getServiceState();
        LOG.info("Service {} failed in state {}",
            getName(), failureState, exception);
      }
    }
  }

  @Override
  public final boolean waitForServiceToStop(long timeout) {
    boolean completed = terminationNotification.get();
    while (!completed) {
      try {
        synchronized(terminationNotification) {
          terminationNotification.wait(timeout);
        }
        // here there has been a timeout, the object has terminated,
        // or there has been a spurious wakeup (which we ignore)
        completed = true;
      } catch (InterruptedException e) {
        // interrupted; have another look at the flag
        completed = terminationNotification.get();
      }
    }
    return terminationNotification.get();
  }

  /* ===================================================================== */
  /* Override Points */
  /* ===================================================================== */

  /**
   * All initialization code needed by a service.
   *
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   *
   * Implementations do not need to be synchronized as the logic
   * in {@link #init(Configuration)} prevents re-entrancy.
   *
   * The base implementation checks to see if the subclass has created
   * a new configuration instance, and if so, updates the base class value
   * @param conf configuration
   * @throws Exception on a failure -these will be caught,
   * possibly wrapped, and will trigger a service stop
   */
  protected void serviceInit(Configuration conf) throws Exception {
    if (conf != config) {
      LOG.debug("Config has been overridden during init");
      setConfig(conf);
    }
  }

  /**
   * Actions called during the INITED to STARTED transition.
   *
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   *
   * Implementations do not need to be synchronized as the logic
   * in {@link #start()} prevents re-entrancy.
   *
   * @throws Exception if needed -these will be caught,
   * wrapped, and trigger a service stop
   */
  protected void serviceStart() throws Exception {

  }

  /**
   * Actions called during the transition to the STOPPED state.
   *
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   *
   * Implementations do not need to be synchronized as the logic
   * in {@link #stop()} prevents re-entrancy.
   *
   * Implementations MUST write this to be robust against failures, including
   * checks for null references -and for the first failure to not stop other
   * attempts to shut down parts of the service.
   *
   * @throws Exception if needed -these will be caught and logged.
   */
  protected void serviceStop() throws Exception {

  }

  @Override
  public void registerServiceListener(ServiceStateChangeListener l) {
    listeners.add(l);
  }

  @Override
  public void unregisterServiceListener(ServiceStateChangeListener l) {
    listeners.remove(l);
  }

  /**
   * Register a global listener, which receives notifications
   * from the state change events of all services in the JVM
   * @param l listener
   */
  public static void registerGlobalListener(ServiceStateChangeListener l) {
    globalListeners.add(l);
  }

  /**
   * unregister a global listener.
   * @param l listener to unregister
   * @return true if the listener was found (and then deleted)
   */
  public static boolean unregisterGlobalListener(ServiceStateChangeListener l) {
    return globalListeners.remove(l);
  }

  /**
   * Package-scoped method for testing -resets the global listener list
   */
  @VisibleForTesting
  static void resetGlobalListeners() {
    globalListeners.reset();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Configuration getConfig() {
    return config;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  /**
   * Notify local and global listeners of state changes.
   * Exceptions raised by listeners are NOT passed up.
   */
  private void notifyListeners() {
    try {
      listeners.notifyListeners(this);
      globalListeners.notifyListeners(this);
    } catch (Throwable e) {
      LOG.warn("Exception while notifying listeners of {}", this, e);
    }
  }

  /**
   * Add a state change event to the lifecycle history
   */
  private void recordLifecycleEvent() {
    LifecycleEvent event = new LifecycleEvent();
    event.time = System.currentTimeMillis();
    event.state = getServiceState();
    lifecycleHistory.add(event);
  }

  @Override
  public synchronized List<LifecycleEvent> getLifecycleHistory() {
    return new ArrayList<LifecycleEvent>(lifecycleHistory);
  }

  /**
   * Enter a state; record this via {@link #recordLifecycleEvent}
   * and log at the info level.
   * @param newState the proposed new state
   * @return the original state
   * it wasn't already in that state, and the state model permits state re-entrancy.
   */
  private STATE enterState(STATE newState) {
    assert stateModel != null : "null state in " + name + " " + this.getClass();
    STATE oldState = stateModel.enterState(newState);
    if (oldState != newState) {
      LOG.debug("Service: {} entered state {}", getName(), getServiceState());

      recordLifecycleEvent();
    }
    return oldState;
  }

  @Override
  public final boolean isInState(Service.STATE expected) {
    return stateModel.isInState(expected);
  }

  @Override
  public String toString() {
    return "Service " + name + " in state " + stateModel;
  }

  /**
   * Put a blocker to the blocker map -replacing any
   * with the same name.
   * @param name blocker name
   * @param details any specifics on the block. This must be non-null.
   */
  protected void putBlocker(String name, String details) {
    synchronized (blockerMap) {
      blockerMap.put(name, details);
    }
  }

  /**
   * Remove a blocker from the blocker map -
   * this is a no-op if the blocker is not present
   * @param name the name of the blocker
   */
  public void removeBlocker(String name) {
    synchronized (blockerMap) {
      blockerMap.remove(name);
    }
  }

  @Override
  public Map<String, String> getBlockers() {
    synchronized (blockerMap) {
      Map<String, String> map = new HashMap<String, String>(blockerMap);
      return map;
    }
  }
}
