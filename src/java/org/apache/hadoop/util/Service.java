/*
 * Copyright  2008 The Apache Software Foundation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.hadoop.util;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.Closeable;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;

/**
 * This is the base class for services that can be deployed. A service is any
 * Hadoop class that has a standard lifecycle
 *
 * The lifecycle of a Service is:
 *
 * <ol>
 *
 * <li>Component is Created, enters the {@link ServiceState#CREATED} state.
 * This happens in the constructor. </li>
 *
 * <li>Component is started
 * through a call to {@link Service#start()} ()}. If successful, it enters the
 * {@link ServiceState#STARTED} state. If not, it enters the {@link
 * ServiceState#FAILED} state. </li>
 *
 * <li>Once the component considers itself
 * fully started, it enters the {@link ServiceState#LIVE} state. This implies it
 * is providing a service to external callers. </li>
 *
 * </ol>
 *
 * From any state, the service can be terminated/closed through a call to
 * {@link Service#close()}, which may throw an {@link IOException}, or
 * {@link Service#closeQuietly()}, which catched and logs any such exception.
 * These are idempotent calls, and will place the service in the
 * {@link ServiceState#CLOSED}, terminated  state, after which
 * it can no longer be used.
 *
 * To implement a Service.
 *
 * <ol>
 *
 * <li>Subclass this class</li>
 *
 * <li>Avoid doing any initialization/startup in the constructors, as this
 * breaks the lifecycle and prevents subclassing. </li>
 *
 * <li>If the service wishes to declare itself as having failed, call
 * {@link #enterFailedState(Throwable)} to enter the failed state.</li>
 *
 * <li>Override the {@link #innerStart()} method to start the service, including
 * starting any worker threads.</li>
 *
 * <li>In the {@link #innerStart()} method, if the service is immediately live
 * to external callers, call {@link #enterLiveState()} to mark the service as
 * live.</li>

 * <li>If startup is performed in separate threads, and includes bootstrap work,
 * call the  {@link #enterLiveState()} in the separate thread <i>when the
 * service is ready</i></li>
 *
 * <li>Override {@link #innerClose()} to perform all shutdown logic.
 * Be robust here and shut down cleanly even if the service did not start up
 * completely. Do not assume all fields are non-null</li>
 *
 *
 * You should not need to worry about making these overridden methods
 * synchronized, as they are only called when a service has entered a specific
 * state -which is synchronized. 
 * each method will only be called at most once in the life of a service instance.
 * However, because findbugs can flag synchronization warnings, it is often
 * simplest and safest to mark the innerX operations as synchronized.
 */

public abstract class Service extends Configured implements Closeable {

  private static final Log LOG = LogFactory.getLog(Service.class);

  /**
   * The initial state of a service is {@link ServiceState#CREATED}
   */
  private volatile ServiceState serviceState = ServiceState.CREATED;

  /**
   * when did the state change?
   */
  private volatile Date lastStateChange = new Date();

  /**
   * A root cause for failure. May be null.
   */
  private Throwable failureCause;
  
  private List<StateChangeListener> stateListeners;

  /**
   * Error string included in {@link ServiceStateException} exceptions
   * when an operation is applied to a service that is not in the correct
   * state for it.
   * value: {@value}
   */
  public static final String ERROR_WRONG_STATE = " is in the wrong state.";

  /**
   * Error string included in {@link ServiceStateException} exceptions
   * when a service with a null configuration is started
   * value: {@value}
   */
  public static final String ERROR_NO_CONFIGURATION
          = "Cannot initialize no Configuration has been provided";

  /**
   * Construct a service with no configuration; one must be called with {@link
   * #setConf(Configuration)} before the service is started
   */
  protected Service() {
  }

  /**
   * Construct a Configured service
   *
   * @param conf the configuration
   */
  protected Service(Configuration conf) {
    super(conf);
  }

  /**
   * Start any work (usually in separate threads).
   *
   * When successful, the service will be in the {@link ServiceState#STARTED}
   * state, or may have already transited to the {@link ServiceState#LIVE}
   * state
   *
   * When unsuccessful, the service will have entered the FAILED state and
   * then attempted to close down.
   * Subclasses must implement their work in {@link #innerStart()}, leaving the
   * start() method to manage state checks and changes.
   * 
   *
   * @throws IOException           for any failure
   * @throws ServiceStateException when the service is not in a state from which
   *                               it can enter this state.
   * @throws InterruptedException if the thread was interrupted on startup
   */
  public final void start() throws IOException, InterruptedException {
    synchronized (this) {
      //this request is idempotent on either live or starting states; either
      //state is ignored
      ServiceState currentState = getServiceState();
      if (currentState == ServiceState.LIVE ||
              currentState == ServiceState.STARTED) {
        return;
      }
      //sanity check: make sure that we are configured
      if (getConf() == null) {
        throw new ServiceStateException(ERROR_NO_CONFIGURATION,
                getServiceState());
      }
      //check and change states
      enterState(ServiceState.STARTED);
    }
    try {
      innerStart();
    } catch (IOException e) {
      enterFailedState(e);
      throw e;
    } catch (InterruptedException e) {
      //interruptions mean "stop trying to start the service"
      enterFailedState(e);
      throw e;
    }
  }


  /**
   * Convert any exception to an {@link IOException}
   * If it already is an IOException, the exception is
   * returned as is. If it is anything else, it is wrapped, with
   * the original message retained.
   * @param thrown the exception to forward
   * @return an IOException representing or containing the forwarded exception
   */
  protected static IOException forwardAsIOException(Throwable thrown) {
    IOException newException;
    if(thrown instanceof IOException) {
      newException = (IOException) thrown;
    } else {
      IOException ioe = new IOException(thrown.toString());
      ioe.initCause(thrown);
      newException = ioe;
    }
    return newException;
  }

  /**
   * Test for a service being in the {@link ServiceState#LIVE} or {@link
   * ServiceState#STARTED}
   *
   * @return true if the service is in the startup or live states.
   */
  public final boolean isRunning() {
    ServiceState currentState = getServiceState();
    return currentState == ServiceState.STARTED
            || currentState == ServiceState.LIVE;
  }

  /**
   * Shut down. This must be idempotent and turn errors into log/warn events -do
   * your best to clean up even in the face of adversity. This method should be
   * idempotent; if already terminated, return. Similarly, do not fail if the
   * component never actually started.
   *
   * The implementation calls {@link #close()} and then
   * {@link #logExceptionDuringQuietClose(Throwable)} if that method throws
   * any exception.
   */
  public final void closeQuietly() {
    try {
      close();
    } catch (Throwable e) {
      logExceptionDuringQuietClose(e);
    }
  }

  /**
   * Closes this service. Subclasses are free to throw an exception, but
   * they are expected to make a best effort attempt to close the service
   * down as thoroughly as possible.
   *
   * @throws IOException if an I/O error occurs
   */
  public final void close() throws IOException {
    if (enterState(ServiceState.CLOSED)) {
      innerClose();
    }
  }

  /**
   * This is a method called when exceptions are being logged and swallowed
   * during termination. It logs the event at the error level.
   *
   * Subclasses may override this to do more advanced error handling/logging.
   *
   * @param thrown whatever was thrown
   */
  protected void logExceptionDuringQuietClose(Throwable thrown) {
    LOG.error("Exception during termination: " + thrown,
            thrown);
  }

  /**
   * This method is designed for overriding, with subclasses implementing
   * startup logic inside it. It is only called when the component is entering
   * the running state; and will be called once only.
   *
   * When the work in here is completed, the component should set the service
   * state to {@link ServiceState#LIVE} to indicate the service is now live.
   *
   * @throws IOException for any problem.
   * @throws InterruptedException if the thread was interrupted on startup
   */
  protected void innerStart() throws IOException, InterruptedException {
  }

  /**
   * This method is designed for overriding, with subclasses implementing
   * termination logic inside it.
   *
   * It is only called when the component is entering the closed state; and
   * will be called once only.
   *
   * @throws IOException exceptions which will be logged
   */
  protected void innerClose() throws IOException {

  }

  /**
   * Get the current state of the service.
   *
   * @return the lifecycle state
   */
  public final ServiceState getServiceState() {
    return serviceState;
  }

  /**
   * This is the state transition graph represented as some nested switches.
   * @return true if the transition is valid. For all states, the result when
   * oldState==newState is false: that is not a transition, after all.
   * @param oldState the old state of the service
   * @param newState the new state
   */
  protected final boolean isValidStateTransition(ServiceState oldState,
                                           ServiceState newState) {
    switch(oldState) {
      case CREATED:
        switch(newState) {
          case STARTED:
          case FAILED:
          case CLOSED:
            return true;
          default:
            return false;
        }
      case STARTED:
        switch (newState) {
          case LIVE:
          case FAILED:
          case CLOSED:
            return true;
          default:
            return false;
        }
      case LIVE:
        switch (newState) {
          case STARTED:
          case FAILED:
          case CLOSED:
            return true;
          default:
            return false;
        }
      case UNDEFINED:
        //if we don't know where we were before (very, very unlikely), then
        //let's get out of it
        return true;
      case FAILED:
        //failure can only enter closed state
        return newState == ServiceState.CLOSED;
      case CLOSED:
        //This is the end state. There is no exit.
      default:
        return false;
    }
  }

  /**
  * Set the service state.
  * If there is a change in state, the {@link #lastStateChange} timestamp
  * is updated and the {@link #onStateChange(ServiceState, ServiceState)} event
  * is invoked.
  * @param serviceState the new state
  */
  protected final void setServiceState(ServiceState serviceState) {
    ServiceState oldState;
    synchronized (this) {
      oldState = this.serviceState;
      this.serviceState = serviceState;
    }
    if (oldState != serviceState) {
      lastStateChange = new Date();
      onStateChange(oldState, serviceState);
    }
  }



  /**
   * When did the service last change state
   * @return the last state change of this service
   */
  public final Date getLastStateChange() {
    return lastStateChange;
  }

  /**
   * Enter a new state if that is permitted from the current state.
   * Does nothing if we are in that state; throws an exception if the
   * state transition is not permitted
   * @param  newState  the new state
   * @return true if the service transitioned into this state, that is, it was
   *         not already in the state
   * @throws ServiceStateException if the service is not in either state
   */
  protected final synchronized boolean enterState(ServiceState newState)
          throws ServiceStateException {
    return enterState(getServiceState(), newState);
  }

  /**
   * Check that a service is in a required entry state, or already in the
   * desired exit state.
   *
   * @param entryState the state that is needed. If set to {@link
   *                   ServiceState#UNDEFINED} then the entry state is not
   *                   checked.
   * @param exitState  the state that is desired when we exit
   * @return true if the service transitioned into this state, that is, it was
   *         not already in the state
   * @throws ServiceStateException if the service is not in either state
   */
  protected final synchronized boolean enterState(ServiceState entryState,
                                            ServiceState exitState)
          throws ServiceStateException {
    ServiceState currentState = getServiceState();
    if (currentState == exitState) {
      return false;
    }
    validateStateTransition(entryState, exitState);
    setServiceState(exitState);
    return true;
  }

  /**
   * Check that the state transition is valid
   * @param entryState the entry state
   * @param exitState the exit state 
   * @throws ServiceStateException if the state transition is not allowed
   */
  protected final void validateStateTransition(ServiceState entryState,
                                         ServiceState exitState)
          throws ServiceStateException {
    if(!isValidStateTransition(entryState, exitState)) {
      throw new ServiceStateException(toString()
              + ERROR_WRONG_STATE
              + " The service cannot move from the state " + entryState
              + "to the state " + exitState,
              entryState);
    }
  }

  /**
   * Verify that a service is in a specific state
   *
   * @param state the state that is required.
   * @throws ServiceStateException if the service is in the wrong state
   */
  public final void verifyServiceState(ServiceState state)
          throws ServiceStateException {
    verifyState(getServiceState(), state, ServiceState.UNDEFINED);
  }

  /**
   * Verify that a service is in either of two specific states
   *
   * @param expected  the state that is expected.
   * @param expected2 a second state, which can be left at {@link
   *                  ServiceState#UNDEFINED} for "do not check this"
   * @throws ServiceStateException if the service is in the wrong state
   */
  public final void verifyServiceState(ServiceState expected, ServiceState expected2)
          throws ServiceStateException {
    verifyState(getServiceState(), expected, expected2);
  }

  /**
   * Internal state verification test
   *
   * @param currentState the current state
   * @param expected     the state that is expected.
   * @param expected2    a second state, which can be left at {@link
   *                     ServiceState#UNDEFINED} for "do not check this"
   * @throws ServiceStateException if the service is in the wrong state
   */
  protected final void verifyState(ServiceState currentState,
                             ServiceState expected,
                             ServiceState expected2)
          throws ServiceStateException {
    boolean expected2defined = expected2 != ServiceState.UNDEFINED;
    if (!(currentState == expected ||
            (expected2defined && currentState == expected2))) {
      throw new ServiceStateException(toString()
              + ERROR_WRONG_STATE
              + " Expected " + expected
              + (expected2defined ? (" or " + expected2) : " ")
              + " but the actual state is " + currentState,
              currentState);
    }
  }

  /**
   * Helper method to enter the {@link ServiceState#FAILED} state.
   *
   * Call this whenever the service considers itself to have failed in a
   * non-restartable manner.
   *
   * If the service was already terminated or failed, this operation does
   * not trigger a state change.
   * @param cause the cause of the failure
   */
  public final void enterFailedState(Throwable cause) {
    synchronized (this) {
      if(failureCause == null) {
        failureCause = cause;
      }
    }
    if(!isTerminated()) {
      setServiceState(ServiceState.FAILED);
    }
  }


  /**
   * Shortcut method to enter the {@link ServiceState#LIVE} state.
   *
   * Call this when a service considers itself live
   * @return true if this state was entered, false if it was already in it
   * @throws ServiceStateException if the service is not currently in the
   * STARTED or LIVE states
   */
  protected final boolean enterLiveState() throws ServiceStateException {
    return enterState(ServiceState.LIVE);
  }

  /**
   * Test for the service being terminated; non-blocking
   *
   * @return true if the service is currently terminated
   */
  public boolean isTerminated() {
    return getServiceState() == ServiceState.CLOSED;
  }


  /**
   * Override point: the name of this service. This is used
   * to construct human-readable descriptions
   * @return the name of this service for string messages
   */
  public String getServiceName() {
    return "Service";
  }

  /**
  * The toString operator returns the super class name/id, and the state. This
  * gives all services a slightly useful message in a debugger or test report
  *
  * @return a string representation of the object.
  */
  @Override
  public String toString() {
    return getServiceName() + " instance " + super.toString() + " in state "
            + getServiceState();
  }


  /**
   * Get the cause of failure -will be null if not failed, and may be
   * null even after failure.
   * @return the exception that triggered entry into the failed state.
   *
   */
  public Throwable getFailureCause() {
    return failureCause;
  }

  /**
   * Initialize and start a service. If the service fails to come up, it is
   * terminated.
   *
   * @param service the service to deploy
   * @throws IOException on any failure to deploy
   */
  public static void startService(Service service)
          throws IOException {
    try {
      service.start();
    } catch (IOException e) {
      //mark as failed
      service.enterFailedState(e);
      //we assume that the service really does know how to terminate
      service.closeQuietly();
      throw e;
    } catch (Throwable t) {
      //mark as failed
      service.enterFailedState(t);
      //we assume that the service really does know how to terminate
      service.closeQuietly();
      throw forwardAsIOException(t);
    }
  }

  /**
   * Terminate a service that is not null, by calling its {@link #closeQuietly()} method
   *
   * @param service a service to terminate
   */
  public static void closeQuietly(Service service) {
    if (service != null) {
      service.closeQuietly();
    }
  }

  /**
   * Terminate a service or other closeable that is not null
   *
   * @param closeable the object to close
   */
  public static void close(Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException e) {
        LOG.info("when closing :" + closeable+ ":" + e, e);
      }
    }
  }


  /**
   * Get the current number of workers
   * @return the worker count, or -1 for "this service has no workers"
   */

  public int getLiveWorkerCount() {
    return -1;
  }

  /**
   * Override point - a method called whenever there is a state change.
   *
   * The base class logs the event and notifies all state change listeners.
   *
   * @param oldState existing state
   * @param newState new state.
   */
  protected void onStateChange(ServiceState oldState,
                               ServiceState newState) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("State Change: " + toString()
              + " transitioned from state " + oldState + " to " + newState);
    }
    
    //copy all the listeners out of the list
    //this is to give us access to an unsynchronized copy of the listeners, which
    //can then have the state notifications called outside of any synchronized 
    //sectoin
    StateChangeListener[] listeners = null;
    synchronized (this) {
      if (stateListeners != null) {
        listeners = new StateChangeListener[stateListeners
                .size()];
        stateListeners.toArray(listeners);
      } else {
        //no listeners, exit here
        return;
      }
    }
    // issue the notifications
    for (StateChangeListener listener : listeners) {
      listener.onStateChange(this, oldState, newState);
    }
  }

  /**
   * Add a new state change listener
   * @param listener a new state change listener
   */
  public synchronized void addStateChangeListener(StateChangeListener listener) {
    if(stateListeners==null) {
      stateListeners = new ArrayList<StateChangeListener>(1);
    }
    stateListeners.add(listener);
  }

  /**
   * Remove a state change listener. This is an idempotent operation; it is 
   * not an error to attempt to remove a listener which is not present
   * @param listener a state change listener
   */
  public synchronized void removeStateChangeListener(StateChangeListener listener) {
    if (stateListeners != null) {
      stateListeners.remove(listener);
    }
  }

  
  /**
   * An exception that indicates there is something wrong with the state of the
   * service
   */
  public static class ServiceStateException extends IOException {
    private ServiceState state;


    /**
     * Create a service state exception with a standard message {@link
     * Service#ERROR_WRONG_STATE} including the string value of the owning
     * service, and the supplied state value
     *
     * @param service owning service
     * @param state current state
     */
    public ServiceStateException(Service service, ServiceState state) {
      this(service.toString()
              + ERROR_WRONG_STATE + " : " + state,
              null,
              state);
    }

    /**
     * Constructs an Exception with the specified detail message and service
     * state.
     *
     * @param message The detail message (which is saved for later retrieval by
     *                the {@link #getMessage()} method)
     * @param state   the current state of the service
     */
    public ServiceStateException(String message, ServiceState state) {
      this(message, null, state);
    }

    /**
     * Constructs an Exception with the specified detail message, cause and
     * service state.
     *
     * @param message message
     * @param cause   optional root cause
     * @param state   the state of the component
     */
    public ServiceStateException(String message,
                                 Throwable cause,
                                 ServiceState state) {
      super(message, cause);
      this.state = state;
    }

    /**
     * Construct an exception. The lifecycle state of the specific component is
     * extracted
     *
     * @param message message
     * @param cause   optional root cause
     * @param service originating service
     */
    public ServiceStateException(String message,
                                 Throwable cause,
                                 Service service) {
      this(message, cause, service.getServiceState());
    }

    /**
     * Get the state when this exception was raised
     *
     * @return the state of the service
     */
    public ServiceState getState() {
      return state;
    }


  }

  /**
   * The state of the service as perceived by the service itself. Failure is the
   * odd one as it often takes a side effecting test (or an outsider) to
   * observe.
   */
  public enum ServiceState {
    /**
     * we don't know or care what state the service is in
     */
    UNDEFINED,
    /**
     * The service has been created
     */
    CREATED,

    /**
     * The service is starting up.
     * Its {@link Service#start()} method has been called.
     * When it is ready for work, it will declare itself LIVE.
     */
    STARTED,
    /**
     * The service is now live and available for external use
     */
    LIVE,
    /**
     * The service has failed
     */
    FAILED,
      /**
     * the service has been shut down
     * The container process may now destroy the instance
     * Its {@link Service#close()} ()} method has been called.
     */
    CLOSED
  }

    /**
     * This is the interface that state change listeners must implement; when registered they will be notified
     * after a service has changed state (in the same thread as the service itself).
     */
  public interface StateChangeListener {
    
    /**
     * This method is called for any listener. 
     *
     * The base class logs the event.
     * @param service the service whose state is changing
     * @param oldState existing state
     * @param newState new state.
     */
    void onStateChange(Service service, 
                       ServiceState oldState,
                       ServiceState newState);

  }
}
