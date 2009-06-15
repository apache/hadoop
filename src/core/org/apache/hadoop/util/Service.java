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
import java.io.Serializable;
import java.io.Closeable;
import java.util.Date;
import java.util.ArrayList;
import java.util.List;

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
 * <li>If the service wishes to declare itself as having failed, so that
 * {@link #ping()} operations automatically fail, call
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
 * <li>Override {@link #innerPing(ServiceStatus)} with any health checks that a service
 * can perform to check that it is still "alive". These should be short lasting
 * and non-side effecting. Simple checks for valid data structures and live
 * worker threads are good examples. When the service thinks that something
 * has failed, throw an IOException with a meaningful error message!
 * </li>
 *
 * <li>Override {@link #innerClose()} to perform all shutdown logic.
 * Be robust here and shut down cleanly even if the service did not start up
 * completely. Do not assume all fields are non-null</li>
 *
 * You should not need to worry about making these overridden methods
 * synchronized, as they are only called when a service has entered a specific
 * state -which is synchronized. Except for {@link #innerPing(ServiceStatus)} ,
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
          = "Cannot initialize when unconfigured";

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
   * When completed, the service will be in the {@link ServiceState#STARTED}
   * state, or may have already transited to the {@link ServiceState#LIVE}
   * state
   *
   * Subclasses must implement their work in {@link #innerStart()}, leaving the
   * start() method to manage state checks and changes.
   *
   * @throws IOException           for any failure
   * @throws ServiceStateException when the service is not in a state from which
   *                               it can enter this state.
   */
  public void start() throws IOException {
    synchronized (this) {
      //this request is idempotent on either live or starting states; either
      //state is ignored
      ServiceState currentState = getServiceState();
      if (currentState == ServiceState.LIVE ||
              currentState == ServiceState.STARTED) {
        return;
      }
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
    }
  }

  /**
   * Ping: checks that a component considers itself live.
   *
   * This may trigger a health check in which the service probes its
   * constituent parts to verify that they are themselves live.
   * The base implementation considers any state other than
   * {@link ServiceState#FAILED} and {@link ServiceState#CLOSED}
   * to be valid, so it is OK to ping a
   * component that is still starting up. However, in such situations, the inner
   * ping health tests are skipped, because they are generally irrelevant.
   *
   * Subclasses should not normally override this method, but instead override
   * {@link #innerPing(ServiceStatus)} with extra health checks that will only
   * be called when a system is actually live.
   * @return the current service state.
   * @throws IOException           for any ping failure
   * @throws ServiceStateException if the component is in a wrong state.
   */
  protected ServiceStatus ping() throws IOException {
    ServiceStatus status = new ServiceStatus(this);
    ServiceState state = status.getState();
    if (state == ServiceState.LIVE) {
      try {
        innerPing(status);
      } catch (Throwable thrown) {
        //TODO: what happens whenthe ping() returns >0 causes of failure but 
        //doesn't throw an exception -this method will not get called. Is 
        //that what we want?
        status = onInnerPingFailure(status, thrown);
      }
    } else {
      //ignore the ping
      LOG.debug("ignoring ping request while in state " + state);
      //but tack on any non-null failure cause, which may be a valid value
      //in FAILED or TERMINATED states.
      status.addThrowable(getFailureCause());
    }
    return status;
  }

  /**
   * This is an override point for services -handle failure of the inner
   * ping operation.
   * The base implementation calls {@link #enterFailedState(Throwable)} and then
   * updates the service status with the (new) state and the throwable
   * that was caught.
   * @param currentStatus the current status structure
   * @param thrown the exception from the failing ping.
   * @return an updated service status structure.
   * @throws IOException for IO problems
   */
  protected ServiceStatus onInnerPingFailure(ServiceStatus currentStatus,
                                             Throwable thrown) 
          throws IOException {
    //something went wrong
    //mark as failed
    //TODO: don't enter failed state on a failing ping? Just report the event
    //to the caller?
    enterFailedState(thrown);
    //update the state
    currentStatus.updateState(this);
    currentStatus.addThrowable(thrown);
    //and return the exception.
    return currentStatus;
  }

  /**
   * Convert any exception to an {@link IOException}
   * If it already is an IOException, the exception is
   * returned as is. If it is anything else, it is wrapped, with
   * the original message retained.
   * @param thrown the exception to forward
   * @return an IOException representing or containing the forwarded exception
   */
  @SuppressWarnings({"ThrowableInstanceNeverThrown"})
  protected IOException forwardAsIOException(Throwable thrown) {
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
   * @return true if the service is in one of the two states.
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
  public void close() throws IOException {
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
   */
  protected void innerStart() throws IOException {
  }


  /**
   * This method is designed for overriding, with subclasses implementing health
   * tests inside it.
   *
   * It is invoked whenever the component is called with {@link Service#ping()}
   * and the call is not rejected.
   * @param status the service status, which can be updated
   * @throws IOException for any problem.
   */
  protected void innerPing(ServiceStatus status) throws IOException {
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
  protected boolean isValidStateTransition(ServiceState oldState,
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
   * Override point - a method called whenever there is a state change.
   *
   * The base class logs the event.
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
        //and wrap the exception in an IOException that is rethrown
        throw (IOException) new IOException(t.toString()).initCause(t);
    }
  }

  /**
   * Terminate a service that is not null
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
   * This is an exception that can be raised on a liveness failure.
   */
  public static class LivenessException extends IOException {

    /**
     * Constructs an exception with {@code null} as its error detail message.
     */
    public LivenessException() {
    }

    /**
     * Constructs an Exception with the specified detail message.
     *
     * @param message The detail message (which is saved for later retrieval by
     *                the {@link #getMessage()} method)
     */
    public LivenessException(String message) {
      super(message);
    }

    /**
     * Constructs an exception with the specified detail message and cause.
     *
     * <p> The detail message associated with {@code cause} is only incorporated
     * into this exception's detail message when the message parameter is null.
     *
     * @param message The detail message (which is saved for later retrieval by
     *                the {@link #getMessage()} method)
     * @param cause   The cause (which is saved for later retrieval by the
     *                {@link #getCause()} method).  (A null value is permitted,
     *                and indicates that the cause is nonexistent or unknown.)
     */
    public LivenessException(String message, Throwable cause) {
      super(message, cause);
    }

    /**
     * Constructs an exception with the specified cause and a detail message of
     * {@code cause.toString())}. A null cause is allowed.
     *
     * @param cause The cause (which is saved for later retrieval by the {@link
     *              #getCause()} method). Can be null.
     */
    public LivenessException(Throwable cause) {
      super(cause);
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
   * This is the full service status
   */
  public static final class ServiceStatus implements Serializable {
    /** enumerated state */
    private ServiceState state;

    /** name of the service */
    private String name;

    /** when did the state change?  */
    private Date lastStateChange;

    /**
     * a possibly null array of exceptions that caused a system failure
     */
    public ArrayList<Throwable> throwables = new ArrayList<Throwable>(0);

    /**
     * Create an empty service status instance
     */
    public ServiceStatus() {
    }

    /**
     * Create a service status instance with the base values set
     * @param name service name
     * @param state current state
     * @param lastStateChange when did the state last change?
     */
    public ServiceStatus(String name, ServiceState state,
                         Date lastStateChange) {
      this.state = state;
      this.name = name;
      this.lastStateChange = lastStateChange;
    }

    /**
     * Create a service status instance from the given service
     *
     * @param service service to read from
     */
    public ServiceStatus(Service service) {
      name = service.getServiceName();
      updateState(service);
    }

    /**
     * Add a new throwable to the list. This is a no-op if it is null.
     * To be safely sent over a network connection, the Throwable (and any
     * chained causes) must be fully serializable.
     * @param thrown the throwable. Can be null; will not be cloned.
     */
    public void addThrowable(Throwable thrown) {
      if (thrown != null) {
        throwables.add(thrown);
      }
    }

    /**
     * Get the list of throwables. This may be null.
     * @return A list of throwables or null
     */
    public List<Throwable> getThrowables() {
      return throwables;
    }

    /**
     * Get the current state
     * @return the state
     */
    public ServiceState getState() {
      return state;
    }

    /**
     * set the state
     * @param state new state
     */
    public void setState(ServiceState state) {
      this.state = state;
    }

    /**
     * Get the name of the service
     * @return the service name
     */
    public String getName() {
      return name;
    }

    /**
     * Set the name of the service
     * @param name the service name
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Get the date of the last state change
     * @return when the service state last changed
     */
    public Date getLastStateChange() {
      return lastStateChange;
    }

    /**
     * Set the last state change
     * @param lastStateChange the timestamp of the last state change
     */
    public void setLastStateChange(Date lastStateChange) {
      this.lastStateChange = lastStateChange;
    }

    /**
     * Update the service state
     * @param service the service to update from
     */
    public void updateState(Service service) {
      synchronized (service) {
        setState(service.getServiceState());
        setLastStateChange(service.lastStateChange);
      }
    }

    /**
     * The string operator includes the messages of every throwable
     * in the list of failures
     * @return the list of throwables
     */
    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append(getName()).append(" in state ").append(getState());
      for (Throwable t : throwables) {
        builder.append("\n ").append(t.toString());
      }
      return builder.toString();
    }
  }
}
