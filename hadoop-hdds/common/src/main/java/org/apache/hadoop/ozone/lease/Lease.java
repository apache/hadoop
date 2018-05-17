/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.lease;

import org.apache.hadoop.util.Time;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * This class represents the lease created on a resource. Callback can be
 * registered on the lease which will be executed in case of timeout.
 *
 * @param <T> Resource type for which the lease can be associated
 */
public class Lease<T> {

  /**
   * The resource for which this lease is created.
   */
  private final T resource;

  private final long creationTime;

  /**
   * Lease lifetime in milliseconds.
   */
  private volatile long leaseTimeout;

  private boolean expired;

  /**
   * Functions to be called in case of timeout.
   */
  private List<Callable<Void>> callbacks;


  /**
   * Creates a lease on the specified resource with given timeout.
   *
   * @param resource
   *        Resource for which the lease has to be created
   * @param timeout
   *        Lease lifetime in milliseconds
   */
  public Lease(T resource, long timeout) {
    this.resource = resource;
    this.leaseTimeout = timeout;
    this.callbacks = Collections.synchronizedList(new ArrayList<>());
    this.creationTime = Time.monotonicNow();
    this.expired = false;
  }

  /**
   * Returns true if the lease has expired, else false.
   *
   * @return true if expired, else false
   */
  public boolean hasExpired() {
    return expired;
  }

  /**
   * Registers a callback which will be executed in case of timeout. Callbacks
   * are executed in a separate Thread.
   *
   * @param callback
   *        The Callable which has to be executed
   * @throws LeaseExpiredException
   *         If the lease has already timed out
   */
  public void registerCallBack(Callable<Void> callback)
      throws LeaseExpiredException {
    if(hasExpired()) {
      throw new LeaseExpiredException("Resource: " + resource);
    }
    callbacks.add(callback);
  }

  /**
   * Returns the time elapsed since the creation of lease.
   *
   * @return elapsed time in milliseconds
   * @throws LeaseExpiredException
   *         If the lease has already timed out
   */
  public long getElapsedTime() throws LeaseExpiredException {
    if(hasExpired()) {
      throw new LeaseExpiredException("Resource: " + resource);
    }
    return Time.monotonicNow() - creationTime;
  }

  /**
   * Returns the time available before timeout.
   *
   * @return remaining time in milliseconds
   * @throws LeaseExpiredException
   *         If the lease has already timed out
   */
  public long getRemainingTime() throws LeaseExpiredException {
    if(hasExpired()) {
      throw new LeaseExpiredException("Resource: " + resource);
    }
    return leaseTimeout - getElapsedTime();
  }

  /**
   * Returns total lease lifetime.
   *
   * @return total lifetime of lease in milliseconds
   * @throws LeaseExpiredException
   *         If the lease has already timed out
   */
  public long getLeaseLifeTime() throws LeaseExpiredException {
    if(hasExpired()) {
      throw new LeaseExpiredException("Resource: " + resource);
    }
    return leaseTimeout;
  }

  /**
   * Renews the lease timeout period.
   *
   * @param timeout
   *        Time to be added to the lease in milliseconds
   * @throws LeaseExpiredException
   *         If the lease has already timed out
   */
  public void renew(long timeout) throws LeaseExpiredException {
    if(hasExpired()) {
      throw new LeaseExpiredException("Resource: " + resource);
    }
    leaseTimeout += timeout;
  }

  @Override
  public int hashCode() {
    return resource.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if(obj instanceof Lease) {
      return resource.equals(((Lease) obj).resource);
    }
    return false;
  }

  @Override
  public String toString() {
    return "Lease<" + resource.toString() + ">";
  }

  /**
   * Returns the callbacks to be executed for the lease in case of timeout.
   *
   * @return callbacks to be executed
   */
  List<Callable<Void>> getCallbacks() {
    return callbacks;
  }

  /**
   * Expires/Invalidates the lease.
   */
  void invalidate() {
    callbacks = null;
    expired = true;
  }

}
