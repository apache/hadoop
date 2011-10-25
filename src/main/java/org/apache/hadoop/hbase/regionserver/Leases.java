/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.HasThread;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Delayed;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

import java.io.IOException;

/**
 * Leases
 *
 * There are several server classes in HBase that need to track external
 * clients that occasionally send heartbeats.
 *
 * <p>These external clients hold resources in the server class.
 * Those resources need to be released if the external client fails to send a
 * heartbeat after some interval of time passes.
 *
 * <p>The Leases class is a general reusable class for this kind of pattern.
 * An instance of the Leases class will create a thread to do its dirty work.
 * You should close() the instance if you want to clean up the thread properly.
 *
 * <p>
 * NOTE: This class extends Thread rather than Chore because the sleep time
 * can be interrupted when there is something to do, rather than the Chore
 * sleep time which is invariant.
 */
public class Leases extends HasThread {
  private static final Log LOG = LogFactory.getLog(Leases.class.getName());
  private final int leasePeriod;
  private final int leaseCheckFrequency;
  private volatile DelayQueue<Lease> leaseQueue = new DelayQueue<Lease>();
  protected final Map<String, Lease> leases = new HashMap<String, Lease>();
  private volatile boolean stopRequested = false;

  /**
   * Creates a lease monitor
   *
   * @param leasePeriod - length of time (milliseconds) that the lease is valid
   * @param leaseCheckFrequency - how often the lease should be checked
   * (milliseconds)
   */
  public Leases(final int leasePeriod, final int leaseCheckFrequency) {
    this.leasePeriod = leasePeriod;
    this.leaseCheckFrequency = leaseCheckFrequency;
    setDaemon(true);
  }

  /**
   * @see java.lang.Thread#run()
   */
  @Override
  public void run() {
    while (!stopRequested || (stopRequested && leaseQueue.size() > 0) ) {
      Lease lease = null;
      try {
        lease = leaseQueue.poll(leaseCheckFrequency, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        continue;
      } catch (ConcurrentModificationException e) {
        continue;
      } catch (Throwable e) {
        LOG.fatal("Unexpected exception killed leases thread", e);
        break;
      }
      if (lease == null) {
        continue;
      }
      // A lease expired.  Run the expired code before removing from queue
      // since its presence in queue is used to see if lease exists still.
      if (lease.getListener() == null) {
        LOG.error("lease listener is null for lease " + lease.getLeaseName());
      } else {
        lease.getListener().leaseExpired();
      }
      synchronized (leaseQueue) {
        leases.remove(lease.getLeaseName());
      }
    }
    close();
  }

  /**
   * Shuts down this lease instance when all outstanding leases expire.
   * Like {@link #close()} but rather than violently end all leases, waits
   * first on extant leases to finish.  Use this method if the lease holders
   * could loose data, leak locks, etc.  Presumes client has shutdown
   * allocation of new leases.
   */
  public void closeAfterLeasesExpire() {
    this.stopRequested = true;
  }

  /**
   * Shut down this Leases instance.  All pending leases will be destroyed,
   * without any cancellation calls.
   */
  public void close() {
    LOG.info(Thread.currentThread().getName() + " closing leases");
    this.stopRequested = true;
    synchronized (leaseQueue) {
      leaseQueue.clear();
      leases.clear();
      leaseQueue.notifyAll();
    }
    LOG.info(Thread.currentThread().getName() + " closed leases");
  }

  /**
   * Obtain a lease
   *
   * @param leaseName name of the lease
   * @param listener listener that will process lease expirations
   * @throws LeaseStillHeldException
   */
  public void createLease(String leaseName, final LeaseListener listener)
  throws LeaseStillHeldException {
    addLease(new Lease(leaseName, listener));
  }

  /**
   * Inserts lease.  Resets expiration before insertion.
   * @param lease
   * @throws LeaseStillHeldException
   */
  public void addLease(final Lease lease) throws LeaseStillHeldException {
    if (this.stopRequested) {
      return;
    }
    lease.setExpirationTime(System.currentTimeMillis() + this.leasePeriod);
    synchronized (leaseQueue) {
      if (leases.containsKey(lease.getLeaseName())) {
        throw new LeaseStillHeldException(lease.getLeaseName());
      }
      leases.put(lease.getLeaseName(), lease);
      leaseQueue.add(lease);
    }
  }

  /**
   * Thrown if we are asked create a lease but lease on passed name already
   * exists.
   */
  @SuppressWarnings("serial")
  public static class LeaseStillHeldException extends IOException {
    private final String leaseName;

    /**
     * @param name
     */
    public LeaseStillHeldException(final String name) {
      this.leaseName = name;
    }

    /** @return name of lease */
    public String getName() {
      return this.leaseName;
    }
  }

  /**
   * Renew a lease
   *
   * @param leaseName name of lease
   * @throws LeaseException
   */
  public void renewLease(final String leaseName) throws LeaseException {
    synchronized (leaseQueue) {
      Lease lease = leases.get(leaseName);
      // We need to check to see if the remove is successful as the poll in the run()
      // method could have completed between the get and the remove which will result
      // in a corrupt leaseQueue.
      if (lease == null || !leaseQueue.remove(lease)) {
        throw new LeaseException("lease '" + leaseName +
        "' does not exist or has already expired");
      }
      lease.setExpirationTime(System.currentTimeMillis() + leasePeriod);
      leaseQueue.add(lease);
    }
  }

  /**
   * Client explicitly cancels a lease.
   * @param leaseName name of lease
   * @throws LeaseException
   */
  public void cancelLease(final String leaseName) throws LeaseException {
    removeLease(leaseName);
  }

  /**
   * Remove named lease.
   * Lease is removed from the list of leases and removed from the delay queue.
   * Lease can be resinserted using {@link #addLease(Lease)}
   *
   * @param leaseName name of lease
   * @throws LeaseException
   * @return Removed lease
   */
  Lease removeLease(final String leaseName) throws LeaseException {
    Lease lease =  null;
    synchronized (leaseQueue) {
      lease = leases.remove(leaseName);
      if (lease == null) {
        throw new LeaseException("lease '" + leaseName + "' does not exist");
      }
      leaseQueue.remove(lease);
    }
    return lease;
  }

  /** This class tracks a single Lease. */
  static class Lease implements Delayed {
    private final String leaseName;
    private final LeaseListener listener;
    private long expirationTime;

    Lease(final String leaseName, LeaseListener listener) {
      this(leaseName, listener, 0);
    }

    Lease(final String leaseName, LeaseListener listener, long expirationTime) {
      this.leaseName = leaseName;
      this.listener = listener;
      this.expirationTime = expirationTime;
    }

    /** @return the lease name */
    public String getLeaseName() {
      return leaseName;
    }

    /** @return listener */
    public LeaseListener getListener() {
      return this.listener;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      return this.hashCode() == ((Lease) obj).hashCode();
    }

    @Override
    public int hashCode() {
      return this.leaseName.hashCode();
    }

    public long getDelay(TimeUnit unit) {
      return unit.convert(this.expirationTime - System.currentTimeMillis(),
          TimeUnit.MILLISECONDS);
    }

    public int compareTo(Delayed o) {
      long delta = this.getDelay(TimeUnit.MILLISECONDS) -
        o.getDelay(TimeUnit.MILLISECONDS);

      return this.equals(o) ? 0 : (delta > 0 ? 1 : -1);
    }

    /** @param expirationTime the expirationTime to set */
    public void setExpirationTime(long expirationTime) {
      this.expirationTime = expirationTime;
    }
  }
}