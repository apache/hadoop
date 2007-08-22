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
package org.apache.hadoop.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.*;
import java.util.*;

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
 */
public class Leases {
  protected static final Log LOG = LogFactory.getLog(Leases.class.getName());

  protected final long leasePeriod;
  protected final long leaseCheckFrequency;
  private final LeaseMonitor leaseMonitor;
  private final Thread leaseMonitorThread;
  protected final Map<LeaseName, Lease> leases =
    new HashMap<LeaseName, Lease>();
  protected final TreeSet<Lease> sortedLeases = new TreeSet<Lease>();
  protected boolean running = true;

  /**
   * Creates a lease
   * 
   * @param leasePeriod - length of time (milliseconds) that the lease is valid
   * @param leaseCheckFrequency - how often the lease should be checked
   * (milliseconds)
   */
  public Leases(long leasePeriod, long leaseCheckFrequency) {
    this.leasePeriod = leasePeriod;
    this.leaseCheckFrequency = leaseCheckFrequency;
    this.leaseMonitor = new LeaseMonitor();
    this.leaseMonitorThread = new Thread(leaseMonitor);
    this.leaseMonitorThread.setName("Lease.monitor");
  }
  
  /** Starts the lease monitor */
  public void start() {
    leaseMonitorThread.start();
  }

  /**
   * Shuts down this lease instance when all outstanding leases expire.
   * Like {@link #close()} but rather than violently end all leases, waits
   * first on extant leases to finish.  Use this method if the lease holders
   * could loose data, leak locks, etc.  Presumes client has shutdown
   * allocation of new leases.
   */
  public void closeAfterLeasesExpire() {
    synchronized(this.leases) {
      while (this.leases.size() > 0) {
        LOG.info(Integer.toString(leases.size()) + " lease(s) " +
          "outstanding. Waiting for them to expire.");
        try {
          this.leases.wait(this.leaseCheckFrequency);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
    // Now call close since no leases outstanding.
    close();
  }
  
  /**
   * Shut down this Leases instance.  All pending leases will be destroyed, 
   * without any cancellation calls.
   */
  public void close() {
    LOG.info("closing leases");

    this.running = false;
    try {
      this.leaseMonitorThread.interrupt();
      this.leaseMonitorThread.join();
    } catch (InterruptedException iex) {
      // Ignore
    }
    synchronized(leases) {
      synchronized(sortedLeases) {
        leases.clear();
        sortedLeases.clear();
      }
    }
    LOG.info("leases closed");
  }

  /* A client obtains a lease... */
  
  /**
   * Obtain a lease
   * 
   * @param holderId id of lease holder
   * @param resourceId id of resource being leased
   * @param listener listener that will process lease expirations
   */
  public void createLease(final long holderId, final long resourceId,
      final LeaseListener listener) {
    LeaseName name = null;
    synchronized(leases) {
      synchronized(sortedLeases) {
        Lease lease = new Lease(holderId, resourceId, listener);
        name = lease.getLeaseName();
        if(leases.get(name) != null) {
          throw new AssertionError("Impossible state for createLease(): " +
            "Lease " + name + " is still held.");
        }
        leases.put(name, lease);
        sortedLeases.add(lease);
      }
    }
//    if (LOG.isDebugEnabled()) {
//      LOG.debug("Created lease " + name);
//    }
  }
  
  /* A client renews a lease... */
  /**
   * Renew a lease
   * 
   * @param holderId id of lease holder
   * @param resourceId id of resource being leased
   * @throws IOException
   */
  public void renewLease(final long holderId, final long resourceId)
  throws IOException {
    LeaseName name = null;
    synchronized(leases) {
      synchronized(sortedLeases) {
        name = createLeaseName(holderId, resourceId);
        Lease lease = leases.get(name);
        if (lease == null) {
          // It's possible that someone tries to renew the lease, but 
          // it just expired a moment ago.  So fail.
          throw new IOException("Cannot renew lease that is not held: " +
            name);
        }
        sortedLeases.remove(lease);
        lease.renew();
        sortedLeases.add(lease);
      }
    }
//    if (LOG.isDebugEnabled()) {
//      LOG.debug("Renewed lease " + name);
//    }
  }

  /**
   * Client explicitly cancels a lease.
   * 
   * @param holderId id of lease holder
   * @param resourceId id of resource being leased
   */
  public void cancelLease(final long holderId, final long resourceId) {
    LeaseName name = null;
    synchronized(leases) {
      synchronized(sortedLeases) {
        name = createLeaseName(holderId, resourceId);
        Lease lease = leases.get(name);
        if (lease == null) {
          // It's possible that someone tries to renew the lease, but 
          // it just expired a moment ago.  So just skip it.
          return;
        }
        sortedLeases.remove(lease);
        leases.remove(name);
      }
    }     
//    if (LOG.isDebugEnabled()) {
//      LOG.debug("Cancel lease " + name);
//    }
  }

  /** LeaseMonitor is a thread that expires Leases that go on too long. */
  class LeaseMonitor implements Runnable {
    /** {@inheritDoc} */
    public void run() {
      while(running) {
        synchronized(leases) {
          synchronized(sortedLeases) {
            Lease top;
            while((sortedLeases.size() > 0)
                && ((top = sortedLeases.first()) != null)) {
              if(top.shouldExpire()) {
                leases.remove(top.getLeaseName());
                sortedLeases.remove(top);
                top.expired();
              } else {
                break;
              }
            }
          }
        }
        try {
          Thread.sleep(leaseCheckFrequency);
        } catch (InterruptedException ie) {
          // continue
        }
      }
    }
  }
  
  /*
   * A Lease name.
   * More lightweight than String or Text.
   */
  @SuppressWarnings("unchecked")
  class LeaseName implements Comparable {
    private final long holderId;
    private final long resourceId;
    
    LeaseName(final long hid, final long rid) {
      this.holderId = hid;
      this.resourceId = rid;
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
      LeaseName other = (LeaseName)obj;
      return this.holderId == other.holderId &&
        this.resourceId == other.resourceId;
    }
    
    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      // Copy OR'ing from javadoc for Long#hashCode.
      int result = (int)(this.holderId ^ (this.holderId >>> 32));
      result ^= (int)(this.resourceId ^ (this.resourceId >>> 32));
      return result;
    }
    
    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Long.toString(this.holderId) + "/" +
        Long.toString(this.resourceId);
    }

    /** {@inheritDoc} */
    public int compareTo(Object obj) {
      LeaseName other = (LeaseName)obj;
      if (this.holderId < other.holderId) {
        return -1;
      }
      if (this.holderId > other.holderId) {
        return 1;
      }
      // holderIds are equal
      if (this.resourceId < other.resourceId) {
        return -1;
      }
      if (this.resourceId > other.resourceId) {
        return 1;
      }
      // Objects are equal
      return 0;
    }
  }
  
  /** Create a lease id out of the holder and resource ids. */
  protected LeaseName createLeaseName(final long hid, final long rid) {
    return new LeaseName(hid, rid);
  }

  /** This class tracks a single Lease. */
  @SuppressWarnings("unchecked")
  private class Lease implements Comparable {
    final long holderId;
    final long resourceId;
    final LeaseListener listener;
    long lastUpdate;
    private LeaseName leaseId;

    Lease(final long holderId, final long resourceId,
        final LeaseListener listener) {
      this.holderId = holderId;
      this.resourceId = resourceId;
      this.listener = listener;
      renew();
    }
    
    synchronized LeaseName getLeaseName() {
      if (this.leaseId == null) {
        this.leaseId = createLeaseName(holderId, resourceId);
      }
      return this.leaseId;
    }
    
    boolean shouldExpire() {
      return (System.currentTimeMillis() - lastUpdate > leasePeriod);
    }
    
    void renew() {
      this.lastUpdate = System.currentTimeMillis();
    }
    
    void expired() {
      LOG.info("Lease expired " + getLeaseName());

      listener.leaseExpired();
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
      return compareTo(obj) == 0;
    }
    
    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      int result = this.getLeaseName().hashCode();
      result ^= Long.valueOf(this.lastUpdate).hashCode();
      return result;
    }
    
    //////////////////////////////////////////////////////////////////////////////
    // Comparable
    //////////////////////////////////////////////////////////////////////////////

    /** {@inheritDoc} */
    public int compareTo(Object o) {
      Lease other = (Lease) o;
      if(this.lastUpdate < other.lastUpdate) {
        return -1;
      } else if(this.lastUpdate > other.lastUpdate) {
        return 1;
      } else {
        return this.getLeaseName().compareTo(other.getLeaseName());
      }
    }
  }
}