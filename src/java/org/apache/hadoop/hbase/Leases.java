/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.hadoop.io.*;

import java.io.*;
import java.util.*;

/**
 * Leases
 *
 * There are several server classes in HBase that need to track external clients
 * that occasionally send heartbeats.
 * 
 * These external clients hold resources in the server class.  Those resources 
 * need to be released if the external client fails to send a heartbeat after 
 * some interval of time passes.
 *
 * The Leases class is a general reusable class for this kind of pattern.
 *
 * An instance of the Leases class will create a thread to do its dirty work.  
 * You should close() the instance if you want to clean up the thread properly.
 */
public class Leases {
  static final Log LOG = LogFactory.getLog(Leases.class.getName());

  long leasePeriod;
  long leaseCheckFrequency;
  LeaseMonitor leaseMonitor;
  Thread leaseMonitorThread;
  TreeMap<Text, Lease> leases = new TreeMap<Text, Lease>();
  TreeSet<Lease> sortedLeases = new TreeSet<Lease>();
  boolean running = true;

  /**
   * Creates a lease
   * 
   * @param leasePeriod - length of time (milliseconds) that the lease is valid
   * @param leaseCheckFrequency - how often the lease should be checked (milliseconds)
   */
  public Leases(long leasePeriod, long leaseCheckFrequency) {
    this.leasePeriod = leasePeriod;
    this.leaseCheckFrequency = leaseCheckFrequency;
    this.leaseMonitor = new LeaseMonitor();
    this.leaseMonitorThread = new Thread(leaseMonitor);
    this.leaseMonitorThread.setName("Lease.monitor");
    leaseMonitorThread.start();
  }

  /**
   * Shut down this Leases instance.  All pending leases will be destroyed, 
   * without any cancellation calls.
   */
  public void close() {
    if(LOG.isDebugEnabled()) {
      LOG.debug("closing leases");
    }
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
    if(LOG.isDebugEnabled()) {
      LOG.debug("leases closed");
    }
  }
  
  String getLeaseName(final Text holderId, final Text resourceId) {
    return "<holderId=" + holderId + ", resourceId=" + resourceId + ">";
  }

  /** A client obtains a lease... */
  /**
   * Obtain a lease
   * 
   * @param holderId - name of lease holder
   * @param resourceId - resource being leased
   * @param listener - listener that will process lease expirations
   */
  public void createLease(Text holderId, Text resourceId,
      final LeaseListener listener) {
    synchronized(leases) {
      synchronized(sortedLeases) {
        Lease lease = new Lease(holderId, resourceId, listener);
        Text leaseId = lease.getLeaseId();
        if(leases.get(leaseId) != null) {
          throw new AssertionError("Impossible state for createLease(): Lease " +
            getLeaseName(holderId, resourceId) + " is still held.");
        }
        leases.put(leaseId, lease);
        sortedLeases.add(lease);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created lease " + getLeaseName(holderId, resourceId));
    }
  }
  
  /** A client renews a lease... */
  /**
   * Renew a lease
   * 
   * @param holderId - name of lease holder
   * @param resourceId - resource being leased
   * @throws IOException
   */
  public void renewLease(Text holderId, Text resourceId) throws IOException {
    synchronized(leases) {
      synchronized(sortedLeases) {
        Text leaseId = createLeaseId(holderId, resourceId);
        Lease lease = leases.get(leaseId);
        if(lease == null) {
          // It's possible that someone tries to renew the lease, but 
          // it just expired a moment ago.  So fail.
          throw new IOException("Cannot renew lease that is not held: " +
            getLeaseName(holderId, resourceId));
        }
        
        sortedLeases.remove(lease);
        lease.renew();
        sortedLeases.add(lease);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Renewed lease " + getLeaseName(holderId, resourceId));
    }
  }

  /**
   * Client explicitly cancels a lease.
   * 
   * @param holderId - name of lease holder
   * @param resourceId - resource being leased
   * @throws IOException
   */
  public void cancelLease(Text holderId, Text resourceId) throws IOException {
    synchronized(leases) {
      synchronized(sortedLeases) {
        Text leaseId = createLeaseId(holderId, resourceId);
        Lease lease = leases.get(leaseId);
        if(lease == null) {
          
          // It's possible that someone tries to renew the lease, but 
          // it just expired a moment ago.  So fail.
          
          throw new IOException("Cannot cancel lease that is not held: " +
            getLeaseName(holderId, resourceId));
        }
        
        sortedLeases.remove(lease);
        leases.remove(leaseId);

      }
    }     
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cancel lease " + getLeaseName(holderId, resourceId));
    }
  }

  /** LeaseMonitor is a thread that expires Leases that go on too long. */
  class LeaseMonitor implements Runnable {
    public void run() {
      while(running) {
        synchronized(leases) {
          synchronized(sortedLeases) {
            Lease top;
            while((sortedLeases.size() > 0)
                && ((top = sortedLeases.first()) != null)) {
              
              if(top.shouldExpire()) {
                leases.remove(top.getLeaseId());
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
          // Ignore
        }
      }
    }
  }

  /** Create a lease id out of the holder and resource ids. */
  Text createLeaseId(Text holderId, Text resourceId) {
    return new Text("_" + holderId + "/" + resourceId + "_");
  }

  /** This class tracks a single Lease. */
  @SuppressWarnings("unchecked")
  private class Lease implements Comparable {
    Text holderId;
    Text resourceId;
    LeaseListener listener;
    long lastUpdate;

    Lease(Text holderId, Text resourceId, LeaseListener listener) {
      this.holderId = holderId;
      this.resourceId = resourceId;
      this.listener = listener;
      renew();
    }
    
    Text getLeaseId() {
      return createLeaseId(holderId, resourceId);
    }
    
    boolean shouldExpire() {
      return (System.currentTimeMillis() - lastUpdate > leasePeriod);
    }
    
    void renew() {
      this.lastUpdate = System.currentTimeMillis();
    }
    
    void expired() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Lease expired " + getLeaseName(this.holderId,
          this.resourceId));
      }
      listener.leaseExpired();
    }
    
    @Override
    public boolean equals(Object obj) {
      return compareTo(obj) == 0;
    }
    
    @Override
    public int hashCode() {
      int result = this.getLeaseId().hashCode();
      result ^= Long.valueOf(this.lastUpdate).hashCode();
      return result;
    }
    
    //////////////////////////////////////////////////////////////////////////////
    // Comparable
    //////////////////////////////////////////////////////////////////////////////

    public int compareTo(Object o) {
      Lease other = (Lease) o;
      if(this.lastUpdate < other.lastUpdate) {
        return -1;
        
      } else if(this.lastUpdate > other.lastUpdate) {
        return 1;
        
      } else {
        return this.getLeaseId().compareTo(other.getLeaseId());
      }
    }
  }
}

