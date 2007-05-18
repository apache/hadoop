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

/*******************************************************************************
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
 ******************************************************************************/
public class Leases {
  private static final Log LOG = LogFactory.getLog(Leases.class);

  long leasePeriod;
  long leaseCheckFrequency;
  LeaseMonitor leaseMonitor;
  Thread leaseMonitorThread;
  TreeMap<Text, Lease> leases = new TreeMap<Text, Lease>();
  TreeSet<Lease> sortedLeases = new TreeSet<Lease>();
  boolean running = true;

  /** Indicate the length of the lease, in milliseconds */
  public Leases(long leasePeriod, long leaseCheckFrequency) {
    this.leasePeriod = leasePeriod;
    this.leaseCheckFrequency = leaseCheckFrequency;
    this.leaseMonitor = new LeaseMonitor();
    this.leaseMonitorThread = new Thread(leaseMonitor);
    this.leaseMonitorThread.setName("Lease.monitor");
    leaseMonitorThread.start();
  }

  /**
   * Shut down this Leases outfit.  All pending leases will be destroyed, 
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

  /** A client obtains a lease... */
  public void createLease(Text holderId, Text resourceId, LeaseListener listener) throws IOException {
    synchronized(leases) {
      synchronized(sortedLeases) {
        Lease lease = new Lease(holderId, resourceId, listener);
        Text leaseId = lease.getLeaseId();
        if(leases.get(leaseId) != null) {
          throw new IOException("Impossible state for createLease(): Lease for holderId " + holderId + " and resourceId " + resourceId + " is still held.");
        }
        leases.put(leaseId, lease);
        sortedLeases.add(lease);
      }
    }
  }
  
  /** A client renews a lease... */
  public void renewLease(Text holderId, Text resourceId) throws IOException {
    synchronized(leases) {
      synchronized(sortedLeases) {
        Text leaseId = createLeaseId(holderId, resourceId);
        Lease lease = leases.get(leaseId);
        if(lease == null) {
          
          // It's possible that someone tries to renew the lease, but 
          // it just expired a moment ago.  So fail.
          
          throw new IOException("Cannot renew lease is not held (holderId=" + holderId + ", resourceId=" + resourceId + ")");
        }
        
        sortedLeases.remove(lease);
        lease.renew();
        sortedLeases.add(lease);
      }
    }
  }

  /** A client explicitly cancels a lease.  The lease-cleanup method is not called. */
  public void cancelLease(Text holderId, Text resourceId) throws IOException {
    synchronized(leases) {
      synchronized(sortedLeases) {
        Text leaseId = createLeaseId(holderId, resourceId);
        Lease lease = leases.get(leaseId);
        if(lease == null) {
          
          // It's possible that someone tries to renew the lease, but 
          // it just expired a moment ago.  So fail.
          
          throw new IOException("Cannot cancel lease that is not held (holderId=" + holderId + ", resourceId=" + resourceId + ")");
        }
        
        sortedLeases.remove(lease);
        leases.remove(leaseId);

        lease.cancelled();
      }
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
        }
      }
    }
  }

  /** Create a lease id out of the holder and resource ids. */
  Text createLeaseId(Text holderId, Text resourceId) {
    return new Text("_" + holderId + "/" + resourceId + "_");
  }

  /** This class tracks a single Lease. */
  class Lease implements Comparable {
    Text holderId;
    Text resourceId;
    LeaseListener listener;
    long lastUpdate;

    public Lease(Text holderId, Text resourceId, LeaseListener listener) {
      this.holderId = holderId;
      this.resourceId = resourceId;
      this.listener = listener;
      renew();
    }
    
    public Text getLeaseId() {
      return createLeaseId(holderId, resourceId);
    }
    
    public boolean shouldExpire() {
      return (System.currentTimeMillis() - lastUpdate > leasePeriod);
    }
    
    public void renew() {
      this.lastUpdate = System.currentTimeMillis();
      listener.leaseRenewed();
    }
    
    public void cancelled() {
      listener.leaseCancelled();
    }
    
    public void expired() {
      listener.leaseExpired();
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

