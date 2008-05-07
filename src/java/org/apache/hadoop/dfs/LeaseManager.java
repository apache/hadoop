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
package org.apache.hadoop.dfs;

import java.io.IOException;
import java.util.*;

import org.apache.commons.logging.Log;

class LeaseManager {
  static final Log LOG = FSNamesystem.LOG;

  private final FSNamesystem fsnamesystem;

  private long softLimit = FSConstants.LEASE_SOFTLIMIT_PERIOD;
  private long hardLimit = FSConstants.LEASE_HARDLIMIT_PERIOD;

  //
  // Used for handling lock-leases
  // Mapping: leaseHolder -> Lease
  //
  private SortedMap<StringBytesWritable, Lease> leases = new TreeMap<StringBytesWritable, Lease>();
  // Set of: Lease
  private SortedSet<Lease> sortedLeases = new TreeSet<Lease>();

  // 
  // Map path names to leases. It is protected by the sortedLeases lock.
  // The map stores pathnames in lexicographical order.
  //
  private SortedMap<String, Lease> sortedLeasesByPath = new TreeMap<String, Lease>();

  LeaseManager(FSNamesystem fsnamesystem) {this.fsnamesystem = fsnamesystem;}

  Lease getLease(String holder) throws IOException {
    return leases.get(new StringBytesWritable(holder));
  }
  
  SortedSet<Lease> getSortedLeases() {return sortedLeases;}

  /** @return the number of leases currently in the system */
  synchronized int countLease() {return sortedLeases.size();}

  /** @return the number of paths contained in all leases */
  synchronized int countPath() {
    int count = 0;
    for(Lease lease : sortedLeases) {
      count += lease.getPaths().size();
    }
    return count;
  }
  
  /**
   * Adds (or re-adds) the lease for the specified file.
   */
  synchronized void addLease(String src, String holder) throws IOException {
    Lease lease = getLease(holder);
    if (lease == null) {
      lease = new Lease(holder);
      leases.put(new StringBytesWritable(holder), lease);
      sortedLeases.add(lease);
    } else {
      sortedLeases.remove(lease);
      lease.renew();
      sortedLeases.add(lease);
    }
    sortedLeasesByPath.put(src, lease);
    lease.startedCreate(src);
  }

  /**
   * deletes the lease for the specified file
   */
  synchronized void removeLease(String src, String holder) throws IOException {
    Lease lease = getLease(holder);
    if (lease != null) {
      lease.completedCreate(src);
      if (!lease.hasPath()) {
        leases.remove(new StringBytesWritable(holder));
        sortedLeases.remove(lease);
        sortedLeasesByPath.remove(src);
      }
    }
  }

  /**
   * Renew the lease(s) held by the given client
   */
  synchronized void renewLease(String holder) throws IOException {
    Lease lease = getLease(holder);
    if (lease != null) {
      sortedLeases.remove(lease);
      lease.renew();
      sortedLeases.add(lease);
    }
  }

  synchronized void handleExpiredSoftLimit(Lease lease) throws IOException {
    lease.releaseLocks();
    leases.remove(lease.holder);
    LOG.info("startFile: Removing lease " + lease);
    if (!sortedLeases.remove(lease)) {
      LOG.error("startFile: Unknown failure trying to remove " + lease + 
                " from lease set.");
    }
  }

  synchronized void abandonLease(String src, String holder) throws IOException {
    // find the lease
    Lease lease = getLease(holder);
    if (lease != null) {
      // remove the file from the lease
      if (lease.completedCreate(src)) {
        // if we found the file in the lease, remove it from pendingCreates
        fsnamesystem.internalReleaseCreate(src, holder);
      } else {
        LOG.warn("Attempt by " + holder + 
                 " to release someone else's create lock on " + src);
      }
    } else {
      LOG.warn("Attempt to release a lock from an unknown lease holder "
               + holder + " for " + src);
    }
  }

  /************************************************************
   * A Lease governs all the locks held by a single client.
   * For each client there's a corresponding lease, whose
   * timestamp is updated when the client periodically
   * checks in.  If the client dies and allows its lease to
   * expire, all the corresponding locks can be released.
   *************************************************************/
  class Lease implements Comparable<Lease> {
    private StringBytesWritable holder;
    private long lastUpdate;
    private Collection<StringBytesWritable> paths = new TreeSet<StringBytesWritable>();
  
    public Lease(String holder) throws IOException {
      this.holder = new StringBytesWritable(holder);
      renew();
    }
    public void renew() {
      this.lastUpdate = FSNamesystem.now();
    }

    /** @return true if the Hard Limit Timer has expired */
    public boolean expiredHardLimit() {
      return FSNamesystem.now() - lastUpdate > hardLimit;
    }

    /** @return true if the Soft Limit Timer has expired */
    public boolean expiredSoftLimit() {
      return FSNamesystem.now() - lastUpdate > softLimit;
    }

    void startedCreate(String src) throws IOException {
      paths.add(new StringBytesWritable(src));
    }

    boolean completedCreate(String src) throws IOException {
      return paths.remove(new StringBytesWritable(src));
    }

    boolean hasPath() {return !paths.isEmpty();}

    void releaseLocks() throws IOException {
      String holderStr = holder.getString();
      for(StringBytesWritable s : paths) {
        fsnamesystem.internalReleaseCreate(s.getString(), holderStr);
      }
      paths.clear();
    }
  
    /** {@inheritDoc} */
    public String toString() {
      return "[Lease.  Holder: " + holder
          + ", pendingcreates: " + paths.size() + "]";
    }
  
    /** {@inheritDoc} */
    public int compareTo(Lease o) {
      Lease l1 = this;
      Lease l2 = o;
      long lu1 = l1.lastUpdate;
      long lu2 = l2.lastUpdate;
      if (lu1 < lu2) {
        return -1;
      } else if (lu1 > lu2) {
        return 1;
      } else {
        return l1.holder.compareTo(l2.holder);
      }
    }
  
    /** {@inheritDoc} */
    public boolean equals(Object o) {
      if (!(o instanceof Lease)) {
        return false;
      }
      Lease obj = (Lease) o;
      if (lastUpdate == obj.lastUpdate &&
          holder.equals(obj.holder)) {
        return true;
      }
      return false;
    }
  
    /** {@inheritDoc} */
    public int hashCode() {
      return holder.hashCode();
    }
    
    Collection<StringBytesWritable> getPaths() {
      return paths;
    }
  
    // If a file with the specified prefix exists, then replace 
    // it with the new prefix.
    //
    void replacePrefix(String src, String overwrite, 
                       String replaceBy) throws IOException {
      List<StringBytesWritable> toAdd = new ArrayList<StringBytesWritable>();
      for (Iterator<StringBytesWritable> f = paths.iterator(); 
           f.hasNext();){
        String path = f.next().getString();
        if (!path.startsWith(src)) {
          continue;
        }
        // remove this filename from this lease.
        f.remove();
  
        // remember new filename
        String newPath = path.replaceFirst(overwrite, replaceBy);
        toAdd.add(new StringBytesWritable(newPath));
        LOG.info("Modified Lease for file " + path +
                 " to new path " + newPath);
      }
      // add modified filenames back into lease.
      for (Iterator<StringBytesWritable> f = toAdd.iterator(); 
           f.hasNext();) {
        paths.add(f.next());
      }
    }
  }

  synchronized void changeLease(String src, String dst,
      String overwrite, String replaceBy) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(getClass().getName() + ".changelease: " +
               " src=" + src + ", dest=" + dst + 
               ", overwrite=" + overwrite +
               ", replaceBy=" + replaceBy);
    }

    Map<String, Lease> addTo = new TreeMap<String, Lease>();
    SortedMap<String, Lease> myset = sortedLeasesByPath.tailMap(src);
    for (Iterator<Map.Entry<String, Lease>> iter = myset.entrySet().iterator(); 
         iter.hasNext();) {
      Map.Entry<String, Lease> value = iter.next();
      String path = (String)value.getKey();
      if (!path.startsWith(src)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("changelease comparing " + path +
                   " with " + src + " and terminating.");
        }
        break;
      }
      Lease lease = (Lease)value.getValue();

      // Fix up all the pathnames in this lease.
      if (LOG.isDebugEnabled()) {
        LOG.debug("changelease comparing " + path +
                  " with " + src + " and replacing ");
      }
      lease.replacePrefix(src, overwrite, replaceBy);

      // Remove this lease from sortedLeasesByPath because the 
      // pathname has changed.
      String newPath = path.replaceFirst(overwrite, replaceBy);
      addTo.put(newPath, lease);
      iter.remove();
    }
    // re-add entries back in sortedLeasesByPath
    sortedLeasesByPath.putAll(addTo);
  }

  void setLeasePeriod(long softLimit, long hardLimit) {
    this.softLimit = softLimit;
    this.hardLimit = hardLimit; 
  }
  
  Monitor createMonitor() {return new Monitor();}

  /******************************************************
   * Monitor checks for leases that have expired,
   * and disposes of them.
   ******************************************************/
  class Monitor implements Runnable {
    public void run() {
      try {
        while (fsnamesystem.fsRunning) {
          synchronized (fsnamesystem) {
            synchronized (sortedLeases) {
              Lease top;
              while ((sortedLeases.size() > 0) &&
                     ((top = sortedLeases.first()) != null)) {
                if (top.expiredHardLimit()) {
                  top.releaseLocks();
                  leases.remove(top.holder);
                  LOG.info("Removing lease " + top
                      + ", leases remaining: " + sortedLeases.size());
                  if (!sortedLeases.remove(top)) {
                    LOG.warn("Unknown failure trying to remove " + top
                        + " from lease set.");
                  }
                } else {
                  break;
                }
              }
            }
          }
          try {
            Thread.sleep(2000);
          } catch(InterruptedException ie) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(getClass().getName() + " is interrupted", ie);
            }
          }
        }
      } catch(Exception e) {
        LOG.error("In " + getClass().getName(), e);
      }
    }
  }
}
