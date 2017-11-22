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
package org.apache.hadoop.hdfs.client.impl;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSClientFaultInjector;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Used by {@link org.apache.hadoop.hdfs.DFSClient} for renewing
 * file-being-written leases on the namenode.
 * When a file is opened for write (create or append),
 * namenode stores a file lease for recording the identity of the writer.
 * The writer (i.e. the DFSClient) is required to renew the lease periodically.
 * When the lease is not renewed before it expires,
 * the namenode considers the writer as failed and then it may either let
 * another writer to obtain the lease or close the file.
 * </p>
 * <p>
 * This class also provides the following functionality:
 * <ul>
 * <li>
 * It maintains a map from (namenode, user) pairs to lease renewers.
 * The same {@link LeaseRenewer} instance is used for renewing lease
 * for all the {@link org.apache.hadoop.hdfs.DFSClient} to the same namenode and
 * the same user.
 * </li>
 * <li>
 * Each renewer maintains a list of {@link org.apache.hadoop.hdfs.DFSClient}.
 * Periodically the leases for all the clients are renewed.
 * A client is removed from the list when the client is closed.
 * </li>
 * <li>
 * A thread per namenode per user is used by the {@link LeaseRenewer}
 * to renew the leases.
 * </li>
 * </ul>
 * </p>
 */
@InterfaceAudience.Private
public class LeaseRenewer {
  static final Logger LOG = LoggerFactory.getLogger(LeaseRenewer.class);

  private static long leaseRenewerGraceDefault = 60*1000L;
  static final long LEASE_RENEWER_SLEEP_DEFAULT = 1000L;

  /** Get a {@link LeaseRenewer} instance */
  public static LeaseRenewer getInstance(final String authority,
      final UserGroupInformation ugi, final DFSClient dfsc) {
    final LeaseRenewer r = Factory.INSTANCE.get(authority, ugi);
    r.addClient(dfsc);
    return r;
  }

  /**
   * A factory for sharing {@link LeaseRenewer} objects
   * among {@link DFSClient} instances
   * so that there is only one renewer per authority per user.
   */
  private static class Factory {
    private static final Factory INSTANCE = new Factory();

    private static class Key {
      /** Namenode info */
      final String authority;
      /** User info */
      final UserGroupInformation ugi;

      private Key(final String authority, final UserGroupInformation ugi) {
        if (authority == null) {
          throw new HadoopIllegalArgumentException("authority == null");
        } else if (ugi == null) {
          throw new HadoopIllegalArgumentException("ugi == null");
        }

        this.authority = authority;
        this.ugi = ugi;
      }

      @Override
      public int hashCode() {
        return authority.hashCode() ^ ugi.hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) {
          return true;
        }
        if (obj != null && obj instanceof Key) {
          final Key that = (Key)obj;
          return this.authority.equals(that.authority)
                 && this.ugi.equals(that.ugi);
        }
        return false;
      }

      @Override
      public String toString() {
        return ugi.getShortUserName() + "@" + authority;
      }
    }

    /** A map for per user per namenode renewers. */
    private final Map<Key, LeaseRenewer> renewers = new HashMap<>();

    /** Get a renewer. */
    private synchronized LeaseRenewer get(final String authority,
        final UserGroupInformation ugi) {
      final Key k = new Key(authority, ugi);
      LeaseRenewer r = renewers.get(k);
      if (r == null) {
        r = new LeaseRenewer(k);
        renewers.put(k, r);
      }
      return r;
    }

    /** Remove the given renewer. */
    private synchronized void remove(final LeaseRenewer r) {
      final LeaseRenewer stored = renewers.get(r.factorykey);
      //Since a renewer may expire, the stored renewer can be different.
      if (r == stored) {
        renewers.remove(r.factorykey);
      }
    }
  }

  /** The time in milliseconds that the map became empty. */
  private long emptyTime = Long.MAX_VALUE;
  /** A fixed lease renewal time period in milliseconds */
  private long renewal = HdfsConstants.LEASE_SOFTLIMIT_PERIOD / 2;

  /** A daemon for renewing lease */
  private Daemon daemon = null;
  /** Only the daemon with currentId should run. */
  private int currentId = 0;

  /**
   * A period in milliseconds that the lease renewer thread should run
   * after the map became empty.
   * In other words,
   * if the map is empty for a time period longer than the grace period,
   * the renewer should terminate.
   */
  private long gracePeriod;
  /**
   * The time period in milliseconds
   * that the renewer sleeps for each iteration.
   */
  private long sleepPeriod;

  private final Factory.Key factorykey;

  /** A list of clients corresponding to this renewer. */
  private final List<DFSClient> dfsclients = new ArrayList<>();

  /**
   * A stringified stack trace of the call stack when the Lease Renewer
   * was instantiated. This is only generated if trace-level logging is
   * enabled on this class.
   */
  private final String instantiationTrace;

  private LeaseRenewer(Factory.Key factorykey) {
    this.factorykey = factorykey;
    unsyncSetGraceSleepPeriod(leaseRenewerGraceDefault);

    if (LOG.isTraceEnabled()) {
      instantiationTrace = StringUtils.stringifyException(
        new Throwable("TRACE"));
    } else {
      instantiationTrace = null;
    }
  }

  /** @return the renewal time in milliseconds. */
  private synchronized long getRenewalTime() {
    return renewal;
  }

  /** Used for testing only. */
  @VisibleForTesting
  public synchronized void setRenewalTime(final long renewal) {
    this.renewal = renewal;
  }

  /** Add a client. */
  private synchronized void addClient(final DFSClient dfsc) {
    for(DFSClient c : dfsclients) {
      if (c == dfsc) {
        //client already exists, nothing to do.
        return;
      }
    }
    //client not found, add it
    dfsclients.add(dfsc);

    //update renewal time
    final int hdfsTimeout = dfsc.getConf().getHdfsTimeout();
    if (hdfsTimeout > 0) {
      final long half = hdfsTimeout/2;
      if (half < renewal) {
        this.renewal = half;
      }
    }
  }

  private synchronized boolean clientsRunning() {
    for(Iterator<DFSClient> i = dfsclients.iterator(); i.hasNext(); ) {
      if (!i.next().isClientRunning()) {
        i.remove();
      }
    }
    return !dfsclients.isEmpty();
  }

  private synchronized long getSleepPeriod() {
    return sleepPeriod;
  }

  /** Set the grace period and adjust the sleep period accordingly. */
  synchronized void setGraceSleepPeriod(final long gracePeriod) {
    unsyncSetGraceSleepPeriod(gracePeriod);
  }

  private void unsyncSetGraceSleepPeriod(final long gracePeriod) {
    if (gracePeriod < 100L) {
      throw new HadoopIllegalArgumentException(gracePeriod
          + " = gracePeriod < 100ms is too small.");
    }
    this.gracePeriod = gracePeriod;
    final long half = gracePeriod/2;
    this.sleepPeriod = half < LEASE_RENEWER_SLEEP_DEFAULT?
        half: LEASE_RENEWER_SLEEP_DEFAULT;
  }

  /** Is the daemon running? */
  synchronized boolean isRunning() {
    return daemon != null && daemon.isAlive();
  }

  /** Does this renewer have nothing to renew? */
  public boolean isEmpty() {
    return dfsclients.isEmpty();
  }

  /** Used only by tests */
  synchronized String getDaemonName() {
    return daemon.getName();
  }

  /** Is the empty period longer than the grace period? */
  private synchronized boolean isRenewerExpired() {
    return emptyTime != Long.MAX_VALUE
        && Time.monotonicNow() - emptyTime > gracePeriod;
  }

  public synchronized void put(final DFSClient dfsc) {
    if (dfsc.isClientRunning()) {
      if (!isRunning() || isRenewerExpired()) {
        //start a new deamon with a new id.
        final int id = ++currentId;
        daemon = new Daemon(new Runnable() {
          @Override
          public void run() {
            try {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Lease renewer daemon for " + clientsString()
                    + " with renew id " + id + " started");
              }
              LeaseRenewer.this.run(id);
            } catch(InterruptedException e) {
              LOG.debug("LeaseRenewer is interrupted.", e);
            } finally {
              synchronized(LeaseRenewer.this) {
                Factory.INSTANCE.remove(LeaseRenewer.this);
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("Lease renewer daemon for " + clientsString()
                    + " with renew id " + id + " exited");
              }
            }
          }

          @Override
          public String toString() {
            return String.valueOf(LeaseRenewer.this);
          }
        });
        daemon.start();
      }
      emptyTime = Long.MAX_VALUE;
    }
  }

  @VisibleForTesting
  synchronized void setEmptyTime(long time) {
    emptyTime = time;
  }

  /** Close the given client. */
  public synchronized void closeClient(final DFSClient dfsc) {
    dfsclients.remove(dfsc);
    if (dfsclients.isEmpty()) {
      if (!isRunning() || isRenewerExpired()) {
        Factory.INSTANCE.remove(LeaseRenewer.this);
        return;
      }
      if (emptyTime == Long.MAX_VALUE) {
        //discover the first time that the client list is empty.
        emptyTime = Time.monotonicNow();
      }
    }

    //update renewal time
    if (renewal == dfsc.getConf().getHdfsTimeout()/2) {
      long min = HdfsConstants.LEASE_SOFTLIMIT_PERIOD;
      for(DFSClient c : dfsclients) {
        final int timeout = c.getConf().getHdfsTimeout();
        if (timeout > 0 && timeout < min) {
          min = timeout;
        }
      }
      renewal = min/2;
    }
  }

  public void interruptAndJoin() throws InterruptedException {
    Daemon daemonCopy = null;
    synchronized (this) {
      if (isRunning()) {
        daemon.interrupt();
        daemonCopy = daemon;
      }
    }

    if (daemonCopy != null) {
      LOG.debug("Wait for lease checker to terminate");
      daemonCopy.join();
    }
  }

  private void renew() throws IOException {
    final List<DFSClient> copies;
    synchronized(this) {
      copies = new ArrayList<>(dfsclients);
    }
    //sort the client names for finding out repeated names.
    Collections.sort(copies, new Comparator<DFSClient>() {
      @Override
      public int compare(final DFSClient left, final DFSClient right) {
        return left.getClientName().compareTo(right.getClientName());
      }
    });
    String previousName = "";
    for (final DFSClient c : copies) {
      //skip if current client name is the same as the previous name.
      if (!c.getClientName().equals(previousName)) {
        if (!c.renewLease()) {
          LOG.debug("Did not renew lease for client {}", c);
          continue;
        }
        previousName = c.getClientName();
        LOG.debug("Lease renewed for client {}", previousName);
      }
    }
  }

  /**
   * Periodically check in with the namenode and renew all the leases
   * when the lease period is half over.
   */
  private void run(final int id) throws InterruptedException {
    for(long lastRenewed = Time.monotonicNow(); !Thread.interrupted();
        Thread.sleep(getSleepPeriod())) {
      final long elapsed = Time.monotonicNow() - lastRenewed;
      if (elapsed >= getRenewalTime()) {
        try {
          renew();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Lease renewer daemon for " + clientsString()
                + " with renew id " + id + " executed");
          }
          lastRenewed = Time.monotonicNow();
        } catch (SocketTimeoutException ie) {
          LOG.warn("Failed to renew lease for " + clientsString() + " for "
              + (elapsed/1000) + " seconds.  Aborting ...", ie);
          List<DFSClient> dfsclientsCopy;
          synchronized (this) {
            DFSClientFaultInjector.get().delayWhenRenewLeaseTimeout();
            dfsclientsCopy = new ArrayList<>(dfsclients);
            dfsclients.clear();
            //Expire the current LeaseRenewer thread.
            emptyTime = 0;
            Factory.INSTANCE.remove(LeaseRenewer.this);
          }
          for (DFSClient dfsClient : dfsclientsCopy) {
            dfsClient.closeAllFilesBeingWritten(true);
          }
          break;
        } catch (IOException ie) {
          LOG.warn("Failed to renew lease for " + clientsString() + " for "
              + (elapsed/1000) + " seconds.  Will retry shortly ...", ie);
        }
      }

      synchronized(this) {
        if (id != currentId || isRenewerExpired()) {
          if (LOG.isDebugEnabled()) {
            if (id != currentId) {
              LOG.debug("Lease renewer daemon for " + clientsString()
                  + " with renew id " + id + " is not current");
            } else {
              LOG.debug("Lease renewer daemon for " + clientsString()
                  + " with renew id " + id + " expired");
            }
          }
          //no longer the current daemon or expired
          return;
        }

        // if no clients are in running state or there is no more clients
        // registered with this renewer, stop the daemon after the grace
        // period.
        if (!clientsRunning() && emptyTime == Long.MAX_VALUE) {
          emptyTime = Time.monotonicNow();
        }
      }
    }
  }

  @Override
  public String toString() {
    String s = getClass().getSimpleName() + ":" + factorykey;
    if (LOG.isTraceEnabled()) {
      return s + ", clients=" +  clientsString()
        + ", created at " + instantiationTrace;
    }
    return s;
  }

  /** Get the names of all clients */
  private synchronized String clientsString() {
    if (dfsclients.isEmpty()) {
      return "[]";
    } else {
      final StringBuilder b = new StringBuilder("[").append(
          dfsclients.get(0).getClientName());
      for(int i = 1; i < dfsclients.size(); i++) {
        b.append(", ").append(dfsclients.get(i).getClientName());
      }
      return b.append("]").toString();
    }
  }

  @VisibleForTesting
  public static void setLeaseRenewerGraceDefault(
      long leaseRenewerGraceDefault) {
    LeaseRenewer.leaseRenewerGraceDefault = leaseRenewerGraceDefault;
  }
}
