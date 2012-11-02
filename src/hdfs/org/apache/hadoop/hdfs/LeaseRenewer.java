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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;

public class LeaseRenewer {
  private static final Log LOG = LogFactory.getLog(LeaseRenewer.class);

  static final long LEASE_RENEWER_GRACE_DEFAULT = 60*1000L;
  static final long LEASE_RENEWER_SLEEP_DEFAULT = 1000L;
  /** A map from src -> DFSOutputStream of files that are currently being
   * written by this client.
   */
  private final SortedMap<String, OutputStream> pendingCreates
      = new TreeMap<String, OutputStream>();
  /** The time in milliseconds that the map became empty. */
  private long emptyTime = Long.MAX_VALUE;
  /** A fixed lease renewal time period in milliseconds */
  private final long renewal;

  /** A daemon for renewing lease */
  private Daemon daemon = null;
  /** Only the daemon with currentId should run. */
  private int currentId = 0;

  /** 
   * A period in milliseconds that the lease renewer thread should run
   * after the map became empty.
   * If the map is empty for a time period longer than the grace period,
   * the renewer should terminate.  
   */
  private long gracePeriod;
  /**
   * The time period in milliseconds
   * that the renewer sleeps for each iteration. 
   */
  private volatile long sleepPeriod;

  private final DFSClient dfsclient;

  LeaseRenewer(final DFSClient dfsclient, final long timeout) {
    this.dfsclient = dfsclient;
    this.renewal = (timeout > 0 && timeout < FSConstants.LEASE_SOFTLIMIT_PERIOD)? 
        timeout/2: FSConstants.LEASE_SOFTLIMIT_PERIOD/2;
    setGraceSleepPeriod(LEASE_RENEWER_GRACE_DEFAULT);
  }

  /** Set the grace period and adjust the sleep period accordingly. */
  void setGraceSleepPeriod(final long gracePeriod) {
    if (gracePeriod < 100L) {
      throw new IllegalArgumentException(gracePeriod
          + " = gracePeriod < 100ms is too small.");
    }
    synchronized(this) {
      this.gracePeriod = gracePeriod;
    }
    final long half = gracePeriod/2;
    this.sleepPeriod = half < LEASE_RENEWER_SLEEP_DEFAULT?
        half: LEASE_RENEWER_SLEEP_DEFAULT;
  }

  /** Is the daemon running? */
  synchronized boolean isRunning() {
    return daemon != null && daemon.isAlive();
  }

  /** Is the empty period longer than the grace period? */  
  private synchronized boolean isRenewerExpired() {
    return emptyTime != Long.MAX_VALUE
        && System.currentTimeMillis() - emptyTime > gracePeriod;
  }

  synchronized void put(String src, OutputStream out) {
    if (dfsclient.clientRunning) {
      if (daemon == null || isRenewerExpired()) {
        //start a new deamon with a new id.
        final int id = ++currentId;
        daemon = new Daemon(new Runnable() {
          @Override
          public void run() {
            try {
              LeaseRenewer.this.run(id);
            } catch(InterruptedException e) {
              if (LOG.isDebugEnabled()) {
                LOG.debug(LeaseRenewer.this.getClass().getSimpleName()
                    + " is interrupted.", e);
              }
            }
          }
        });
        daemon.start();
      }
      pendingCreates.put(src, out);
      emptyTime = Long.MAX_VALUE;
    }
  }
  
  synchronized void remove(String src) {
    pendingCreates.remove(src);
    if (pendingCreates.isEmpty() && emptyTime == Long.MAX_VALUE) {
      //discover the first time that the map is empty.
      emptyTime = System.currentTimeMillis();
    }
  }
  
  void interruptAndJoin() throws InterruptedException {
    Daemon daemonCopy = null;
    synchronized (this) {
      if (isRunning()) {
        daemon.interrupt();
        daemonCopy = daemon;
      }
    }
   
    if (daemonCopy != null) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Wait for lease checker to terminate");
      }
      daemonCopy.join();
    }
  }

  void close() {
    while (true) {
      String src;
      OutputStream out;
      synchronized (this) {
        if (pendingCreates.isEmpty()) {
          return;
        }
        src = pendingCreates.firstKey();
        out = pendingCreates.remove(src);
      }
      if (out != null) {
        try {
          out.close();
        } catch (IOException ie) {
          LOG.error("Exception closing file " + src+ " : " + ie, ie);
        }
      }
    }
  }

  /**
   * Abort all open files. Release resources held. Ignore all errors.
   */
  synchronized void abort() {
    dfsclient.clientRunning = false;
    while (!pendingCreates.isEmpty()) {
      String src = pendingCreates.firstKey();
      DFSClient.DFSOutputStream out = (DFSClient.DFSOutputStream) pendingCreates
          .remove(src);
      if (out != null) {
          // TODO:
//        try {
//          out.abort();
//          
//        } catch (IOException ie) {
//          LOG.error("Exception aborting file " + src+ ": ", ie);
//        }
      }
    }
    RPC.stopProxy(dfsclient.namenode); // close connections to the namenode
  }

  private void renew() throws IOException {
    synchronized(this) {
      if (pendingCreates.isEmpty()) {
        return;
      }
    }
    dfsclient.namenode.renewLease(dfsclient.clientName);
  }

  /**
   * Periodically check in with the namenode and renew all the leases
   * when the lease period is half over.
   */
  private void run(final int id) throws InterruptedException {
    for(long lastRenewed = System.currentTimeMillis();
        dfsclient.clientRunning && !Thread.interrupted();
        Thread.sleep(sleepPeriod)) {
      if (System.currentTimeMillis() - lastRenewed >= renewal) {
        try {
          renew();
          lastRenewed = System.currentTimeMillis();
        } catch (SocketTimeoutException ie) {
          LOG.warn("Failed to renew lease for " + dfsclient.clientName + " for "
              + (renewal/1000) + " seconds.  Aborting ...", ie);
          abort();
          break;
        } catch (IOException ie) {
          LOG.warn("Failed to renew lease for " + dfsclient.clientName + " for "
              + (renewal/1000) + " seconds.  Will retry shortly ...", ie);
        }
      }

      synchronized(this) {
        if (id != currentId || isRenewerExpired()) {
          //no longer the current daemon or expired
          return;
        }
      }
    }
  }

  /** {@inheritDoc} */
  public String toString() {
    String s = getClass().getSimpleName();
    if (LOG.isTraceEnabled()) {
      return s + "@" + dfsclient + ": "
             + StringUtils.stringifyException(new Throwable("for testing"));
    }
    return s;
  }
}
