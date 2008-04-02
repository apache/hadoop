/**
 * Copyright 2008 The Apache Software Foundation
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

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.HashSet;
import java.util.Set;
import java.util.ConcurrentModificationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;

/** Flush cache upon request */
class Flusher extends Thread implements CacheFlushListener {
  static final Log LOG = LogFactory.getLog(Flusher.class);
  private final BlockingQueue<HRegion> flushQueue =
    new LinkedBlockingQueue<HRegion>();
  
  private final HashSet<HRegion> regionsInQueue = new HashSet<HRegion>();

  private final long threadWakeFrequency;
  private final long optionalFlushPeriod;
  private final HRegionServer server;
  private final Integer lock = new Integer(0);
  
  /**
   * @param conf
   * @param server
   */
  public Flusher(final HBaseConfiguration conf, final HRegionServer server) {
    super();
    this.server = server;
    this.optionalFlushPeriod = conf.getLong(
        "hbase.regionserver.optionalcacheflushinterval", 30 * 60 * 1000L);
    this.threadWakeFrequency = conf.getLong(
        HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
  }
  
  /** {@inheritDoc} */
  @Override
  public void run() {
    long lastOptionalCheck = System.currentTimeMillis(); 
    while (!server.isStopRequested()) {
      HRegion r = null;
      try {
        long now = System.currentTimeMillis();
        if (now - threadWakeFrequency > lastOptionalCheck) {
          lastOptionalCheck = now;
          // Queue up regions for optional flush if they need it
          Set<HRegion> regions = server.getRegionsToCheck();
          for (HRegion region: regions) {
            synchronized (regionsInQueue) {
              if (!regionsInQueue.contains(region) &&
                  (now - optionalFlushPeriod) > region.getLastFlushTime()) {
                regionsInQueue.add(region);
                flushQueue.add(region);
                region.setLastFlushTime(now);
              }
            }
          }
        }
        r = flushQueue.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
        if (r != null) {
          synchronized (regionsInQueue) {
            regionsInQueue.remove(r);
          }
          synchronized (lock) { // Don't interrupt while we're working
            if (r.flushcache()) {
              server.compactSplitThread.compactionRequested(r);
            }
          }
        }
      } catch (InterruptedException ex) {
        continue;
      } catch (ConcurrentModificationException ex) {
        continue;
      } catch (DroppedSnapshotException ex) {
        // Cache flush can fail in a few places.  If it fails in a critical
        // section, we get a DroppedSnapshotException and a replay of hlog
        // is required. Currently the only way to do this is a restart of
        // the server.
        LOG.fatal("Replay of hlog required. Forcing server restart", ex);
        if (!server.checkFileSystem()) {
          break;
        }
        server.stop();
      } catch (IOException ex) {
        LOG.error("Cache flush failed" +
          (r != null ? (" for region " + r.getRegionName()) : ""),
          RemoteExceptionHandler.checkIOException(ex));
        if (!server.checkFileSystem()) {
          break;
        }
      } catch (Exception ex) {
        LOG.error("Cache flush failed" +
          (r != null ? (" for region " + r.getRegionName()) : ""),
          ex);
        if (!server.checkFileSystem()) {
          break;
        }
      }
    }
    regionsInQueue.clear();
    flushQueue.clear();
    LOG.info(getName() + " exiting");
  }
  
  /** {@inheritDoc} */
  public void flushRequested(HRegion r) {
    synchronized (regionsInQueue) {
      if (!regionsInQueue.contains(r)) {
        regionsInQueue.add(r);
        flushQueue.add(r);
      }
    }
  }
  
  /**
   * Only interrupt once it's done with a run through the work loop.
   */ 
  void interruptPolitely() {
    synchronized (lock) {
      interrupt();
    }
  }
}