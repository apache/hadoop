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
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.Iterator;
import java.util.ConcurrentModificationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;

/** Flush cache upon request */
class Flusher extends Thread implements CacheFlushListener {
  static final Log LOG = LogFactory.getLog(Flusher.class);
  private final DelayQueue<QueueEntry> flushQueue =
    new DelayQueue<QueueEntry>();

  private final long optionalFlushPeriod;
  private final HRegionServer server;
  private final HBaseConfiguration conf;
  private final Integer lock = new Integer(0);
  
  /** constructor */
  public Flusher(final HRegionServer server) {
    super();
    this.server = server;
    conf = server.conf;
    this.optionalFlushPeriod = conf.getLong(
      "hbase.regionserver.optionalcacheflushinterval", 30 * 60 * 1000L);
  }
  
  /** {@inheritDoc} */
  @Override
  public void run() {
    while (!server.isStopRequested()) {
      QueueEntry e = null;
      try {
        e = flushQueue.poll(server.threadWakeFrequency, TimeUnit.MILLISECONDS);
        if (e == null) {
          continue;
        }
        synchronized(lock) { // Don't interrupt while we're working
          if (e.getRegion().flushcache()) {
            server.compactionRequested(e);
          }
            
          e.setExpirationTime(System.currentTimeMillis() +
              optionalFlushPeriod);
          flushQueue.add(e);
        }
        
        // Now ensure that all the active regions are in the queue
        Set<HRegion> regions = server.getRegionsToCheck();
        for (HRegion r: regions) {
          e = new QueueEntry(r, r.getLastFlushTime() + optionalFlushPeriod);
          synchronized (flushQueue) {
            if (!flushQueue.contains(e)) {
              flushQueue.add(e);
            }
          }
        }

        // Now make sure that the queue only contains active regions
        synchronized (flushQueue) {
          for (Iterator<QueueEntry> i = flushQueue.iterator(); i.hasNext();  ) {
            e = i.next();
            if (!regions.contains(e.getRegion())) {
              i.remove();
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
          (e != null ? (" for region " + e.getRegion().getRegionName()) : ""),
          RemoteExceptionHandler.checkIOException(ex));
        if (!server.checkFileSystem()) {
          break;
        }
      } catch (Exception ex) {
        LOG.error("Cache flush failed" +
          (e != null ? (" for region " + e.getRegion().getRegionName()) : ""),
          ex);
        if (!server.checkFileSystem()) {
          break;
        }
      }
    }
    flushQueue.clear();
    LOG.info(getName() + " exiting");
  }
  
  /** {@inheritDoc} */
  public void flushRequested(HRegion region) {
    QueueEntry e = new QueueEntry(region, System.currentTimeMillis());
    synchronized (flushQueue) {
      if (flushQueue.contains(e)) {
        flushQueue.remove(e);
      }
      flushQueue.add(e);
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