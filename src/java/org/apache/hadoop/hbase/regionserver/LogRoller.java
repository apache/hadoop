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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.RemoteExceptionHandler;

/** Runs periodically to determine if the HLog should be rolled */
class LogRoller extends Thread implements LogRollListener {
  static final Log LOG = LogFactory.getLog(LogRoller.class);  
  private final Integer rollLock = new Integer(0);
  private final long optionalLogRollInterval;
  private long lastLogRollTime;
  private volatile boolean rollLog;
  private final HRegionServer server;
  private final HBaseConfiguration conf;
  
  /** constructor */
  public LogRoller(final HRegionServer server) {
    super();
    this.server = server;
    conf = server.conf;
    this.optionalLogRollInterval = conf.getLong(
      "hbase.regionserver.optionallogrollinterval", 30L * 60L * 1000L);
    this.rollLog = false;
    lastLogRollTime = System.currentTimeMillis();
  }

  /** {@inheritDoc} */
  @Override
  public void run() {
    while (!server.isStopRequested()) {
      while (!rollLog && !server.isStopRequested()) {
        long now = System.currentTimeMillis();
        if (this.lastLogRollTime + this.optionalLogRollInterval <= now) {
          rollLog = true;
          this.lastLogRollTime = now;
        } else {
          synchronized (rollLock) {
            try {
              rollLock.wait(server.threadWakeFrequency);
            } catch (InterruptedException e) {
              continue;
            }
          }
        }
      }
      if (!rollLog) {
        // There's only two reasons to break out of the while loop.
        // 1. Log roll requested
        // 2. Stop requested
        // so if a log roll was not requested, continue and break out of loop
        continue;
      }
      synchronized (server.logRollerLock) {
        try {
          LOG.info("Rolling hlog. Number of entries: " + server.getLog().getNumEntries());
          server.getLog().rollWriter();
        } catch (IOException ex) {
          LOG.error("Log rolling failed",
            RemoteExceptionHandler.checkIOException(ex));
          server.checkFileSystem();
        } catch (Exception ex) {
          LOG.error("Log rolling failed", ex);
          server.checkFileSystem();
        } finally {
          rollLog = false;
        }
      }
    }
  }

  /** {@inheritDoc} */
  public void logRollRequested() {
    synchronized (rollLock) {
      rollLog = true;
      rollLock.notifyAll();
    }
  }
}