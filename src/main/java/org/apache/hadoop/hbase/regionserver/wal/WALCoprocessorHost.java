
/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.coprocessor.Coprocessor.Priority;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.conf.Configuration;

/**
 * Implements the coprocessor environment and runtime support for coprocessors
 * loaded within a {@link HLog}.
 */
public class WALCoprocessorHost
    extends CoprocessorHost<WALCoprocessorHost.WALEnvironment> {
  
  private static final Log LOG = LogFactory.getLog(WALCoprocessorHost.class);

  /**
   * Encapsulation of the environment of each coprocessor
   */
  static class WALEnvironment extends CoprocessorHost.Environment
    implements WALCoprocessorEnvironment {

    private HLog wal;

    @Override
    public HLog getWAL() {
      return wal;
    }

    /**
     * Constructor
     * @param impl the coprocessor instance
     * @param priority chaining priority
     */
    public WALEnvironment(Class<?> implClass, final Coprocessor impl,
        Coprocessor.Priority priority, final HLog hlog) {
      super(impl, priority);
      this.wal = hlog;
    }
  }

  HLog wal;
  /**
   * Constructor
   * @param region the region
   * @param rsServices interface to available region server functionality
   * @param conf the configuration
   */
  public WALCoprocessorHost(final HLog log, final Configuration conf) {
    this.wal = log;
    // load system default cp's from configuration.
    loadSystemCoprocessors(conf, WAL_COPROCESSOR_CONF_KEY);
  }

  @Override
  public WALEnvironment createEnvironment(Class<?> implClass,
      Coprocessor instance, Priority priority) {
    // TODO Auto-generated method stub
    return new WALEnvironment(implClass, instance, priority, this.wal);
  }

  /**
   * @param info
   * @param logKey
   * @param logEdit
   * @return true if default behavior should be bypassed, false otherwise
   * @throws IOException
   */
  public boolean preWALWrite(HRegionInfo info, HLogKey logKey, WALEdit logEdit)
      throws IOException {
    try {
      boolean bypass = false;
      coprocessorLock.readLock().lock();
      for (WALEnvironment env: coprocessors) {
        if (env.getInstance() instanceof 
            org.apache.hadoop.hbase.coprocessor.WALObserver) {
          ((org.apache.hadoop.hbase.coprocessor.WALObserver)env.getInstance()).
              preWALWrite(env, info, logKey, logEdit);
          bypass |= env.shouldBypass();
          if (env.shouldComplete()) {
            break;
          }
        }
      }
      return bypass;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param info
   * @param logKey
   * @param logEdit
   * @throws IOException
   */
  public void postWALWrite(HRegionInfo info, HLogKey logKey, WALEdit logEdit)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (WALEnvironment env: coprocessors) {
        if (env.getInstance() instanceof 
            org.apache.hadoop.hbase.coprocessor.WALObserver) {
          ((org.apache.hadoop.hbase.coprocessor.WALObserver)env.getInstance()).
              postWALWrite(env, info, logKey, logEdit);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }
}
