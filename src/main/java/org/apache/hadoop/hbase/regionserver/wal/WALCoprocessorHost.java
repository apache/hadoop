
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

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.conf.Configuration;

/**
 * Implements the coprocessor environment and runtime support for coprocessors
 * loaded within a {@link HLog}.
 */
public class WALCoprocessorHost
    extends CoprocessorHost<WALCoprocessorHost.WALEnvironment> {
  
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
     * @param seq load sequence
     * @param conf configuration
     * @param hlog HLog
     */
    public WALEnvironment(Class<?> implClass, final Coprocessor impl,
        final int priority, final int seq, final Configuration conf,
        final HLog hlog) {
      super(impl, priority, seq, conf);
      this.wal = hlog;
    }
  }

  HLog wal;
  /**
   * Constructor
   * @param log the write ahead log
   * @param conf the configuration
   */
  public WALCoprocessorHost(final HLog log, final Configuration conf) {
    this.wal = log;
    // load system default cp's from configuration.
    loadSystemCoprocessors(conf, WAL_COPROCESSOR_CONF_KEY);
  }

  @Override
  public WALEnvironment createEnvironment(final Class<?> implClass,
      final Coprocessor instance, final int priority, final int seq,
      final Configuration conf) {
    return new WALEnvironment(implClass, instance, priority, seq, conf,
        this.wal);
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
    boolean bypass = false;
    ObserverContext<WALCoprocessorEnvironment> ctx = null;
    for (WALEnvironment env: coprocessors) {
      if (env.getInstance() instanceof
          org.apache.hadoop.hbase.coprocessor.WALObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        ((org.apache.hadoop.hbase.coprocessor.WALObserver)env.getInstance()).
            preWALWrite(ctx, info, logKey, logEdit);
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  /**
   * @param info
   * @param logKey
   * @param logEdit
   * @throws IOException
   */
  public void postWALWrite(HRegionInfo info, HLogKey logKey, WALEdit logEdit)
      throws IOException {
    ObserverContext<WALCoprocessorEnvironment> ctx = null;
    for (WALEnvironment env: coprocessors) {
      if (env.getInstance() instanceof
          org.apache.hadoop.hbase.coprocessor.WALObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        ((org.apache.hadoop.hbase.coprocessor.WALObserver)env.getInstance()).
            postWALWrite(ctx, info, logKey, logEdit);
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }
}
