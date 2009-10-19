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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.regionserver.wal.HLog;

/**
 * LogFlusher is a Chore that wakes every threadWakeInterval and calls
 * the HLog to do an optional sync if there are unflushed entries, and the
 * optionalFlushInterval has passed since the last flush.
 */
public class LogFlusher extends Chore {
  static final Log LOG = LogFactory.getLog(LogFlusher.class);
  
  private final AtomicReference<HLog> log =
    new AtomicReference<HLog>(null);
  
  public LogFlusher(final int period, final AtomicBoolean stop) {
    super(period, stop);
  }
  
  void setHLog(HLog log) {
    this.log.set(log);
  }

  @Override
  protected void chore() {
    HLog hlog = log.get();
    if (hlog != null) {
      hlog.optionalSync();
    }
  }
}