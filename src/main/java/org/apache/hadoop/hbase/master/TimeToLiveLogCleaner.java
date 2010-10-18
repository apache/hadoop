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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Log cleaner that uses the timestamp of the hlog to determine if it should
 * be deleted. By default they are allowed to live for 10 minutes.
 */
public class TimeToLiveLogCleaner implements LogCleanerDelegate {
  static final Log LOG = LogFactory.getLog(TimeToLiveLogCleaner.class.getName());
  private Configuration conf;
  // Configured time a log can be kept after it was closed
  private long ttl;
  private boolean stopped = false;

  @Override
  public boolean isLogDeletable(Path filePath) {
    long time = 0;
    long currentTime = System.currentTimeMillis();
    String[] parts = filePath.getName().split("\\.");
    try {
      time = Long.parseLong(parts[parts.length-1]);
    } catch (NumberFormatException e) {
      LOG.error("Unable to parse the timestamp in " + filePath.getName() +
          ", deleting it since it's invalid and may not be a hlog", e);
      return true;
    }
    long life = currentTime - time;
    if (life < 0) {
      LOG.warn("Found a log newer than current time, " +
          "probably a clock skew");
      return false;
    }
    return life > ttl;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.ttl = conf.getLong("hbase.master.logcleaner.ttl", 600000);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void stop(String why) {
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }
}