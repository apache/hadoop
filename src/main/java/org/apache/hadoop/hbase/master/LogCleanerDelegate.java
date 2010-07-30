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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Path;

/**
 * Interface for the log cleaning function inside the master. By default, three
 * cleaners <code>TimeToLiveLogCleaner</code>,  <code>ReplicationLogCleaner</code>,
 * <code>SnapshotLogCleaner</code> are called in order. So if other effects are
 * needed, implement your own LogCleanerDelegate and add it to the configuration
 * "hbase.master.logcleaner.plugins", which is a comma-separated list of fully
 * qualified class names. LogsCleaner will add it to the chain.
 *
 * HBase ships with LogsCleaner as the default implementation.
 *
 * This interface extends Configurable, so setConf needs to be called once
 * before using the cleaner.
 * Since LogCleanerDelegates are created in LogsCleaner by reflection. Classes
 * that implements this interface should provide a default constructor.
 */
public interface LogCleanerDelegate extends Configurable {

  /**
   * Should the master delete the log or keep it?
   * @param filePath full path to log.
   * @return true if the log is deletable, false if not
   */
  public boolean isLogDeletable(Path filePath);
}

