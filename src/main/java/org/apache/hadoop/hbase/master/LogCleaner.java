/**
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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.FSUtils;

import static org.apache.hadoop.hbase.HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS;

/**
 * This Chore, everytime it runs, will clear the HLogs in the old logs folder
 * that are deletable for each log cleaner in the chain.
 */
public class LogCleaner extends Chore {
  static final Log LOG = LogFactory.getLog(LogCleaner.class.getName());

  private final FileSystem fs;
  private final Path oldLogDir;
  private List<LogCleanerDelegate> logCleanersChain;
  private final Configuration conf;

  /**
   *
   * @param p the period of time to sleep between each run
   * @param s the stopper
   * @param conf configuration to use
   * @param fs handle to the FS
   * @param oldLogDir the path to the archived logs
   */
  public LogCleaner(final int p, final Stoppable s,
                        Configuration conf, FileSystem fs,
                        Path oldLogDir) {
    super("LogsCleaner", p, s);
    this.fs = fs;
    this.oldLogDir = oldLogDir;
    this.conf = conf;
    this.logCleanersChain = new LinkedList<LogCleanerDelegate>();

    initLogCleanersChain();
  }

  /*
   * Initialize the chain of log cleaners from the configuration. The default
   * three LogCleanerDelegates in this chain are: TimeToLiveLogCleaner,
   * ReplicationLogCleaner and SnapshotLogCleaner.
   */
  private void initLogCleanersChain() {
    String[] logCleaners = conf.getStrings(HBASE_MASTER_LOGCLEANER_PLUGINS);
    if (logCleaners != null) {
      for (String className : logCleaners) {
        LogCleanerDelegate logCleaner = newLogCleaner(className, conf);
        addLogCleaner(logCleaner);
      }
    }
  }

  /**
   * A utility method to create new instances of LogCleanerDelegate based
   * on the class name of the LogCleanerDelegate.
   * @param className fully qualified class name of the LogCleanerDelegate
   * @param conf
   * @return the new instance
   */
  public static LogCleanerDelegate newLogCleaner(String className, Configuration conf) {
    try {
      Class c = Class.forName(className);
      LogCleanerDelegate cleaner = (LogCleanerDelegate) c.newInstance();
      cleaner.setConf(conf);
      return cleaner;
    } catch(Exception e) {
      LOG.warn("Can NOT create LogCleanerDelegate: " + className, e);
      // skipping if can't instantiate
      return null;
    }
  }

  /**
   * Add a LogCleanerDelegate to the log cleaner chain. A log file is deletable
   * if it is deletable for each LogCleanerDelegate in the chain.
   * @param logCleaner
   */
  public void addLogCleaner(LogCleanerDelegate logCleaner) {
    if (logCleaner != null && !logCleanersChain.contains(logCleaner)) {
      logCleanersChain.add(logCleaner);
      LOG.debug("Add log cleaner in chain: " + logCleaner.getClass().getName());
    }
  }

  @Override
  protected void chore() {
    try {
      FileStatus [] files = FSUtils.listStatus(this.fs, this.oldLogDir, null);
      if (files == null) return;
      FILE: for (FileStatus file : files) {
        Path filePath = file.getPath();
        if (HLog.validateHLogFilename(filePath.getName())) {
          for (LogCleanerDelegate logCleaner : logCleanersChain) {
            if (logCleaner.isStopped()) {
              LOG.warn("A log cleaner is stopped, won't delete any log.");
              return;
            }

            if (!logCleaner.isLogDeletable(filePath) ) {
              // this log is not deletable, continue to process next log file
              continue FILE;
            }
          }
          // delete this log file if it passes all the log cleaners
          this.fs.delete(filePath, true);
        } else {
          LOG.warn("Found a wrongly formated file: "
              + file.getPath().getName());
          this.fs.delete(filePath, true);
        }
      }
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.warn("Error while cleaning the logs", e);
    }
  }

  @Override
  public void run() {
    try {
      super.run();
    } finally {
      for (LogCleanerDelegate lc: this.logCleanersChain) {
        try {
          lc.stop("Exiting");
        } catch (Throwable t) {
          LOG.warn("Stopping", t);
        }
      }
    }
  }
}
