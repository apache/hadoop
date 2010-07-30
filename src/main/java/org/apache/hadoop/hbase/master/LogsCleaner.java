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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.regionserver.wal.HLog;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This Chore, everytime it runs, will clear the logs in the old logs folder
 * that are deletable for each log cleaner in the chain, in order to limit the
 * number of deletes it sends, will only delete maximum 20 in a single run.
 */
public class LogsCleaner extends Chore {

  static final Log LOG = LogFactory.getLog(LogsCleaner.class.getName());

  // Max number we can delete on every chore, this is to make sure we don't
  // issue thousands of delete commands around the same time
  private final int maxDeletedLogs;
  private final FileSystem fs;
  private final Path oldLogDir;
  private List<LogCleanerDelegate> logCleanersChain;
  private final Configuration conf;

  /**
   *
   * @param p the period of time to sleep between each run
   * @param s the stopper boolean
   * @param conf configuration to use
   * @param fs handle to the FS
   * @param oldLogDir the path to the archived logs
   */
  public LogsCleaner(final int p, final AtomicBoolean s,
                        Configuration conf, FileSystem fs,
                        Path oldLogDir) {
    super("LogsCleaner", p, s);

    this.maxDeletedLogs =
        conf.getInt("hbase.master.logcleaner.maxdeletedlogs", 20);
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
    String[] logCleaners = conf.getStrings("hbase.master.logcleaner.plugins");
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
      FileStatus[] files = this.fs.listStatus(this.oldLogDir);
      int nbDeletedLog = 0;
      FILE: for (FileStatus file : files) {
        Path filePath = file.getPath();
        if (HLog.validateHLogFilename(filePath.getName())) {
          for (LogCleanerDelegate logCleaner : logCleanersChain) {
            if (!logCleaner.isLogDeletable(filePath) ) {
              // this log is not deletable, continue to process next log file
              continue FILE;
            }
          }
          // delete this log file if it passes all the log cleaners
          this.fs.delete(filePath, true);
          nbDeletedLog++;
        } else {
          LOG.warn("Found a wrongly formated file: "
              + file.getPath().getName());
          this.fs.delete(filePath, true);
          nbDeletedLog++;
        }
        if (nbDeletedLog >= maxDeletedLogs) {
          break;
        }
      }
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.warn("Error while cleaning the logs", e);
    }
  }
}
