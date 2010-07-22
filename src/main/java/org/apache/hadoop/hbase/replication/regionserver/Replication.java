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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.LogEntryVisitor;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationZookeeperWrapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Replication serves as an umbrella over the setup of replication and
 * is used by HRS.
 */
public class Replication implements LogEntryVisitor {

  private final boolean replication;
  private final ReplicationSourceManager replicationManager;
  private boolean replicationMaster;
  private final AtomicBoolean replicating = new AtomicBoolean(true);
  private final ReplicationZookeeperWrapper zkHelper;
  private final Configuration conf;
  private final AtomicBoolean  stopRequested;
  private ReplicationSink replicationSink;

  /**
   * Instantiate the replication management (if rep is enabled).
   * @param conf conf to use
   * @param hsi the info if this region server
   * @param fs handle to the filesystem
   * @param oldLogDir directory where logs are archived
   * @param stopRequested boolean that tells us if we are shutting down
   * @throws IOException
   */
  public Replication(Configuration conf, HServerInfo hsi,
                     FileSystem fs, Path logDir, Path oldLogDir,
                     AtomicBoolean stopRequested) throws IOException {
    this.conf = conf;
    this.stopRequested = stopRequested;
    this.replication =
        conf.getBoolean(HConstants.REPLICATION_ENABLE_KEY, false);
    if (replication) {
      this.zkHelper = new ReplicationZookeeperWrapper(
        ZooKeeperWrapper.getInstance(conf, hsi.getServerName()), conf,
        this.replicating, hsi.getServerName());
      this.replicationMaster = zkHelper.isReplicationMaster();
      this.replicationManager = this.replicationMaster ?
        new ReplicationSourceManager(zkHelper, conf, stopRequested,
          fs, this.replicating, logDir, oldLogDir) : null;
    } else {
      replicationManager = null;
      zkHelper = null;
    }
  }

  /**
   * Join with the replication threads
   */
  public void join() {
    if (this.replication) {
      if (this.replicationMaster) {
        this.replicationManager.join();
      }
      this.zkHelper.deleteOwnRSZNode();
    }
  }

  /**
   * Carry on the list of log entries down to the sink
   * @param entries list of entries to replicate
   * @throws IOException
   */
  public void replicateLogEntries(HLog.Entry[] entries) throws IOException {
    if (this.replication && !this.replicationMaster) {
      this.replicationSink.replicateEntries(entries);
    }
  }

  /**
   * If replication is enabled and this cluster is a master,
   * it starts
   * @throws IOException
   */
  public void startReplicationServices() throws IOException {
    if (this.replication) {
      if (this.replicationMaster) {
        this.replicationManager.init();
      } else {
        this.replicationSink =
            new ReplicationSink(this.conf, this.stopRequested);
      }
    }
  }

  /**
   * Get the replication sources manager
   * @return the manager if replication is enabled, else returns false
   */
  public ReplicationSourceManager getReplicationManager() {
    return replicationManager;
  }

  @Override
  public void visitLogEntryBeforeWrite(HRegionInfo info, HLogKey logKey,
                                       WALEdit logEdit) {
    NavigableMap<byte[], Integer> scopes =
        new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    byte[] family;
    for (KeyValue kv : logEdit.getKeyValues()) {
      family = kv.getFamily();
      int scope = info.getTableDesc().getFamily(family).getScope();
      if (scope != HConstants.REPLICATION_SCOPE_LOCAL &&
          !scopes.containsKey(family)) {
        scopes.put(family, scope);
      }
    }
    if (!scopes.isEmpty()) {
      logEdit.setScopes(scopes);
    }
  }

  /**
   * Add this class as a log entry visitor for HLog if replication is enabled
   * @param hlog log that was add ourselves on
   */
  public void addLogEntryVisitor(HLog hlog) {
    if (replication) {
      hlog.addLogEntryVisitor(this);
    }
  }
}
