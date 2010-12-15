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

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.regionserver.wal.WALObserver;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper;
import org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

import static org.apache.hadoop.hbase.HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS;
import static org.apache.hadoop.hbase.HConstants.REPLICATION_ENABLE_KEY;
import static org.apache.hadoop.hbase.HConstants.REPLICATION_SCOPE_LOCAL;

/**
 * Gateway to Replication.  Used by {@link org.apache.hadoop.hbase.regionserver.HRegionServer}.
 */
public class Replication implements WALObserver {
  private final boolean replication;
  private final ReplicationSourceManager replicationManager;
  private final AtomicBoolean replicating = new AtomicBoolean(true);
  private final ReplicationZookeeper zkHelper;
  private final Configuration conf;
  private ReplicationSink replicationSink;
  // Hosting server
  private final Server server;

  /**
   * Instantiate the replication management (if rep is enabled).
   * @param server Hosting server
   * @param fs handle to the filesystem
   * @param logDir
   * @param oldLogDir directory where logs are archived
   * @throws IOException
   * @throws KeeperException 
   */
  public Replication(final Server server, final FileSystem fs,
      final Path logDir, final Path oldLogDir)
  throws IOException, KeeperException {
    this.server = server;
    this.conf = this.server.getConfiguration();
    this.replication = isReplication(this.conf);
    if (replication) {
      this.zkHelper = new ReplicationZookeeper(server, this.replicating);
      this.replicationManager = new ReplicationSourceManager(zkHelper, conf,
          this.server, fs, this.replicating, logDir, oldLogDir) ;
    } else {
      this.replicationManager = null;
      this.zkHelper = null;
    }
  }

  /**
   * @param c Configuration to look at
   * @return True if replication is enabled.
   */
  public static boolean isReplication(final Configuration c) {
    return c.getBoolean(REPLICATION_ENABLE_KEY, false);
  }

  /**
   * Join with the replication threads
   */
  public void join() {
    if (this.replication) {
      this.replicationManager.join();
    }
  }

  /**
   * Carry on the list of log entries down to the sink
   * @param entries list of entries to replicate
   * @throws IOException
   */
  public void replicateLogEntries(HLog.Entry[] entries) throws IOException {
    if (this.replication) {
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
      this.replicationManager.init();
      this.replicationSink = new ReplicationSink(this.conf, this.server);
    }
  }

  /**
   * Get the replication sources manager
   * @return the manager if replication is enabled, else returns false
   */
  public ReplicationSourceManager getReplicationManager() {
    return this.replicationManager;
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
      if (scope != REPLICATION_SCOPE_LOCAL &&
          !scopes.containsKey(family)) {
        scopes.put(family, scope);
      }
    }
    if (!scopes.isEmpty()) {
      logEdit.setScopes(scopes);
    }
  }

  @Override
  public void logRolled(Path p) {
    getReplicationManager().logRolled(p);
  }

  /**
   * This method modifies the master's configuration in order to inject
   * replication-related features
   * @param conf
   */
  public static void decorateMasterConfiguration(Configuration conf) {
    if (!isReplication(conf)) {
      return;
    }
    String plugins = conf.get(HBASE_MASTER_LOGCLEANER_PLUGINS);
    if (!plugins.contains(ReplicationLogCleaner.class.toString())) {
      conf.set(HBASE_MASTER_LOGCLEANER_PLUGINS,
          plugins + "," + ReplicationLogCleaner.class.getCanonicalName());
    }
  }

  @Override
  public void logRollRequested() {
    // Not interested
  }

  @Override
  public void logCloseRequested() {
    // not interested
  }
}
