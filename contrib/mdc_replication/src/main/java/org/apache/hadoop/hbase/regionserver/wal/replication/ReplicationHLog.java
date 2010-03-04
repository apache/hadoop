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
package org.apache.hadoop.hbase.regionserver.wal.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.replication.ReplicationSource;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.LogRollListener;

import java.io.IOException;

/**
 * HLog specialized in replication. It replicates every entry from every
 * user table at the moment.
 */
public class ReplicationHLog extends HLog {

  static final Log LOG = LogFactory.getLog(ReplicationHLog.class);

  private ReplicationSource replicationSource;

  private final boolean isReplicator;

  /**
   * New constructor used for replication
   * @param fs filesystem to use
   * @param dir directory to store the wal
   * @param conf conf ot use
   * @param listener log listener to pass to super class
   * @param replicationSource where to put the entries
   * @throws IOException
   */
  public ReplicationHLog(final FileSystem fs, final Path dir,
                         final Path oldLogDir, final Configuration conf,
                         final LogRollListener listener,
                         ReplicationSource replicationSource)
                         throws IOException {
    super(fs, dir, oldLogDir, conf, listener);
    this.replicationSource = replicationSource;
    this.isReplicator = this.replicationSource != null;
  }

  @Override
  protected void doWrite(HRegionInfo info, HLogKey logKey,
                         KeyValue logEdit)
      throws IOException {
    logKey.setScope(info.getTableDesc().getFamily(logEdit.getFamily()).getScope());
    super.doWrite(info, logKey, logEdit);
    if(this.isReplicator && ! (info.isMetaRegion() || info.isRootRegion()) &&
        logKey.getScope() == HConstants.REPLICATION_SCOPE_GLOBAL) {
      this.replicationSource.enqueueLog(new Entry(logKey, logEdit));
    }

  }

  public ReplicationSource getReplicationSource() {
    return this.replicationSource;
  }


}
