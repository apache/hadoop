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
package org.apache.hadoop.hbase.regionserver.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.ReplicationRPC;
import org.apache.hadoop.hbase.ipc.ReplicationRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.replication.ReplicationHLog;
import org.apache.hadoop.hbase.replication.ReplicationZookeeperHelper;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReplicationRegionServer extends HRegionServer
    implements ReplicationRegionInterface {

  static {
    ReplicationRPC.initialize();
  }

  protected static final Log LOG =
      LogFactory.getLog(ReplicationRegionServer.class);

  private final ReplicationSource replicationSource;
  private ReplicationSink replicationSink;
  private final boolean isMaster;
  private final AtomicBoolean isReplicating = new AtomicBoolean(true);

  private final ReplicationZookeeperHelper zkHelper;


  /**
   * Starts a HRegionServer at the default location
   *
   * @param conf
   * @throws java.io.IOException
   */
  public ReplicationRegionServer(Configuration conf) throws IOException {
    super(conf);

    this.zkHelper = new ReplicationZookeeperHelper(
        this.getZooKeeperWrapper(), this.conf, this.isReplicating);
    this.isMaster = zkHelper.isMaster();

    this.replicationSink = null;
    this.replicationSource = this.isMaster ? new ReplicationSource(this,
        super.stopRequested, this.isReplicating) : null;
  }

  @Override
  protected HLog instantiateHLog(Path logdir) throws IOException {
    HLog newlog = new ReplicationHLog(super.getFileSystem(),
        logdir, conf, super.getLogRoller(),
        this.replicationSource);
    return newlog;
  }

  @Override
  protected void init(final MapWritable c) throws IOException {
    super.init(c);
    String n = Thread.currentThread().getName();

    String repLogPathStr =
      ReplicationSink.getRepLogPath(getHServerInfo().getServerName());
    Path repLogPath = new Path(getRootDir(), repLogPathStr);


    Thread.UncaughtExceptionHandler handler =
        new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(final Thread t, final Throwable e) {
        abort();
        LOG.fatal("Set stop flag in " + t.getName(), e);
      }
    };
    if(this.isMaster) {
      Threads.setDaemonThreadRunning(
          this.replicationSource, n + ".replicationSource", handler);
    } else {
      this.replicationSink =
        new ReplicationSink(conf,super.stopRequested,
            repLogPath, getFileSystem(), getThreadWakeFrequency());
      Threads.setDaemonThreadRunning(
          this.replicationSink, n + ".replicationSink", handler);
    }
  }

  @Override
  protected HRegion instantiateRegion(final HRegionInfo regionInfo)
      throws IOException {
    HRegion r = new ReplicationRegion(HTableDescriptor.getTableDir(super
        .getRootDir(), regionInfo.getTableDesc().getName()), super.hlog, super
        .getFileSystem(), super.conf, regionInfo,
        super.getFlushRequester(), this.replicationSource);

    r.initialize(null, new Progressable() {
      public void progress() {
        addProcessingMessage(regionInfo);
      }
    });
    return r;
  }


  @Override
  public void replicateLogEntries(HLog.Entry[] entries) throws IOException {
    this.replicationSink.replicateEntries(entries);
  }

  /**
   *
   * @param protocol
   * @param clientVersion
   * @return
   * @throws IOException
   */
  public long getProtocolVersion(final String protocol,
      final long clientVersion)
  throws IOException {
    if (protocol.equals(ReplicationRegionInterface.class.getName())) {
      return HBaseRPCProtocolVersion.versionID;
    }
    throw new IOException("Unknown protocol to name node: " + protocol);
  }

  /**
   *
   * @return
   */
  public ReplicationZookeeperHelper getZkHelper() {
    return zkHelper;
  }

  protected void join() {
    super.join();
    if(this.isMaster) {
      Threads.shutdown(this.replicationSource);
    } else {
      Threads.shutdown(this.replicationSink);
    }
  }
}
