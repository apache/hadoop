/**
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
package org.apache.hadoop.hdfs.nfs.nfs3;

import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.source.JvmMetrics;

/**
 * This class is for maintaining the various NFS gateway activity statistics and
 * publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@Metrics(about = "Nfs3 metrics", context = "dfs")
public class Nfs3Metrics {
  // All mutable rates are in nanoseconds
  // No metric for nullProcedure;
  @Metric MutableRate getattr;
  @Metric MutableRate setattr;
  @Metric MutableRate lookup;
  @Metric MutableRate access;
  @Metric MutableRate readlink;
  @Metric MutableRate read;
  final MutableQuantiles[] readNanosQuantiles;
  @Metric MutableRate write;
  final MutableQuantiles[] writeNanosQuantiles;
  @Metric MutableRate create;
  @Metric MutableRate mkdir;
  @Metric MutableRate symlink;
  @Metric MutableRate mknod;
  @Metric MutableRate remove;
  @Metric MutableRate rmdir;
  @Metric MutableRate rename;
  @Metric MutableRate link;
  @Metric MutableRate readdir;
  @Metric MutableRate readdirplus;
  @Metric MutableRate fsstat;
  @Metric MutableRate fsinfo;
  @Metric MutableRate pathconf;
  @Metric MutableRate commit;
  final MutableQuantiles[] commitNanosQuantiles;

  @Metric MutableCounterLong bytesWritten;
  @Metric MutableCounterLong bytesRead;

  final MetricsRegistry registry = new MetricsRegistry("nfs3");
  final String name;
  JvmMetrics jvmMetrics = null;

  public Nfs3Metrics(String name, String sessionId, int[] intervals,
      final JvmMetrics jvmMetrics) {
    this.name = name;
    this.jvmMetrics = jvmMetrics;
    registry.tag(SessionId, sessionId);

    final int len = intervals.length;
    readNanosQuantiles = new MutableQuantiles[len];
    writeNanosQuantiles = new MutableQuantiles[len];
    commitNanosQuantiles = new MutableQuantiles[len];

    for (int i = 0; i < len; i++) {
      int interval = intervals[i];
      readNanosQuantiles[i] = registry.newQuantiles("readProcessNanos"
          + interval + "s", "Read process in ns", "ops", "latency", interval);
      writeNanosQuantiles[i] = registry.newQuantiles("writeProcessNanos"
          + interval + "s", "Write process in ns", "ops", "latency", interval);
      commitNanosQuantiles[i] = registry.newQuantiles("commitProcessNanos"
          + interval + "s", "Commit process in ns", "ops", "latency", interval);
    }
  }

  public static Nfs3Metrics create(Configuration conf, String gatewayName) {
    String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
    MetricsSystem ms = DefaultMetricsSystem.instance();
    JvmMetrics jm = JvmMetrics.create(gatewayName, sessionId, ms);

    // Percentile measurement is [50th,75th,90th,95th,99th] currently 
    int[] intervals = conf
        .getInts(NfsConfigKeys.NFS_METRICS_PERCENTILES_INTERVALS_KEY);
    return ms.register(new Nfs3Metrics(gatewayName, sessionId, intervals, jm));
  }
  
  public String name() {
    return name;
  }

  public JvmMetrics getJvmMetrics() {
    return jvmMetrics;
  }

  public void incrBytesWritten(long bytes) {
    bytesWritten.incr(bytes);
  }

  public void incrBytesRead(long bytes) {
    bytesRead.incr(bytes);
  }

  public void addGetattr(long latencyNanos) {
    getattr.add(latencyNanos);
  }

  public void addSetattr(long latencyNanos) {
    setattr.add(latencyNanos);
  }

  public void addLookup(long latencyNanos) {
    lookup.add(latencyNanos);
  }

  public void addAccess(long latencyNanos) {
    access.add(latencyNanos);
  }

  public void addReadlink(long latencyNanos) {
    readlink.add(latencyNanos);
  }

  public void addRead(long latencyNanos) {
    read.add(latencyNanos);
    for (MutableQuantiles q : readNanosQuantiles) {
      q.add(latencyNanos);
    }
  }

  public void addWrite(long latencyNanos) {
    write.add(latencyNanos);
    for (MutableQuantiles q : writeNanosQuantiles) {
      q.add(latencyNanos);
    }
  }

  public void addCreate(long latencyNanos) {
    create.add(latencyNanos);
  }

  public void addMkdir(long latencyNanos) {
    mkdir.add(latencyNanos);
  }

  public void addSymlink(long latencyNanos) {
    symlink.add(latencyNanos);
  }

  public void addMknod(long latencyNanos) {
    mknod.add(latencyNanos);
  }

  public void addRemove(long latencyNanos) {
    remove.add(latencyNanos);
  }

  public void addRmdir(long latencyNanos) {
    rmdir.add(latencyNanos);
  }

  public void addRename(long latencyNanos) {
    rename.add(latencyNanos);
  }

  public void addLink(long latencyNanos) {
    link.add(latencyNanos);
  }

  public void addReaddir(long latencyNanos) {
    readdir.add(latencyNanos);
  }

  public void addReaddirplus(long latencyNanos) {
    readdirplus.add(latencyNanos);
  }

  public void addFsstat(long latencyNanos) {
    fsstat.add(latencyNanos);
  }

  public void addFsinfo(long latencyNanos) {
    fsinfo.add(latencyNanos);
  }

  public void addPathconf(long latencyNanos) {
    pathconf.add(latencyNanos);
  }

  public void addCommit(long latencyNanos) {
    commit.add(latencyNanos);
    for (MutableQuantiles q : commitNanosQuantiles) {
      q.add(latencyNanos);
    }
  }

}
