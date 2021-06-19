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
package org.apache.hadoop.hdfs.server.mover;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;

/**
 * Metrics for HDFS Mover of a blockpool.
 */
@Metrics(about="Mover metrics", context="dfs")
final class MoverMetrics {

  private final Mover mover;

  @Metric("If mover is processing namespace.")
  private MutableGaugeInt processingNamespace;

  @Metric("Number of blocks being scheduled.")
  private MutableCounterLong blocksScheduled;

  @Metric("Number of files being processed.")
  private MutableCounterLong filesProcessed;

  private MoverMetrics(Mover m) {
    this.mover = m;
  }

  public static MoverMetrics create(Mover mover) {
    MoverMetrics m = new MoverMetrics(mover);
    return DefaultMetricsSystem.instance().register(
        m.getName(), null, m);
  }

  String getName() {
    return "Mover-" + mover.getNnc().getBlockpoolID();
  }

  @Metric("Bytes that already moved by mover.")
  public long getBytesMoved() {
    return mover.getNnc().getBytesMoved().get();
  }

  @Metric("Number of blocks that successfully moved by mover.")
  public long getBlocksMoved() {
    return mover.getNnc().getBlocksMoved().get();
  }

  @Metric("Number of blocks that failed moved by mover.")
  public long getBlocksFailed() {
    return mover.getNnc().getBlocksFailed().get();
  }

  void setProcessingNamespace(boolean processingNamespace) {
    this.processingNamespace.set(processingNamespace ? 1 : 0);
  }

  void incrBlocksScheduled() {
    this.blocksScheduled.incr();
  }

  void incrFilesProcessed() {
    this.filesProcessed.incr();
  }
}
