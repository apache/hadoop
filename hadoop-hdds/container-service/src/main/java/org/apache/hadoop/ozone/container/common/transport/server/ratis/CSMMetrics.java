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
package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.ratis.protocol.RaftGroupId;

/**
 * This class is for maintaining Container State Machine statistics.
 */
@InterfaceAudience.Private
@Metrics(about="Container State Machine Metrics", context="dfs")
public class CSMMetrics {
  public static final String SOURCE_NAME =
      CSMMetrics.class.getSimpleName();

  // ratis op metrics metrics
  private @Metric MutableCounterLong numWriteStateMachineOps;
  private @Metric MutableCounterLong numQueryStateMachineOps;
  private @Metric MutableCounterLong numApplyTransactionOps;
  private @Metric MutableCounterLong numReadStateMachineOps;
  private @Metric MutableCounterLong numBytesWrittenCount;
  private @Metric MutableCounterLong numBytesCommittedCount;

  private @Metric MutableRate transactionLatency;
  private MutableRate[] opsLatency;
  private MetricsRegistry registry = null;

  // Failure Metrics
  private @Metric MutableCounterLong numWriteStateMachineFails;
  private @Metric MutableCounterLong numWriteDataFails;
  private @Metric MutableCounterLong numQueryStateMachineFails;
  private @Metric MutableCounterLong numApplyTransactionFails;
  private @Metric MutableCounterLong numReadStateMachineFails;
  private @Metric MutableCounterLong numReadStateMachineMissCount;
  private @Metric MutableCounterLong numStartTransactionVerifyFailures;
  private @Metric MutableCounterLong numContainerNotOpenVerifyFailures;

  public CSMMetrics() {
    int numCmdTypes = ContainerProtos.Type.values().length;
    this.opsLatency = new MutableRate[numCmdTypes];
    this.registry = new MetricsRegistry(CSMMetrics.class.getSimpleName());
    for (int i = 0; i < numCmdTypes; i++) {
      opsLatency[i] = registry.newRate(
          ContainerProtos.Type.forNumber(i + 1).toString(),
          ContainerProtos.Type.forNumber(i + 1) + " op");
    }
  }

  public static CSMMetrics create(RaftGroupId gid) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME + gid.toString(),
        "Container State Machine",
        new CSMMetrics());
  }

  public void incNumWriteStateMachineOps() {
    numWriteStateMachineOps.incr();
  }

  public void incNumQueryStateMachineOps() {
    numQueryStateMachineOps.incr();
  }

  public void incNumReadStateMachineOps() {
    numReadStateMachineOps.incr();
  }

  public void incNumApplyTransactionsOps() {
    numApplyTransactionOps.incr();
  }

  public void incNumWriteStateMachineFails() {
    numWriteStateMachineFails.incr();
  }

  public void incNumWriteDataFails() {
    numWriteDataFails.incr();
  }

  public void incNumQueryStateMachineFails() {
    numQueryStateMachineFails.incr();
  }

  public void incNumBytesWrittenCount(long value) {
    numBytesWrittenCount.incr(value);
  }

  public void incNumBytesCommittedCount(long value) {
    numBytesCommittedCount.incr(value);
  }

  public void incNumReadStateMachineFails() {
    numReadStateMachineFails.incr();
  }

  public void incNumReadStateMachineMissCount() {
    numReadStateMachineMissCount.incr();
  }

  public void incNumApplyTransactionsFails() {
    numApplyTransactionFails.incr();
  }

  @VisibleForTesting
  public long getNumWriteStateMachineOps() {
    return numWriteStateMachineOps.value();
  }

  @VisibleForTesting
  public long getNumQueryStateMachineOps() {
    return numQueryStateMachineOps.value();
  }

  @VisibleForTesting
  public long getNumApplyTransactionsOps() {
    return numApplyTransactionOps.value();
  }

  @VisibleForTesting
  public long getNumWriteStateMachineFails() {
    return numWriteStateMachineFails.value();
  }

  @VisibleForTesting
  public long getNumWriteDataFails() {
    return numWriteDataFails.value();
  }

  @VisibleForTesting
  public long getNumQueryStateMachineFails() {
    return numQueryStateMachineFails.value();
  }

  @VisibleForTesting
  public long getNumApplyTransactionsFails() {
    return numApplyTransactionFails.value();
  }

  @VisibleForTesting
  public long getNumReadStateMachineFails() {
    return numReadStateMachineFails.value();
  }

  @VisibleForTesting
  public long getNumReadStateMachineMissCount() {
    return numReadStateMachineMissCount.value();
  }

  @VisibleForTesting
  public long getNumBytesWrittenCount() {
    return numBytesWrittenCount.value();
  }

  @VisibleForTesting
  public long getNumBytesCommittedCount() {
    return numBytesCommittedCount.value();
  }

  public void incPipelineLatency(ContainerProtos.Type type, long latencyNanos) {
    opsLatency[type.ordinal()].add(latencyNanos);
    transactionLatency.add(latencyNanos);
  }

  public void incNumStartTransactionVerifyFailures() {
    numStartTransactionVerifyFailures.incr();
  }

  public void incNumContainerNotOpenVerifyFailures() {
    numContainerNotOpenVerifyFailures.incr();
  }


  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }
}
