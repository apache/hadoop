/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.genesis;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.container.states.ContainerStateMap;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.util.Time;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;

/**
 * Benchmarks ContainerStateMap class.
 */
@State(Scope.Thread)
public class BenchMarkContainerStateMap {
  private ContainerStateMap stateMap;
  private AtomicInteger containerID;
  private AtomicInteger runCount;
  private static int errorFrequency = 100;

  @Setup(Level.Trial)
  public void initialize() throws IOException {
    stateMap = new ContainerStateMap();
    runCount = new AtomicInteger(0);
    Pipeline pipeline = createSingleNodePipeline(UUID.randomUUID().toString());
    Preconditions.checkNotNull(pipeline, "Pipeline cannot be null.");
    int currentCount = 1;
    for (int x = 1; x < 1000; x++) {
      try {
        ContainerInfo containerInfo = new ContainerInfo.Builder()
            .setState(CLOSED)
            .setPipelineID(pipeline.getId())
            .setReplicationType(pipeline.getType())
            .setReplicationFactor(pipeline.getFactor())
            .setUsedBytes(0)
            .setNumberOfKeys(0)
            .setStateEnterTime(Time.monotonicNow())
            .setOwner("OZONE")
            .setContainerID(x)
            .setDeleteTransactionId(0)
            .build();
        stateMap.addContainer(containerInfo);
        currentCount++;
      } catch (SCMException e) {
        e.printStackTrace();
      }
    }
    for (int y = currentCount; y < 50000; y++) {
      try {
        ContainerInfo containerInfo = new ContainerInfo.Builder()
            .setState(OPEN)
            .setPipelineID(pipeline.getId())
            .setReplicationType(pipeline.getType())
            .setReplicationFactor(pipeline.getFactor())
            .setUsedBytes(0)
            .setNumberOfKeys(0)
            .setStateEnterTime(Time.monotonicNow())
            .setOwner("OZONE")
            .setContainerID(y)
            .setDeleteTransactionId(0)
            .build();
        stateMap.addContainer(containerInfo);
        currentCount++;
      } catch (SCMException e) {
        e.printStackTrace();
      }
    }
    try {
      ContainerInfo containerInfo = new ContainerInfo.Builder()
          .setState(OPEN)
          .setPipelineID(pipeline.getId())
          .setReplicationType(pipeline.getType())
          .setReplicationFactor(pipeline.getFactor())
          .setUsedBytes(0)
          .setNumberOfKeys(0)
          .setStateEnterTime(Time.monotonicNow())
          .setOwner("OZONE")
          .setContainerID(currentCount++)
          .setDeleteTransactionId(0)
          .build();
      stateMap.addContainer(containerInfo);
    } catch (SCMException e) {
      e.printStackTrace();
    }

    containerID = new AtomicInteger(currentCount++);

  }

  public static Pipeline createSingleNodePipeline(String containerName)
      throws IOException {
    return createPipeline(containerName, 1);
  }

  /**
   * Create a pipeline with single node replica.
   *
   * @return Pipeline with single node in it.
   * @throws IOException
   */
  public static Pipeline createPipeline(String containerName, int numNodes)
      throws IOException {
    Preconditions.checkArgument(numNodes >= 1);
    final List<DatanodeDetails> ids = new ArrayList<>(numNodes);
    for (int i = 0; i < numNodes; i++) {
      ids.add(GenesisUtil.createDatanodeDetails(UUID.randomUUID().toString()));
    }
    return createPipeline(containerName, ids);
  }

  public static Pipeline createPipeline(String containerName,
      Iterable<DatanodeDetails> ids) throws IOException {
    Objects.requireNonNull(ids, "ids == null");
    Preconditions.checkArgument(ids.iterator().hasNext());
    List<DatanodeDetails> dns = new ArrayList<>();
    ids.forEach(dns::add);
    final Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
        .setFactor(HddsProtos.ReplicationFactor.ONE)
        .setNodes(dns)
        .build();
    return pipeline;
  }

  @Benchmark
  public void createContainerBenchMark(BenchMarkContainerStateMap state,
      Blackhole bh) throws IOException {
    ContainerInfo containerInfo = getContainerInfo(state);
    state.stateMap.addContainer(containerInfo);
  }

  private ContainerInfo getContainerInfo(BenchMarkContainerStateMap state)
      throws IOException {
    Pipeline pipeline = createSingleNodePipeline(UUID.randomUUID().toString());
    int cid = state.containerID.incrementAndGet();
    return new ContainerInfo.Builder()
        .setState(CLOSED)
        .setPipelineID(pipeline.getId())
        .setReplicationType(pipeline.getType())
        .setReplicationFactor(pipeline.getFactor())
        .setUsedBytes(0)
        .setNumberOfKeys(0)
        .setStateEnterTime(Time.monotonicNow())
        .setOwner("OZONE")
        .setContainerID(cid)
        .setDeleteTransactionId(0)
        .build();
  }

  @Benchmark
  public void getMatchingContainerBenchMark(BenchMarkContainerStateMap state,
      Blackhole bh) throws IOException {
    if(runCount.incrementAndGet() % errorFrequency == 0) {
      state.stateMap.addContainer(getContainerInfo(state));
    }
    bh.consume(state.stateMap
        .getMatchingContainerIDs(OPEN, "OZONE", ReplicationFactor.ONE,
            ReplicationType.STAND_ALONE));
  }
}
