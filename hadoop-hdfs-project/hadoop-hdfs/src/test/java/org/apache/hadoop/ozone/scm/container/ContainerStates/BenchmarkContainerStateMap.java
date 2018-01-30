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

package org.apache.hadoop.ozone.scm.container.ContainerStates;

import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.scm.exceptions.SCMException;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.ozone.protocol.proto.OzoneProtos
    .LifeCycleState.OPEN;
import static org.apache.hadoop.ozone.protocol.proto.OzoneProtos
    .ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneProtos
    .ReplicationType.STAND_ALONE;

public class BenchmarkContainerStateMap {
  @Test
  public void testRunBenchMarks() throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(this.getClass().getName() + ".*")
        .mode(Mode.Throughput)
        .timeUnit(TimeUnit.SECONDS)
        .warmupTime(TimeValue.seconds(1))
        .warmupIterations(2)
        .measurementTime(TimeValue.seconds(1))
        .measurementIterations(2)
        .threads(2)
        .forks(1)
        .shouldFailOnError(true)
        .shouldDoGC(true)
        .build();
    new Runner(opt).run();
  }

  @Benchmark
  public void createContainerBenchMark(BenchmarkState state, Blackhole bh)
      throws IOException {
    Pipeline pipeline = ContainerTestHelper
        .createSingleNodePipeline(UUID.randomUUID().toString());
    int cid = state.containerID.incrementAndGet();
    ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setContainerName(pipeline.getContainerName())
        .setState(OzoneProtos.LifeCycleState.CLOSED)
        .setPipeline(null)
        // This is bytes allocated for blocks inside container, not the
        // container size
        .setAllocatedBytes(0)
        .setUsedBytes(0)
        .setNumberOfKeys(0)
        .setStateEnterTime(Time.monotonicNow())
        .setOwner("OZONE")
        .setContainerID(cid)
        .build();
    state.stateMap.addContainer(containerInfo);
  }

  @Benchmark
  public void getMatchingContainerBenchMark(BenchmarkState state,
      Blackhole bh) {
    state.stateMap.getMatchingContainerIDs(OPEN, "BILBO", ONE, STAND_ALONE);
  }

  @State(Scope.Thread)
  public static class BenchmarkState {
    public ContainerStateMap stateMap;
    public AtomicInteger containerID;

    @Setup(Level.Trial)
    public void initialize() throws IOException {
      stateMap = new ContainerStateMap();
      Pipeline pipeline = ContainerTestHelper
          .createSingleNodePipeline(UUID.randomUUID().toString());


      int currentCount = 1;
      for (int x = 1; x < 1000 * 1000; x++) {
        try {
          ContainerInfo containerInfo = new ContainerInfo.Builder()
              .setContainerName(pipeline.getContainerName())
              .setState(OzoneProtos.LifeCycleState.CLOSED)
              .setPipeline(null)
              // This is bytes allocated for blocks inside container, not the
              // container size
              .setAllocatedBytes(0)
              .setUsedBytes(0)
              .setNumberOfKeys(0)
              .setStateEnterTime(Time.monotonicNow())
              .setOwner("OZONE")
              .setContainerID(x)
              .build();
          stateMap.addContainer(containerInfo);
          currentCount++;
        } catch (SCMException e) {
          e.printStackTrace();
        }
      }
      for (int y = currentCount; y < 2000; y++) {
        try {
          ContainerInfo containerInfo = new ContainerInfo.Builder()
              .setContainerName(pipeline.getContainerName())
              .setState(OzoneProtos.LifeCycleState.OPEN)
              .setPipeline(null)
              // This is bytes allocated for blocks inside container, not the
              // container size
              .setAllocatedBytes(0)
              .setUsedBytes(0)
              .setNumberOfKeys(0)
              .setStateEnterTime(Time.monotonicNow())
              .setOwner("OZONE")
              .setContainerID(y)
              .build();
          stateMap.addContainer(containerInfo);
          currentCount++;
        } catch (SCMException e) {
          e.printStackTrace();
        }

      }
      try {

        ContainerInfo containerInfo = new ContainerInfo.Builder()
            .setContainerName(pipeline.getContainerName())
            .setState(OzoneProtos.LifeCycleState.OPEN)
            .setPipeline(null)
            // This is bytes allocated for blocks inside container, not the
            // container size
            .setAllocatedBytes(0)
            .setUsedBytes(0)
            .setNumberOfKeys(0)
            .setStateEnterTime(Time.monotonicNow())
            .setOwner("OZONE")
            .setContainerID(currentCount++)
            .build();
        stateMap.addContainer(containerInfo);
      } catch (SCMException e) {
        e.printStackTrace();
      }

      containerID = new AtomicInteger(currentCount++);

    }
  }
}
