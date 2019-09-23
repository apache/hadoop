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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED;

/**
 * Benchmarks BlockManager class.
 */
@State(Scope.Thread)
public class BenchMarkSCM {

  private static String testDir;
  private static StorageContainerManager scm;
  private static BlockManager blockManager;
  private static ReentrantLock lock = new ReentrantLock();

  @Param({ "1", "10", "100", "1000", "10000", "100000" })
  private static int numPipelines;
  @Param({ "3", "10", "100" })
  private static int numContainersPerPipeline;

  @Setup(Level.Trial)
  public static void initialize()
      throws IOException, AuthenticationException, InterruptedException {
    try {
      lock.lock();
      if (scm == null) {
        OzoneConfiguration conf = new OzoneConfiguration();
        conf.setBoolean(OZONE_ENABLED, true);
        testDir = GenesisUtil.getTempPath()
            .resolve(RandomStringUtils.randomNumeric(7)).toString();
        conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir);

        GenesisUtil.configureSCM(conf, 10);
        conf.setInt(OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
            numContainersPerPipeline);
        GenesisUtil.addPipelines(ReplicationFactor.THREE, numPipelines, conf);

        scm = GenesisUtil.getScm(conf, new SCMConfigurator());
        scm.start();
        blockManager = scm.getScmBlockManager();

        // prepare SCM
        PipelineManager pipelineManager = scm.getPipelineManager();
        for (Pipeline pipeline : pipelineManager
            .getPipelines(ReplicationType.RATIS, ReplicationFactor.THREE)) {
          pipelineManager.openPipeline(pipeline.getId());
        }
        scm.getEventQueue().fireEvent(SCMEvents.SAFE_MODE_STATUS,
            new SCMSafeModeManager.SafeModeStatus(false));
        Thread.sleep(1000);
      }
    } finally {
      lock.unlock();
    }
  }

  @TearDown(Level.Trial)
  public static void tearDown() {
    try {
      lock.lock();
      if (scm != null) {
        scm.stop();
        scm.join();
        scm = null;
        FileUtil.fullyDelete(new File(testDir));
      }
    } finally {
      lock.unlock();
    }
  }

  @Threads(4)
  @Benchmark
  public void allocateBlockBenchMark(BenchMarkSCM state,
      Blackhole bh) throws IOException {
    state.blockManager
        .allocateBlock(50, ReplicationType.RATIS, ReplicationFactor.THREE,
            "Genesis", new ExcludeList());
  }
}
