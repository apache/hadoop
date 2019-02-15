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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_MB;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_PIPELINE_DB;

/**
 * Benchmarks BlockManager class.
 */
@State(Scope.Thread)
public class BenchMarkBlockManager {

  private StorageContainerManager scm;
  private PipelineManager pipelineManager;
  private BlockManager blockManager;

  private static StorageContainerManager getScm(OzoneConfiguration conf,
      SCMConfigurator configurator) throws IOException,
      AuthenticationException {
    conf.setBoolean(OZONE_ENABLED, true);
    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
    if(scmStore.getState() != Storage.StorageState.INITIALIZED) {
      String clusterId = UUID.randomUUID().toString();
      String scmId = UUID.randomUUID().toString();
      scmStore.setClusterId(clusterId);
      scmStore.setScmId(scmId);
      // writes the version file properties
      scmStore.initialize();
    }
    return new StorageContainerManager(conf, configurator);
  }

  @Setup(Level.Trial)
  public void initialize()
      throws IOException, AuthenticationException, InterruptedException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        GenesisUtil.getTempPath().resolve(RandomStringUtils.randomNumeric(7))
            .toString());
    conf.setInt(OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 100);
    final File metaDir = ServerUtils.getScmDbDir(conf);
    final File pipelineDBPath = new File(metaDir, SCM_PIPELINE_DB);
    int cacheSize = conf.getInt(OZONE_SCM_DB_CACHE_SIZE_MB,
        OZONE_SCM_DB_CACHE_SIZE_DEFAULT);
    MetadataStore pipelineStore =
        MetadataStoreBuilder.newBuilder()
            .setCreateIfMissing(true)
            .setConf(conf)
            .setDbFile(pipelineDBPath)
            .setCacheSize(cacheSize * OzoneConsts.MB)
            .build();
    addPipelines(100, ReplicationFactor.THREE, pipelineStore);
    pipelineStore.close();
    scm = getScm(conf, new SCMConfigurator());
    pipelineManager = scm.getPipelineManager();
    for (Pipeline pipeline : pipelineManager
        .getPipelines(ReplicationType.RATIS, ReplicationFactor.THREE)) {
      pipelineManager.openPipeline(pipeline.getId());
    }
    blockManager = scm.getScmBlockManager();
    scm.getEventQueue().fireEvent(SCMEvents.CHILL_MODE_STATUS, false);
    Thread.sleep(1000);
  }

  @Setup(Level.Trial)
  public void tearDown() {
    if (scm != null) {
      scm.stop();
      scm.join();
    }
  }

  private void addPipelines(int numPipelines, ReplicationFactor factor,
      MetadataStore pipelineStore) throws IOException {
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < factor.getNumber(); i++) {
      nodes
          .add(GenesisUtil.createDatanodeDetails(UUID.randomUUID().toString()));
    }
    for (int i = 0; i < numPipelines; i++) {
      Pipeline pipeline =
          Pipeline.newBuilder()
              .setState(Pipeline.PipelineState.OPEN)
              .setId(PipelineID.randomId())
              .setType(ReplicationType.RATIS)
              .setFactor(factor)
              .setNodes(nodes)
              .build();
      pipelineStore.put(pipeline.getId().getProtobuf().toByteArray(),
          pipeline.getProtobufMessage().toByteArray());
    }
  }

  @Benchmark
  public void allocateBlockBenchMark(BenchMarkBlockManager state,
      Blackhole bh) throws IOException {
    state.blockManager
        .allocateBlock(50, ReplicationType.RATIS, ReplicationFactor.THREE,
            "Genesis");
  }
}
