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

package org.apache.hadoop.hdds.scm.pipeline;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.utils.MetadataKeyFilters;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;
import org.apache.hadoop.utils.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.hdds.scm
    .ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm
    .ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_MB;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_PIPELINE_DB;

/**
 * Implements api needed for management of pipelines. All the write operations
 * for pipelines must come via PipelineManager. It synchronises all write
 * and read operations via a ReadWriteLock.
 */
public class SCMPipelineManager implements PipelineManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMPipelineManager.class);

  private final ReadWriteLock lock;
  private final PipelineFactory pipelineFactory;
  private final PipelineStateManager stateManager;
  private final BackgroundPipelineCreator backgroundPipelineCreator;
  private Scheduler scheduler;
  private MetadataStore pipelineStore;

  private final EventPublisher eventPublisher;
  private final NodeManager nodeManager;
  private final SCMPipelineMetrics metrics;
  private final Configuration conf;
  // Pipeline Manager MXBean
  private ObjectName pmInfoBean;

  public SCMPipelineManager(Configuration conf, NodeManager nodeManager,
      EventPublisher eventPublisher) throws IOException {
    this.lock = new ReentrantReadWriteLock();
    this.conf = conf;
    this.stateManager = new PipelineStateManager(conf);
    this.pipelineFactory = new PipelineFactory(nodeManager, stateManager, conf);
    // TODO: See if thread priority needs to be set for these threads
    scheduler = new Scheduler("RatisPipelineUtilsThread", false, 1);
    this.backgroundPipelineCreator =
        new BackgroundPipelineCreator(this, scheduler, conf);
    int cacheSize = conf.getInt(OZONE_SCM_DB_CACHE_SIZE_MB,
        OZONE_SCM_DB_CACHE_SIZE_DEFAULT);
    final File metaDir = ServerUtils.getScmDbDir(conf);
    final File pipelineDBPath = new File(metaDir, SCM_PIPELINE_DB);
    this.pipelineStore =
        MetadataStoreBuilder.newBuilder()
            .setCreateIfMissing(true)
            .setConf(conf)
            .setDbFile(pipelineDBPath)
            .setCacheSize(cacheSize * OzoneConsts.MB)
            .build();
    this.eventPublisher = eventPublisher;
    this.nodeManager = nodeManager;
    this.metrics = SCMPipelineMetrics.create();
    this.pmInfoBean = MBeans.register("SCMPipelineManager",
        "SCMPipelineManagerInfo", this);
    initializePipelineState();
  }

  public PipelineStateManager getStateManager() {
    return stateManager;
  }

  public void setPipelineProvider(ReplicationType replicationType,
                                  PipelineProvider provider) {
    pipelineFactory.setProvider(replicationType, provider);
  }

  private void initializePipelineState() throws IOException {
    if (pipelineStore.isEmpty()) {
      LOG.info("No pipeline exists in current db");
      return;
    }
    List<Map.Entry<byte[], byte[]>> pipelines =
        pipelineStore.getSequentialRangeKVs(null, Integer.MAX_VALUE,
            (MetadataKeyFilters.MetadataKeyFilter[])null);

    for (Map.Entry<byte[], byte[]> entry : pipelines) {
      HddsProtos.Pipeline.Builder pipelineBuilder = HddsProtos.Pipeline
          .newBuilder(HddsProtos.Pipeline.PARSER.parseFrom(entry.getValue()));
      Pipeline pipeline = Pipeline.getFromProtobuf(pipelineBuilder.setState(
          HddsProtos.PipelineState.PIPELINE_ALLOCATED).build());
      Preconditions.checkNotNull(pipeline);
      stateManager.addPipeline(pipeline);
      nodeManager.addPipeline(pipeline);
    }
  }

  @Override
  public synchronized Pipeline createPipeline(
      ReplicationType type, ReplicationFactor factor) throws IOException {
    lock.writeLock().lock();
    try {
      Pipeline pipeline = pipelineFactory.create(type, factor);
      pipelineStore.put(pipeline.getId().getProtobuf().toByteArray(),
          pipeline.getProtobufMessage().toByteArray());
      stateManager.addPipeline(pipeline);
      nodeManager.addPipeline(pipeline);
      metrics.incNumPipelineCreated();
      return pipeline;
    } catch (InsufficientDatanodesException idEx) {
      throw idEx;
    } catch (IOException ex) {
      metrics.incNumPipelineCreationFailed();
      throw ex;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Pipeline createPipeline(ReplicationType type, ReplicationFactor factor,
                                 List<DatanodeDetails> nodes) {
    // This will mostly be used to create dummy pipeline for SimplePipelines.
    // We don't update the metrics for SimplePipelines.
    lock.writeLock().lock();
    try {
      return pipelineFactory.create(type, factor, nodes);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Pipeline getPipeline(PipelineID pipelineID)
      throws PipelineNotFoundException {
    lock.readLock().lock();
    try {
      return stateManager.getPipeline(pipelineID);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines() {
    lock.readLock().lock();
    try {
      return stateManager.getPipelines();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationType type) {
    lock.readLock().lock();
    try {
      return stateManager.getPipelines(type);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationType type,
      ReplicationFactor factor) {
    lock.readLock().lock();
    try {
      return stateManager.getPipelines(type, factor);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationType type,
      ReplicationFactor factor, Pipeline.PipelineState state) {
    lock.readLock().lock();
    try {
      return stateManager.getPipelines(type, factor, state);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationType type,
      ReplicationFactor factor, Pipeline.PipelineState state,
      Collection<DatanodeDetails> excludeDns,
      Collection<PipelineID> excludePipelines) {
    lock.readLock().lock();
    try {
      return stateManager
          .getPipelines(type, factor, state, excludeDns, excludePipelines);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void addContainerToPipeline(PipelineID pipelineID,
      ContainerID containerID) throws IOException {
    lock.writeLock().lock();
    try {
      stateManager.addContainerToPipeline(pipelineID, containerID);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void removeContainerFromPipeline(PipelineID pipelineID,
      ContainerID containerID) throws IOException {
    lock.writeLock().lock();
    try {
      stateManager.removeContainerFromPipeline(pipelineID, containerID);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public NavigableSet<ContainerID> getContainersInPipeline(
      PipelineID pipelineID) throws IOException {
    lock.readLock().lock();
    try {
      return stateManager.getContainers(pipelineID);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public int getNumberOfContainers(PipelineID pipelineID) throws IOException {
    return stateManager.getNumberOfContainers(pipelineID);
  }

  @Override
  public void openPipeline(PipelineID pipelineId) throws IOException {
    lock.writeLock().lock();
    try {
      stateManager.openPipeline(pipelineId);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Finalizes pipeline in the SCM. Removes pipeline and makes rpc call to
   * destroy pipeline on the datanodes immediately or after timeout based on the
   * value of onTimeout parameter.
   *
   * @param pipeline        - Pipeline to be destroyed
   * @param onTimeout       - if true pipeline is removed and destroyed on
   *                        datanodes after timeout
   * @throws IOException
   */
  @Override
  public void finalizeAndDestroyPipeline(Pipeline pipeline, boolean onTimeout)
      throws IOException {
    LOG.info("destroying pipeline:{}", pipeline);
    finalizePipeline(pipeline.getId());
    if (onTimeout) {
      long pipelineDestroyTimeoutInMillis =
          conf.getTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT,
              ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT_DEFAULT,
              TimeUnit.MILLISECONDS);
      scheduler.schedule(() -> destroyPipeline(pipeline),
          pipelineDestroyTimeoutInMillis, TimeUnit.MILLISECONDS, LOG,
          String.format("Destroy pipeline failed for pipeline:%s", pipeline));
    } else {
      destroyPipeline(pipeline);
    }
  }

  @Override
  public Map<String, Integer> getPipelineInfo() {
    final Map<String, Integer> pipelineInfo = new HashMap<>();
    for (Pipeline.PipelineState state : Pipeline.PipelineState.values()) {
      pipelineInfo.put(state.toString(), 0);
    }
    stateManager.getPipelines().forEach(pipeline ->
        pipelineInfo.computeIfPresent(
            pipeline.getPipelineState().toString(), (k, v) -> v + 1));
    return pipelineInfo;
  }

  /**
   * Schedules a fixed interval job to create pipelines.
   */
  @Override
  public void startPipelineCreator() {
    backgroundPipelineCreator.startFixedIntervalPipelineCreator();
  }

  /**
   * Triggers pipeline creation after the specified time.
   */
  @Override
  public void triggerPipelineCreation() {
    backgroundPipelineCreator.triggerPipelineCreation();
  }

  /**
   * Moves the pipeline to CLOSED state and sends close container command for
   * all the containers in the pipeline.
   *
   * @param pipelineId - ID of the pipeline to be moved to CLOSED state.
   * @throws IOException
   */
  private void finalizePipeline(PipelineID pipelineId) throws IOException {
    lock.writeLock().lock();
    try {
      stateManager.finalizePipeline(pipelineId);
      Set<ContainerID> containerIDs = stateManager.getContainers(pipelineId);
      for (ContainerID containerID : containerIDs) {
        eventPublisher.fireEvent(SCMEvents.CLOSE_CONTAINER, containerID);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Removes pipeline from SCM. Sends ratis command to destroy pipeline on all
   * the datanodes for ratis pipelines.
   *
   * @param pipeline        - Pipeline to be destroyed
   * @throws IOException
   */
  private void destroyPipeline(Pipeline pipeline) throws IOException {
    RatisPipelineUtils.destroyPipeline(pipeline, conf);
    // remove the pipeline from the pipeline manager
    removePipeline(pipeline.getId());
    triggerPipelineCreation();
  }

  /**
   * Removes the pipeline from the db and pipeline state map.
   *
   * @param pipelineId - ID of the pipeline to be removed
   * @throws IOException
   */
  private void removePipeline(PipelineID pipelineId) throws IOException {
    lock.writeLock().lock();
    try {
      pipelineStore.delete(pipelineId.getProtobuf().toByteArray());
      Pipeline pipeline = stateManager.removePipeline(pipelineId);
      nodeManager.removePipeline(pipeline);
      metrics.incNumPipelineDestroyed();
    } catch (IOException ex) {
      metrics.incNumPipelineDestroyFailed();
      throw ex;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws IOException {
    if (scheduler != null) {
      scheduler.close();
      scheduler = null;
    }

    if (pipelineStore != null) {
      pipelineStore.close();
      pipelineStore = null;
    }
    if(pmInfoBean != null) {
      MBeans.unregister(this.pmInfoBean);
      pmInfoBean = null;
    }
    if(metrics != null) {
      metrics.unRegister();
    }
  }
}
