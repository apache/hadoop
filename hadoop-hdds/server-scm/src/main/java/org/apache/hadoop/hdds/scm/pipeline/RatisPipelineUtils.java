/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.ratis.RatisHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.util.function.CheckedBiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utility class for Ratis pipelines. Contains methods to create and destroy
 * ratis pipelines.
 */
public final class RatisPipelineUtils {

  private static AtomicBoolean isPipelineCreatorRunning =
      new AtomicBoolean(false);

  private static final Logger LOG =
      LoggerFactory.getLogger(RatisPipelineUtils.class);

  private RatisPipelineUtils() {
  }

  /**
   * Sends ratis command to create pipeline on all the datanodes.
   *
   * @param pipeline  - Pipeline to be created
   * @param ozoneConf - Ozone Confinuration
   * @throws IOException if creation fails
   */
  public static void createPipeline(Pipeline pipeline, Configuration ozoneConf)
      throws IOException {
    final RaftGroup group = RatisHelper.newRaftGroup(pipeline);
    LOG.debug("creating pipeline:{} with {}", pipeline.getId(), group);
    callRatisRpc(pipeline.getNodes(), ozoneConf,
        (raftClient, peer) -> {
          RaftClientReply reply = raftClient.groupAdd(group, peer.getId());
          if (reply == null || !reply.isSuccess()) {
            String msg = "Pipeline initialization failed for pipeline:"
                + pipeline.getId() + " node:" + peer.getId();
            LOG.error(msg);
            throw new IOException(msg);
          }
        });
  }

  /**
   * Removes pipeline from SCM. Sends ratis command to destroy pipeline on all
   * the datanodes.
   *
   * @param pipelineManager - SCM pipeline manager
   * @param pipeline        - Pipeline to be destroyed
   * @param ozoneConf       - Ozone configuration
   * @throws IOException
   */
  public static void destroyPipeline(PipelineManager pipelineManager,
      Pipeline pipeline, Configuration ozoneConf) throws IOException {
    final RaftGroup group = RatisHelper.newRaftGroup(pipeline);
    LOG.debug("destroying pipeline:{} with {}", pipeline.getId(), group);
    for (DatanodeDetails dn : pipeline.getNodes()) {
      try {
        destroyPipeline(dn, pipeline.getId(), ozoneConf);
      } catch (IOException e) {
        LOG.warn("Pipeline destroy failed for pipeline={} dn={}",
            pipeline.getId(), dn);
      }
    }
    // remove the pipeline from the pipeline manager
    pipelineManager.removePipeline(pipeline.getId());
    triggerPipelineCreation(pipelineManager, ozoneConf, 0);
  }

  /**
   * Finalizes pipeline in the SCM. Removes pipeline and sends ratis command to
   * destroy pipeline on the datanodes immediately or after timeout based on the
   * value of onTimeout parameter.
   *
   * @param pipelineManager - SCM pipeline manager
   * @param pipeline        - Pipeline to be destroyed
   * @param ozoneConf       - Ozone Configuration
   * @param onTimeout       - if true pipeline is removed and destroyed on
   *                        datanodes after timeout
   * @throws IOException
   */
  public static void finalizeAndDestroyPipeline(PipelineManager pipelineManager,
      Pipeline pipeline, Configuration ozoneConf, boolean onTimeout)
      throws IOException {
    final RaftGroup group = RatisHelper.newRaftGroup(pipeline);
    LOG.info("destroying pipeline:{} with {}", pipeline.getId(), group);
    pipelineManager.finalizePipeline(pipeline.getId());
    if (onTimeout) {
      long pipelineDestroyTimeoutInMillis = ozoneConf
          .getTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT,
              ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT_DEFAULT,
              TimeUnit.MILLISECONDS);
      RatisPipelineProvider.getScheduler()
          .schedule(() -> destroyPipeline(pipelineManager, pipeline, ozoneConf),
              pipelineDestroyTimeoutInMillis, TimeUnit.MILLISECONDS, LOG, String
                  .format("Destroy pipeline failed for pipeline:%s with %s",
                      pipeline.getId(), group));
    } else {
      destroyPipeline(pipelineManager, pipeline, ozoneConf);
    }
  }

  /**
   * Sends ratis command to destroy pipeline on the given datanode.
   *
   * @param dn         - Datanode on which pipeline needs to be destroyed
   * @param pipelineID - ID of pipeline to be destroyed
   * @param ozoneConf  - Ozone configuration
   * @throws IOException
   */
  static void destroyPipeline(DatanodeDetails dn, PipelineID pipelineID,
      Configuration ozoneConf) throws IOException {
    final String rpcType = ozoneConf
        .get(ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
            ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    final RetryPolicy retryPolicy = RatisHelper.createRetryPolicy(ozoneConf);
    final RaftPeer p = RatisHelper.toRaftPeer(dn);
    final int maxOutstandingRequests =
        HddsClientUtils.getMaxOutstandingRequests(ozoneConf);
    final GrpcTlsConfig tlsConfig = RatisHelper.createTlsClientConfig(
        new SecurityConfig(ozoneConf));
    RaftClient client = RatisHelper
        .newRaftClient(SupportedRpcType.valueOfIgnoreCase(rpcType), p,
            retryPolicy, maxOutstandingRequests, tlsConfig);
    client
        .groupRemove(RaftGroupId.valueOf(pipelineID.getId()), true, p.getId());
  }

  private static void callRatisRpc(List<DatanodeDetails> datanodes,
      Configuration ozoneConf,
      CheckedBiConsumer<RaftClient, RaftPeer, IOException> rpc)
      throws IOException {
    if (datanodes.isEmpty()) {
      return;
    }

    final String rpcType = ozoneConf
        .get(ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
            ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    final RetryPolicy retryPolicy = RatisHelper.createRetryPolicy(ozoneConf);
    final List<IOException> exceptions =
        Collections.synchronizedList(new ArrayList<>());
    final int maxOutstandingRequests =
        HddsClientUtils.getMaxOutstandingRequests(ozoneConf);
    final GrpcTlsConfig tlsConfig = RatisHelper.createTlsClientConfig(new
        SecurityConfig(ozoneConf));

    datanodes.parallelStream().forEach(d -> {
      final RaftPeer p = RatisHelper.toRaftPeer(d);
      try (RaftClient client = RatisHelper
          .newRaftClient(SupportedRpcType.valueOfIgnoreCase(rpcType), p,
              retryPolicy, maxOutstandingRequests, tlsConfig)) {
        rpc.accept(client, p);
      } catch (IOException ioe) {
        exceptions.add(
            new IOException("Failed invoke Ratis rpc " + rpc + " for " +
                d.getUuid(), ioe));
      }
    });
    if (!exceptions.isEmpty()) {
      throw MultipleIOException.createIOException(exceptions);
    }
  }

  /**
   * Schedules a fixed interval job to create pipelines.
   *
   * @param pipelineManager - Pipeline manager
   * @param conf            - Configuration
   */
  public static void scheduleFixedIntervalPipelineCreator(
      PipelineManager pipelineManager, Configuration conf) {
    long intervalInMillis = conf
        .getTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL,
            ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    // TODO: #CLUTIL We can start the job asap
    RatisPipelineProvider.getScheduler().scheduleWithFixedDelay(() -> {
      if (!isPipelineCreatorRunning.compareAndSet(false, true)) {
        return;
      }
      createPipelines(pipelineManager, conf);
    }, intervalInMillis, intervalInMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * Triggers pipeline creation after the specified time.
   *
   * @param pipelineManager - Pipeline manager
   * @param conf            - Configuration
   * @param afterMillis     - Time after which pipeline creation needs to be
   *                        triggered
   */
  public static void triggerPipelineCreation(PipelineManager pipelineManager,
      Configuration conf, long afterMillis) {
    // TODO: #CLUTIL introduce a better mechanism to not have more than one
    // job of a particular type running, probably via ratis.
    if (!isPipelineCreatorRunning.compareAndSet(false, true)) {
      return;
    }
    RatisPipelineProvider.getScheduler()
        .schedule(() -> createPipelines(pipelineManager, conf), afterMillis,
            TimeUnit.MILLISECONDS);
  }

  private static void createPipelines(PipelineManager pipelineManager,
      Configuration conf) {
    // TODO: #CLUTIL Different replication factor may need to be supported
    HddsProtos.ReplicationType type = HddsProtos.ReplicationType.valueOf(
        conf.get(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
            OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT));

    for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
        .values()) {
      while (true) {
        try {
          if (RatisPipelineProvider.getScheduler().isClosed()) {
            break;
          }
          pipelineManager.createPipeline(type, factor);
        } catch (IOException ioe) {
          break;
        } catch (Throwable t) {
          LOG.error("Error while creating pipelines {}", t);
          break;
        }
      }
    }
    isPipelineCreatorRunning.set(false);
  }
}
