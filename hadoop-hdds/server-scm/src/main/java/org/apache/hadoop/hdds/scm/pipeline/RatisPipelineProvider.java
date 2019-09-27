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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedBiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implements Api for creating ratis pipelines.
 */
public class RatisPipelineProvider implements PipelineProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(RatisPipelineProvider.class);

  private final NodeManager nodeManager;
  private final PipelineStateManager stateManager;
  private final Configuration conf;
  private final PipelinePlacementPolicy placementPolicy;

  // Set parallelism at 3, as now in Ratis we create 1 and 3 node pipelines.
  private final int parallelismForPool = 3;

  private final ForkJoinPool.ForkJoinWorkerThreadFactory factory =
      (pool -> {
        final ForkJoinWorkerThread worker = ForkJoinPool.
            defaultForkJoinWorkerThreadFactory.newThread(pool);
        worker.setName("RATISCREATEPIPELINE" + worker.getPoolIndex());
        return worker;
      });

  private final ForkJoinPool forkJoinPool = new ForkJoinPool(
      parallelismForPool, factory, null, false);
  private final GrpcTlsConfig tlsConfig;

  RatisPipelineProvider(NodeManager nodeManager,
      PipelineStateManager stateManager, Configuration conf,
      GrpcTlsConfig tlsConfig) {
    this.nodeManager = nodeManager;
    this.stateManager = stateManager;
    this.conf = conf;
    this.tlsConfig = tlsConfig;
    this.placementPolicy =
        new PipelinePlacementPolicy(nodeManager, stateManager, conf);
  }

  private List<DatanodeDetails> pickNodesNeverUsed(ReplicationFactor factor)
      throws SCMException {
    Set<DatanodeDetails> dnsUsed = new HashSet<>();
    stateManager.getPipelines(ReplicationType.RATIS, factor)
        .stream().filter(
          p -> p.getPipelineState().equals(PipelineState.OPEN) ||
              p.getPipelineState().equals(PipelineState.DORMANT) ||
              p.getPipelineState().equals(PipelineState.ALLOCATED))
        .forEach(p -> dnsUsed.addAll(p.getNodes()));

    // Get list of healthy nodes
    List<DatanodeDetails> dns = nodeManager
        .getNodes(HddsProtos.NodeState.HEALTHY)
        .parallelStream()
        .filter(dn -> !dnsUsed.contains(dn))
        .limit(factor.getNumber())
        .collect(Collectors.toList());
    if (dns.size() < factor.getNumber()) {
      String e = String
          .format("Cannot create pipeline of factor %d using %d nodes." +
                  " Used %d nodes. Healthy nodes %d", factor.getNumber(),
              dns.size(), dnsUsed.size(),
              nodeManager.getNodes(HddsProtos.NodeState.HEALTHY).size());
      throw new SCMException(e,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    return dns;
  }

  @Override
  public Pipeline create(ReplicationFactor factor) throws IOException {
    List<DatanodeDetails> dns;

    switch(factor) {
    case ONE:
      dns = pickNodesNeverUsed(ReplicationFactor.ONE);
      break;
    case THREE:
      dns = placementPolicy.chooseDatanodes(null,
          null, factor.getNumber(), 0);
      break;
    default:
      throw new IllegalStateException("Unknown factor: " + factor.name());
    }

    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.OPEN)
        .setType(ReplicationType.RATIS)
        .setFactor(factor)
        .setNodes(dns)
        .build();
    initializePipeline(pipeline);
    return pipeline;
  }

  @Override
  public Pipeline create(ReplicationFactor factor,
      List<DatanodeDetails> nodes) {
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.OPEN)
        .setType(ReplicationType.RATIS)
        .setFactor(factor)
        .setNodes(nodes)
        .build();
  }


  @Override
  public void shutdown() {
    forkJoinPool.shutdownNow();
    try {
      forkJoinPool.awaitTermination(60, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.error("Unexpected exception occurred during shutdown of " +
              "RatisPipelineProvider", e);
    }
  }

  protected void initializePipeline(Pipeline pipeline) throws IOException {
    final RaftGroup group = RatisHelper.newRaftGroup(pipeline);
    LOG.debug("creating pipeline:{} with {}", pipeline.getId(), group);
    callRatisRpc(pipeline.getNodes(),
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

  private void callRatisRpc(List<DatanodeDetails> datanodes,
      CheckedBiConsumer< RaftClient, RaftPeer, IOException> rpc)
      throws IOException {
    if (datanodes.isEmpty()) {
      return;
    }

    final String rpcType = conf
        .get(ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
            ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    final RetryPolicy retryPolicy = RatisHelper.createRetryPolicy(conf);
    final List< IOException > exceptions =
        Collections.synchronizedList(new ArrayList<>());
    final int maxOutstandingRequests =
        HddsClientUtils.getMaxOutstandingRequests(conf);
    final TimeDuration requestTimeout =
        RatisHelper.getClientRequestTimeout(conf);
    try {
      forkJoinPool.submit(() -> {
        datanodes.parallelStream().forEach(d -> {
          final RaftPeer p = RatisHelper.toRaftPeer(d);
          try (RaftClient client = RatisHelper
              .newRaftClient(SupportedRpcType.valueOfIgnoreCase(rpcType), p,
                  retryPolicy, maxOutstandingRequests, tlsConfig,
                  requestTimeout)) {
            rpc.accept(client, p);
          } catch (IOException ioe) {
            String errMsg =
                "Failed invoke Ratis rpc " + rpc + " for " + d.getUuid();
            LOG.error(errMsg, ioe);
            exceptions.add(new IOException(errMsg, ioe));
          }
        });
      }).get();
    } catch (ExecutionException | RejectedExecutionException ex) {
      LOG.error(ex.getClass().getName() + " exception occurred during " +
          "createPipeline", ex);
      throw new IOException(ex.getClass().getName() + " exception occurred " +
          "during createPipeline", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupt exception occurred during " +
          "createPipeline", ex);
    }
    if (!exceptions.isEmpty()) {
      throw MultipleIOException.createIOException(exceptions);
    }
  }
}
