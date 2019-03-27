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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.node.NodeManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates pipeline based on replication type.
 */
public final class PipelineFactory {

  private Map<ReplicationType, PipelineProvider> providers;

  PipelineFactory(NodeManager nodeManager, PipelineStateManager stateManager,
      Configuration conf) {
    providers = new HashMap<>();
    providers.put(ReplicationType.STAND_ALONE,
        new SimplePipelineProvider(nodeManager));
    providers.put(ReplicationType.RATIS,
        new RatisPipelineProvider(nodeManager, stateManager, conf));
  }

  @VisibleForTesting
  void setProvider(ReplicationType replicationType,
                     PipelineProvider provider) {
    providers.put(replicationType, provider);
  }

  public Pipeline create(ReplicationType type, ReplicationFactor factor)
      throws IOException {
    return providers.get(type).create(factor);
  }

  public Pipeline create(ReplicationType type, ReplicationFactor factor,
      List<DatanodeDetails> nodes) {
    return providers.get(type).create(factor, nodes);
  }
}
