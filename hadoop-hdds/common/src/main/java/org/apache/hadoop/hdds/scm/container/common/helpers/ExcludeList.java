/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container.common.helpers;


import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

import java.util.ArrayList;
import java.util.List;
import java.util.Collection;

/**
 * This class contains set of dns and containers which ozone client provides
 * to be handed over to SCM when block allocation request comes.
 */
public class ExcludeList {

  private final List<DatanodeDetails> datanodes;
  private final List<ContainerID> containerIds;
  private final List<PipelineID> pipelineIds;


  public ExcludeList() {
    datanodes = new ArrayList<>();
    containerIds = new ArrayList<>();
    pipelineIds = new ArrayList<>();
  }

  public List<ContainerID> getContainerIds() {
    return containerIds;
  }

  public List<DatanodeDetails> getDatanodes() {
    return datanodes;
  }

  public void addDatanodes(Collection<DatanodeDetails> dns) {
    datanodes.addAll(dns);
  }

  public void addDatanode(DatanodeDetails dn) {
    datanodes.add(dn);
  }

  public void addConatinerId(ContainerID containerId) {
    containerIds.add(containerId);
  }

  public void addPipeline(PipelineID pipelineId) {
    pipelineIds.add(pipelineId);
  }

  public List<PipelineID> getPipelineIds() {
    return pipelineIds;
  }

  public HddsProtos.ExcludeListProto getProtoBuf() {
    HddsProtos.ExcludeListProto.Builder builder =
        HddsProtos.ExcludeListProto.newBuilder();
    containerIds
        .forEach(id -> builder.addContainerIds(id.getId()));
    datanodes.forEach(dn -> {
      builder.addDatanodes(dn.getUuidString());
    });
    pipelineIds.forEach(pipelineID -> {
      builder.addPipelineIds(pipelineID.getProtobuf());
    });
    return builder.build();
  }

  public static ExcludeList getFromProtoBuf(
      HddsProtos.ExcludeListProto excludeListProto) {
    ExcludeList excludeList = new ExcludeList();
    excludeListProto.getContainerIdsList().forEach(id -> {
      excludeList.addConatinerId(ContainerID.valueof(id));
    });
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    excludeListProto.getDatanodesList().forEach(dn -> {
      builder.setUuid(dn);
      excludeList.addDatanode(builder.build());
    });
    excludeListProto.getPipelineIdsList().forEach(pipelineID -> {
      excludeList.addPipeline(PipelineID.getFromProtobuf(pipelineID));
    });
    return excludeList;
  }

  public void clear() {
    datanodes.clear();
    containerIds.clear();
    pipelineIds.clear();
  }
}
