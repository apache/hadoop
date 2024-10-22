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
package org.apache.hadoop.hdfs.server.federation.store.records.impl.pb;

import java.io.IOException;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeMembershipStatsRecordProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeMembershipStatsRecordProto.Builder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeMembershipStatsRecordProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationProtocolPBTranslator;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipStats;

import org.apache.hadoop.thirdparty.protobuf.Message;

/**
 * Protobuf implementation of the MembershipStats record.
 */
public class MembershipStatsPBImpl extends MembershipStats
    implements PBRecord {

  private FederationProtocolPBTranslator<NamenodeMembershipStatsRecordProto,
      Builder, NamenodeMembershipStatsRecordProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<NamenodeMembershipStatsRecordProto,
          Builder, NamenodeMembershipStatsRecordProtoOrBuilder>(
              NamenodeMembershipStatsRecordProto.class);

  public MembershipStatsPBImpl() {
  }

  @Override
  public NamenodeMembershipStatsRecordProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Message proto) {
    this.translator.setProto(proto);
  }

  @Override
  public void readInstance(String base64String) throws IOException {
    this.translator.readInstance(base64String);
  }

  @Override
  public void setTotalSpace(long space) {
    this.translator.getBuilder().setTotalSpace(space);
  }

  @Override
  public long getTotalSpace() {
    return this.translator.getProtoOrBuilder().getTotalSpace();
  }

  @Override
  public void setAvailableSpace(long space) {
    this.translator.getBuilder().setAvailableSpace(space);
  }

  @Override
  public long getAvailableSpace() {
    return this.translator.getProtoOrBuilder().getAvailableSpace();
  }

  @Override
  public void setProvidedSpace(long capacity) {
    this.translator.getBuilder().setProvidedSpace(capacity);
  }

  @Override
  public long getProvidedSpace() {
    return this.translator.getProtoOrBuilder().getProvidedSpace();
  }

  @Override
  public void setNumOfFiles(long files) {
    this.translator.getBuilder().setNumOfFiles(files);
  }

  @Override
  public long getNumOfFiles() {
    return this.translator.getProtoOrBuilder().getNumOfFiles();
  }

  @Override
  public void setNumOfBlocks(long blocks) {
    this.translator.getBuilder().setNumOfBlocks(blocks);
  }

  @Override
  public long getNumOfBlocks() {
    return this.translator.getProtoOrBuilder().getNumOfBlocks();
  }

  @Override
  public void setNumOfBlocksMissing(long blocks) {
    this.translator.getBuilder().setNumOfBlocksMissing(blocks);
  }

  @Override
  public long getNumOfBlocksMissing() {
    return this.translator.getProtoOrBuilder().getNumOfBlocksMissing();
  }

  @Override
  public void setNumOfBlocksPendingReplication(long blocks) {
    this.translator.getBuilder().setNumOfBlocksPendingReplication(blocks);
  }

  @Override
  public long getNumOfBlocksPendingReplication() {
    return this.translator.getProtoOrBuilder()
        .getNumOfBlocksPendingReplication();
  }

  @Override
  public void setNumOfBlocksUnderReplicated(long blocks) {
    this.translator.getBuilder().setNumOfBlocksUnderReplicated(blocks);
  }

  @Override
  public long getNumOfBlocksUnderReplicated() {
    return this.translator.getProtoOrBuilder().getNumOfBlocksUnderReplicated();
  }

  @Override
  public void setNumOfBlocksPendingDeletion(long blocks) {
    this.translator.getBuilder().setNumOfBlocksPendingDeletion(blocks);
  }

  @Override
  public long getNumOfBlocksPendingDeletion() {
    return this.translator.getProtoOrBuilder().getNumOfBlocksPendingDeletion();
  }

  @Override
  public void setNumOfActiveDatanodes(int nodes) {
    this.translator.getBuilder().setNumOfActiveDatanodes(nodes);
  }

  @Override
  public int getNumOfActiveDatanodes() {
    return this.translator.getProtoOrBuilder().getNumOfActiveDatanodes();
  }

  @Override
  public void setNumOfDeadDatanodes(int nodes) {
    this.translator.getBuilder().setNumOfDeadDatanodes(nodes);
  }

  @Override
  public int getNumOfDeadDatanodes() {
    return this.translator.getProtoOrBuilder().getNumOfDeadDatanodes();
  }

  @Override
  public void setNumOfStaleDatanodes(int nodes) {
    this.translator.getBuilder().setNumOfStaleDatanodes(nodes);
  }

  @Override
  public int getNumOfStaleDatanodes() {
    return this.translator.getProtoOrBuilder().getNumOfStaleDatanodes();
  }

  @Override
  public void setNumOfDecommissioningDatanodes(int nodes) {
    this.translator.getBuilder().setNumOfDecommissioningDatanodes(nodes);
  }

  @Override
  public int getNumOfDecommissioningDatanodes() {
    return this.translator.getProtoOrBuilder()
        .getNumOfDecommissioningDatanodes();
  }

  @Override
  public void setNumOfDecomActiveDatanodes(int nodes) {
    this.translator.getBuilder().setNumOfDecomActiveDatanodes(nodes);
  }

  @Override
  public int getNumOfDecomActiveDatanodes() {
    return this.translator.getProtoOrBuilder().getNumOfDecomActiveDatanodes();
  }

  @Override
  public void setNumOfDecomDeadDatanodes(int nodes) {
    this.translator.getBuilder().setNumOfDecomDeadDatanodes(nodes);
  }

  @Override
  public int getNumOfDecomDeadDatanodes() {
    return this.translator.getProtoOrBuilder().getNumOfDecomDeadDatanodes();
  }

  @Override
  public void setNumOfInMaintenanceLiveDataNodes(int nodes) {
    this.translator.getBuilder().setNumOfInMaintenanceLiveDataNodes(nodes);
  }

  @Override
  public int getNumOfInMaintenanceLiveDataNodes() {
    return this.translator.getProtoOrBuilder()
        .getNumOfInMaintenanceLiveDataNodes();
  }

  @Override
  public void setNumOfInMaintenanceDeadDataNodes(int nodes) {
    this.translator.getBuilder().setNumOfInMaintenanceDeadDataNodes(nodes);
  }

  @Override
  public int getNumOfInMaintenanceDeadDataNodes() {
    return this.translator.getProtoOrBuilder()
        .getNumOfInMaintenanceDeadDataNodes();
  }

  @Override
  public void setNumOfEnteringMaintenanceDataNodes(int nodes) {
    this.translator.getBuilder().setNumOfEnteringMaintenanceDataNodes(nodes);
  }

  @Override
  public int getNumOfEnteringMaintenanceDataNodes() {
    return this.translator.getProtoOrBuilder()
        .getNumOfEnteringMaintenanceDataNodes();
  }

  @Override
  public void setCorruptFilesCount(int num) {
    this.translator.getBuilder().setCorruptFilesCount(num);
  }

  @Override
  public int getCorruptFilesCount() {
    return this.translator.getProtoOrBuilder().getCorruptFilesCount();
  }

  @Override
  public void setScheduledReplicationBlocks(long blocks) {
    this.translator.getBuilder().setScheduledReplicationBlocks(blocks);
  }

  @Override
  public long getScheduledReplicationBlocks() {
    return this.translator.getProtoOrBuilder().getScheduledReplicationBlocks();
  }

  @Override
  public void setNumberOfMissingBlocksWithReplicationFactorOne(long blocks) {
    this.translator.getBuilder()
        .setNumberOfMissingBlocksWithReplicationFactorOne(blocks);
  }

  @Override
  public long getNumberOfMissingBlocksWithReplicationFactorOne() {
    return this.translator.getProtoOrBuilder()
        .getNumberOfMissingBlocksWithReplicationFactorOne();
  }

  @Override
  public void setNumberOfBadlyDistributedBlocks(long blocks) {
    this.translator.getBuilder()
        .setBadlyDistributedBlocks(blocks);
  }

  @Override
  public long getNumberOfBadlyDistributedBlocks() {
    return this.translator.getProtoOrBuilder()
        .getBadlyDistributedBlocks();
  }

  @Override
  public void setHighestPriorityLowRedundancyReplicatedBlocks(long blocks) {
    this.translator.getBuilder()
        .setHighestPriorityLowRedundancyReplicatedBlocks(blocks);
  }

  @Override
  public long getHighestPriorityLowRedundancyReplicatedBlocks() {
    return this.translator.getProtoOrBuilder()
        .getHighestPriorityLowRedundancyReplicatedBlocks();
  }

  @Override
  public void setHighestPriorityLowRedundancyECBlocks(long blocks) {
    this.translator.getBuilder()
        .setHighestPriorityLowRedundancyECBlocks(blocks);
  }

  @Override
  public long getHighestPriorityLowRedundancyECBlocks() {
    return this.translator.getProtoOrBuilder()
        .getHighestPriorityLowRedundancyECBlocks();
  }

  @Override
  public void setPendingSPSPaths(int pendingSPSPaths) {
    this.translator.getBuilder().setPendingSPSPaths(pendingSPSPaths);
  }

  @Override
  public int getPendingSPSPaths() {
    return this.translator.getProtoOrBuilder().getPendingSPSPaths();
  }
}
