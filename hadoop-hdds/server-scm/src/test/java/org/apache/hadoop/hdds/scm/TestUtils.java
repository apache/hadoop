/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ClosePipelineInfo;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineActionsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto
        .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server
    .SCMDatanodeHeartbeatDispatcher.PipelineActionsFromDatanode;
import org.apache.hadoop.hdds.scm.server
    .SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol
    .proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol
    .proto.StorageContainerDatanodeProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol
    .proto.StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto
        .StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.StorageTypeProto;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.security.authentication.client
    .AuthenticationException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED;

/**
 * Stateless helper functions to handler scm/datanode connection.
 */
public final class TestUtils {

  private static ThreadLocalRandom random = ThreadLocalRandom.current();

  private TestUtils() {
  }

  /**
   * Creates DatanodeDetails with random UUID.
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails randomDatanodeDetails() {
    return createDatanodeDetails(UUID.randomUUID());
  }

  /**
   * Creates DatanodeDetails using the given UUID.
   *
   * @param uuid Datanode's UUID
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails createDatanodeDetails(UUID uuid) {
    String ipAddress = random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256);
    return createDatanodeDetails(uuid.toString(), "localhost", ipAddress);
  }

  /**
   * Generates DatanodeDetails from RegisteredCommand.
   *
   * @param registeredCommand registration response from SCM
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails getDatanodeDetails(
      RegisteredCommand registeredCommand) {
    return createDatanodeDetails(registeredCommand.getDatanodeUUID(),
        registeredCommand.getHostName(), registeredCommand.getIpAddress());
  }

  /**
   * Creates DatanodeDetails with the given information.
   *
   * @param uuid      Datanode's UUID
   * @param hostname  hostname of Datanode
   * @param ipAddress ip address of Datanode
   *
   * @return DatanodeDetails
   */
  private static DatanodeDetails createDatanodeDetails(String uuid,
      String hostname, String ipAddress) {
    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, 0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(uuid)
        .setHostName(hostname)
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }

  /**
   * Creates a random DatanodeDetails and register it with the given
   * NodeManager.
   *
   * @param nodeManager NodeManager
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails createRandomDatanodeAndRegister(
      SCMNodeManager nodeManager) {
    return getDatanodeDetails(
        nodeManager.register(randomDatanodeDetails(), null,
                getRandomPipelineReports()));
  }

  /**
   * Get specified number of DatanodeDetails and register them with node
   * manager.
   *
   * @param nodeManager node manager to register the datanode ids.
   * @param count       number of DatanodeDetails needed.
   *
   * @return list of DatanodeDetails
   */
  public static List<DatanodeDetails> getListOfRegisteredDatanodeDetails(
      SCMNodeManager nodeManager, int count) {
    ArrayList<DatanodeDetails> datanodes = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      datanodes.add(createRandomDatanodeAndRegister(nodeManager));
    }
    return datanodes;
  }

  /**
   * Generates a random NodeReport.
   *
   * @return NodeReportProto
   */
  public static NodeReportProto getRandomNodeReport() {
    return getRandomNodeReport(1);
  }

  /**
   * Generates random NodeReport with the given number of storage report in it.
   *
   * @param numberOfStorageReport number of storage report this node report
   *                              should have
   * @return NodeReportProto
   */
  public static NodeReportProto getRandomNodeReport(int numberOfStorageReport) {
    UUID nodeId = UUID.randomUUID();
    return getRandomNodeReport(nodeId, File.separator + nodeId,
        numberOfStorageReport);
  }

  /**
   * Generates random NodeReport for the given nodeId with the given
   * base path and number of storage report in it.
   *
   * @param nodeId                datanode id
   * @param basePath              base path of storage directory
   * @param numberOfStorageReport number of storage report
   *
   * @return NodeReportProto
   */
  public static NodeReportProto getRandomNodeReport(UUID nodeId,
      String basePath, int numberOfStorageReport) {
    List<StorageReportProto> storageReports = new ArrayList<>();
    for (int i = 0; i < numberOfStorageReport; i++) {
      storageReports.add(getRandomStorageReport(nodeId,
          basePath + File.separator + i));
    }
    return createNodeReport(storageReports);
  }

  /**
   * Creates NodeReport with the given storage reports.
   *
   * @param reports one or more storage report
   *
   * @return NodeReportProto
   */
  public static NodeReportProto createNodeReport(
      StorageReportProto... reports) {
    return createNodeReport(Arrays.asList(reports));
  }

  /**
   * Creates NodeReport with the given storage reports.
   *
   * @param reports storage reports to be included in the node report.
   *
   * @return NodeReportProto
   */
  public static NodeReportProto createNodeReport(
      List<StorageReportProto> reports) {
    NodeReportProto.Builder nodeReport = NodeReportProto.newBuilder();
    nodeReport.addAllStorageReport(reports);
    return nodeReport.build();
  }

  /**
   * Generates random storage report.
   *
   * @param nodeId datanode id for which the storage report belongs to
   * @param path   path of the storage
   *
   * @return StorageReportProto
   */
  public static StorageReportProto getRandomStorageReport(UUID nodeId,
      String path) {
    return createStorageReport(nodeId, path,
        random.nextInt(1000),
        random.nextInt(500),
        random.nextInt(500),
        StorageTypeProto.DISK);
  }

  /**
   * Creates storage report with the given information.
   *
   * @param nodeId    datanode id
   * @param path      storage dir
   * @param capacity  storage size
   * @param used      space used
   * @param remaining space remaining
   * @param type      type of storage
   *
   * @return StorageReportProto
   */
  public static StorageReportProto createStorageReport(UUID nodeId, String path,
      long capacity, long used, long remaining, StorageTypeProto type) {
    Preconditions.checkNotNull(nodeId);
    Preconditions.checkNotNull(path);
    StorageReportProto.Builder srb = StorageReportProto.newBuilder();
    srb.setStorageUuid(nodeId.toString())
        .setStorageLocation(path)
        .setCapacity(capacity)
        .setScmUsed(used)
        .setRemaining(remaining);
    StorageTypeProto storageTypeProto =
        type == null ? StorageTypeProto.DISK : type;
    srb.setStorageType(storageTypeProto);
    return srb.build();
  }


  /**
   * Generates random container reports.
   *
   * @return ContainerReportsProto
   */
  public static ContainerReportsProto getRandomContainerReports() {
    return getRandomContainerReports(1);
  }

  /**
   * Generates random container report with the given number of containers.
   *
   * @param numberOfContainers number of containers to be in container report
   *
   * @return ContainerReportsProto
   */
  public static ContainerReportsProto getRandomContainerReports(
      int numberOfContainers) {
    List<ContainerReplicaProto> containerInfos = new ArrayList<>();
    for (int i = 0; i < numberOfContainers; i++) {
      containerInfos.add(getRandomContainerInfo(i));
    }
    return getContainerReports(containerInfos);
  }


  public static PipelineReportsProto getRandomPipelineReports() {
    return PipelineReportsProto.newBuilder().build();
  }

  public static PipelineReportFromDatanode getPipelineReportFromDatanode(
      DatanodeDetails dn, PipelineID... pipelineIDs) {
    PipelineReportsProto.Builder reportBuilder =
        PipelineReportsProto.newBuilder();
    for (PipelineID pipelineID : pipelineIDs) {
      reportBuilder.addPipelineReport(
          PipelineReport.newBuilder().setPipelineID(pipelineID.getProtobuf()));
    }
    return new PipelineReportFromDatanode(dn, reportBuilder.build());
  }

  public static PipelineActionsFromDatanode getPipelineActionFromDatanode(
      DatanodeDetails dn, PipelineID... pipelineIDs) {
    PipelineActionsProto.Builder actionsProtoBuilder =
        PipelineActionsProto.newBuilder();
    for (PipelineID pipelineID : pipelineIDs) {
      ClosePipelineInfo closePipelineInfo =
          ClosePipelineInfo.newBuilder().setPipelineID(pipelineID.getProtobuf())
              .setReason(ClosePipelineInfo.Reason.PIPELINE_FAILED)
              .setDetailedReason("").build();
      actionsProtoBuilder.addPipelineActions(PipelineAction.newBuilder()
          .setClosePipeline(closePipelineInfo)
          .setAction(PipelineAction.Action.CLOSE)
          .build());
    }
    return new PipelineActionsFromDatanode(dn, actionsProtoBuilder.build());
  }

  /**
   * Creates container report with the given ContainerInfo(s).
   *
   * @param containerInfos one or more ContainerInfo
   *
   * @return ContainerReportsProto
   */
  public static ContainerReportsProto getContainerReports(
      ContainerReplicaProto... containerInfos) {
    return getContainerReports(Arrays.asList(containerInfos));
  }

  /**
   * Creates container report with the given ContainerInfo(s).
   *
   * @param containerInfos list of ContainerInfo
   *
   * @return ContainerReportsProto
   */
  public static ContainerReportsProto getContainerReports(
      List<ContainerReplicaProto> containerInfos) {
    ContainerReportsProto.Builder
        reportsBuilder = ContainerReportsProto.newBuilder();
    for (ContainerReplicaProto containerInfo : containerInfos) {
      reportsBuilder.addReports(containerInfo);
    }
    return reportsBuilder.build();
  }

  /**
   * Generates random ContainerInfo.
   *
   * @param containerId container id of the ContainerInfo
   *
   * @return ContainerInfo
   */
  public static ContainerReplicaProto getRandomContainerInfo(
      long containerId) {
    return createContainerInfo(containerId,
        OzoneConsts.GB * 5,
        random.nextLong(1000),
        OzoneConsts.GB * random.nextInt(5),
        random.nextLong(1000),
        OzoneConsts.GB * random.nextInt(2),
        random.nextLong(1000),
        OzoneConsts.GB * random.nextInt(5));
  }

  /**
   * Creates ContainerInfo with the given details.
   *
   * @param containerId id of the container
   * @param size        size of container
   * @param keyCount    number of keys
   * @param bytesUsed   bytes used by the container
   * @param readCount   number of reads
   * @param readBytes   bytes read
   * @param writeCount  number of writes
   * @param writeBytes  bytes written
   *
   * @return ContainerInfo
   */
  @SuppressWarnings("parameternumber")
  public static ContainerReplicaProto createContainerInfo(
      long containerId, long size, long keyCount, long bytesUsed,
      long readCount, long readBytes, long writeCount, long writeBytes) {
    return ContainerReplicaProto.newBuilder()
        .setContainerID(containerId)
        .setState(ContainerReplicaProto.State.OPEN)
        .setSize(size)
        .setKeyCount(keyCount)
        .setUsed(bytesUsed)
        .setReadCount(readCount)
        .setReadBytes(readBytes)
        .setWriteCount(writeCount)
        .setWriteBytes(writeBytes)
        .build();
  }

  /**
   * Create Command Status report object.
   * @return CommandStatusReportsProto
   */
  public static CommandStatusReportsProto createCommandStatusReport(
      List<CommandStatus> reports) {
    CommandStatusReportsProto.Builder report = CommandStatusReportsProto
        .newBuilder();
    report.addAllCmdStatus(reports);
    return report.build();
  }

  public static org.apache.hadoop.hdds.scm.container.ContainerInfo
      allocateContainer(ContainerManager containerManager)
      throws IOException {
    return containerManager
        .allocateContainer(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, "root");

  }

  public static void closeContainer(ContainerManager containerManager,
      ContainerID id) throws IOException {
    containerManager.updateContainerState(
        id, HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager.updateContainerState(
        id, HddsProtos.LifeCycleEvent.CLOSE);

  }

  /**
   * Move the container to Quaise close state.
   * @param containerManager
   * @param id
   * @throws IOException
   */
  public static void quasiCloseContainer(ContainerManager containerManager,
      ContainerID id) throws IOException {
    containerManager.updateContainerState(
        id, HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager.updateContainerState(
        id, HddsProtos.LifeCycleEvent.QUASI_CLOSE);

  }

  /**
   * Construct and returns StorageContainerManager instance using the given
   * configuration. The ports used by this StorageContainerManager are
   * randomly selected from free ports available.
   *
   * @param conf OzoneConfiguration
   * @return StorageContainerManager instance
   * @throws IOException
   * @throws AuthenticationException
   */
  public static StorageContainerManager getScm(OzoneConfiguration conf)
      throws IOException, AuthenticationException {
    return getScm(conf, new SCMConfigurator());
  }

  /**
   * Construct and returns StorageContainerManager instance using the given
   * configuration and the configurator. The ports used by this
   * StorageContainerManager are randomly selected from free ports available.
   *
   * @param conf OzoneConfiguration
   * @param configurator SCMConfigurator
   * @return StorageContainerManager instance
   * @throws IOException
   * @throws AuthenticationException
   */
  public static StorageContainerManager getScm(OzoneConfiguration conf,
                                               SCMConfigurator configurator)
      throws IOException, AuthenticationException {
    conf.setBoolean(OZONE_ENABLED, true);
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, "127.0.0.1:0");
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

  public static ContainerInfo getContainer(
      final HddsProtos.LifeCycleState state) {
    return new ContainerInfo.Builder()
        .setContainerID(RandomUtils.nextLong())
        .setReplicationType(HddsProtos.ReplicationType.RATIS)
        .setReplicationFactor(HddsProtos.ReplicationFactor.THREE)
        .setState(state)
        .setSequenceId(10000L)
        .setOwner("TEST")
        .build();
  }

  public static Set<ContainerReplica> getReplicas(
      final ContainerID containerId,
      final ContainerReplicaProto.State state,
      final DatanodeDetails... datanodeDetails) {
    return getReplicas(containerId, state, 10000L, datanodeDetails);
  }

  public static Set<ContainerReplica> getReplicas(
      final ContainerID containerId,
      final ContainerReplicaProto.State state,
      final long sequenceId,
      final DatanodeDetails... datanodeDetails) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (DatanodeDetails datanode : datanodeDetails) {
      replicas.add(getReplicas(containerId, state,
          sequenceId, datanode.getUuid(), datanode));
    }
    return replicas;
  }

  public static ContainerReplica getReplicas(
      final ContainerID containerId,
      final ContainerReplicaProto.State state,
      final long sequenceId,
      final UUID originNodeId,
      final DatanodeDetails datanodeDetails) {
    return ContainerReplica.newBuilder()
        .setContainerID(containerId)
        .setContainerState(state)
        .setDatanodeDetails(datanodeDetails)
        .setOriginNodeId(originNodeId)
        .setSequenceId(sequenceId)
        .build();
  }

}
