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
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerInfo;
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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

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
  private static DatanodeDetails createDatanodeDetails(UUID uuid) {
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
        nodeManager.register(randomDatanodeDetails(), null));
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
   * Generates random container reports
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
   List<ContainerInfo> containerInfos = new ArrayList<>();
    for (int i = 0; i < numberOfContainers; i++) {
      containerInfos.add(getRandomContainerInfo(i));
    }
    return getContainerReports(containerInfos);
  }

  /**
   * Creates container report with the given ContainerInfo(s).
   *
   * @param containerInfos one or more ContainerInfo
   *
   * @return ContainerReportsProto
   */
  public static ContainerReportsProto getContainerReports(
      ContainerInfo... containerInfos) {
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
      List<ContainerInfo> containerInfos) {
    ContainerReportsProto.Builder
        reportsBuilder = ContainerReportsProto.newBuilder();
    for (ContainerInfo containerInfo : containerInfos) {
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
  public static ContainerInfo getRandomContainerInfo(long containerId) {
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
  public static ContainerInfo createContainerInfo(
      long containerId, long size, long keyCount, long bytesUsed,
      long readCount, long readBytes, long writeCount, long writeBytes) {
    return ContainerInfo.newBuilder()
        .setContainerID(containerId)
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


}
