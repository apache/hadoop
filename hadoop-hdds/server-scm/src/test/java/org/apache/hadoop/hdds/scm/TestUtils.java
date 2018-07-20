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
    .StorageContainerDatanodeProtocolProtos;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Stateless helper functions to handler scm/datanode connection.
 */
public final class TestUtils {

  private TestUtils() {
  }

  public static DatanodeDetails getDatanodeDetails(SCMNodeManager nodeManager) {

    return getDatanodeDetails(nodeManager, UUID.randomUUID().toString());
  }

  /**
   * Create a new DatanodeDetails with NodeID set to the string.
   *
   * @param uuid - node ID, it is generally UUID.
   * @return DatanodeID.
   */
  public static DatanodeDetails getDatanodeDetails(SCMNodeManager nodeManager,
      String uuid) {
    DatanodeDetails datanodeDetails = getDatanodeDetails(uuid);
    nodeManager.register(datanodeDetails, null);
    return datanodeDetails;
  }

  /**
   * Create Node Report object.
   * @return NodeReportProto
   */
  public static NodeReportProto createNodeReport(
      List<StorageReportProto> reports) {
    NodeReportProto.Builder nodeReport = NodeReportProto.newBuilder();
    nodeReport.addAllStorageReport(reports);
    return nodeReport.build();
  }

  /**
   * Create SCM Storage Report object.
   * @return list of SCMStorageReport
   */
  public static List<StorageReportProto> createStorageReport(long capacity,
      long used, long remaining, String path, StorageTypeProto type, String id,
      int count) {
    List<StorageReportProto> reportList = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Preconditions.checkNotNull(path);
      Preconditions.checkNotNull(id);
      StorageReportProto.Builder srb = StorageReportProto.newBuilder();
      srb.setStorageUuid(id).setStorageLocation(path).setCapacity(capacity)
          .setScmUsed(used).setRemaining(remaining);
      StorageTypeProto storageTypeProto =
          type == null ? StorageTypeProto.DISK : type;
      srb.setStorageType(storageTypeProto);
      reportList.add(srb.build());
    }
    return reportList;
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


  /**
   * Get specified number of DatanodeDetails and registered them with node
   * manager.
   *
   * @param nodeManager - node manager to register the datanode ids.
   * @param count       - number of DatanodeDetails needed.
   * @return
   */
  public static List<DatanodeDetails> getListOfRegisteredDatanodeDetails(
      SCMNodeManager nodeManager, int count) {
    ArrayList<DatanodeDetails> datanodes = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      datanodes.add(getDatanodeDetails(nodeManager));
    }
    return datanodes;
  }

  /**
   * Get a datanode details.
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails getDatanodeDetails() {
    return getDatanodeDetails(UUID.randomUUID().toString());
  }

  private static DatanodeDetails getDatanodeDetails(String uuid) {
    Random random = new Random();
    String ipAddress =
        random.nextInt(256) + "." + random.nextInt(256) + "." + random
            .nextInt(256) + "." + random.nextInt(256);

    String hostName = uuid;
    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, 0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(uuid)
        .setHostName("localhost")
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }

  /**
   * Get specified number of list of DatanodeDetails.
   *
   * @param count - number of datanode IDs needed.
   * @return
   */
  public static List<DatanodeDetails> getListOfDatanodeDetails(int count) {
    ArrayList<DatanodeDetails> datanodes = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      datanodes.add(getDatanodeDetails());
    }
    return datanodes;
  }
}
