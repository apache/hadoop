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
    nodeManager.register(datanodeDetails.getProtoBufMessage());
    return datanodeDetails;
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
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(uuid)
        .setHostName("localhost")
        .setIpAddress(ipAddress)
        .setContainerPort(0)
        .setRatisPort(0)
        .setOzoneRestPort(0);
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
