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

package org.apache.hadoop.hdds.protocol;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.util.UUID;

/**
 * DatanodeDetails class contains details about DataNode like:
 * - UUID of the DataNode.
 * - IP and Hostname details.
 * - Port details to which the DataNode will be listening.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class DatanodeDetails implements Comparable<DatanodeDetails> {

  /**
   * DataNode's unique identifier in the cluster.
   */
  private final UUID uuid;

  private String ipAddress;
  private String hostName;
  private Integer containerPort;
  private Integer ratisPort;
  private Integer ozoneRestPort;


  /**
   * Constructs DatanodeDetails instance. DatanodeDetails.Builder is used
   * for instantiating DatanodeDetails.
   * @param uuid DataNode's UUID
   * @param ipAddress IP Address of this DataNode
   * @param hostName DataNode's hostname
   * @param containerPort Container Port
   * @param ratisPort Ratis Port
   * @param ozoneRestPort Rest Port
   */
  private DatanodeDetails(String uuid, String ipAddress, String hostName,
      Integer containerPort, Integer ratisPort, Integer ozoneRestPort) {
    this.uuid = UUID.fromString(uuid);
    this.ipAddress = ipAddress;
    this.hostName = hostName;
    this.containerPort = containerPort;
    this.ratisPort = ratisPort;
    this.ozoneRestPort = ozoneRestPort;
  }

  /**
   * Returns the DataNode UUID.
   *
   * @return UUID of DataNode
   */
  public UUID getUuid() {
    return uuid;
  }

  /**
   * Returns the string representation of DataNode UUID.
   *
   * @return UUID of DataNode
   */
  public String getUuidString() {
    return uuid.toString();
  }

  /**
   * Sets the IP address of Datanode.
   *
   * @param ip IP Address
   */
  public void setIpAddress(String ip) {
    this.ipAddress = ip;
  }

  /**
   * Returns IP address of DataNode.
   *
   * @return IP address
   */
  public String getIpAddress() {
    return ipAddress;
  }

  /**
   * Sets the Datanode hostname.
   *
   * @param host hostname
   */
  public void setHostName(String host) {
    this.hostName = host;
  }

  /**
   * Returns Hostname of DataNode.
   *
   * @return Hostname
   */
  public String getHostName() {
    return hostName;
  }

  /**
   * Sets the Container Port.
   * @param port ContainerPort
   */
  public void setContainerPort(int port) {
    containerPort = port;
  }

  /**
   * Returns standalone container Port.
   *
   * @return Container Port
   */
  public int getContainerPort() {
    return containerPort;
  }

  /**
   * Sets Ratis Port.
   * @param port RatisPort
   */
  public void setRatisPort(int port) {
    ratisPort = port;
  }


  /**
   * Returns Ratis Port.
   * @return Ratis Port
   */
  public int getRatisPort() {
    return ratisPort;
  }


  /**
   * Sets OzoneRestPort.
   * @param port OzoneRestPort
   */
  public void setOzoneRestPort(int port) {
    ozoneRestPort = port;
  }

  /**
   * Returns Ozone Rest Port.
   * @return OzoneRestPort
   */
  public int getOzoneRestPort() {
    return ozoneRestPort;
  }

  /**
   * Returns a DatanodeDetails from the protocol buffers.
   *
   * @param datanodeDetailsProto - protoBuf Message
   * @return DatanodeDetails
   */
  public static DatanodeDetails getFromProtoBuf(
      HddsProtos.DatanodeDetailsProto datanodeDetailsProto) {
    DatanodeDetails.Builder builder = newBuilder();
    builder.setUuid(datanodeDetailsProto.getUuid());
    if (datanodeDetailsProto.hasIpAddress()) {
      builder.setIpAddress(datanodeDetailsProto.getIpAddress());
    }
    if (datanodeDetailsProto.hasHostName()) {
      builder.setHostName(datanodeDetailsProto.getHostName());
    }
    if (datanodeDetailsProto.hasContainerPort()) {
      builder.setContainerPort(datanodeDetailsProto.getContainerPort());
    }
    if (datanodeDetailsProto.hasRatisPort()) {
      builder.setRatisPort(datanodeDetailsProto.getRatisPort());
    }
    if (datanodeDetailsProto.hasOzoneRestPort()) {
      builder.setOzoneRestPort(datanodeDetailsProto.getOzoneRestPort());
    }
    return builder.build();
  }

  /**
   * Returns a DatanodeDetails protobuf message from a datanode ID.
   * @return HddsProtos.DatanodeDetailsProto
   */
  public HddsProtos.DatanodeDetailsProto getProtoBufMessage() {
    HddsProtos.DatanodeDetailsProto.Builder builder =
        HddsProtos.DatanodeDetailsProto.newBuilder()
            .setUuid(getUuidString());
    if (ipAddress != null) {
      builder.setIpAddress(ipAddress);
    }
    if (hostName != null) {
      builder.setHostName(hostName);
    }
    if (containerPort != null) {
      builder.setContainerPort(containerPort);
    }
    if (ratisPort != null) {
      builder.setRatisPort(ratisPort);
    }
    if (ozoneRestPort != null) {
      builder.setOzoneRestPort(ozoneRestPort);
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return uuid.toString() + "{" +
        "ip: " +
        ipAddress +
        ", host: " +
        hostName +
        "}";
  }

  @Override
  public int compareTo(DatanodeDetails that) {
    return this.getUuid().compareTo(that.getUuid());
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof DatanodeDetails &&
        uuid.equals(((DatanodeDetails) obj).uuid);
  }

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  /**
   * Returns DatanodeDetails.Builder instance.
   *
   * @return DatanodeDetails.Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for building DatanodeDetails.
   */
  public static class Builder {
    private String id;
    private String ipAddress;
    private String hostName;
    private Integer containerPort;
    private Integer ratisPort;
    private Integer ozoneRestPort;

    /**
     * Sets the DatanodeUuid.
     *
     * @param uuid DatanodeUuid
     * @return DatanodeDetails.Builder
     */
    public Builder setUuid(String uuid) {
      this.id = uuid;
      return this;
    }

    /**
     * Sets the IP address of DataNode.
     *
     * @param ip address
     * @return DatanodeDetails.Builder
     */
    public Builder setIpAddress(String ip) {
      this.ipAddress = ip;
      return this;
    }

    /**
     * Sets the hostname of DataNode.
     *
     * @param host hostname
     * @return DatanodeDetails.Builder
     */
    public Builder setHostName(String host) {
      this.hostName = host;
      return this;
    }
    /**
     * Sets the ContainerPort.
     *
     * @param port ContainerPort
     * @return DatanodeDetails.Builder
     */
    public Builder setContainerPort(Integer port) {
      this.containerPort = port;
      return this;
    }

    /**
     * Sets the RatisPort.
     *
     * @param port RatisPort
     * @return DatanodeDetails.Builder
     */
    public Builder setRatisPort(Integer port) {
      this.ratisPort = port;
      return this;
    }

    /**
     * Sets the OzoneRestPort.
     *
     * @param port OzoneRestPort
     * @return DatanodeDetails.Builder
     */
    public Builder setOzoneRestPort(Integer port) {
      this.ozoneRestPort = port;
      return this;
    }

    /**
     * Builds and returns DatanodeDetails instance.
     *
     * @return DatanodeDetails
     */
    public DatanodeDetails build() {
      Preconditions.checkNotNull(id);
      return new DatanodeDetails(id, ipAddress, hostName, containerPort,
          ratisPort, ozoneRestPort);
    }

  }

}
