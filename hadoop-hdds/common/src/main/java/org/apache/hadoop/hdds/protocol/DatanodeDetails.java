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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * DatanodeDetails class contains details about DataNode like:
 * - UUID of the DataNode.
 * - IP and Hostname details.
 * - Port details to which the DataNode will be listening.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeDetails implements Comparable<DatanodeDetails> {

  /**
   * DataNode's unique identifier in the cluster.
   */
  private final UUID uuid;

  private String ipAddress;
  private String hostName;
  private List<Port> ports;
  private String certSerialId;


  /**
   * Constructs DatanodeDetails instance. DatanodeDetails.Builder is used
   * for instantiating DatanodeDetails.
   * @param uuid DataNode's UUID
   * @param ipAddress IP Address of this DataNode
   * @param hostName DataNode's hostname
   * @param ports Ports used by the DataNode
   * @param certSerialId serial id from SCM issued certificate.
   */
  private DatanodeDetails(String uuid, String ipAddress, String hostName,
      List<Port> ports, String certSerialId) {
    this.uuid = UUID.fromString(uuid);
    this.ipAddress = ipAddress;
    this.hostName = hostName;
    this.ports = ports;
    this.certSerialId = certSerialId;
  }

  protected DatanodeDetails(DatanodeDetails datanodeDetails) {
    this.uuid = datanodeDetails.uuid;
    this.ipAddress = datanodeDetails.ipAddress;
    this.hostName = datanodeDetails.hostName;
    this.ports = datanodeDetails.ports;
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
   * Sets a DataNode Port.
   *
   * @param port DataNode port
   */
  public void setPort(Port port) {
    // If the port is already in the list remove it first and add the
    // new/updated port value.
    ports.remove(port);
    ports.add(port);
  }

  /**
   * Returns all the Ports used by DataNode.
   *
   * @return DataNode Ports
   */
  public List<Port> getPorts() {
    return ports;
  }

  /**
   * Given the name returns port number, null if the asked port is not found.
   *
   * @param name Name of the port
   *
   * @return Port
   */
  public Port getPort(Port.Name name) {
    for (Port port : ports) {
      if (port.getName().equals(name)) {
        return port;
      }
    }
    return null;
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
    if (datanodeDetailsProto.hasCertSerialId()) {
      builder.setCertSerialId(datanodeDetailsProto.getCertSerialId());
    }
    for (HddsProtos.Port port : datanodeDetailsProto.getPortsList()) {
      builder.addPort(newPort(
          Port.Name.valueOf(port.getName().toUpperCase()), port.getValue()));
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
    if (certSerialId != null) {
      builder.setCertSerialId(certSerialId);
    }
    for (Port port : ports) {
      builder.addPorts(HddsProtos.Port.newBuilder()
          .setName(port.getName().toString())
          .setValue(port.getValue())
          .build());
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
        ", certSerialId: " + certSerialId +
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
  public static final class Builder {
    private String id;
    private String ipAddress;
    private String hostName;
    private List<Port> ports;
    private String certSerialId;

    /**
     * Default private constructor. To create Builder instance use
     * DatanodeDetails#newBuilder.
     */
    private Builder() {
      ports = new ArrayList<>();
    }

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
     * Adds a DataNode Port.
     *
     * @param port DataNode port
     *
     * @return DatanodeDetails.Builder
     */
    public Builder addPort(Port port) {
      this.ports.add(port);
      return this;
    }

    /**
     * Adds certificate serial id.
     *
     * @param certId Serial id of SCM issued certificate.
     *
     * @return DatanodeDetails.Builder
     */
    public Builder setCertSerialId(String certId) {
      this.certSerialId = certId;
      return this;
    }

    /**
     * Builds and returns DatanodeDetails instance.
     *
     * @return DatanodeDetails
     */
    public DatanodeDetails build() {
      Preconditions.checkNotNull(id);
      return new DatanodeDetails(id, ipAddress, hostName, ports, certSerialId);
    }

  }

  /**
   * Constructs a new Port with name and value.
   *
   * @param name Name of the port
   * @param value Port number
   *
   * @return {@code Port} instance
   */
  public static Port newPort(Port.Name name, Integer value) {
    return new Port(name, value);
  }

  /**
   * Container to hold DataNode Port details.
   */
  public static final class Port {

    /**
     * Ports that are supported in DataNode.
     */
    public enum Name {
      STANDALONE, RATIS, REST
    }

    private Name name;
    private Integer value;

    /**
     * Private constructor for constructing Port object. Use
     * DatanodeDetails#newPort to create a new Port object.
     *
     * @param name
     * @param value
     */
    private Port(Name name, Integer value) {
      this.name = name;
      this.value = value;
    }

    /**
     * Returns the name of the port.
     *
     * @return Port name
     */
    public Name getName() {
      return name;
    }

    /**
     * Returns the port number.
     *
     * @return Port number
     */
    public Integer getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }

    /**
     * Ports are considered equal if they have the same name.
     *
     * @param anObject
     *          The object to compare this {@code Port} against
     * @return {@code true} if the given object represents a {@code Port}
               and has the same name, {@code false} otherwise
     */
    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof Port) {
        return name.equals(((Port) anObject).name);
      }
      return false;
    }
  }

  /**
   * Returns serial id of SCM issued certificate.
   *
   * @return certificate serial id
   */
  public String getCertSerialId() {
    return certSerialId;
  }

  /**
   * Set certificate serial id of SCM issued certificate.
   *
   */
  public void setCertSerialId(String certSerialId) {
    this.certSerialId = certSerialId;
  }

}
