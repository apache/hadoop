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
package org.apache.hadoop.ozone.protocol.commands;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdsl.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMRegisteredCmdResponseProto;
import org.apache.hadoop.hdsl.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMRegisteredCmdResponseProto
    .ErrorCode;
import org.apache.hadoop.hdsl.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCmdType;

/**
 * Response to Datanode Register call.
 */
public class RegisteredCommand extends
    SCMCommand<SCMRegisteredCmdResponseProto> {
  private String datanodeUUID;
  private String clusterID;
  private ErrorCode error;

  public RegisteredCommand(final ErrorCode error, final String datanodeUUID,
                           final String clusterID) {
    this.datanodeUUID = datanodeUUID;
    this.clusterID = clusterID;
    this.error = error;
  }

  /**
   * Returns a new builder.
   *
   * @return - Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Returns the type of this command.
   *
   * @return Type
   */
  @Override
  public SCMCmdType getType() {
    return SCMCmdType.registeredCommand;
  }

  /**
   * Returns datanode UUID.
   *
   * @return - Datanode ID.
   */
  public String getDatanodeUUID() {
    return datanodeUUID;
  }

  /**
   * Returns cluster ID.
   *
   * @return -- ClusterID
   */
  public String getClusterID() {
    return clusterID;
  }

  /**
   * Returns ErrorCode.
   *
   * @return - ErrorCode
   */
  public ErrorCode getError() {
    return error;
  }

  /**
   * Gets the protobuf message of this object.
   *
   * @return A protobuf message.
   */
  @Override
  public byte[] getProtoBufMessage() {
    return SCMRegisteredCmdResponseProto.newBuilder()
        .setClusterID(this.clusterID)
        .setDatanodeUUID(this.datanodeUUID)
        .setErrorCode(this.error)
        .build().toByteArray();
  }

  /**
   * A builder class to verify all values are sane.
   */
  public static class Builder {
    private String datanodeUUID;
    private String clusterID;
    private ErrorCode error;

    /**
     * sets UUID.
     *
     * @param dnUUID - datanode UUID
     * @return Builder
     */
    public Builder setDatanodeUUID(String dnUUID) {
      this.datanodeUUID = dnUUID;
      return this;
    }

    /**
     * Create this object from a Protobuf message.
     *
     * @param response - RegisteredCmdResponseProto
     * @return RegisteredCommand
     */
    public  RegisteredCommand getFromProtobuf(SCMRegisteredCmdResponseProto
                                                        response) {
      Preconditions.checkNotNull(response);
      return new RegisteredCommand(response.getErrorCode(),
          response.hasDatanodeUUID() ? response.getDatanodeUUID(): "",
          response.hasClusterID() ? response.getClusterID(): "");
    }

    /**
     * Sets cluster ID.
     *
     * @param cluster - clusterID
     * @return Builder
     */
    public Builder setClusterID(String cluster) {
      this.clusterID = cluster;
      return this;
    }

    /**
     * Sets Error code.
     *
     * @param errorCode - error code
     * @return Builder
     */
    public Builder setErrorCode(ErrorCode errorCode) {
      this.error = errorCode;
      return this;
    }

    /**
     * Build the command object.
     *
     * @return RegisteredCommand
     */
    public RegisteredCommand build() {
      if ((this.error == ErrorCode.success) &&
          (this.datanodeUUID == null || this.datanodeUUID.isEmpty()) ||
          (this.clusterID == null || this.clusterID.isEmpty())) {
        throw new IllegalArgumentException("On success, RegisteredCommand " +
            "needs datanodeUUID and ClusterID.");
      }

      return new
          RegisteredCommand(this.error, this.datanodeUUID, this.clusterID);
    }
  }
}
