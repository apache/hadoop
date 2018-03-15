/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.protocol.commands;

import org.apache.hadoop.hdsl.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SendContainerReportProto;
import org.apache.hadoop.hdsl.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCmdType;

/**
 * Allows a Datanode to send in the container report.
 */
public class SendContainerCommand extends SCMCommand<SendContainerReportProto> {
  /**
   * Returns a NullCommand class from NullCommandResponse Proto.
   * @param unused  - unused
   * @return NullCommand
   */
  public static SendContainerCommand getFromProtobuf(
      final SendContainerReportProto unused) {
    return new SendContainerCommand();
  }

  /**
   * returns a new builder.
   * @return Builder
   */
  public static SendContainerCommand.Builder newBuilder() {
    return new SendContainerCommand.Builder();
  }

  /**
   * Returns the type of this command.
   *
   * @return Type
   */
  @Override
  public SCMCmdType getType() {
    return SCMCmdType.sendContainerReport;
  }

  /**
   * Gets the protobuf message of this object.
   *
   * @return A protobuf message.
   */
  @Override
  public byte[] getProtoBufMessage() {
    return SendContainerReportProto.newBuilder().build().toByteArray();
  }

  /**
   * A Builder class this is the standard pattern we are using for all commands.
   */
  public static class Builder {
    /**
     * Return a null command.
     * @return - NullCommand.
     */
    public SendContainerCommand build() {
      return new SendContainerCommand();
    }
  }
}
