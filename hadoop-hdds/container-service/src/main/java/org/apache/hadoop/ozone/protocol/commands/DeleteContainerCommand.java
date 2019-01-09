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
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeleteContainerCommandProto;

/**
 * SCM command which tells the datanode to delete a container.
 */
public class DeleteContainerCommand extends
    SCMCommand<DeleteContainerCommandProto> {

  private final long containerId;

  public DeleteContainerCommand(long containerId) {
    this.containerId = containerId;
  }

  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.deleteContainerCommand;
  }

  @Override
  public DeleteContainerCommandProto getProto() {
    DeleteContainerCommandProto.Builder builder =
        DeleteContainerCommandProto.newBuilder();
    builder.setCmdId(getId())
        .setContainerID(getContainerID());
    return builder.build();
  }

  public long getContainerID() {
    return containerId;
  }

  public static DeleteContainerCommand getFromProtobuf(
      DeleteContainerCommandProto protoMessage) {
    Preconditions.checkNotNull(protoMessage);
    return new DeleteContainerCommand(protoMessage.getContainerID());
  }
}
