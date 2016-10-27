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

import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandResponseProto.Type;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.NullCmdResponseProto;



/**
 * For each command that SCM can return we have a class in this commands
 * directory. This is the Null command, that tells datanode that no action is
 * needed from it.
 */
public class NullCommand extends SCMCommand<NullCmdResponseProto> {
  /**
   * Returns the type of this command.
   *
   * @return Type
   */
  @Override
  public Type getType() {
    return Type.nullCmd;
  }

  /**
   * Gets the protobuf message of this object.
   *
   * @return A protobuf message.
   */
  @Override
  public NullCmdResponseProto getProtoBufMessage() {
    return NullCmdResponseProto.newBuilder().build();
  }

  /**
   * Returns a NullCommand class from NullCommandResponse Proto.
   * @param unused  - unused
   * @return  NullCommand
   */
  public static NullCommand getFromProtobuf(final NullCmdResponseProto
                                                unused) {
    return new NullCommand();
  }

  /**
   * returns a new builder.
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * A Builder class this is the standard pattern we are using for all commands.
   */
  public static class Builder {
    /**
     * Return a null command.
     * @return - NullCommand.
     */
    public NullCommand build() {
      return new NullCommand();
    }
  }
}
