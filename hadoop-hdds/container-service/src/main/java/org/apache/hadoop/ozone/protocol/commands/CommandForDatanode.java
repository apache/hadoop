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

import java.util.UUID;

import com.google.protobuf.GeneratedMessage;
import org.apache.hadoop.hdds.server.events.IdentifiableEventPayload;

/**
 * Command for the datanode with the destination address.
 */
public class CommandForDatanode<T extends GeneratedMessage> implements
    IdentifiableEventPayload {

  private final UUID datanodeId;

  private final SCMCommand<T> command;

  // TODO: Command for datanode should take DatanodeDetails as parameter.
  public CommandForDatanode(UUID datanodeId, SCMCommand<T> command) {
    this.datanodeId = datanodeId;
    this.command = command;
  }

  public UUID getDatanodeId() {
    return datanodeId;
  }

  public SCMCommand<T> getCommand() {
    return command;
  }

  public long getId() {
    return command.getId();
  }
}
