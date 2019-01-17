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

import com.google.protobuf.GeneratedMessage;
import org.apache.hadoop.hdds.HddsIdFactory;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.server.events.IdentifiableEventPayload;

/**
 * A class that acts as the base class to convert between Java and SCM
 * commands in protobuf format.
 * @param <T>
 */
public abstract class SCMCommand<T extends GeneratedMessage> implements
    IdentifiableEventPayload {
  private long id;

  SCMCommand() {
    this.id = HddsIdFactory.getLongId();
  }

  SCMCommand(long id) {
    this.id = id;
  }
  /**
   * Returns the type of this command.
   * @return Type
   */
  public  abstract SCMCommandProto.Type getType();

  /**
   * Gets the protobuf message of this object.
   * @return A protobuf message.
   */
  public abstract T getProto();

  /**
   * Gets the commandId of this object.
   * @return uuid.
   */
  public long getId() {
    return id;
  }

}
