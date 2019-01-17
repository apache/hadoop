/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.common.helpers;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;

/**
 * Exceptions thrown when a write/update opearation is done on non-open
 * container.
 */
public class ContainerNotOpenException extends StorageContainerException {

  /**
   * Constructs an {@code IOException} with the specified detail message.
   *
   * @param message The detail message (which is saved for later retrieval by
   * the {@link #getMessage()} method)
   */
  public ContainerNotOpenException(String message) {
    super(message, ContainerProtos.Result.CONTAINER_NOT_OPEN);
  }
}