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

package org.apache.hadoop.ozone.client.io;


import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos.KeyData;

/**
 * This class contains methods that define the translation between the Ozone
 * domain model and the storage container domain model.
 */
final class OzoneContainerTranslation {

  /**
   * Creates key data intended for reading a container key.
   *
   * @param containerName container name
   * @param containerKey container key
   * @return KeyData intended for reading the container key
   */
  public static KeyData containerKeyDataForRead(String containerName,
      String containerKey) {
    return KeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(containerKey)
        .build();
  }

  /**
   * There is no need to instantiate this class.
   */
  private OzoneContainerTranslation() {
  }
}
