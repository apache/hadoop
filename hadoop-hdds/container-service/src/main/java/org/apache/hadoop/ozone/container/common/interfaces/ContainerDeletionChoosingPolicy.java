/**
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
package org.apache.hadoop.ozone.container.common.interfaces;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;

import java.util.List;
import java.util.Map;

/**
 * This interface is used for choosing desired containers for
 * block deletion.
 */
public interface ContainerDeletionChoosingPolicy {

  /**
   * Chooses desired containers for block deletion.
   * @param count
   *          how many to return
   * @param candidateContainers
   *          candidate containers collection
   * @return container data list
   * @throws StorageContainerException
   */
  List<ContainerData> chooseContainerForBlockDeletion(int count,
      Map<Long, ContainerData> candidateContainers)
      throws StorageContainerException;

  /**
   * Determine if the container has suitable type for this policy.
   * @param type  type of the container
   * @return whether the container type suitable for this policy.
   */
  default boolean isValidContainerType(ContainerProtos.ContainerType type) {
    if (type == ContainerProtos.ContainerType.KeyValueContainer) {
      return true;
    }
    return false;
  }
}
