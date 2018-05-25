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


import org.apache.hadoop.hdds.scm.container.common.helpers.
    StorageContainerException;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;

import java.security.NoSuchAlgorithmException;

/**
 * Interface for Container Operations.
 */
public interface Container {

  /**
   * Creates a container.
   *
   * @throws StorageContainerException
   */
  void create(ContainerData containerData) throws StorageContainerException;

  /**
   * Deletes the container.
   *
   * @param forceDelete   - whether this container should be deleted forcibly.
   * @throws StorageContainerException
   */
  void delete(boolean forceDelete) throws StorageContainerException;

  /**
   * Update the container.
   *
   * @param forceUpdate if true, update container forcibly.
   * @throws StorageContainerException
   */
  void update(boolean forceUpdate)
      throws StorageContainerException;

  /**
   * Get metadata about the container.
   *
   * @return ContainerData - Container Data.
   * @throws StorageContainerException
   */
  ContainerData getContainerData() throws StorageContainerException;

  /**
   * Closes a open container, if it is already closed or does not exist a
   * StorageContainerException is thrown.
   *
   * @throws StorageContainerException
   */
  void close() throws StorageContainerException,
      NoSuchAlgorithmException;


}
