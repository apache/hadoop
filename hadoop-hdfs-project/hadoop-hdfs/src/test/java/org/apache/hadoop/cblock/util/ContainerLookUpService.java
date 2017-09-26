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
package org.apache.hadoop.cblock.util;

import org.apache.hadoop.cblock.meta.ContainerDescriptor;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NOTE : This class is only for testing purpose.
 *
 * Mock an underlying container storage layer, expose to CBlock to perform
 * IO. While in this mock implementation, a container is nothing more than
 * a in memory hashmap.
 *
 * This is to allow volume creation call and perform standalone tests.
 */
public final class ContainerLookUpService {
  private static ConcurrentHashMap<String, ContainerDescriptor>
      containers = new ConcurrentHashMap<>();

  /**
   * Return an *existing* container with given Id.
   *
   * TODO : for testing purpose, return a new container if the given Id
   * is not found
   *
   * found
   * @param containerID
   * @return the corresponding pipeline instance (create if not exist)
   */
  public static ContainerDescriptor lookUp(String containerID)
      throws IOException {
    if (!containers.containsKey(containerID)) {
      Pipeline pipeline = ContainerTestHelper.createSingleNodePipeline(
          containerID);
      ContainerDescriptor cd = new ContainerDescriptor(containerID);
      cd.setPipeline(pipeline);
      containers.put(containerID, cd);
    }
    return containers.get(containerID);
  }

  public static void addContainer(String containerID) throws IOException {
    Pipeline pipeline = ContainerTestHelper.createSingleNodePipeline(
        containerID);
    ContainerDescriptor cd = new ContainerDescriptor(containerID);
    cd.setPipeline(pipeline);
    containers.put(containerID, cd);
  }

  private ContainerLookUpService() {

  }
}