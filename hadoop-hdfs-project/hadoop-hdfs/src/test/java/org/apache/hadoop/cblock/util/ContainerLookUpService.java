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

import java.util.HashMap;

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
  private static HashMap<String, ContainerDescriptor>
      containers = new HashMap<>();

  /**
   * Return an *existing* container with given Id.
   *
   * TODO : for testing purpose, return a new container if the given Id
   * is not found
   *
   * found
   * @param containerID
   * @return
   */
  public static ContainerDescriptor lookUp(String containerID) {
    if (!containers.containsKey(containerID)) {
      System.err.println("A container id never seen, return a new one " +
          "for testing purpose:" + containerID);
      containers.put(containerID, new ContainerDescriptor(containerID));
    }
    return containers.get(containerID);
  }

  public static void addContainer(String containerID) {
    containers.put(containerID, new ContainerDescriptor(containerID));
  }

  private ContainerLookUpService() {

  }
}
