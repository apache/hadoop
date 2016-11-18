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
package org.apache.hadoop.cblock.meta;

/**
 *
 * The internal representation of a container maintained by CBlock server.
 * Include enough information to exactly identify a container for read/write
 * operation.
 *
 * NOTE that this class is work-in-progress. Depends on HDFS-7240 container
 * implementation. Currently only to allow testing.
 */
public class ContainerDescriptor {
  private final String containerID;
  // the index of this container with in a volume
  // on creation, there is no way to know the index of the container
  // as it is a volume specific information
  private int containerIndex;

  public ContainerDescriptor(String containerID) {
    this.containerID = containerID;
  }

  public void setContainerIndex(int idx) {
    this.containerIndex = idx;
  }

  public String getContainerID() {
    return containerID;
  }

  public int getContainerIndex() {
    return containerIndex;
  }

  public long getUtilization() {
    return 0;
  }
}
