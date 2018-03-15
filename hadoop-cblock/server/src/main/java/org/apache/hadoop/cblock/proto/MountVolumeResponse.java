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
package org.apache.hadoop.cblock.proto;

import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.util.HashMap;
import java.util.List;

/**
 * The response message of mounting a volume. Including enough information
 * for the client to communicate (perform IO) with the volume containers
 * directly.
 */
public class MountVolumeResponse {
  private final boolean isValid;
  private final String userName;
  private final String volumeName;
  private final long volumeSize;
  private final int blockSize;
  private List<Pipeline> containerList;
  private HashMap<String, Pipeline> pipelineMap;

  public MountVolumeResponse(boolean isValid, String userName,
      String volumeName, long volumeSize, int blockSize,
      List<Pipeline> containerList,
      HashMap<String, Pipeline> pipelineMap) {
    this.isValid = isValid;
    this.userName = userName;
    this.volumeName = volumeName;
    this.volumeSize = volumeSize;
    this.blockSize = blockSize;
    this.containerList = containerList;
    this.pipelineMap = pipelineMap;
  }

  public boolean getIsValid() {
    return isValid;
  }

  public String getUserName() {
    return userName;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public long getVolumeSize() {
    return volumeSize;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public List<Pipeline> getContainerList() {
    return containerList;
  }

  public HashMap<String, Pipeline> getPipelineMap() {
    return pipelineMap;
  }
}
