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
package org.apache.hadoop.ozone.container.common.impl;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDeletionChoosingPolicy;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Randomly choosing containers for block deletion.
 */
public class RandomContainerDeletionChoosingPolicy
    implements ContainerDeletionChoosingPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(RandomContainerDeletionChoosingPolicy.class);

  @Override
  public List<ContainerData> chooseContainerForBlockDeletion(int count,
      Map<String, ContainerStatus> candidateContainers)
      throws StorageContainerException {
    Preconditions.checkNotNull(candidateContainers,
        "Internal assertion: candidate containers cannot be null");

    int currentCount = 0;
    List<ContainerData> result = new LinkedList<>();
    ContainerStatus[] values = new ContainerStatus[candidateContainers.size()];
    // to get a shuffle list
    for (ContainerStatus entry : DFSUtil.shuffle(
        candidateContainers.values().toArray(values))) {
      if (currentCount < count) {
        result.add(entry.getContainer());
        currentCount++;

        LOG.debug("Select container {} for block deletion, "
            + "pending deletion blocks num: {}.",
            entry.getContainer().getContainerName(),
            entry.getNumPendingDeletionBlocks());
      } else {
        break;
      }
    }

    return result;
  }
}
