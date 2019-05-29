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

package org.apache.hadoop.ozone.container.common.volume;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Choose volumes in round-robin order.
 * The caller should synchronize access to the list of volumes.
 */
public class RoundRobinVolumeChoosingPolicy implements VolumeChoosingPolicy {

  public static final Log LOG = LogFactory.getLog(
      RoundRobinVolumeChoosingPolicy.class);

  // Stores the index of the next volume to be returned.
  private AtomicInteger nextVolumeIndex = new AtomicInteger(0);

  @Override
  public HddsVolume chooseVolume(List<HddsVolume> volumes,
      long maxContainerSize) throws IOException {

    // No volumes available to choose from
    if (volumes.size() < 1) {
      throw new DiskOutOfSpaceException("No more available volumes");
    }

    // since volumes could've been removed because of the failure
    // make sure we are not out of bounds
    int nextIndex = nextVolumeIndex.get();
    int currentVolumeIndex = nextIndex < volumes.size() ? nextIndex : 0;

    int startVolumeIndex = currentVolumeIndex;
    long maxAvailable = 0;

    while (true) {
      final HddsVolume volume = volumes.get(currentVolumeIndex);
      // adjust for remaining capacity in Open containers
      long availableVolumeSize = volume.getAvailable()
          - volume.getCommittedBytes();

      currentVolumeIndex = (currentVolumeIndex + 1) % volumes.size();

      if (availableVolumeSize > maxContainerSize) {
        nextVolumeIndex.compareAndSet(nextIndex, currentVolumeIndex);
        return volume;
      }

      if (availableVolumeSize > maxAvailable) {
        maxAvailable = availableVolumeSize;
      }

      if (currentVolumeIndex == startVolumeIndex) {
        throw new DiskOutOfSpaceException("Out of space: "
            + "The volume with the most available space (=" + maxAvailable
            + " B) is less than the container size (=" + maxContainerSize
            + " B).");
      }

    }
  }
}
