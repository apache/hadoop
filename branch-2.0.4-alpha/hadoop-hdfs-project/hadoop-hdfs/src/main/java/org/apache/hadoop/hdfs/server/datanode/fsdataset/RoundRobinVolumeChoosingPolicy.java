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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

/**
 * Choose volumes in round-robin order.
 */
public class RoundRobinVolumeChoosingPolicy<V extends FsVolumeSpi>
    implements VolumeChoosingPolicy<V> {

  private int curVolume = 0;

  @Override
  public synchronized V chooseVolume(final List<V> volumes, final long blockSize
      ) throws IOException {
    if(volumes.size() < 1) {
      throw new DiskOutOfSpaceException("No more available volumes");
    }
    
    // since volumes could've been removed because of the failure
    // make sure we are not out of bounds
    if(curVolume >= volumes.size()) {
      curVolume = 0;
    }
    
    int startVolume = curVolume;
    long maxAvailable = 0;
    
    while (true) {
      final V volume = volumes.get(curVolume);
      curVolume = (curVolume + 1) % volumes.size();
      long availableVolumeSize = volume.getAvailable();
      if (availableVolumeSize > blockSize) { return volume; }
      
      if (availableVolumeSize > maxAvailable) {
        maxAvailable = availableVolumeSize;
      }
      
      if (curVolume == startVolume) {
        throw new DiskOutOfSpaceException("Out of space: "
            + "The volume with the most available space (=" + maxAvailable
            + " B) is less than the block size (=" + blockSize + " B).");
      }
    }
  }
}
