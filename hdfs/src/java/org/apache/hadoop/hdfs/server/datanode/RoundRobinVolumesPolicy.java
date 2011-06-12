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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

public class RoundRobinVolumesPolicy implements BlockVolumeChoosingPolicy {

  private int curVolume = 0;

  @Override
  public synchronized FSVolume chooseVolume(List<FSVolume> volumes, long blockSize)
      throws IOException {
    if(volumes.size() < 1) {
      throw new DiskOutOfSpaceException("No more available volumes");
    }
    
    // since volumes could've been removed because of the failure
    // make sure we are not out of bounds
    if(curVolume >= volumes.size()) {
      curVolume = 0;
    }
    
    int startVolume = curVolume;
    
    while (true) {
      FSVolume volume = volumes.get(curVolume);
      curVolume = (curVolume + 1) % volumes.size();
      if (volume.getAvailable() > blockSize) { return volume; }
      if (curVolume == startVolume) {
        throw new DiskOutOfSpaceException("Insufficient space for an additional block");
      }
    }
  }

}
