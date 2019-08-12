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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

/**
 * This exception collects all IOExceptions thrown when adding block pools and
 * scanning volumes. It keeps the information about which volume is associated
 * with an exception.
 *
 */
public class AddBlockPoolException extends RuntimeException {
  private Map<FsVolumeSpi, IOException> unhealthyDataDirs;
  public AddBlockPoolException(Map<FsVolumeSpi, IOException>
      unhealthyDataDirs) {
    this.unhealthyDataDirs = unhealthyDataDirs;
  }

  public AddBlockPoolException() {
    this.unhealthyDataDirs = new ConcurrentHashMap<FsVolumeSpi, IOException>();
  }

  public void mergeException(AddBlockPoolException e) {
    if (e == null) {
      return;
    }
    for(FsVolumeSpi v : e.unhealthyDataDirs.keySet()) {
      // If there is already an exception for this volume, keep the original
      // exception and discard the new one. It is likely the first
      // exception caused the second or they were both due to the disk issue
      if (!unhealthyDataDirs.containsKey(v)) {
        unhealthyDataDirs.put(v, e.unhealthyDataDirs.get(v));
      }
    }
  }

  public boolean hasExceptions() {
    return !unhealthyDataDirs.isEmpty();
  }

  public Map<FsVolumeSpi, IOException> getFailingVolumes() {
    return unhealthyDataDirs;
  }
  @Override
  public String toString() {
    return getClass().getName() + ": " + unhealthyDataDirs.toString();
  }
}