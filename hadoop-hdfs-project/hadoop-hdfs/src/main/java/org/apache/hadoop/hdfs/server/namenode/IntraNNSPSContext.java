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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier;

/**
 * This class is the Namenode implementation for analyzing the file blocks which
 * are expecting to change its storages and assigning the block storage
 * movements to satisfy the storage policy.
 */
// TODO: Now, added one API which is required for sps package. Will refine
// this interface via HDFS-12911.
public class IntraNNSPSContext implements StoragePolicySatisfier.Context {
  private final Namesystem namesystem;

  public IntraNNSPSContext(Namesystem namesystem) {
    this.namesystem = namesystem;
  }

  @Override
  public int getNumLiveDataNodes() {
    return namesystem.getFSDirectory().getBlockManager().getDatanodeManager()
        .getNumLiveDataNodes();
  }
}
