/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

import javax.annotation.Nullable;

/**
 * Injects faults in the metadata and data related operations on datanode
 * volumes.
 */
@InterfaceAudience.Private
public class FaultInjectorFileIoEvents {

  private final boolean isEnabled;

  public FaultInjectorFileIoEvents(@Nullable Configuration conf) {
    if (conf != null) {
      isEnabled = conf.getBoolean(DFSConfigKeys
          .DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_KEY, DFSConfigKeys
          .DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_DEFAULT);
    } else {
      isEnabled = false;
    }
  }

  public void beforeMetadataOp(
      @Nullable FsVolumeSpi volume, FileIoProvider.OPERATION op) {
  }

  public void beforeFileIo(
      @Nullable FsVolumeSpi volume, FileIoProvider.OPERATION op, long len) {
  }
}
