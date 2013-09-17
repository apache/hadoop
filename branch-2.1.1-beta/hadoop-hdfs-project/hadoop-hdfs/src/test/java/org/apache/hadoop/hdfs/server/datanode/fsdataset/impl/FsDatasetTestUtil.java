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

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;

public class FsDatasetTestUtil {

  public static File getFile(FsDatasetSpi<?> fsd, String bpid, long bid) {
    return ((FsDatasetImpl)fsd).getFile(bpid, bid);
  }

  public static File getBlockFile(FsDatasetSpi<?> fsd, String bpid, Block b
      ) throws IOException {
    return ((FsDatasetImpl)fsd).getBlockFile(bpid, b);
  }

  public static File getMetaFile(FsDatasetSpi<?> fsd, String bpid, Block b)
      throws IOException {
    return FsDatasetUtil.getMetaFile(getBlockFile(fsd, bpid, b), b
        .getGenerationStamp());
  }
  
  public static boolean unlinkBlock(FsDatasetSpi<?> fsd,
      ExtendedBlock block, int numLinks) throws IOException {
    final ReplicaInfo info = ((FsDatasetImpl)fsd).getReplicaInfo(block);
    return info.unlinkBlock(numLinks);
  }

  public static ReplicaInfo fetchReplicaInfo (final FsDatasetSpi<?> fsd,
      final String bpid, final long blockId) {
    return ((FsDatasetImpl)fsd).fetchReplicaInfo(bpid, blockId);
  }

  public static long getPendingAsyncDeletions(FsDatasetSpi<?> fsd) {
    return ((FsDatasetImpl)fsd).asyncDiskService.countPendingDeletions();
  }
  
  public static Collection<ReplicaInfo> getReplicas(FsDatasetSpi<?> fsd,
      String bpid) {
    return ((FsDatasetImpl)fsd).volumeMap.replicas(bpid);
  }
}
