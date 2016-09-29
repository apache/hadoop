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

import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;

import java.io.Closeable;
import java.io.IOException;

/**
 * This class includes a replica being actively written and the reference to
 * the fs volume where this replica is located.
 */
public class ReplicaHandler implements Closeable {
  private final ReplicaInPipeline replica;
  private final FsVolumeReference volumeReference;

  public ReplicaHandler(
      ReplicaInPipeline replica, FsVolumeReference reference) {
    this.replica = replica;
    this.volumeReference = reference;
  }

  @Override
  public void close() throws IOException {
    if (this.volumeReference != null) {
      volumeReference.close();
    }
  }

  public ReplicaInPipeline getReplica() {
    return replica;
  }
}
