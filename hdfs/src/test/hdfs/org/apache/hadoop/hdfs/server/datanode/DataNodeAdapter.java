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

/**
 * WARNING!! This is TEST ONLY class: it never has to be used
 * for ANY development purposes.
 * 
 * This is a utility class to expose DataNode functionality for
 * unit and functional tests.
 */
public class DataNodeAdapter {
  /**
   * Fetch a copy of ReplicaInfo from a datanode by block id
   * @param dn datanode to retrieve a replicainfo object from
   * @param blkId id of the replica's block
   * @return copy of ReplicaInfo object @link{FSDataset#fetchReplicaInfo}
   */
  public static ReplicaInfo fetchReplicaInfo (final DataNode dn,
                                              final long blkId) {
    return ((FSDataset)dn.data).fetchReplicaInfo(blkId);
  }
}
