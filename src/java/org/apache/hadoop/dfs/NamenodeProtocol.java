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

package org.apache.hadoop.dfs;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

/*****************************************************************************
 * Protocol that a secondary NameNode uses to communicate with the NameNode.
 * It's used to get part of the name node state
 *****************************************************************************/
interface NamenodeProtocol extends VersionedProtocol {
  /**
   * 1: changed the serialization in DatanodeInfo
   */
  public static final long versionID = 1L;

  /** Get a list of blocks belonged to <code>datanode</code>
    * whose total size is equal to <code>size</code>
   * @param datanode  a data node
   * @param size      requested size
   * @return          a list of blocks & their locations
   * @Exception RemoteException if size is less than or equal to 0 or
                                   datanode does not exist
   */
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
  throws IOException;

}
