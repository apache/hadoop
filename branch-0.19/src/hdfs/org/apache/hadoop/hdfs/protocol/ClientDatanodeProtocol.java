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
package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.VersionedProtocol;

/** An client-datanode protocol for block recovery
 */
public interface ClientDatanodeProtocol extends VersionedProtocol {
  public static final Log LOG = LogFactory.getLog(ClientDatanodeProtocol.class);

  /**
   * 3: add keepLength parameter.
   */
  public static final long versionID = 3L;

  /** Start generation-stamp recovery for specified block
   * @param block the specified block
   * @param keepLength keep the block length
   * @param targets the list of possible locations of specified block
   * @return the new blockid if recovery successful and the generation stamp
   * got updated as part of the recovery, else returns null if the block id
   * not have any data and the block was deleted.
   * @throws IOException
   */
  LocatedBlock recoverBlock(Block block, boolean keepLength,
      DatanodeInfo[] targets) throws IOException;
}
