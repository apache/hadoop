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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

public interface RemotePeerFactory {
  /**
   * @param addr          The address to connect to.
   * @param blockToken    Token used during optional SASL negotiation
   * @param datanodeId    ID of destination DataNode
   * @return              A new Peer connected to the address.
   *
   * @throws IOException  If there was an error connecting or creating 
   *                      the remote socket, encrypted stream, etc.
   */
  Peer newConnectedPeer(InetSocketAddress addr,
      Token<BlockTokenIdentifier> blockToken, DatanodeID datanodeId)
      throws IOException;
}
