/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.syncservice.executor;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsTracer;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.ClientContext;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.client.impl.BlockReaderFactory;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.EnumSet;

/**
 * BlockSyncReaderFactory constructs a block reader in the Datanode for the
 * Sync Command to read blocks that will be written to the synchronization
 * remote endpoint.
 */
public class BlockSyncReaderFactory {

  public static BlockReader createBlockReader(DataNode dataNode,
      LocatedBlock locatedBlock, Configuration conf) throws IOException {
    ClientContext clientContext = ClientContext.getFromConf(conf);
    Token<BlockTokenIdentifier> accessToken = dataNode.getBlockAccessToken(
        locatedBlock.getBlock(),
        EnumSet.of(BlockTokenIdentifier.AccessMode.READ),
        locatedBlock.getStorageTypes(), locatedBlock.getStorageIDs());

    DatanodeInfo datanodeInfo = locatedBlock.getLocations()[0];

    Socket socked = NetUtils.getDefaultSocketFactory(conf).createSocket();
    InetSocketAddress resolvedAddress =
        datanodeInfo.getResolvedAddress();
    socked.connect(resolvedAddress);

    return new BlockReaderFactory(new DfsClientConf(conf))
        .setConfiguration(conf)
        .setBlock(locatedBlock.getBlock())
        .setBlockToken(accessToken)
        .setStartOffset(0)
        .setLength(locatedBlock.getBlock().getNumBytes())
        .setInetSocketAddress(datanodeInfo.getResolvedAddress())
        .setVerifyChecksum(true)
        .setDatanodeInfo(datanodeInfo)
        .setClientName("BlockSyncOperationExecutor")
        .setCachingStrategy(CachingStrategy.newDefaultStrategy())
        .setRemotePeerFactory((addr, blockToken, datanodeId) -> {
          Peer peer = null;
          Socket sock = NetUtils.getDefaultSocketFactory(conf).createSocket();
          try {
            sock.connect(addr, HdfsConstants.READ_TIMEOUT);
            sock.setSoTimeout(HdfsConstants.READ_TIMEOUT);
            peer = DFSUtilClient.peerFromSocket(sock);
          } finally {
            if (peer == null) {
              IOUtils.closeQuietly(sock);
            }
          }
          return peer;
        })
        .setClientCacheContext(clientContext)
        .build();
  }
}
