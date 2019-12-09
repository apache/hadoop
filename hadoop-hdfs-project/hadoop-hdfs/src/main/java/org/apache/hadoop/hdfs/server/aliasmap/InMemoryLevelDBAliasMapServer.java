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
package org.apache.hadoop.hdfs.server.aliasmap;

import com.google.protobuf.BlockingService;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.protocolPB.AliasMapProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.AliasMapProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.RPC;
import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_BIND_HOST;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_SERVER_LOG;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_SERVER_LOG_DEFAULT;
import static org.apache.hadoop.hdfs.DFSUtil.getBindAddress;
import static org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos.*;
import static org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap.CheckedFunction2;

/**
 * InMemoryLevelDBAliasMapServer is the entry point from the Namenode into
 * the {@link InMemoryAliasMap}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InMemoryLevelDBAliasMapServer implements InMemoryAliasMapProtocol,
    Configurable, Closeable {

  private static final Logger LOG = LoggerFactory
      .getLogger(InMemoryLevelDBAliasMapServer.class);
  private final CheckedFunction2<Configuration, String, InMemoryAliasMap>
      initFun;
  private RPC.Server aliasMapServer;
  private Configuration conf;
  private InMemoryAliasMap aliasMap;
  private String blockPoolId;

  public InMemoryLevelDBAliasMapServer(
          CheckedFunction2<Configuration, String, InMemoryAliasMap> initFun,
      String blockPoolId) {
    this.initFun = initFun;
    this.blockPoolId = blockPoolId;
  }

  public void start() throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      throw new UnsupportedOperationException("Unable to start "
          + "InMemoryLevelDBAliasMapServer as security is enabled");
    }
    RPC.setProtocolEngine(getConf(), AliasMapProtocolPB.class,
        ProtobufRpcEngine.class);
    AliasMapProtocolServerSideTranslatorPB aliasMapProtocolXlator =
        new AliasMapProtocolServerSideTranslatorPB(this);

    BlockingService aliasMapProtocolService =
        AliasMapProtocolService
            .newReflectiveBlockingService(aliasMapProtocolXlator);

    InetSocketAddress rpcAddress = getBindAddress(conf,
        DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS,
        DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS_DEFAULT,
        DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_BIND_HOST);

    boolean setVerbose = conf.getBoolean(
        DFS_PROVIDED_ALIASMAP_INMEMORY_SERVER_LOG,
        DFS_PROVIDED_ALIASMAP_INMEMORY_SERVER_LOG_DEFAULT);

    aliasMapServer = new RPC.Builder(conf)
        .setProtocol(AliasMapProtocolPB.class)
        .setInstance(aliasMapProtocolService)
        .setBindAddress(rpcAddress.getHostName())
        .setPort(rpcAddress.getPort())
        .setNumHandlers(1)
        .setVerbose(setVerbose)
        .build();

    LOG.info("Starting InMemoryLevelDBAliasMapServer on {}", rpcAddress);
    aliasMapServer.start();
  }

  @Override
  public InMemoryAliasMap.IterationResult list(Optional<Block> marker)
      throws IOException {
    return aliasMap.list(marker);
  }

  @Nonnull
  @Override
  public Optional<ProvidedStorageLocation> read(@Nonnull Block block)
      throws IOException {
    return aliasMap.read(block);
  }

  @Override
  public void write(@Nonnull Block block,
      @Nonnull ProvidedStorageLocation providedStorageLocation)
      throws IOException {
    aliasMap.write(block, providedStorageLocation);
  }

  @Override
  public String getBlockPoolId() {
    return blockPoolId;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    try {
      this.aliasMap = initFun.apply(conf, blockPoolId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void close() {
    LOG.info("Stopping InMemoryLevelDBAliasMapServer");
    try {
      if (aliasMap != null) {
        aliasMap.close();
      }
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    if (aliasMapServer != null) {
      aliasMapServer.stop();
    }
  }

}
