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
package org.apache.hadoop.hdfs.protocolPB;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMapProtocol;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos.*;
import static org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.*;

/**
 * This class is the client side translator to translate requests made to the
 * {@link InMemoryAliasMapProtocol} interface to the RPC server implementing
 * {@link AliasMapProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InMemoryAliasMapProtocolClientSideTranslatorPB
    implements InMemoryAliasMapProtocol {

  private static final Logger LOG =
      LoggerFactory
          .getLogger(InMemoryAliasMapProtocolClientSideTranslatorPB.class);

  private AliasMapProtocolPB rpcProxy;

  public InMemoryAliasMapProtocolClientSideTranslatorPB(Configuration conf) {
    String addr = conf.getTrimmed(DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS,
        DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS_DEFAULT);
    InetSocketAddress aliasMapAddr = NetUtils.createSocketAddr(addr);

    RPC.setProtocolEngine(conf, AliasMapProtocolPB.class,
        ProtobufRpcEngine.class);
    LOG.info("Connecting to address: " + addr);
    try {
      rpcProxy = RPC.getProxy(AliasMapProtocolPB.class,
          RPC.getProtocolVersion(AliasMapProtocolPB.class), aliasMapAddr, null,
          conf, NetUtils.getDefaultSocketFactory(conf), 0);
    } catch (IOException e) {
      throw new RuntimeException(
          "Error in connecting to " + addr + " Got: " + e);
    }
  }

  @Override
  public InMemoryAliasMap.IterationResult list(Optional<Block> marker)
      throws IOException {
    ListRequestProto.Builder builder = ListRequestProto.newBuilder();
    if (marker.isPresent()) {
      builder.setMarker(PBHelperClient.convert(marker.get()));
    }
    ListRequestProto request = builder.build();
    try {
      ListResponseProto response = rpcProxy.list(null, request);
      List<KeyValueProto> fileRegionsList = response.getFileRegionsList();

      List<FileRegion> fileRegions = fileRegionsList
          .stream()
          .map(kv -> new FileRegion(
              PBHelperClient.convert(kv.getKey()),
              PBHelperClient.convert(kv.getValue())
          ))
          .collect(Collectors.toList());
      BlockProto nextMarker = response.getNextMarker();

      if (nextMarker.isInitialized()) {
        return new InMemoryAliasMap.IterationResult(fileRegions,
            Optional.of(PBHelperClient.convert(nextMarker)));
      } else {
        return new InMemoryAliasMap.IterationResult(fileRegions,
            Optional.empty());
      }

    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Nonnull
  @Override
  public Optional<ProvidedStorageLocation> read(@Nonnull Block block)
      throws IOException {

    ReadRequestProto request =
        ReadRequestProto
            .newBuilder()
            .setKey(PBHelperClient.convert(block))
            .build();
    try {
      ReadResponseProto response = rpcProxy.read(null, request);

      ProvidedStorageLocationProto providedStorageLocation =
          response.getValue();
      if (providedStorageLocation.isInitialized()) {
        return Optional.of(PBHelperClient.convert(providedStorageLocation));
      }
      return Optional.empty();

    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void write(@Nonnull Block block,
      @Nonnull ProvidedStorageLocation providedStorageLocation)
      throws IOException {
    WriteRequestProto request =
        WriteRequestProto
            .newBuilder()
            .setKeyValuePair(KeyValueProto.newBuilder()
                .setKey(PBHelperClient.convert(block))
                .setValue(PBHelperClient.convert(providedStorageLocation))
                .build())
            .build();

    try {
      rpcProxy.write(null, request);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public String getBlockPoolId() throws IOException {
    try {
      BlockPoolResponseProto response = rpcProxy.getBlockPoolId(null,
          BlockPoolRequestProto.newBuilder().build());
      return response.getBlockPoolId();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  public void stop() {
    RPC.stopProxy(rpcProxy);
  }
}
