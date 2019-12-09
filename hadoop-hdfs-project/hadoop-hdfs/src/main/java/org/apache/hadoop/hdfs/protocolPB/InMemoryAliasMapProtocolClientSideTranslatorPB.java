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
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMapProtocol;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.namenode.ha.AbstractNNFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.ha.InMemoryAliasMapFailoverProxyProvider;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS;
import static org.apache.hadoop.hdfs.DFSUtil.addKeySuffixes;
import static org.apache.hadoop.hdfs.DFSUtil.createUri;
import static org.apache.hadoop.hdfs.DFSUtilClient.getNameServiceIds;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX;
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
    implements InMemoryAliasMapProtocol, Closeable {

  private static final Logger LOG =
      LoggerFactory
          .getLogger(InMemoryAliasMapProtocolClientSideTranslatorPB.class);

  private AliasMapProtocolPB rpcProxy;

  public InMemoryAliasMapProtocolClientSideTranslatorPB(
      AliasMapProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  public static Collection<InMemoryAliasMapProtocol> init(Configuration conf) {
    Collection<InMemoryAliasMapProtocol> aliasMaps = new ArrayList<>();
    // Try to connect to all configured nameservices as it is not known which
    // nameservice supports the AliasMap.
    for (String nsId : getNameServiceIds(conf)) {
      try {
        URI namenodeURI = null;
        Configuration newConf = new Configuration(conf);
        if (HAUtil.isHAEnabled(conf, nsId)) {
          // set the failover-proxy provider if HA is enabled.
          newConf.setClass(
              addKeySuffixes(PROXY_PROVIDER_KEY_PREFIX, nsId),
              InMemoryAliasMapFailoverProxyProvider.class,
              AbstractNNFailoverProxyProvider.class);
          namenodeURI = new URI(HdfsConstants.HDFS_URI_SCHEME + "://" + nsId);
        } else {
          String key =
              addKeySuffixes(DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS, nsId);
          String addr = conf.get(key);
          if (addr != null) {
            namenodeURI = createUri(HdfsConstants.HDFS_URI_SCHEME,
                NetUtils.createSocketAddr(addr));
          }
        }
        if (namenodeURI != null) {
          aliasMaps.add(NameNodeProxies
              .createProxy(newConf, namenodeURI, InMemoryAliasMapProtocol.class)
              .getProxy());
          LOG.info("Connected to InMemoryAliasMap at {}", namenodeURI);
        }
      } catch (IOException | URISyntaxException e) {
        LOG.warn("Exception in connecting to InMemoryAliasMap for nameservice "
            + "{}: {}", nsId, e);
      }
    }
    // if a separate AliasMap is configured using
    // DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS, try to connect it.
    if (conf.get(DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS) != null) {
      URI uri = createUri("hdfs", NetUtils.createSocketAddr(
          conf.get(DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS)));
      try {
        aliasMaps.add(NameNodeProxies
            .createProxy(conf, uri, InMemoryAliasMapProtocol.class).getProxy());
        LOG.info("Connected to InMemoryAliasMap at {}", uri);
      } catch (IOException e) {
        LOG.warn("Exception in connecting to InMemoryAliasMap at {}: {}", uri,
            e);
      }
    }
    return aliasMaps;
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

    if (block == null) {
      throw new IOException("Block cannot be null");
    }
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
    if (block == null || providedStorageLocation == null) {
      throw new IOException("Provided block and location cannot be null");
    }
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

  @Override
  public void close() throws IOException {
    LOG.info("Stopping rpcProxy in" +
        "InMemoryAliasMapProtocolClientSideTranslatorPB");
    if (rpcProxy != null) {
      RPC.stopProxy(rpcProxy);
    }
  }
}
