/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.cblock;

import com.google.protobuf.BlockingService;
import org.apache.hadoop.cblock.meta.VolumeDescriptor;
import org.apache.hadoop.cblock.meta.VolumeInfo;
import org.apache.hadoop.cblock.proto.CBlockClientServerProtocol;
import org.apache.hadoop.cblock.proto.CBlockServiceProtocol;
import org.apache.hadoop.cblock.proto.MountVolumeResponse;
import org.apache.hadoop.cblock.protocol.proto.CBlockClientServerProtocolProtos;
import org.apache.hadoop.cblock.protocol.proto.CBlockServiceProtocolProtos;
import org.apache.hadoop.cblock.protocolPB.CBlockClientServerProtocolPB;
import org.apache.hadoop.cblock.protocolPB.CBlockClientServerProtocolServerSideTranslatorPB;
import org.apache.hadoop.cblock.protocolPB.CBlockServiceProtocolPB;
import org.apache.hadoop.cblock.protocolPB.CBlockServiceProtocolServerSideTranslatorPB;
import org.apache.hadoop.cblock.storage.IStorageClient;
import org.apache.hadoop.cblock.storage.StorageManager;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_JSCSIRPC_ADDRESS_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_JSCSIRPC_BIND_HOST_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_SERVICERPC_ADDRESS_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_SERVICERPC_ADDRESS_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_SERVICERPC_BIND_HOST_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_SERVICERPC_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_SERVICERPC_HANDLER_COUNT_KEY;

/**
 * The main entry point of CBlock operations, ALL the CBlock operations
 * will go through this class. But NOTE that:
 *
 * volume operations (create/
 * delete/info) are:
 *    client -> CBlockManager -> StorageManager -> CBlock client
 *
 * IO operations (put/get block) are;
 *    client -> CBlock client -> container
 *
 */
public class CBlockManager implements CBlockServiceProtocol,
    CBlockClientServerProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(CBlockManager.class);

  private final RPC.Server cblockService;
  private final RPC.Server cblockServer;

  private final StorageManager storageManager;

  public CBlockManager(CBlockConfiguration conf, IStorageClient storageClient
  ) throws IOException {
    storageManager = new StorageManager(storageClient);

    RPC.setProtocolEngine(conf, CBlockServiceProtocolPB.class,
        ProtobufRpcEngine.class);
    RPC.setProtocolEngine(conf, CBlockClientServerProtocolPB.class,
        ProtobufRpcEngine.class);
    // start service for client command-to-cblock server service
    InetSocketAddress serviceRpcAddr = NetUtils.createSocketAddr(
        conf.getTrimmed(DFS_CBLOCK_SERVICERPC_ADDRESS_KEY,
            DFS_CBLOCK_SERVICERPC_ADDRESS_DEFAULT), -1,
        DFS_CBLOCK_SERVICERPC_ADDRESS_KEY);
    BlockingService cblockProto =
        CBlockServiceProtocolProtos
            .CBlockServiceProtocolService
            .newReflectiveBlockingService(
                new CBlockServiceProtocolServerSideTranslatorPB(this)
            );
    cblockService = startRpcServer(conf, CBlockServiceProtocolPB.class,
        cblockProto, serviceRpcAddr,
        DFS_CBLOCK_SERVICERPC_BIND_HOST_KEY,
        DFS_CBLOCK_SERVICERPC_HANDLER_COUNT_KEY,
        DFS_CBLOCK_SERVICERPC_HANDLER_COUNT_DEFAULT);

    LOG.info("CBlock manager listening for client commands on: {}",
        serviceRpcAddr);
    // now start service for cblock client-to-cblock server communication
    InetSocketAddress serverRpcAddr = NetUtils.createSocketAddr(
        conf.get(DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY,
            DFS_CBLOCK_JSCSIRPC_ADDRESS_DEFAULT), -1,
        DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY);
    BlockingService serverProto =
        CBlockClientServerProtocolProtos
            .CBlockClientServerProtocolService
            .newReflectiveBlockingService(
                new CBlockClientServerProtocolServerSideTranslatorPB(this)
            );
    cblockServer = startRpcServer(
        conf, CBlockClientServerProtocolPB.class,
        serverProto, serverRpcAddr,
        DFS_CBLOCK_JSCSIRPC_BIND_HOST_KEY,
        DFS_CBLOCK_SERVICERPC_HANDLER_COUNT_KEY,
        DFS_CBLOCK_SERVICERPC_HANDLER_COUNT_DEFAULT);
    LOG.info("CBlock server listening for client commands on: {}",
        serverRpcAddr);
  }

  public void start() {
    cblockService.start();
    cblockServer.start();
    LOG.info("CBlock manager started!");
  }

  public void stop() {
    cblockService.stop();
    cblockServer.stop();
  }

  public void join() {
    try {
      cblockService.join();
      cblockServer.join();
    } catch (InterruptedException e) {
      LOG.error("Interrupted during join");
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Starts an RPC server, if configured.
   *
   * @param conf configuration
   * @param protocol RPC protocol provided by RPC server
   * @param instance RPC protocol implementation instance
   * @param addr configured address of RPC server
   * @param bindHostKey configuration key for setting explicit bind host.  If
   *     the property is not configured, then the bind host is taken from addr.
   * @param handlerCountKey configuration key for RPC server handler count
   * @param handlerCountDefault default RPC server handler count if unconfigured
   * @return RPC server, or null if addr is null
   * @throws IOException if there is an I/O error while creating RPC server
   */
  private static RPC.Server startRpcServer(CBlockConfiguration conf,
      Class<?> protocol, BlockingService instance,
      InetSocketAddress addr, String bindHostKey,
      String handlerCountKey, int handlerCountDefault) throws IOException {
    if (addr == null) {
      return null;
    }
    String bindHost = conf.getTrimmed(bindHostKey);
    if (bindHost == null || bindHost.isEmpty()) {
      bindHost = addr.getHostName();
    }
    int numHandlers = conf.getInt(handlerCountKey, handlerCountDefault);
    RPC.Server rpcServer = new RPC.Builder(conf)
        .setProtocol(protocol)
        .setInstance(instance)
        .setBindAddress(bindHost)
        .setPort(addr.getPort())
        .setNumHandlers(numHandlers)
        .setVerbose(false)
        .setSecretManager(null)
        .build();
    return rpcServer;
  }

  @Override
  public MountVolumeResponse mountVolume(
      String userName, String volumeName) throws IOException {
    return storageManager.isVolumeValid(userName, volumeName);
  }

  @Override
  public void createVolume(String userName, String volumeName,
      long volumeSize, int blockSize) throws IOException {
    LOG.info("Create volume received: userName: {} volumeName: {} " +
        "volumeSize: {} blockSize: {}", userName, volumeName,
        volumeSize, blockSize);
    // It is important to create in-memory representation of the
    // volume first, then writes to persistent storage (levelDB)
    // such that it is guaranteed that when there is an entry in
    // levelDB, the volume is allocated. (more like a UNDO log fashion)
    // TODO: what if creation failed? we allocated containers but lost
    // the reference to the volume and all it's containers. How to release
    // the containers?
    storageManager.createVolume(userName, volumeName, volumeSize, blockSize);
    VolumeDescriptor volume = storageManager.getVolume(userName, volumeName);
    if (volume == null) {
      throw new IOException("Volume creation failed!");
    }
  }

  @Override
  public void deleteVolume(String userName,
      String volumeName, boolean force) throws IOException {
    LOG.info("Delete volume received: volume:" + volumeName
        + " force?:" + force);
    storageManager.deleteVolume(userName, volumeName, force);
  }

  @Override
  public VolumeInfo infoVolume(String userName, String volumeName
  ) throws IOException {
    LOG.info("Info volume received: volume: {}", volumeName);
    return storageManager.infoVolume(userName, volumeName);
  }

  @Override
  public List<VolumeInfo> listVolume(String userName) throws IOException {
    ArrayList<VolumeInfo> response = new ArrayList<>();
    List<VolumeDescriptor> allVolumes =
        storageManager.getAllVolume(userName);
    for (VolumeDescriptor volume : allVolumes) {
      VolumeInfo info =
          new VolumeInfo(volume.getUserName(), volume.getVolumeName(),
              volume.getVolumeSize(), volume.getBlockSize());
      response.add(info);
    }
    return response;
  }
}
