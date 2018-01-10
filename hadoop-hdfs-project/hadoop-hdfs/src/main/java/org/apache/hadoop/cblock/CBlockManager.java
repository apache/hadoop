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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;
import org.apache.hadoop.cblock.meta.VolumeDescriptor;
import org.apache.hadoop.cblock.meta.VolumeInfo;
import org.apache.hadoop.cblock.proto.CBlockClientProtocol;
import org.apache.hadoop.cblock.proto.CBlockServiceProtocol;
import org.apache.hadoop.cblock.proto.MountVolumeResponse;
import org.apache.hadoop.cblock.protocol.proto
    .CBlockClientServerProtocolProtos;
import org.apache.hadoop.cblock.protocol.proto.CBlockServiceProtocolProtos;
import org.apache.hadoop.cblock.protocolPB.CBlockClientServerProtocolPB;
import org.apache.hadoop.cblock.protocolPB
    .CBlockClientServerProtocolServerSideTranslatorPB;
import org.apache.hadoop.cblock.protocolPB.CBlockServiceProtocolPB;
import org.apache.hadoop.cblock.protocolPB
    .CBlockServiceProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.client.ContainerOperationClient;
import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.cblock.storage.StorageManager;
import org.apache.hadoop.cblock.util.KeyUtil;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.utils.LevelDBStore;
import org.iq80.leveldb.DBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CONTAINER_SIZE_GB_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CONTAINER_SIZE_GB_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_JSCSIRPC_BIND_HOST_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SCM_IPADDRESS_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SCM_IPADDRESS_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SCM_PORT_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SCM_PORT_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICERPC_ADDRESS_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICERPC_BIND_HOST_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICERPC_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICERPC_HANDLER_COUNT_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICE_LEVELDB_PATH_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICE_LEVELDB_PATH_KEY;

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
    CBlockClientProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(CBlockManager.class);

  private final RPC.Server cblockService;
  private final RPC.Server cblockServer;

  private final StorageManager storageManager;

  private final LevelDBStore levelDBStore;
  private final String dbPath;

  private Charset encoding = Charset.forName("UTF-8");

  public CBlockManager(OzoneConfiguration conf,
      ScmClient storageClient) throws IOException {
    // Fix the cBlockManagerId generattion code here. Should support
    // cBlockManager --init command which will generate a cBlockManagerId and
    // persist it locally.
    storageManager =
        new StorageManager(storageClient, conf, "CBLOCK");

    dbPath = conf.getTrimmed(DFS_CBLOCK_SERVICE_LEVELDB_PATH_KEY,
        DFS_CBLOCK_SERVICE_LEVELDB_PATH_DEFAULT);
    levelDBStore = new LevelDBStore(new File(dbPath), true);
    LOG.info("Try to load exising volume information");
    readFromPersistentStore();

    RPC.setProtocolEngine(conf, CBlockServiceProtocolPB.class,
        ProtobufRpcEngine.class);
    RPC.setProtocolEngine(conf, CBlockClientServerProtocolPB.class,
        ProtobufRpcEngine.class);
    // start service for client command-to-cblock server service
    InetSocketAddress serviceRpcAddr =
        OzoneClientUtils.getCblockServiceRpcAddr(conf);
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
    InetSocketAddress cblockServiceRpcAddress =
        OzoneClientUtils.updateRPCListenAddress(conf,
            DFS_CBLOCK_SERVICERPC_ADDRESS_KEY, serviceRpcAddr, cblockService);
    LOG.info("CBlock manager listening for client commands on: {}",
        cblockServiceRpcAddress);
    // now start service for cblock client-to-cblock server communication

    InetSocketAddress serverRpcAddr =
        OzoneClientUtils.getCblockServerRpcAddr(conf);
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
    InetSocketAddress cblockServerRpcAddress =
        OzoneClientUtils.updateRPCListenAddress(conf,
            DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY, serverRpcAddr, cblockServer);
    LOG.info("CBlock server listening for client commands on: {}",
        cblockServerRpcAddress);
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
  private static RPC.Server startRpcServer(OzoneConfiguration conf,
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
  public synchronized MountVolumeResponse mountVolume(
      String userName, String volumeName) throws IOException {
    return storageManager.isVolumeValid(userName, volumeName);
  }

  @Override
  public synchronized void createVolume(String userName, String volumeName,
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
    String volumeKey = KeyUtil.getVolumeKey(userName, volumeName);
    writeToPersistentStore(volumeKey.getBytes(encoding),
        volume.toProtobuf().toByteArray());
  }

  @Override
  public synchronized void deleteVolume(String userName,
      String volumeName, boolean force) throws IOException {
    LOG.info("Delete volume received: volume: {} {} ", volumeName, force);
    storageManager.deleteVolume(userName, volumeName, force);
    // being here means volume is successfully deleted now
    String volumeKey = KeyUtil.getVolumeKey(userName, volumeName);
    removeFromPersistentStore(volumeKey.getBytes(encoding));
  }

  // No need to synchronize on the following three methods, since write and
  // remove's caller are synchronized. read's caller is the constructor and
  // no other method call can happen at that time.
  @VisibleForTesting
  public void writeToPersistentStore(byte[] key, byte[] value) {
    levelDBStore.put(key, value);
  }

  @VisibleForTesting
  public void removeFromPersistentStore(byte[] key) {
    levelDBStore.delete(key);
  }

  public void readFromPersistentStore() throws IOException {
    try (DBIterator iter = levelDBStore.getIterator()) {
      iter.seekToFirst();
      while (iter.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iter.next();
        String volumeKey = new String(entry.getKey(), encoding);
        try {
          VolumeDescriptor volumeDescriptor =
              VolumeDescriptor.fromProtobuf(entry.getValue());
          storageManager.addVolume(volumeDescriptor);
        } catch (IOException e) {
          LOG.error("Loading volume " + volumeKey + " error " + e);
        }
      }
    }
  }

  @Override
  public synchronized VolumeInfo infoVolume(String userName, String volumeName
  ) throws IOException {
    LOG.info("Info volume received: volume: {}", volumeName);
    return storageManager.infoVolume(userName, volumeName);
  }

  @VisibleForTesting
  public synchronized List<VolumeDescriptor> getAllVolumes() {
    return storageManager.getAllVolume(null);
  }

  public synchronized List<VolumeDescriptor> getAllVolumes(String userName) {
    return storageManager.getAllVolume(userName);
  }

  public synchronized void close() {
    try {
      levelDBStore.close();
    } catch (IOException e) {
      LOG.error("Error when closing levelDB " + e);
    }
  }

  public synchronized void clean() {
    try {
      levelDBStore.close();
      levelDBStore.destroy();
    } catch (IOException e) {
      LOG.error("Error when deleting levelDB " + e);
    }
  }

  @Override
  public synchronized List<VolumeInfo> listVolume(String userName)
      throws IOException {
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

  public static void main(String[] args) throws Exception {
    long version = RPC.getProtocolVersion(
        StorageContainerLocationProtocolPB.class);
    OzoneConfiguration ozoneConf = new OzoneConfiguration();
    String scmAddress = ozoneConf.get(DFS_CBLOCK_SCM_IPADDRESS_KEY,
        DFS_CBLOCK_SCM_IPADDRESS_DEFAULT);
    int scmPort = ozoneConf.getInt(DFS_CBLOCK_SCM_PORT_KEY,
        DFS_CBLOCK_SCM_PORT_DEFAULT);
    int containerSizeGB = ozoneConf.getInt(DFS_CBLOCK_CONTAINER_SIZE_GB_KEY,
        DFS_CBLOCK_CONTAINER_SIZE_GB_DEFAULT);
    ContainerOperationClient.setContainerSizeB(containerSizeGB* OzoneConsts.GB);
    InetSocketAddress address = new InetSocketAddress(scmAddress, scmPort);

    ozoneConf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);
    LOG.info(
        "Creating StorageContainerLocationProtocol RPC client with address {}",
        address);
    RPC.setProtocolEngine(ozoneConf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    StorageContainerLocationProtocolClientSideTranslatorPB client =
        new StorageContainerLocationProtocolClientSideTranslatorPB(
            RPC.getProxy(StorageContainerLocationProtocolPB.class, version,
                address, UserGroupInformation.getCurrentUser(), ozoneConf,
                NetUtils.getDefaultSocketFactory(ozoneConf),
                Client.getRpcTimeout(ozoneConf)));
    ScmClient storageClient = new ContainerOperationClient(
        client, new XceiverClientManager(ozoneConf));
    CBlockManager cbm = new CBlockManager(ozoneConf, storageClient);
    cbm.start();
    cbm.join();
  }
}
