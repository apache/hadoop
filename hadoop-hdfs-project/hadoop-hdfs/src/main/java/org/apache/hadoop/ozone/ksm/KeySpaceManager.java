/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.ksm;

import com.google.protobuf.BlockingService;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ksm.helpers.VolumeArgs;
import org.apache.hadoop.ksm.protocol.KeyspaceManagerProtocol;
import org.apache.hadoop.ksm.protocolPB.KeySpaceManagerProtocolPB;
import org.apache.hadoop.ozone.OzoneClientUtils;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.protocolPB
    .KeyspaceManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.scm.StorageContainerManager;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import static org.apache.hadoop.ozone.ksm.KSMConfigKeys.OZONE_KSM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys
    .OZONE_KSM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys
    .OZONE_KSM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos
    .KeyspaceManagerService.newReflectiveBlockingService;
import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * Ozone Keyspace manager is the metadata manager of ozone.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "CBLOCK", "OZONE", "HBASE"})
public class KeySpaceManager implements KeyspaceManagerProtocol {
  // TODO: Support JMX
  private static final Logger LOG =
      LoggerFactory.getLogger(KeySpaceManager.class);

  private final RPC.Server ksmRpcServer;
  private final InetSocketAddress ksmRpcAddress;

  public KeySpaceManager(OzoneConfiguration conf) throws IOException {
    final int handlerCount = conf.getInt(OZONE_KSM_HANDLER_COUNT_KEY,
        OZONE_KSM_HANDLER_COUNT_DEFAULT);

    RPC.setProtocolEngine(conf, KeySpaceManagerProtocolPB.class,
        ProtobufRpcEngine.class);

    BlockingService ksmService = newReflectiveBlockingService(
        new KeyspaceManagerProtocolServerSideTranslatorPB(this));
    final InetSocketAddress ksmNodeRpcAddr = OzoneClientUtils.
        getKsmAddress(conf);
    ksmRpcServer = startRpcServer(conf, ksmNodeRpcAddr,
        KeySpaceManagerProtocolPB.class, ksmService,
        handlerCount);
    ksmRpcAddress = updateListenAddress(conf,
        OZONE_KSM_ADDRESS_KEY, ksmNodeRpcAddr, ksmRpcServer);

    //TODO : Add call to register MXBean for JMX.
  }

  /**
   * Starts an RPC server, if configured.
   *
   * @param conf configuration
   * @param addr configured address of RPC server
   * @param protocol RPC protocol provided by RPC server
   * @param instance RPC protocol implementation instance
   * @param handlerCount RPC server handler count
   *
   * @return RPC server
   * @throws IOException if there is an I/O error while creating RPC server
   */
  private static RPC.Server startRpcServer(OzoneConfiguration conf,
      InetSocketAddress addr, Class<?> protocol, BlockingService instance,
      int handlerCount) throws IOException {
    RPC.Server rpcServer = new RPC.Builder(conf)
        .setProtocol(protocol)
        .setInstance(instance)
        .setBindAddress(addr.getHostString())
        .setPort(addr.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .setSecretManager(null)
        .build();

    DFSUtil.addPBProtocol(conf, protocol, instance, rpcServer);
    return rpcServer;
  }

  /**
   * Main entry point for starting KeySpaceManager.
   *
   * @param argv arguments
   * @throws IOException if startup fails due to I/O error
   */
  public static void main(String[] argv) throws IOException {
    StringUtils.startupShutdownMessage(StorageContainerManager.class,
        argv, LOG);
    try {
      KeySpaceManager ksm = new KeySpaceManager(new OzoneConfiguration());
      ksm.start();
      ksm.join();
    } catch (Throwable t) {
      LOG.error("Failed to start the KeyspaceManager.", t);
      terminate(1, t);
    }
  }

  /**
   * Builds a message for logging startup information about an RPC server.
   *
   * @param description RPC server description
   * @param addr RPC server listening address
   * @return server startup message
   */
  private static String buildRpcServerStartMessage(String description,
      InetSocketAddress addr) {
    return addr != null ? String.format("%s is listening at %s",
        description, addr.toString()) :
        String.format("%s not started", description);
  }

  /**
   * After starting an RPC server, updates configuration with the actual
   * listening address of that server. The listening address may be different
   * from the configured address if, for example, the configured address uses
   * port 0 to request use of an ephemeral port.
   *
   * @param conf configuration to update
   * @param rpcAddressKey configuration key for RPC server address
   * @param addr configured address
   * @param rpcServer started RPC server.
   */
  private static InetSocketAddress updateListenAddress(OzoneConfiguration conf,
      String rpcAddressKey, InetSocketAddress addr, RPC.Server rpcServer) {
    InetSocketAddress listenAddr = rpcServer.getListenerAddress();
    InetSocketAddress updatedAddr = new InetSocketAddress(
        addr.getHostString(), listenAddr.getPort());
    conf.set(rpcAddressKey,
        listenAddr.getHostString() + ":" + listenAddr.getPort());
    return updatedAddr;
  }

  /**
   * Start service.
   */
  public void start() {
    LOG.info(buildRpcServerStartMessage("KeyspaceManager RPC server",
        ksmRpcAddress));
    ksmRpcServer.start();
  }

  /**
   * Wait until service has completed shutdown.
   */
  public void join() {
    try {
      ksmRpcServer.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted during KeyspaceManager join.");
    }
  }

  /**
   * Creates a volume.
   *
   * @param args - Arguments to create Volume.
   * @throws IOException
   */
  @Override
  public void createVolume(VolumeArgs args) throws IOException {

  }

  /**
   * Changes the owner of a volume.
   *
   * @param volume - Name of the volume.
   * @param owner - Name of the owner.
   * @throws IOException
   */
  @Override
  public void setOwner(String volume, String owner) throws IOException {

  }

  /**
   * Changes the Quota on a volume.
   *
   * @param volume - Name of the volume.
   * @param quota - Quota in bytes.
   * @throws IOException
   */
  @Override
  public void setQuota(String volume, long quota) throws IOException {

  }

  /**
   * Checks if the specified user can access this volume.
   *
   * @param volume - volume
   * @param userName - user name
   * @throws IOException
   */
  @Override
  public void checkVolumeAccess(String volume, String userName) throws
      IOException {

  }

  /**
   * Gets the volume information.
   *
   * @param volume - Volume name.s
   * @return VolumeArgs or exception is thrown.
   * @throws IOException
   */
  @Override
  public VolumeArgs getVolumeinfo(String volume) throws IOException {
    return null;
  }

  /**
   * Deletes the an exisiting empty volume.
   *
   * @param volume - Name of the volume.
   * @throws IOException
   */
  @Override
  public void deleteVolume(String volume) throws IOException {

  }

  /**
   * Lists volume owned by a specific user.
   *
   * @param userName - user name
   * @param prefix - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the
   * prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  @Override
  public List<VolumeArgs> listVolumeByUser(String userName, String prefix,
      String prevKey, long maxKeys) throws IOException {
    return null;
  }

  /**
   * Lists volume all volumes in the cluster.
   *
   * @param prefix - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the
   * prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  @Override
  public List<VolumeArgs> listAllVolumes(String prefix, String prevKey, long
      maxKeys) throws IOException {
    return null;
  }
}
