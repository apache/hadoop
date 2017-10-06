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
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.jmx.ServiceRuntimeInfoImpl;
import org.apache.hadoop.ozone.ksm.helpers.KsmBucketArgs;
import org.apache.hadoop.ozone.ksm.helpers.KsmBucketInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyLocationInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmVolumeArgs;
import org.apache.hadoop.ozone.ksm.helpers.OpenKeySession;
import org.apache.hadoop.ozone.ksm.protocol.KeySpaceManagerProtocol;
import org.apache.hadoop.ozone.ksm.protocolPB.KeySpaceManagerProtocolPB;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocolPB
    .KeySpaceManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.scm.protocolPB.ScmBlockLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.ksm.KSMConfigKeys
    .OZONE_KSM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys
    .OZONE_KSM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys
    .OZONE_KSM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.KeySpaceManagerService
    .newReflectiveBlockingService;
import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * Ozone Keyspace manager is the metadata manager of ozone.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "CBLOCK", "OZONE", "HBASE"})
public class KeySpaceManager extends ServiceRuntimeInfoImpl
    implements KeySpaceManagerProtocol, KSMMXBean {
  private static final Logger LOG =
      LoggerFactory.getLogger(KeySpaceManager.class);

  private final RPC.Server ksmRpcServer;
  private final InetSocketAddress ksmRpcAddress;
  private final KSMMetadataManager metadataManager;
  private final VolumeManager volumeManager;
  private final BucketManager bucketManager;
  private final KeyManager keyManager;
  private final KSMMetrics metrics;
  private final KeySpaceManagerHttpServer httpServer;
  private ObjectName ksmInfoBeanName;

  public KeySpaceManager(OzoneConfiguration conf) throws IOException {
    final int handlerCount = conf.getInt(OZONE_KSM_HANDLER_COUNT_KEY,
        OZONE_KSM_HANDLER_COUNT_DEFAULT);

    RPC.setProtocolEngine(conf, KeySpaceManagerProtocolPB.class,
        ProtobufRpcEngine.class);

    BlockingService ksmService = newReflectiveBlockingService(
        new KeySpaceManagerProtocolServerSideTranslatorPB(this));
    final InetSocketAddress ksmNodeRpcAddr = OzoneClientUtils.
        getKsmAddress(conf);
    ksmRpcServer = startRpcServer(conf, ksmNodeRpcAddr,
        KeySpaceManagerProtocolPB.class, ksmService,
        handlerCount);
    ksmRpcAddress = OzoneClientUtils.updateRPCListenAddress(conf,
        OZONE_KSM_ADDRESS_KEY, ksmNodeRpcAddr, ksmRpcServer);
    metadataManager = new KSMMetadataManagerImpl(conf);
    volumeManager = new VolumeManagerImpl(metadataManager, conf);
    bucketManager = new BucketManagerImpl(metadataManager);
    metrics = KSMMetrics.create();
    keyManager = new KeyManagerImpl(
        getScmBlockClient(conf), metadataManager, conf);
    httpServer = new KeySpaceManagerHttpServer(conf);
  }

  /**
   * Create a scm block client, used by putKey() and getKey().
   *
   * @param conf
   * @return
   * @throws IOException
   */
  private ScmBlockLocationProtocol getScmBlockClient(OzoneConfiguration conf)
      throws IOException {
    RPC.setProtocolEngine(conf, ScmBlockLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    long scmVersion =
        RPC.getProtocolVersion(ScmBlockLocationProtocolPB.class);
    InetSocketAddress scmBlockAddress =
        OzoneClientUtils.getScmAddressForBlockClients(conf);
    ScmBlockLocationProtocolClientSideTranslatorPB scmBlockLocationClient =
        new ScmBlockLocationProtocolClientSideTranslatorPB(
            RPC.getProxy(ScmBlockLocationProtocolPB.class, scmVersion,
                scmBlockAddress, UserGroupInformation.getCurrentUser(), conf,
                NetUtils.getDefaultSocketFactory(conf),
                Client.getRpcTimeout(conf)));
    return scmBlockLocationClient;
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
   * Get metadata manager.
   * @return metadata manager.
   */
  public KSMMetadataManager getMetadataManager() {
    return metadataManager;
  }

  public KSMMetrics getMetrics() {
    return metrics;
  }

  /**
   * Main entry point for starting KeySpaceManager.
   *
   * @param argv arguments
   * @throws IOException if startup fails due to I/O error
   */
  public static void main(String[] argv) throws IOException {
    StringUtils.startupShutdownMessage(KeySpaceManager.class, argv, LOG);
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
   * Start service.
   */
  public void start() throws IOException {
    LOG.info(buildRpcServerStartMessage("KeyspaceManager RPC server",
        ksmRpcAddress));
    DefaultMetricsSystem.initialize("KeySpaceManager");
    metadataManager.start();
    keyManager.start();
    ksmRpcServer.start();
    httpServer.start();
    registerMXBean();
    setStartTime();
  }

  /**
   * Stop service.
   */
  public void stop() {
    try {
      metadataManager.stop();
      ksmRpcServer.stop();
      keyManager.stop();
      httpServer.stop();
      metrics.unRegister();
      unregisterMXBean();
    } catch (Exception e) {
      LOG.error("Key Space Manager stop failed.", e);
    }
  }

  /**
   * Wait until service has completed shutdown.
   */
  public void join() {
    try {
      ksmRpcServer.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted during KeyspaceManager join.", e);
    }
  }

  /**
   * Creates a volume.
   *
   * @param args - Arguments to create Volume.
   * @throws IOException
   */
  @Override
  public void createVolume(KsmVolumeArgs args) throws IOException {
    try {
      metrics.incNumVolumeCreates();
      volumeManager.createVolume(args);
    } catch (Exception ex) {
      metrics.incNumVolumeCreateFails();
      throw ex;
    }
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
    try {
      metrics.incNumVolumeUpdates();
      volumeManager.setOwner(volume, owner);
    } catch (Exception ex) {
      metrics.incNumVolumeUpdateFails();
      throw ex;
    }
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
    try {
      metrics.incNumVolumeUpdates();
      volumeManager.setQuota(volume, quota);
    } catch (Exception ex) {
      metrics.incNumVolumeUpdateFails();
      throw ex;
    }
  }

  /**
   * Checks if the specified user can access this volume.
   *
   * @param volume - volume
   * @param userAcl - user acls which needs to be checked for access
   * @return true if the user has required access for the volume,
   *         false otherwise
   * @throws IOException
   */
  @Override
  public boolean checkVolumeAccess(String volume, OzoneAclInfo userAcl)
      throws IOException {
    try {
      metrics.incNumVolumeCheckAccesses();
      return volumeManager.checkVolumeAccess(volume, userAcl);
    } catch (Exception ex) {
      metrics.incNumVolumeCheckAccessFails();
      throw ex;
    }
  }

  /**
   * Gets the volume information.
   *
   * @param volume - Volume name.
   * @return VolumeArgs or exception is thrown.
   * @throws IOException
   */
  @Override
  public KsmVolumeArgs getVolumeInfo(String volume) throws IOException {
    try {
      metrics.incNumVolumeInfos();
      return volumeManager.getVolumeInfo(volume);
    } catch (Exception ex) {
      metrics.incNumVolumeInfoFails();
      throw ex;
    }
  }

  /**
   * Deletes an existing empty volume.
   *
   * @param volume - Name of the volume.
   * @throws IOException
   */
  @Override
  public void deleteVolume(String volume) throws IOException {
    try {
      metrics.incNumVolumeDeletes();
      volumeManager.deleteVolume(volume);
    } catch (Exception ex) {
      metrics.incNumVolumeDeleteFails();
      throw ex;
    }
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
  public List<KsmVolumeArgs> listVolumeByUser(String userName, String prefix,
      String prevKey, int maxKeys) throws IOException {
    try {
      metrics.incNumVolumeLists();
      return volumeManager.listVolumes(userName, prefix, prevKey, maxKeys);
    } catch (Exception ex) {
      metrics.incNumVolumeListFails();
      throw ex;
    }
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
  public List<KsmVolumeArgs> listAllVolumes(String prefix, String prevKey, int
      maxKeys) throws IOException {
    try {
      metrics.incNumVolumeLists();
      return volumeManager.listVolumes(null, prefix, prevKey, maxKeys);
    } catch (Exception ex) {
      metrics.incNumVolumeListFails();
      throw ex;
    }
  }

  /**
   * Creates a bucket.
   *
   * @param bucketInfo - BucketInfo to create bucket.
   * @throws IOException
   */
  @Override
  public void createBucket(KsmBucketInfo bucketInfo) throws IOException {
    try {
      metrics.incNumBucketCreates();
      bucketManager.createBucket(bucketInfo);
    } catch (Exception ex) {
      metrics.incNumBucketCreateFails();
      throw ex;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<KsmBucketInfo> listBuckets(String volumeName,
      String startKey, String prefix, int maxNumOfBuckets)
      throws IOException {
    try {
      metrics.incNumBucketLists();
      return bucketManager.listBuckets(volumeName,
          startKey, prefix, maxNumOfBuckets);
    } catch (IOException ex) {
      metrics.incNumBucketListFails();
      throw ex;
    }
  }

  /**
   * Gets the bucket information.
   *
   * @param volume - Volume name.
   * @param bucket - Bucket name.
   * @return KsmBucketInfo or exception is thrown.
   * @throws IOException
   */
  @Override
  public KsmBucketInfo getBucketInfo(String volume, String bucket)
      throws IOException {
    try {
      metrics.incNumBucketInfos();
      return bucketManager.getBucketInfo(volume, bucket);
    } catch (Exception ex) {
      metrics.incNumBucketInfoFails();
      throw ex;
    }
  }

  /**
   * Allocate a key.
   *
   * @param args - attributes of the key.
   * @return KsmKeyInfo - the info about the allocated key.
   * @throws IOException
   */
  @Override
  public OpenKeySession openKey(KsmKeyArgs args) throws IOException {
    try {
      metrics.incNumKeyAllocates();
      return keyManager.openKey(args);
    } catch (Exception ex) {
      metrics.incNumKeyAllocateFails();
      throw ex;
    }
  }

  @Override
  public void commitKey(KsmKeyArgs args, int clientID)
      throws IOException {
    try {
      metrics.incNumKeyCommits();
      keyManager.commitKey(args, clientID);
    } catch (Exception ex) {
      metrics.incNumKeyCommitFails();
      throw ex;
    }
  }

  @Override
  public KsmKeyLocationInfo allocateBlock(KsmKeyArgs args, int clientID)
      throws IOException {
    try {
      metrics.incNumBlockAllocateCalls();
      return keyManager.allocateBlock(args, clientID);
    } catch (Exception ex) {
      metrics.incNumBlockAllocateCallFails();
      throw ex;
    }
  }

  /**
   * Lookup a key.
   *
   * @param args - attributes of the key.
   * @return KsmKeyInfo - the info about the requested key.
   * @throws IOException
   */
  @Override
  public KsmKeyInfo lookupKey(KsmKeyArgs args) throws IOException {
    try {
      metrics.incNumKeyLookups();
      return keyManager.lookupKey(args);
    } catch (Exception ex) {
      metrics.incNumKeyLookupFails();
      throw ex;
    }
  }

  /**
   * Deletes an existing key.
   *
   * @param args - attributes of the key.
   * @throws IOException
   */
  @Override
  public void deleteKey(KsmKeyArgs args) throws IOException {
    try {
      metrics.incNumKeyDeletes();
      keyManager.deleteKey(args);
    } catch (Exception ex) {
      metrics.incNumKeyDeleteFails();
      throw ex;
    }
  }

  @Override
  public List<KsmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix, int maxKeys) throws IOException {
    try {
      metrics.incNumKeyLists();
      return keyManager.listKeys(volumeName, bucketName,
          startKey, keyPrefix, maxKeys);
    } catch (IOException ex) {
      metrics.incNumKeyListFails();
      throw ex;
    }
  }

  /**
   * Sets bucket property from args.
   * @param args - BucketArgs.
   * @throws IOException
   */
  @Override
  public void setBucketProperty(KsmBucketArgs args)
      throws IOException {
    try {
      metrics.incNumBucketUpdates();
      bucketManager.setBucketProperty(args);
    } catch (Exception ex) {
      metrics.incNumBucketUpdateFails();
      throw ex;
    }
  }


  /**
   * Deletes an existing empty bucket from volume.
   * @param volume - Name of the volume.
   * @param bucket - Name of the bucket.
   * @throws IOException
   */
  public void deleteBucket(String volume, String bucket) throws IOException {
    try {
      metrics.incNumBucketDeletes();
      bucketManager.deleteBucket(volume, bucket);
    } catch (Exception ex) {
      metrics.incNumBucketDeleteFails();
      throw ex;
    }
  }

  private void registerMXBean() {
    Map<String, String> jmxProperties = new HashMap<String, String>();
    jmxProperties.put("component", "ServerRuntime");
    this.ksmInfoBeanName =
        MBeans.register("KeySpaceManager",
            "KeySpaceManagerInfo",
            jmxProperties,
            this);
  }

  private void unregisterMXBean() {
    if (this.ksmInfoBeanName != null) {
      MBeans.unregister(this.ksmInfoBeanName);
      this.ksmInfoBeanName = null;
    }
  }

  @Override
  public String getRpcPort() {
    return "" + ksmRpcAddress.getPort();
  }
}
