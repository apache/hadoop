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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ksm.helpers.KsmBucketInfo;
import org.apache.hadoop.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ksm.helpers.KsmVolumeArgs;
import org.apache.hadoop.ksm.protocolPB
    .KeySpaceManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ksm.protocolPB
    .KeySpaceManagerProtocolPB;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.io.OzoneInputStream;
import org.apache.hadoop.ozone.io.OzoneOutputStream;
import org.apache.hadoop.ozone.ksm.KSMConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts.Versioning;
import org.apache.hadoop.ozone.protocolPB.KSMPBHelper;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.protocolPB
    .StorageContainerLocationProtocolPB;
import org.apache.hadoop.scm.storage.ChunkOutputStream;
import org.apache.hadoop.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Ozone Client Implementation, it connects to KSM, SCM and DataNode
 * to execute client calls. This uses RPC protocol for communication
 * with the servers.
 */
public class OzoneClientImpl implements OzoneClient, Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneClient.class);

  private final StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private final KeySpaceManagerProtocolClientSideTranslatorPB
      keySpaceManagerClient;
  private final XceiverClientManager xceiverClientManager;
  private final int chunkSize;


  private final UserGroupInformation ugi;
  private final OzoneAcl.OzoneACLRights userRights;
  private final OzoneAcl.OzoneACLRights groupRights;

  /**
   * Creates OzoneClientImpl instance with new OzoneConfiguration.
   *
   * @throws IOException
   */
  public OzoneClientImpl() throws IOException {
    this(new OzoneConfiguration());
  }

   /**
    * Creates OzoneClientImpl instance with the given configuration.
    *
    * @param conf
    *
    * @throws IOException
    */
  public OzoneClientImpl(Configuration conf) throws IOException {
    Preconditions.checkNotNull(conf);
    this.ugi = UserGroupInformation.getCurrentUser();
    this.userRights = conf.getEnum(KSMConfigKeys.OZONE_KSM_USER_RIGHTS,
        KSMConfigKeys.OZONE_KSM_USER_RIGHTS_DEFAULT);
    this.groupRights = conf.getEnum(KSMConfigKeys.OZONE_KSM_GROUP_RIGHTS,
        KSMConfigKeys.OZONE_KSM_GROUP_RIGHTS_DEFAULT);

    long scmVersion =
        RPC.getProtocolVersion(StorageContainerLocationProtocolPB.class);
    InetSocketAddress scmAddress =
        OzoneClientUtils.getScmAddressForClients(conf);
    RPC.setProtocolEngine(conf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    this.storageContainerLocationClient =
        new StorageContainerLocationProtocolClientSideTranslatorPB(
            RPC.getProxy(StorageContainerLocationProtocolPB.class, scmVersion,
                scmAddress, UserGroupInformation.getCurrentUser(), conf,
                NetUtils.getDefaultSocketFactory(conf),
                Client.getRpcTimeout(conf)));

    long ksmVersion =
        RPC.getProtocolVersion(KeySpaceManagerProtocolPB.class);
    InetSocketAddress ksmAddress = OzoneClientUtils.getKsmAddress(conf);
    RPC.setProtocolEngine(conf, KeySpaceManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    this.keySpaceManagerClient =
        new KeySpaceManagerProtocolClientSideTranslatorPB(
            RPC.getProxy(KeySpaceManagerProtocolPB.class, ksmVersion,
                ksmAddress, UserGroupInformation.getCurrentUser(), conf,
                NetUtils.getDefaultSocketFactory(conf),
                Client.getRpcTimeout(conf)));

    this.xceiverClientManager = new XceiverClientManager(conf);

    int configuredChunkSize = conf.getInt(
        ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY,
        ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT);
    if(configuredChunkSize > ScmConfigKeys.OZONE_SCM_CHUNK_MAX_SIZE) {
      LOG.warn("The chunk size ({}) is not allowed to be more than"
              + " the maximum size ({}),"
              + " resetting to the maximum size.",
          configuredChunkSize, ScmConfigKeys.OZONE_SCM_CHUNK_MAX_SIZE);
      chunkSize = ScmConfigKeys.OZONE_SCM_CHUNK_MAX_SIZE;
    } else {
      chunkSize = configuredChunkSize;
    }
  }

  @Override
  public void createVolume(String volumeName)
      throws IOException {
    createVolume(volumeName, ugi.getUserName());
  }

  @Override
  public void createVolume(String volumeName, String owner)
      throws IOException {

    createVolume(volumeName, owner, OzoneConsts.MAX_QUOTA_IN_BYTES,
        (OzoneAcl[])null);
  }

  @Override
  public void createVolume(String volumeName, String owner,
                           OzoneAcl... acls)
      throws IOException {
    createVolume(volumeName, owner, OzoneConsts.MAX_QUOTA_IN_BYTES, acls);
  }

  @Override
  public void createVolume(String volumeName, String owner,
                           long quota)
      throws IOException {
    createVolume(volumeName, owner, quota, (OzoneAcl[])null);
  }

  @Override
  public void createVolume(String volumeName, String owner,
                           long quota, OzoneAcl... acls)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(owner);
    Preconditions.checkNotNull(quota);
    Preconditions.checkState(quota >= 0);
    OzoneAcl userAcl =
        new OzoneAcl(OzoneAcl.OzoneACLType.USER,
            owner, userRights);
    KsmVolumeArgs.Builder builder = KsmVolumeArgs.newBuilder();
    builder.setAdminName(ugi.getUserName())
        .setOwnerName(owner)
        .setVolume(volumeName)
        .setQuotaInBytes(quota)
        .addOzoneAcls(KSMPBHelper.convertOzoneAcl(userAcl));

    List<OzoneAcl> listOfAcls = new ArrayList<>();

    //Group ACLs of the User
    List<String> userGroups = Arrays.asList(UserGroupInformation
        .createRemoteUser(owner).getGroupNames());
    userGroups.stream().forEach((group) -> listOfAcls.add(
        new OzoneAcl(OzoneAcl.OzoneACLType.GROUP, group, groupRights)));

    //ACLs passed as argument
    if(acls != null) {
      listOfAcls.addAll(Arrays.asList(acls));
    }

    //Remove duplicates and set
    for (OzoneAcl ozoneAcl :
        listOfAcls.stream().distinct().collect(Collectors.toList())) {
      builder.addOzoneAcls(KSMPBHelper.convertOzoneAcl(ozoneAcl));
    }

    LOG.info("Creating Volume: {}, with {} as owner and quota set to {} bytes.",
        volumeName, owner, quota);
    keySpaceManagerClient.createVolume(builder.build());
  }

  @Override
  public void setVolumeOwner(String volumeName, String owner)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(owner);
    keySpaceManagerClient.setOwner(volumeName, owner);
  }

  @Override
  public void setVolumeQuota(String volumeName, long quota)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(quota);
    Preconditions.checkState(quota >= 0);
    keySpaceManagerClient.setQuota(volumeName, quota);
  }

  @Override
  public OzoneVolume getVolumeDetails(String volumeName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    KsmVolumeArgs volumeArgs =
        keySpaceManagerClient.getVolumeInfo(volumeName);
    return new OzoneVolume(volumeArgs);
  }

  @Override
  public boolean checkVolumeAccess(String volumeName, OzoneAcl acl)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    return keySpaceManagerClient.checkVolumeAccess(volumeName,
        KSMPBHelper.convertOzoneAcl(acl));
  }

  @Override
  public void deleteVolume(String volumeName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    keySpaceManagerClient.deleteVolume(volumeName);
  }

  @Override
  public Iterator<OzoneVolume> listVolumes(String volumePrefix)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public Iterator<OzoneVolume> listVolumes(String volumePrefix,
                                             String user)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void createBucket(String volumeName, String bucketName)
      throws IOException {
    createBucket(volumeName, bucketName, Versioning.NOT_DEFINED,
        StorageType.DEFAULT, (OzoneAcl[])null);
  }

  @Override
  public void createBucket(String volumeName, String bucketName,
                           Versioning versioning)
      throws IOException {
    createBucket(volumeName, bucketName, versioning,
        StorageType.DEFAULT, (OzoneAcl[])null);
  }

  @Override
  public void createBucket(String volumeName, String bucketName,
                           StorageType storageType)
      throws IOException {
    createBucket(volumeName, bucketName, Versioning.NOT_DEFINED,
        storageType, (OzoneAcl[])null);
  }

  @Override
  public void createBucket(String volumeName, String bucketName,
                           OzoneAcl... acls)
      throws IOException {
    createBucket(volumeName, bucketName, Versioning.NOT_DEFINED,
        StorageType.DEFAULT, acls);
  }

  @Override
  public void createBucket(String volumeName, String bucketName,
                           Versioning versioning, StorageType storageType,
                           OzoneAcl... acls)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(versioning);
    Preconditions.checkNotNull(storageType);

    KsmBucketInfo.Builder builder = KsmBucketInfo.newBuilder();
    builder.setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setStorageType(storageType)
        .setIsVersionEnabled(getBucketVersioningProtobuf(
        versioning));

    String owner = ugi.getUserName();
    final List<OzoneAcl> listOfAcls = new ArrayList<>();

    //User ACL
    OzoneAcl userAcl =
        new OzoneAcl(OzoneAcl.OzoneACLType.USER,
            owner, userRights);
    listOfAcls.add(userAcl);

    //Group ACLs of the User
    List<String> userGroups = Arrays.asList(UserGroupInformation
        .createRemoteUser(owner).getGroupNames());
    userGroups.stream().forEach((group) -> listOfAcls.add(
        new OzoneAcl(OzoneAcl.OzoneACLType.GROUP, group, groupRights)));

    //ACLs passed as argument
    if(acls != null) {
      Arrays.stream(acls).forEach((acl) -> listOfAcls.add(acl));
    }

    //Remove duplicates and set
    builder.setAcls(listOfAcls.stream().distinct()
        .collect(Collectors.toList()));
    LOG.info("Creating Bucket: {}/{}, with Versioning {} and " +
        "Storage Type set to {}", volumeName, bucketName, versioning,
        storageType);
    keySpaceManagerClient.createBucket(builder.build());
  }

  /**
   * Converts OzoneConts.Versioning enum to boolean.
   *
   * @param version
   * @return corresponding boolean value
   */
  private boolean getBucketVersioningProtobuf(
      Versioning version) {
    if(version != null) {
      switch(version) {
      case ENABLED:
        return true;
      case NOT_DEFINED:
      case DISABLED:
      default:
        return false;
      }
    }
    return false;
  }

  @Override
  public void addBucketAcls(String volumeName, String bucketName,
                            List<OzoneAcl> addAcls)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void removeBucketAcls(String volumeName, String bucketName,
                               List<OzoneAcl> removeAcls)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void setBucketVersioning(String volumeName, String bucketName,
                                  Versioning versioning)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void setBucketStorageType(String volumeName, String bucketName,
                                   StorageType storageType)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void deleteBucket(String volumeName, String bucketName)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void checkBucketAccess(String volumeName, String bucketName)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public OzoneBucket getBucketDetails(String volumeName,
                                        String bucketName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    KsmBucketInfo bucketInfo =
        keySpaceManagerClient.getBucketInfo(volumeName, bucketName);
    return new OzoneBucket(bucketInfo);
  }

  @Override
  public Iterator<OzoneBucket> listBuckets(String volumeName,
                                            String bucketPrefix)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public OzoneOutputStream createKey(String volumeName, String bucketName,
                                     String keyName, long size)
      throws IOException {
    String requestId = UUID.randomUUID().toString();
    KsmKeyArgs keyArgs = new KsmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(size)
        .build();

    String containerKey = buildContainerKey(volumeName, bucketName, keyName);
    KsmKeyInfo keyInfo = keySpaceManagerClient.allocateKey(keyArgs);
    // TODO: the following createContainer and key writes may fail, in which
    // case we should revert the above allocateKey to KSM.
    String containerName = keyInfo.getContainerName();
    XceiverClientSpi xceiverClient = getContainer(containerName);
    if (keyInfo.getShouldCreateContainer()) {
      LOG.debug("Need to create container {} for key: {}/{}/{}", containerName,
          volumeName, bucketName, keyName);
      ContainerProtocolCalls.createContainer(xceiverClient, requestId);
    }
    // establish a connection to the container to write the key
    ChunkOutputStream outputStream = new ChunkOutputStream(containerKey,
        keyName, xceiverClientManager, xceiverClient, requestId, chunkSize);
    return new OzoneOutputStream(outputStream);
  }

  /**
   * Creates a container key from any number of components by combining all
   * components with a delimiter.
   *
   * @param parts container key components
   * @return container key
   */
  private static String buildContainerKey(String... parts) {
    return '/' + StringUtils.join('/', parts);
  }

  private XceiverClientSpi getContainer(String containerName)
      throws IOException {
    Pipeline pipeline =
        storageContainerLocationClient.getContainer(containerName);
    return xceiverClientManager.acquireClient(pipeline);
  }

  @Override
  public OzoneInputStream getKey(String volumeName, String bucketName,
                                 String keyName)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public void deleteKey(String volumeName, String bucketName,
                        String keyName)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public List<OzoneKey> listKeys(String volumeName, String bucketName,
                                   String keyPrefix)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public OzoneKey getkeyDetails(String volumeName, String bucketName,
                                  String keyName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(keyName);
    KsmKeyArgs keyArgs = new KsmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
    KsmKeyInfo keyInfo =
        keySpaceManagerClient.lookupKey(keyArgs);
    return new OzoneKey(keyInfo);
  }

  @Override
  public void close() throws IOException {
    if(xceiverClientManager != null) {
      xceiverClientManager.close();
    }
  }
}
