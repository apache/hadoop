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

package org.apache.hadoop.ozone.web.storage;

import com.google.common.base.Strings;
import org.apache.hadoop.hdfs.ozone.protocol.proto
    .ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdfs.ozone.protocol.proto
    .ContainerProtos.GetKeyResponseProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto
    .ContainerProtos.KeyData;
import org.apache.hadoop.hdfs.server.datanode.fsdataset
    .LengthInputStream;
import org.apache.hadoop.ksm.helpers.KsmBucketArgs;
import org.apache.hadoop.ksm.helpers.KsmBucketInfo;
import org.apache.hadoop.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ksm.helpers.KsmVolumeArgs;
import org.apache.hadoop.ksm.protocolPB
    .KeySpaceManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneConsts.Versioning;
import org.apache.hadoop.ozone.protocolPB.KSMPBHelper;
import org.apache.hadoop.ozone.ksm.KSMConfigKeys;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.ListArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.response.ListVolumes;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.response.VolumeOwner;
import org.apache.hadoop.ozone.web.response.ListBuckets;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.ListKeys;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.storage.ChunkInputStream;
import org.apache.hadoop.scm.storage.ChunkOutputStream;
import org.apache.hadoop.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.Locale;
import java.util.List;

/**
 * A {@link StorageHandler} implementation that distributes object storage
 * across the nodes of an HDFS cluster.
 */
public final class DistributedStorageHandler implements StorageHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(DistributedStorageHandler.class);

  private final StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private final KeySpaceManagerProtocolClientSideTranslatorPB
      keySpaceManagerClient;
  private final XceiverClientManager xceiverClientManager;
  private final OzoneAcl.OzoneACLRights userRights;
  private final OzoneAcl.OzoneACLRights groupRights;
  private int chunkSize;

  /**
   * Creates a new DistributedStorageHandler.
   *
   * @param conf configuration
   * @param storageContainerLocation StorageContainerLocationProtocol proxy
   * @param keySpaceManagerClient KeySpaceManager proxy
   */
  public DistributedStorageHandler(OzoneConfiguration conf,
      StorageContainerLocationProtocolClientSideTranslatorPB
                                       storageContainerLocation,
      KeySpaceManagerProtocolClientSideTranslatorPB
                                       keySpaceManagerClient) {
    this.keySpaceManagerClient = keySpaceManagerClient;
    this.storageContainerLocationClient = storageContainerLocation;
    this.xceiverClientManager = new XceiverClientManager(conf);

    chunkSize = conf.getInt(ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY,
        ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT);
    userRights = conf.getEnum(KSMConfigKeys.OZONE_KSM_USER_RIGHTS,
        KSMConfigKeys.OZONE_KSM_USER_RIGHTS_DEFAULT);
    groupRights = conf.getEnum(KSMConfigKeys.OZONE_KSM_GROUP_RIGHTS,
        KSMConfigKeys.OZONE_KSM_GROUP_RIGHTS_DEFAULT);
    if(chunkSize > ScmConfigKeys.OZONE_SCM_CHUNK_MAX_SIZE) {
      LOG.warn("The chunk size ({}) is not allowed to be more than"
          + " the maximum size ({}),"
          + " resetting to the maximum size.",
          chunkSize, ScmConfigKeys.OZONE_SCM_CHUNK_MAX_SIZE);
      chunkSize = ScmConfigKeys.OZONE_SCM_CHUNK_MAX_SIZE;
    }
  }

  @Override
  public void createVolume(VolumeArgs args) throws IOException, OzoneException {
    long quota = args.getQuota() == null ?
        OzoneConsts.MAX_QUOTA_IN_BYTES : args.getQuota().sizeInBytes();
    OzoneAcl userAcl =
        new OzoneAcl(OzoneAcl.OzoneACLType.USER,
            args.getUserName(), userRights);
    KsmVolumeArgs.Builder builder = KsmVolumeArgs.newBuilder();
    builder.setAdminName(args.getAdminName())
        .setOwnerName(args.getUserName())
        .setVolume(args.getVolumeName())
        .setQuotaInBytes(quota)
        .addOzoneAcls(KSMPBHelper.convertOzoneAcl(userAcl));
    if (args.getGroups() != null) {
      for (String group : args.getGroups()) {
        OzoneAcl groupAcl =
            new OzoneAcl(OzoneAcl.OzoneACLType.GROUP, group, groupRights);
        builder.addOzoneAcls(KSMPBHelper.convertOzoneAcl(groupAcl));
      }
    }
    keySpaceManagerClient.createVolume(builder.build());
  }

  @Override
  public void setVolumeOwner(VolumeArgs args) throws
      IOException, OzoneException {
    keySpaceManagerClient.setOwner(args.getVolumeName(), args.getUserName());
  }

  @Override
  public void setVolumeQuota(VolumeArgs args, boolean remove)
      throws IOException, OzoneException {
    long quota = remove ? OzoneConsts.MAX_QUOTA_IN_BYTES :
                                   args.getQuota().sizeInBytes();
    keySpaceManagerClient.setQuota(args.getVolumeName(), quota);
  }

  @Override
  public boolean checkVolumeAccess(String volume, OzoneAcl acl)
      throws IOException, OzoneException {
    return keySpaceManagerClient
        .checkVolumeAccess(volume, KSMPBHelper.convertOzoneAcl(acl));
  }

  @Override
  public ListVolumes listVolumes(ListArgs args)
      throws IOException, OzoneException {
    throw new UnsupportedOperationException("listVolumes not implemented");
  }

  @Override
  public void deleteVolume(VolumeArgs args)
      throws IOException, OzoneException {
    keySpaceManagerClient.deleteVolume(args.getVolumeName());
  }

  @Override
  public VolumeInfo getVolumeInfo(VolumeArgs args)
      throws IOException, OzoneException {
    KsmVolumeArgs volumeArgs =
        keySpaceManagerClient.getVolumeInfo(args.getVolumeName());
    //TODO: add support for createdOn and other fields in getVolumeInfo
    VolumeInfo volInfo =
        new VolumeInfo(volumeArgs.getVolume(), null,
            volumeArgs.getAdminName());
    volInfo.setOwner(new VolumeOwner(volumeArgs.getOwnerName()));
    volInfo.setQuota(OzoneQuota.getOzoneQuota(volumeArgs.getQuotaInBytes()));
    return volInfo;
  }

  @Override
  public void createBucket(final BucketArgs args)
      throws IOException, OzoneException {
    KsmBucketInfo.Builder builder = KsmBucketInfo.newBuilder();
    builder.setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName());
    if(args.getAddAcls() != null) {
      builder.setAcls(args.getAddAcls());
    }
    if(args.getStorageType() != null) {
      builder.setStorageType(args.getStorageType());
    }
    if(args.getVersioning() != null) {
      builder.setIsVersionEnabled(getBucketVersioningProtobuf(
          args.getVersioning()));
    }
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
  public void setBucketAcls(BucketArgs args)
      throws IOException, OzoneException {
    List<OzoneAcl> removeAcls = args.getRemoveAcls();
    List<OzoneAcl> addAcls = args.getAddAcls();
    if(removeAcls != null || addAcls != null) {
      KsmBucketArgs.Builder builder = KsmBucketArgs.newBuilder();
      builder.setVolumeName(args.getVolumeName())
          .setBucketName(args.getBucketName());
      if(removeAcls != null && !removeAcls.isEmpty()) {
        builder.setRemoveAcls(args.getRemoveAcls());
      }
      if(addAcls != null && !addAcls.isEmpty()) {
        builder.setAddAcls(args.getAddAcls());
      }
      keySpaceManagerClient.setBucketProperty(builder.build());
    }
  }

  @Override
  public void setBucketVersioning(BucketArgs args)
      throws IOException, OzoneException {
    KsmBucketArgs.Builder builder = KsmBucketArgs.newBuilder();
    builder.setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setIsVersionEnabled(getBucketVersioningProtobuf(
            args.getVersioning()));
    keySpaceManagerClient.setBucketProperty(builder.build());
  }

  @Override
  public void setBucketStorageClass(BucketArgs args)
      throws IOException, OzoneException {
    KsmBucketArgs.Builder builder = KsmBucketArgs.newBuilder();
    builder.setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setStorageType(args.getStorageType());
    keySpaceManagerClient.setBucketProperty(builder.build());
  }

  @Override
  public void deleteBucket(BucketArgs args)
      throws IOException, OzoneException {
    keySpaceManagerClient.deleteBucket(args.getVolumeName(),
        args.getBucketName());
  }

  @Override
  public void checkBucketAccess(BucketArgs args)
      throws IOException, OzoneException {
    throw new UnsupportedOperationException(
        "checkBucketAccess not implemented");
  }

  @Override
  public ListBuckets listBuckets(ListArgs args)
      throws IOException, OzoneException {
    ListBuckets result = new ListBuckets();
    UserArgs userArgs = args.getArgs();
    if (userArgs instanceof VolumeArgs) {
      VolumeArgs va = (VolumeArgs) userArgs;
      if (Strings.isNullOrEmpty(va.getVolumeName())) {
        throw new IllegalArgumentException("Illegal argument,"
            + " volume name cannot be null or empty.");
      }

      int maxNumOfKeys = args.getMaxKeys();
      if (maxNumOfKeys <= 0 ||
          maxNumOfKeys > OzoneConsts.MAX_LISTBUCKETS_SIZE) {
        throw new IllegalArgumentException(
            String.format("Illegal max number of keys specified,"
                + " the value must be in range (0, %d], actual : %d.",
                OzoneConsts.MAX_LISTBUCKETS_SIZE, maxNumOfKeys));
      }

      List<KsmBucketInfo> buckets =
          keySpaceManagerClient.listBuckets(va.getVolumeName(),
              args.getPrevKey(), args.getPrefix(), args.getMaxKeys());

      // Convert the result for the web layer.
      for (KsmBucketInfo bucketInfo : buckets) {
        BucketInfo bk = new BucketInfo();
        bk.setVolumeName(bucketInfo.getVolumeName());
        bk.setBucketName(bucketInfo.getBucketName());
        bk.setStorageType(bucketInfo.getStorageType());
        bk.setAcls(bucketInfo.getAcls());
        result.addBucket(bk);
      }
      return result;
    } else {
      throw new IllegalArgumentException("Illegal argument provided,"
          + " expecting VolumeArgs type but met "
          + userArgs.getClass().getSimpleName());
    }
  }

  @Override
  public BucketInfo getBucketInfo(BucketArgs args)
      throws IOException {
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    KsmBucketInfo ksmBucketInfo = keySpaceManagerClient.getBucketInfo(
        volumeName, bucketName);
    BucketInfo bucketInfo = new BucketInfo(ksmBucketInfo.getVolumeName(),
        ksmBucketInfo.getBucketName());
    if(ksmBucketInfo.getIsVersionEnabled()) {
      bucketInfo.setVersioning(Versioning.ENABLED);
    } else {
      bucketInfo.setVersioning(Versioning.DISABLED);
    }
    bucketInfo.setStorageType(ksmBucketInfo.getStorageType());
    bucketInfo.setAcls(ksmBucketInfo.getAcls());
    return bucketInfo;
  }

  @Override
  public OutputStream newKeyWriter(KeyArgs args) throws IOException,
      OzoneException {
    KsmKeyArgs keyArgs = new KsmKeyArgs.Builder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getSize())
        .build();
    // contact KSM to allocate a block for key.
    String containerKey = buildContainerKey(args.getVolumeName(),
        args.getBucketName(), args.getKeyName());
    KsmKeyInfo keyInfo = keySpaceManagerClient.allocateKey(keyArgs);
    // TODO the following createContainer and key writes may fail, in which
    // case we should revert the above allocateKey to KSM.
    String containerName = keyInfo.getContainerName();
    XceiverClientSpi xceiverClient = getContainer(containerName);
    if (keyInfo.getShouldCreateContainer()) {
      LOG.debug("Need to create container {} for key: {}/{}/{}", containerName,
          args.getVolumeName(), args.getBucketName(), args.getKeyName());
      ContainerProtocolCalls.createContainer(
          xceiverClient, args.getRequestID());
    }
    // establish a connection to the container to write the key
    return new ChunkOutputStream(containerKey, args.getKeyName(),
        xceiverClientManager, xceiverClient, args.getRequestID(), chunkSize);
  }

  @Override
  public void commitKey(KeyArgs args, OutputStream stream) throws
      IOException, OzoneException {
    stream.close();
  }

  @Override
  public LengthInputStream newKeyReader(KeyArgs args) throws IOException,
      OzoneException {
    KsmKeyArgs keyArgs = new KsmKeyArgs.Builder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getSize())
        .build();
    KsmKeyInfo keyInfo = keySpaceManagerClient.lookupKey(keyArgs);
    String containerKey = buildContainerKey(args.getVolumeName(),
        args.getBucketName(), args.getKeyName());
    String containerName = keyInfo.getContainerName();
    XceiverClientSpi xceiverClient = getContainer(containerName);
    boolean success = false;
    try {
      LOG.debug("get key accessing {} {}",
          xceiverClient.getPipeline().getContainerName(), containerKey);
      KeyData containerKeyData = OzoneContainerTranslation
          .containerKeyDataForRead(
              xceiverClient.getPipeline().getContainerName(), containerKey);
      GetKeyResponseProto response = ContainerProtocolCalls
          .getKey(xceiverClient, containerKeyData, args.getRequestID());
      long length = 0;
      List<ChunkInfo> chunks = response.getKeyData().getChunksList();
      for (ChunkInfo chunk : chunks) {
        length += chunk.getLen();
      }
      success = true;
      return new LengthInputStream(new ChunkInputStream(
          containerKey, xceiverClientManager, xceiverClient,
          chunks, args.getRequestID()), length);
    } finally {
      if (!success) {
        xceiverClientManager.releaseClient(xceiverClient);
      }
    }
  }

  @Override
  public void deleteKey(KeyArgs args) throws IOException, OzoneException {
    KsmKeyArgs keyArgs = new KsmKeyArgs.Builder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .build();
    keySpaceManagerClient.deleteKey(keyArgs);
  }

  @Override
  public ListKeys listKeys(ListArgs args) throws IOException, OzoneException {
    throw new UnsupportedOperationException("listKeys not implemented");
  }

  private XceiverClientSpi getContainer(String containerName)
      throws IOException {
    Pipeline pipeline =
        storageContainerLocationClient.getContainer(containerName);
    return xceiverClientManager.acquireClient(pipeline);
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

  /**
   * Formats a date in the expected string format.
   *
   * @param date the date to format
   * @return formatted string representation of date
   */
  private static String dateToString(Date date) {
    SimpleDateFormat sdf =
        new SimpleDateFormat(OzoneConsts.OZONE_DATE_FORMAT, Locale.US);
    sdf.setTimeZone(TimeZone.getTimeZone(OzoneConsts.OZONE_TIME_ZONE));
    return sdf.format(date);
  }
}
