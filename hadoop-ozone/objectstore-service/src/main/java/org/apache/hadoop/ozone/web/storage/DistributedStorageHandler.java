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
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.scm.ByteStringHelper;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ChecksumType;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.LengthInputStream;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneConsts.Versioning;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.ListArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.response.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A {@link StorageHandler} implementation that distributes object storage
 * across the nodes of an HDFS cluster.
 */
public final class DistributedStorageHandler implements StorageHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(DistributedStorageHandler.class);

  private final StorageContainerLocationProtocol
      storageContainerLocationClient;
  private final OzoneManagerProtocol
      ozoneManagerClient;
  private final XceiverClientManager xceiverClientManager;
  private final OzoneAcl.OzoneACLRights userRights;
  private final OzoneAcl.OzoneACLRights groupRights;
  private int chunkSize;
  private final long streamBufferFlushSize;
  private final long streamBufferMaxSize;
  private final long watchTimeout;
  private final long blockSize;
  private final ChecksumType checksumType;
  private final int bytesPerChecksum;
  private final boolean verifyChecksum;
  private final int maxRetryCount;

  /**
   * Creates a new DistributedStorageHandler.
   *
   * @param conf configuration
   * @param storageContainerLocation StorageContainerLocationProtocol proxy
   * @param ozoneManagerClient OzoneManager proxy
   */
  public DistributedStorageHandler(OzoneConfiguration conf,
      StorageContainerLocationProtocol storageContainerLocation,
      OzoneManagerProtocol ozoneManagerClient) {
    this.ozoneManagerClient = ozoneManagerClient;
    this.storageContainerLocationClient = storageContainerLocation;
    this.xceiverClientManager = new XceiverClientManager(conf);

    chunkSize = (int)conf.getStorageSize(ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY,
        ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT, StorageUnit.BYTES);
    userRights = conf.getEnum(OMConfigKeys.OZONE_OM_USER_RIGHTS,
        OMConfigKeys.OZONE_OM_USER_RIGHTS_DEFAULT);
    groupRights = conf.getEnum(OMConfigKeys.OZONE_OM_GROUP_RIGHTS,
        OMConfigKeys.OZONE_OM_GROUP_RIGHTS_DEFAULT);
    if(chunkSize > OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE) {
      LOG.warn("The chunk size ({}) is not allowed to be more than"
              + " the maximum size ({}),"
              + " resetting to the maximum size.",
          chunkSize, OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE);
      chunkSize = OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE;
    }
    streamBufferFlushSize = (long) conf
        .getStorageSize(OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_FLUSH_SIZE,
            OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_FLUSH_SIZE_DEFAULT,
            StorageUnit.BYTES);
    streamBufferMaxSize = (long) conf
        .getStorageSize(OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_MAX_SIZE,
            OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_MAX_SIZE_DEFAULT,
            StorageUnit.BYTES);
    blockSize = (long) conf.getStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE,
        OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
    watchTimeout =
        conf.getTimeDuration(OzoneConfigKeys.OZONE_CLIENT_WATCH_REQUEST_TIMEOUT,
            OzoneConfigKeys.OZONE_CLIENT_WATCH_REQUEST_TIMEOUT_DEFAULT,
            TimeUnit.MILLISECONDS);

    int configuredChecksumSize = (int) conf.getStorageSize(
        OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM,
        OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM_DEFAULT,
        StorageUnit.BYTES);

    if(configuredChecksumSize <
        OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM_MIN_SIZE) {
      LOG.warn("The checksum size ({}) is not allowed to be less than the " +
              "minimum size ({}), resetting to the minimum size.",
          configuredChecksumSize,
          OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM_MIN_SIZE);
      bytesPerChecksum =
          OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM_MIN_SIZE;
    } else {
      bytesPerChecksum = configuredChecksumSize;
    }
    String checksumTypeStr = conf.get(
        OzoneConfigKeys.OZONE_CLIENT_CHECKSUM_TYPE,
        OzoneConfigKeys.OZONE_CLIENT_CHECKSUM_TYPE_DEFAULT);
    this.checksumType = ChecksumType.valueOf(checksumTypeStr);
    this.verifyChecksum =
        conf.getBoolean(OzoneConfigKeys.OZONE_CLIENT_VERIFY_CHECKSUM,
            OzoneConfigKeys.OZONE_CLIENT_VERIFY_CHECKSUM_DEFAULT);
    this.maxRetryCount =
        conf.getInt(OzoneConfigKeys.OZONE_CLIENT_MAX_RETRIES, OzoneConfigKeys.
            OZONE_CLIENT_MAX_RETRIES_DEFAULT);
    boolean isUnsafeByteOperationsEnabled = conf.getBoolean(
        OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED,
        OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED_DEFAULT);
    ByteStringHelper.init(isUnsafeByteOperationsEnabled);

  }

  @Override
  public void createVolume(VolumeArgs args) throws IOException, OzoneException {
    long quota = args.getQuota() == null ?
        OzoneConsts.MAX_QUOTA_IN_BYTES : args.getQuota().sizeInBytes();
    OzoneAcl userAcl =
        new OzoneAcl(OzoneAcl.OzoneACLType.USER,
            args.getUserName(), userRights);
    OmVolumeArgs.Builder builder = OmVolumeArgs.newBuilder();
    builder.setAdminName(args.getAdminName())
        .setOwnerName(args.getUserName())
        .setVolume(args.getVolumeName())
        .setQuotaInBytes(quota)
        .addOzoneAcls(OMPBHelper.convertOzoneAcl(userAcl));
    if (args.getGroups() != null) {
      for (String group : args.getGroups()) {
        OzoneAcl groupAcl =
            new OzoneAcl(OzoneAcl.OzoneACLType.GROUP, group, groupRights);
        builder.addOzoneAcls(OMPBHelper.convertOzoneAcl(groupAcl));
      }
    }
    ozoneManagerClient.createVolume(builder.build());
  }

  @Override
  public void setVolumeOwner(VolumeArgs args) throws
      IOException, OzoneException {
    ozoneManagerClient.setOwner(args.getVolumeName(), args.getUserName());
  }

  @Override
  public void setVolumeQuota(VolumeArgs args, boolean remove)
      throws IOException, OzoneException {
    long quota = remove ? OzoneConsts.MAX_QUOTA_IN_BYTES :
        args.getQuota().sizeInBytes();
    ozoneManagerClient.setQuota(args.getVolumeName(), quota);
  }

  @Override
  public boolean checkVolumeAccess(String volume, OzoneAcl acl)
      throws IOException, OzoneException {
    return ozoneManagerClient
        .checkVolumeAccess(volume, OMPBHelper.convertOzoneAcl(acl));
  }

  @Override
  public ListVolumes listVolumes(ListArgs args)
      throws IOException, OzoneException {
    int maxNumOfKeys = args.getMaxKeys();
    if (maxNumOfKeys <= 0 ||
        maxNumOfKeys > OzoneConsts.MAX_LISTVOLUMES_SIZE) {
      throw new IllegalArgumentException(
          String.format("Illegal max number of keys specified,"
                  + " the value must be in range (0, %d], actual : %d.",
              OzoneConsts.MAX_LISTVOLUMES_SIZE, maxNumOfKeys));
    }

    List<OmVolumeArgs> listResult;
    if (args.isRootScan()) {
      listResult = ozoneManagerClient.listAllVolumes(args.getPrefix(),
          args.getPrevKey(), args.getMaxKeys());
    } else {
      UserArgs userArgs = args.getArgs();
      if (userArgs == null || userArgs.getUserName() == null) {
        throw new IllegalArgumentException("Illegal argument,"
            + " missing user argument.");
      }
      listResult = ozoneManagerClient.listVolumeByUser(
          args.getArgs().getUserName(), args.getPrefix(), args.getPrevKey(),
          args.getMaxKeys());
    }

    // TODO Add missing fields createdBy, bucketCount and bytesUsed
    ListVolumes result = new ListVolumes();
    for (OmVolumeArgs volumeArgs : listResult) {
      VolumeInfo info = new VolumeInfo();
      OzoneManagerProtocolProtos.VolumeInfo
          infoProto = volumeArgs.getProtobuf();
      info.setOwner(new VolumeOwner(infoProto.getOwnerName()));
      info.setQuota(OzoneQuota.getOzoneQuota(infoProto.getQuotaInBytes()));
      info.setVolumeName(infoProto.getVolume());
      info.setCreatedOn(
          HddsClientUtils.formatDateTime(infoProto.getCreationTime()));
      result.addVolume(info);
    }

    return result;
  }

  @Override
  public void deleteVolume(VolumeArgs args)
      throws IOException, OzoneException {
    ozoneManagerClient.deleteVolume(args.getVolumeName());
  }

  @Override
  public VolumeInfo getVolumeInfo(VolumeArgs args)
      throws IOException, OzoneException {
    OmVolumeArgs volumeArgs =
        ozoneManagerClient.getVolumeInfo(args.getVolumeName());
    //TODO: add support for createdOn and other fields in getVolumeInfo
    VolumeInfo volInfo =
        new VolumeInfo(volumeArgs.getVolume(), null,
            volumeArgs.getAdminName());
    volInfo.setOwner(new VolumeOwner(volumeArgs.getOwnerName()));
    volInfo.setQuota(OzoneQuota.getOzoneQuota(volumeArgs.getQuotaInBytes()));
    volInfo.setCreatedOn(
        HddsClientUtils.formatDateTime(volumeArgs.getCreationTime()));
    return volInfo;
  }

  @Override
  public void createBucket(final BucketArgs args)
      throws IOException, OzoneException {
    OmBucketInfo.Builder builder = OmBucketInfo.newBuilder();
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
    ozoneManagerClient.createBucket(builder.build());
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
      OmBucketArgs.Builder builder = OmBucketArgs.newBuilder();
      builder.setVolumeName(args.getVolumeName())
          .setBucketName(args.getBucketName());
      if(removeAcls != null && !removeAcls.isEmpty()) {
        builder.setRemoveAcls(args.getRemoveAcls());
      }
      if(addAcls != null && !addAcls.isEmpty()) {
        builder.setAddAcls(args.getAddAcls());
      }
      ozoneManagerClient.setBucketProperty(builder.build());
    }
  }

  @Override
  public void setBucketVersioning(BucketArgs args)
      throws IOException, OzoneException {
    OmBucketArgs.Builder builder = OmBucketArgs.newBuilder();
    builder.setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setIsVersionEnabled(getBucketVersioningProtobuf(
            args.getVersioning()));
    ozoneManagerClient.setBucketProperty(builder.build());
  }

  @Override
  public void setBucketStorageClass(BucketArgs args)
      throws IOException, OzoneException {
    OmBucketArgs.Builder builder = OmBucketArgs.newBuilder();
    builder.setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setStorageType(args.getStorageType());
    ozoneManagerClient.setBucketProperty(builder.build());
  }

  @Override
  public void deleteBucket(BucketArgs args)
      throws IOException, OzoneException {
    ozoneManagerClient.deleteBucket(args.getVolumeName(),
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

      List<OmBucketInfo> buckets =
          ozoneManagerClient.listBuckets(va.getVolumeName(),
              args.getPrevKey(), args.getPrefix(), args.getMaxKeys());

      // Convert the result for the web layer.
      for (OmBucketInfo bucketInfo : buckets) {
        BucketInfo bk = new BucketInfo();
        bk.setVolumeName(bucketInfo.getVolumeName());
        bk.setBucketName(bucketInfo.getBucketName());
        bk.setStorageType(bucketInfo.getStorageType());
        bk.setAcls(bucketInfo.getAcls());
        bk.setCreatedOn(
            HddsClientUtils.formatDateTime(bucketInfo.getCreationTime()));
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
    OmBucketInfo omBucketInfo = ozoneManagerClient.getBucketInfo(
        volumeName, bucketName);
    BucketInfo bucketInfo = new BucketInfo(omBucketInfo.getVolumeName(),
        omBucketInfo.getBucketName());
    if(omBucketInfo.getIsVersionEnabled()) {
      bucketInfo.setVersioning(Versioning.ENABLED);
    } else {
      bucketInfo.setVersioning(Versioning.DISABLED);
    }
    bucketInfo.setStorageType(omBucketInfo.getStorageType());
    bucketInfo.setAcls(omBucketInfo.getAcls());
    bucketInfo.setCreatedOn(
        HddsClientUtils.formatDateTime(omBucketInfo.getCreationTime()));
    return bucketInfo;
  }

  @Override
  public OutputStream newKeyWriter(KeyArgs args) throws IOException,
      OzoneException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getSize())
        .setType(xceiverClientManager.getType())
        .setFactor(xceiverClientManager.getFactor())
        .build();
    // contact OM to allocate a block for key.
    OpenKeySession openKey = ozoneManagerClient.openKey(keyArgs);
    KeyOutputStream keyOutputStream =
        new KeyOutputStream.Builder()
            .setHandler(openKey)
            .setXceiverClientManager(xceiverClientManager)
            .setOmClient(ozoneManagerClient)
            .setChunkSize(chunkSize)
            .setRequestID(args.getRequestID())
            .setType(xceiverClientManager.getType())
            .setFactor(xceiverClientManager.getFactor())
            .setStreamBufferFlushSize(streamBufferFlushSize)
            .setStreamBufferMaxSize(streamBufferMaxSize)
            .setBlockSize(blockSize)
            .setWatchTimeout(watchTimeout)
            .setChecksumType(checksumType)
            .setBytesPerChecksum(bytesPerChecksum)
            .setMaxRetryCount(maxRetryCount)
            .build();
    keyOutputStream.addPreallocateBlocks(
        openKey.getKeyInfo().getLatestVersionLocations(),
        openKey.getOpenVersion());
    return new OzoneOutputStream(keyOutputStream);
  }

  @Override
  public void commitKey(KeyArgs args, OutputStream stream) throws
      IOException, OzoneException {
    stream.close();
  }

  @Override
  public LengthInputStream newKeyReader(KeyArgs args) throws IOException,
      OzoneException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getSize())
        .setRefreshPipeline(true)
        .build();
    OmKeyInfo keyInfo = ozoneManagerClient.lookupKey(keyArgs);
    return KeyInputStream.getFromOmKeyInfo(
        keyInfo, xceiverClientManager, storageContainerLocationClient,
        args.getRequestID(), verifyChecksum);
  }

  @Override
  public void deleteKey(KeyArgs args) throws IOException, OzoneException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .build();
    ozoneManagerClient.deleteKey(keyArgs);
  }

  @Override
  public void renameKey(KeyArgs args, String toKeyName)
      throws IOException, OzoneException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .build();
    ozoneManagerClient.renameKey(keyArgs, toKeyName);
  }

  @Override
  public KeyInfo getKeyInfo(KeyArgs args) throws IOException, OzoneException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setRefreshPipeline(true)
        .build();

    OmKeyInfo omKeyInfo = ozoneManagerClient.lookupKey(keyArgs);
    KeyInfo keyInfo = new KeyInfo();
    keyInfo.setVersion(0);
    keyInfo.setKeyName(omKeyInfo.getKeyName());
    keyInfo.setSize(omKeyInfo.getDataSize());
    keyInfo.setCreatedOn(
        HddsClientUtils.formatDateTime(omKeyInfo.getCreationTime()));
    keyInfo.setModifiedOn(
        HddsClientUtils.formatDateTime(omKeyInfo.getModificationTime()));
    keyInfo.setType(ReplicationType.valueOf(omKeyInfo.getType().toString()));
    return keyInfo;
  }

  @Override
  public KeyInfo getKeyInfoDetails(KeyArgs args) throws IOException{
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setRefreshPipeline(true)
        .build();
    OmKeyInfo omKeyInfo = ozoneManagerClient.lookupKey(keyArgs);
    List<KeyLocation> keyLocations = new ArrayList<>();
    omKeyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly()
        .forEach((a) -> keyLocations.add(new KeyLocation(a.getContainerID(),
            a.getLocalID(), a.getLength(), a.getOffset())));
    KeyInfoDetails keyInfoDetails = new KeyInfoDetails();
    keyInfoDetails.setVersion(0);
    keyInfoDetails.setKeyName(omKeyInfo.getKeyName());
    keyInfoDetails.setSize(omKeyInfo.getDataSize());
    keyInfoDetails.setCreatedOn(
        HddsClientUtils.formatDateTime(omKeyInfo.getCreationTime()));
    keyInfoDetails.setModifiedOn(
        HddsClientUtils.formatDateTime(omKeyInfo.getModificationTime()));
    keyInfoDetails.setKeyLocations(keyLocations);
    keyInfoDetails.setType(ReplicationType.valueOf(omKeyInfo.getType()
        .toString()));
    return keyInfoDetails;
  }

  @Override
  public ListKeys listKeys(ListArgs args) throws IOException, OzoneException {
    ListKeys result = new ListKeys();
    UserArgs userArgs = args.getArgs();
    if (userArgs instanceof BucketArgs) {
      BucketArgs bucketArgs = (BucketArgs) userArgs;
      if (Strings.isNullOrEmpty(bucketArgs.getVolumeName())) {
        throw new IllegalArgumentException("Illegal argument,"
            + " volume name cannot be null or empty.");
      }

      if (Strings.isNullOrEmpty(bucketArgs.getBucketName())) {
        throw new IllegalArgumentException("Illegal argument,"
            + " bucket name cannot be null or empty.");
      }

      int maxNumOfKeys = args.getMaxKeys();
      if (maxNumOfKeys <= 0 ||
          maxNumOfKeys > OzoneConsts.MAX_LISTKEYS_SIZE) {
        throw new IllegalArgumentException(
            String.format("Illegal max number of keys specified,"
                + " the value must be in range (0, %d], actual : %d.",
                OzoneConsts.MAX_LISTKEYS_SIZE, maxNumOfKeys));
      }

      List<OmKeyInfo> keys=
          ozoneManagerClient.listKeys(bucketArgs.getVolumeName(),
              bucketArgs.getBucketName(),
              args.getPrevKey(), args.getPrefix(), args.getMaxKeys());

      // Convert the result for the web layer.
      for (OmKeyInfo info : keys) {
        KeyInfo tempInfo = new KeyInfo();
        tempInfo.setVersion(0);
        tempInfo.setKeyName(info.getKeyName());
        tempInfo.setSize(info.getDataSize());
        tempInfo.setCreatedOn(
            HddsClientUtils.formatDateTime(info.getCreationTime()));
        tempInfo.setModifiedOn(
            HddsClientUtils.formatDateTime(info.getModificationTime()));
        tempInfo.setType(ReplicationType.valueOf(info.getType().toString()));

        result.addKey(tempInfo);
      }
      return result;
    } else {
      throw new IllegalArgumentException("Illegal argument provided,"
          + " expecting BucketArgs type but met "
          + userArgs.getClass().getSimpleName());
    }
  }

  /**
   * Closes DistributedStorageHandler.
   */
  @Override
  public void close() {
    IOUtils.cleanupWithLogger(LOG, xceiverClientManager);
    IOUtils.cleanupWithLogger(LOG, ozoneManagerClient);
    IOUtils.cleanupWithLogger(LOG, storageContainerLocationClient);
  }
}
