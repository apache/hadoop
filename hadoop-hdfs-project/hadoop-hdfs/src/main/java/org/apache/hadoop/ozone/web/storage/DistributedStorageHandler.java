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

import org.apache.hadoop.hdfs.ozone.protocol.proto
    .ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdfs.ozone.protocol.proto
    .ContainerProtos.GetKeyResponseProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto
    .ContainerProtos.KeyData;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
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
import org.apache.hadoop.ozone.web.request.OzoneAcl;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.protocol.LocatedContainer;
import org.apache.hadoop.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.ListArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
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
import java.util.Set;
import java.util.TimeZone;
import java.util.Locale;
import java.util.HashSet;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
    KsmVolumeArgs volumeArgs = KsmVolumeArgs.newBuilder()
        .setAdminName(args.getAdminName())
        .setOwnerName(args.getUserName())
        .setVolume(args.getVolumeName())
        .setQuotaInBytes(quota)
        .build();
    keySpaceManagerClient.createVolume(volumeArgs);
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
  public boolean checkVolumeAccess(VolumeArgs args)
      throws IOException, OzoneException {
    throw new UnsupportedOperationException("checkVolumeAccessnot implemented");
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
      builder.setAcls(args.getAddAcls().stream().map(
          KSMPBHelper::convertOzoneAcl).collect(Collectors.toList()));
    }
    if(args.getStorageType() != null) {
      builder.setStorageType(PBHelperClient.convertStorageType(
          args.getStorageType()));
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
        builder.setRemoveAcls(args.getRemoveAcls().stream().map(
            KSMPBHelper::convertOzoneAcl).collect(Collectors.toList()));
      }
      if(addAcls != null && !addAcls.isEmpty()) {
        builder.setAddAcls(args.getAddAcls().stream().map(
            KSMPBHelper::convertOzoneAcl).collect(Collectors.toList()));
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
        .setStorageType(PBHelperClient.convertStorageType(
            args.getStorageType()));
    keySpaceManagerClient.setBucketProperty(builder.build());
  }

  @Override
  public void deleteBucket(BucketArgs args)
      throws IOException, OzoneException {
    throw new UnsupportedOperationException("deleteBucket not implemented");
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
    throw new UnsupportedOperationException("listBuckets not implemented");
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
    bucketInfo.setStorageType(PBHelperClient.convertStorageType(
        ksmBucketInfo.getStorageType()));
    bucketInfo.setAcls(ksmBucketInfo.getAcls().stream().map(
        KSMPBHelper::convertOzoneAcl).collect(Collectors.toList()));
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
    throw new UnsupportedOperationException("deleteKey not implemented");
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
   * Acquires an {@link XceiverClientSpi} connected to a {@link Pipeline}
   * of nodes capable of serving container protocol operations.
   * The container is selected based on the specified container key.
   *
   * @param containerKey container key
   * @return XceiverClient connected to a container
   * @throws IOException if an XceiverClient cannot be acquired
   */
  private XceiverClientSpi acquireXceiverClient(String containerKey)
      throws IOException {
    Set<LocatedContainer> locatedContainers =
        storageContainerLocationClient.getStorageContainerLocations(
            new HashSet<>(Arrays.asList(containerKey)));
    Pipeline pipeline = newPipelineFromLocatedContainer(
        locatedContainers.iterator().next());
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

  /**
   * Translates a set of container locations, ordered such that the first is the
   * leader, into a corresponding {@link Pipeline} object.
   *
   * @param locatedContainer container location
   * @return pipeline corresponding to container locations
   */
  private static Pipeline newPipelineFromLocatedContainer(
      LocatedContainer locatedContainer) {
    Set<DatanodeInfo> locations = locatedContainer.getLocations();
    String leaderId = locations.iterator().next().getDatanodeUuid();
    Pipeline pipeline = new Pipeline(leaderId);
    for (DatanodeInfo location : locations) {
      pipeline.addMember(location);
    }
    pipeline.setContainerName(locatedContainer.getContainerName());
    return pipeline;
  }
}
