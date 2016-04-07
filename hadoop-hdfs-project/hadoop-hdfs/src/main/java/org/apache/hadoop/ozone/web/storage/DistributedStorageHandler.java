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

import static org.apache.hadoop.ozone.web.storage.ContainerProtocolCalls.*;
import static org.apache.hadoop.ozone.web.storage.OzoneContainerTranslation.*;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.GetKeyResponseProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.KeyData;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.container.common.transport.client.XceiverClient;
import org.apache.hadoop.ozone.container.common.transport.client.XceiverClientManager;
import org.apache.hadoop.ozone.protocol.LocatedContainer;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.ListArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.KeyInfo;
import org.apache.hadoop.ozone.web.response.ListBuckets;
import org.apache.hadoop.ozone.web.response.ListKeys;
import org.apache.hadoop.ozone.web.response.ListVolumes;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.response.VolumeOwner;
import org.apache.hadoop.util.StringUtils;

/**
 * A {@link StorageHandler} implementation that distributes object storage
 * across the nodes of an HDFS cluster.
 */
public final class DistributedStorageHandler implements StorageHandler {

  private final StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocation;
  private final XceiverClientManager xceiverClientManager;

  /**
   * Creates a new DistributedStorageHandler.
   *
   * @param conf configuration
   * @param storageContainerLocation StorageContainerLocationProtocol proxy
   */
  public DistributedStorageHandler(OzoneConfiguration conf,
      StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocation) {
    this.storageContainerLocation = storageContainerLocation;
    this.xceiverClientManager = new XceiverClientManager(conf);
  }

  @Override
  public void createVolume(VolumeArgs args) throws IOException, OzoneException {
    String containerKey = buildContainerKey(args.getVolumeName());
    XceiverClient xceiverClient = acquireXceiverClient(containerKey);
    try {
      VolumeInfo volume = new VolumeInfo();
      volume.setVolumeName(args.getVolumeName());
      volume.setQuota(args.getQuota());
      volume.setOwner(new VolumeOwner(args.getUserName()));
      volume.setCreatedOn(dateToString(new Date()));
      volume.setCreatedBy(args.getAdminName());
      KeyData containerKeyData = fromVolumeToContainerKeyData(
          xceiverClient.getPipeline().getContainerName(), containerKey, volume);
      putKey(xceiverClient, containerKeyData, args);
    } finally {
      xceiverClientManager.releaseClient(xceiverClient);
    }
  }

  @Override
  public void setVolumeOwner(VolumeArgs args) throws
      IOException, OzoneException {
    throw new UnsupportedOperationException("setVolumeOwner not implemented");
  }

  @Override
  public void setVolumeQuota(VolumeArgs args, boolean remove)
      throws IOException, OzoneException {
    throw new UnsupportedOperationException("setVolumeQuota not implemented");
  }

  @Override
  public boolean checkVolumeAccess(VolumeArgs args)
      throws IOException, OzoneException {
    throw new UnsupportedOperationException("checkVolumeAccessnot implemented");
  }

  @Override
  public ListVolumes listVolumes(UserArgs args)
      throws IOException, OzoneException {
    throw new UnsupportedOperationException("listVolumes not implemented");
  }

  @Override
  public void deleteVolume(VolumeArgs args)
      throws IOException, OzoneException {
    throw new UnsupportedOperationException("deleteVolume not implemented");
  }

  @Override
  public VolumeInfo getVolumeInfo(VolumeArgs args)
      throws IOException, OzoneException {
    String containerKey = buildContainerKey(args.getVolumeName());
    XceiverClient xceiverClient = acquireXceiverClient(containerKey);
    try {
      KeyData containerKeyData = containerKeyDataForRead(
          xceiverClient.getPipeline().getContainerName(), containerKey);
      GetKeyResponseProto response = getKey(xceiverClient, containerKeyData,
          args);
      return fromContainerKeyValueListToVolume(
          response.getKeyData().getMetadataList());
    } finally {
      xceiverClientManager.releaseClient(xceiverClient);
    }
  }

  @Override
  public void createBucket(final BucketArgs args)
      throws IOException, OzoneException {
    String containerKey = buildContainerKey(args.getVolumeName(),
        args.getBucketName());
    XceiverClient xceiverClient = acquireXceiverClient(containerKey);
    try {
      BucketInfo bucket = new BucketInfo();
      bucket.setVolumeName(args.getVolumeName());
      bucket.setBucketName(args.getBucketName());
      bucket.setAcls(args.getAddAcls());
      bucket.setVersioning(args.getVersioning());
      bucket.setStorageType(args.getStorageType());
      KeyData containerKeyData = fromBucketToContainerKeyData(
          xceiverClient.getPipeline().getContainerName(), containerKey, bucket);
      putKey(xceiverClient, containerKeyData, args);
    } finally {
      xceiverClientManager.releaseClient(xceiverClient);
    }
  }

  @Override
  public void setBucketAcls(BucketArgs args)
      throws IOException, OzoneException {
    throw new UnsupportedOperationException("setBucketAcls not implemented");
  }

  @Override
  public void setBucketVersioning(BucketArgs args)
      throws IOException, OzoneException {
    throw new UnsupportedOperationException(
        "setBucketVersioning not implemented");
  }

  @Override
  public void setBucketStorageClass(BucketArgs args)
      throws IOException, OzoneException {
    throw new UnsupportedOperationException(
        "setBucketStorageClass not implemented");
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
  public ListBuckets listBuckets(VolumeArgs args)
      throws IOException, OzoneException {
    throw new UnsupportedOperationException("listBuckets not implemented");
  }

  @Override
  public BucketInfo getBucketInfo(BucketArgs args)
      throws IOException, OzoneException {
    String containerKey = buildContainerKey(args.getVolumeName(),
        args.getBucketName());
    XceiverClient xceiverClient = acquireXceiverClient(containerKey);
    try {
      KeyData containerKeyData = containerKeyDataForRead(
          xceiverClient.getPipeline().getContainerName(), containerKey);
      GetKeyResponseProto response = getKey(xceiverClient, containerKeyData,
          args);
      return fromContainerKeyValueListToBucket(
          response.getKeyData().getMetadataList());
    } finally {
      xceiverClientManager.releaseClient(xceiverClient);
    }
  }

  @Override
  public OutputStream newKeyWriter(KeyArgs args) throws IOException,
      OzoneException {
    String containerKey = buildContainerKey(args.getVolumeName(),
        args.getBucketName(), args.getKeyName());
    KeyInfo key = new KeyInfo();
    key.setKeyName(args.getKeyName());
    key.setCreatedOn(dateToString(new Date()));
    XceiverClient xceiverClient = acquireXceiverClient(containerKey);
    return new ChunkOutputStream(containerKey, key, xceiverClientManager,
        xceiverClient, args);
  }

  @Override
  public void commitKey(KeyArgs args, OutputStream stream) throws
      IOException, OzoneException {
    stream.close();
  }

  @Override
  public LengthInputStream newKeyReader(KeyArgs args) throws IOException,
      OzoneException {
    String containerKey = buildContainerKey(args.getVolumeName(),
        args.getBucketName(), args.getKeyName());
    XceiverClient xceiverClient = acquireXceiverClient(containerKey);
    boolean success = false;
    try {
      KeyData containerKeyData = containerKeyDataForRead(
          xceiverClient.getPipeline().getContainerName(), containerKey);
      GetKeyResponseProto response = getKey(xceiverClient, containerKeyData,
          args);
      long length = 0;
      List<ChunkInfo> chunks = response.getKeyData().getChunksList();
      for (ChunkInfo chunk : chunks) {
        length += chunk.getLen();
      }
      success = true;
      return new LengthInputStream(new ChunkInputStream(
          containerKey, xceiverClientManager, xceiverClient, chunks, args),
          length);
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

  /**
   * Acquires an {@link XceiverClient} connected to a {@link Pipeline} of nodes
   * capable of serving container protocol operations.  The container is
   * selected based on the specified container key.
   *
   * @param containerKey container key
   * @return XceiverClient connected to a container
   * @throws IOException if an XceiverClient cannot be acquired
   */
  private XceiverClient acquireXceiverClient(String containerKey)
      throws IOException {
    Set<LocatedContainer> locatedContainers =
        storageContainerLocation.getStorageContainerLocations(
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
