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
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeList;
import org.apache.hadoop.utils.RocksDBStore;
import org.apache.hadoop.utils.db.BatchOperation;

import com.google.common.base.Preconditions;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USER_MAX_VOLUME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USER_MAX_VOLUME_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OM volume management code.
 */
public class VolumeManagerImpl implements VolumeManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(VolumeManagerImpl.class);

  private final OMMetadataManager metadataManager;
  private final int maxUserVolumeCount;

  /**
   * Constructor.
   * @param conf - Ozone configuration.
   * @throws IOException
   */
  public VolumeManagerImpl(OMMetadataManager metadataManager,
      OzoneConfiguration conf) throws IOException {
    this.metadataManager = metadataManager;
    this.maxUserVolumeCount = conf.getInt(OZONE_OM_USER_MAX_VOLUME,
        OZONE_OM_USER_MAX_VOLUME_DEFAULT);
  }

  // Helpers to add and delete volume from user list
  private void addVolumeToOwnerList(String volume, String owner,
      BatchOperation batchOperation) throws IOException {
    // Get the volume list
    String dbUserKey = metadataManager.getUserKey(owner);
    VolumeList volumeList = metadataManager.getUserTable().get(dbUserKey);
    List<String> prevVolList = new ArrayList<>();
    if (volumeList != null) {
      prevVolList.addAll(volumeList.getVolumeNamesList());
    }

    // Check the volume count
    if (prevVolList.size() >= maxUserVolumeCount) {
      LOG.debug("Too many volumes for user:{}", owner);
      throw new OMException(ResultCodes.USER_TOO_MANY_VOLUMES);
    }

    // Add the new volume to the list
    prevVolList.add(volume);
    VolumeList newVolList = VolumeList.newBuilder()
        .addAllVolumeNames(prevVolList).build();
    metadataManager.getUserTable().putWithBatch(batchOperation,
        dbUserKey, newVolList);
  }

  private void delVolumeFromOwnerList(String volume, String owner,
      BatchOperation batch) throws RocksDBException, IOException {
    // Get the volume list
    String dbUserKey = metadataManager.getUserKey(owner);
    VolumeList volumeList = metadataManager.getUserTable().get(dbUserKey);
    List<String> prevVolList = new ArrayList<>();
    if (volumeList != null) {
      prevVolList.addAll(volumeList.getVolumeNamesList());
    } else {
      LOG.debug("volume:{} not found for user:{}");
      throw new OMException(ResultCodes.USER_NOT_FOUND);
    }

    // Remove the volume from the list
    prevVolList.remove(volume);
    if (prevVolList.size() == 0) {
      metadataManager.getUserTable().deleteWithBatch(batch, dbUserKey);
    } else {
      VolumeList newVolList = VolumeList.newBuilder()
          .addAllVolumeNames(prevVolList).build();
      metadataManager.getUserTable().putWithBatch(batch,
          dbUserKey, newVolList);
    }
  }

  /**
   * Creates a volume.
   * @param args - OmVolumeArgs.
   */
  @Override
  public void createVolume(OmVolumeArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    metadataManager.getLock().acquireUserLock(args.getOwnerName());
    metadataManager.getLock().acquireVolumeLock(args.getVolume());
    try {
      String dbVolumeKey = metadataManager.getVolumeKey(args.getVolume());
      OmVolumeArgs volumeInfo =
          metadataManager.getVolumeTable().get(dbVolumeKey);

      // Check of the volume already exists
      if (volumeInfo != null) {
        LOG.debug("volume:{} already exists", args.getVolume());
        throw new OMException(ResultCodes.VOLUME_ALREADY_EXISTS);
      }

      try (BatchOperation batch = metadataManager.getStore()
          .initBatchOperation()) {
        // Write the vol info
        metadataManager.getVolumeTable().putWithBatch(batch,
            dbVolumeKey, args);

        // Add volume to user list
        addVolumeToOwnerList(args.getVolume(), args.getOwnerName(), batch);
        metadataManager.getStore().commitBatchOperation(batch);
      }
      LOG.debug("created volume:{} user:{}", args.getVolume(),
          args.getOwnerName());
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Volume creation failed for user:{} volume:{}",
            args.getOwnerName(), args.getVolume(), ex);
      } else {
        throw (IOException) ex;
      }
    } finally {
      metadataManager.getLock().releaseVolumeLock(args.getVolume());
      metadataManager.getLock().releaseUserLock(args.getOwnerName());
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
    Preconditions.checkNotNull(volume);
    Preconditions.checkNotNull(owner);
    metadataManager.getLock().acquireUserLock(owner);
    metadataManager.getLock().acquireVolumeLock(volume);
    try {
      String dbVolumeKey = metadataManager.getVolumeKey(volume);
      OmVolumeArgs volumeArgs = metadataManager
          .getVolumeTable().get(dbVolumeKey);
      if (volumeArgs == null) {
        LOG.debug("Changing volume ownership failed for user:{} volume:{}",
            owner, volume);
        throw new OMException("Volume " + volume + " is not found",
            ResultCodes.VOLUME_NOT_FOUND);
      }

      Preconditions.checkState(volume.equals(volumeArgs.getVolume()));

      try (BatchOperation batch = metadataManager.getStore()
          .initBatchOperation()) {
        delVolumeFromOwnerList(volume, volumeArgs.getOwnerName(), batch);
        addVolumeToOwnerList(volume, owner, batch);

        OmVolumeArgs newVolumeArgs =
            OmVolumeArgs.newBuilder().setVolume(volumeArgs.getVolume())
                .setAdminName(volumeArgs.getAdminName())
                .setOwnerName(owner)
                .setQuotaInBytes(volumeArgs.getQuotaInBytes())
                .setCreationTime(volumeArgs.getCreationTime())
                .build();

        metadataManager.getVolumeTable().putWithBatch(batch,
            dbVolumeKey, newVolumeArgs);
        metadataManager.getStore().commitBatchOperation(batch);
      }
    } catch (RocksDBException | IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Changing volume ownership failed for user:{} volume:{}",
            owner, volume, ex);
      }
      if(ex instanceof RocksDBException) {
        throw RocksDBStore.toIOException("Volume creation failed.",
            (RocksDBException) ex);
      } else {
        throw (IOException) ex;
      }
    } finally {
      metadataManager.getLock().releaseVolumeLock(volume);
      metadataManager.getLock().releaseUserLock(owner);
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
    Preconditions.checkNotNull(volume);
    metadataManager.getLock().acquireVolumeLock(volume);
    try {
      String dbVolumeKey = metadataManager.getVolumeKey(volume);
      OmVolumeArgs volumeArgs =
          metadataManager.getVolumeTable().get(dbVolumeKey);
      if (volumeArgs == null) {
        LOG.debug("volume:{} does not exist", volume);
        throw new OMException(ResultCodes.VOLUME_NOT_FOUND);
      }

      Preconditions.checkState(volume.equals(volumeArgs.getVolume()));

      OmVolumeArgs newVolumeArgs =
          OmVolumeArgs.newBuilder()
              .setVolume(volumeArgs.getVolume())
              .setAdminName(volumeArgs.getAdminName())
              .setOwnerName(volumeArgs.getOwnerName())
              .setQuotaInBytes(quota)
              .setCreationTime(volumeArgs.getCreationTime()).build();

      metadataManager.getVolumeTable().put(dbVolumeKey, newVolumeArgs);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Changing volume quota failed for volume:{} quota:{}", volume,
            quota, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseVolumeLock(volume);
    }
  }

  /**
   * Gets the volume information.
   * @param volume - Volume name.
   * @return VolumeArgs or exception is thrown.
   * @throws IOException
   */
  @Override
  public OmVolumeArgs getVolumeInfo(String volume) throws IOException {
    Preconditions.checkNotNull(volume);
    metadataManager.getLock().acquireVolumeLock(volume);
    try {
      String dbVolumeKey = metadataManager.getVolumeKey(volume);
      OmVolumeArgs volumeArgs =
          metadataManager.getVolumeTable().get(dbVolumeKey);
      if (volumeArgs == null) {
        LOG.debug("volume:{} does not exist", volume);
        throw new OMException("Volume " + volume + " is not found",
            ResultCodes.VOLUME_NOT_FOUND);
      }

      return volumeArgs;
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.warn("Info volume failed for volume:{}", volume, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseVolumeLock(volume);
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
    Preconditions.checkNotNull(volume);
    String owner;
    metadataManager.getLock().acquireVolumeLock(volume);
    try {
      owner = getVolumeInfo(volume).getOwnerName();
    } finally {
      metadataManager.getLock().releaseVolumeLock(volume);
    }
    metadataManager.getLock().acquireUserLock(owner);
    metadataManager.getLock().acquireVolumeLock(volume);
    try {

      String dbVolumeKey = metadataManager.getVolumeKey(volume);
      OmVolumeArgs volumeArgs =
          metadataManager.getVolumeTable().get(dbVolumeKey);
      if (volumeArgs == null) {
        LOG.debug("volume:{} does not exist", volume);
        throw new OMException("Volume " + volume + " is not found",
            ResultCodes.VOLUME_NOT_FOUND);
      }

      if (!metadataManager.isVolumeEmpty(volume)) {
        LOG.debug("volume:{} is not empty", volume);
        throw new OMException(ResultCodes.VOLUME_NOT_EMPTY);
      }
      Preconditions.checkState(volume.equals(volumeArgs.getVolume()));
      // delete the volume from the owner list
      // as well as delete the volume entry
      try (BatchOperation batch = metadataManager.getStore()
          .initBatchOperation()) {
        delVolumeFromOwnerList(volume, volumeArgs.getOwnerName(), batch);
        metadataManager.getVolumeTable().deleteWithBatch(batch, dbVolumeKey);
        metadataManager.getStore().commitBatchOperation(batch);
      }
    } catch (RocksDBException| IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Delete volume failed for volume:{}", volume, ex);
      }
      if(ex instanceof RocksDBException) {
        throw RocksDBStore.toIOException("Volume creation failed.",
            (RocksDBException) ex);
      } else {
        throw (IOException) ex;
      }
    } finally {
      metadataManager.getLock().releaseVolumeLock(volume);
      metadataManager.getLock().releaseUserLock(owner);
    }
  }

  /**
   * Checks if the specified user with a role can access this volume.
   *
   * @param volume - volume
   * @param userAcl - user acl which needs to be checked for access
   * @return true if the user has access for the volume, false otherwise
   * @throws IOException
   */
  @Override
  public boolean checkVolumeAccess(String volume, OzoneAclInfo userAcl)
      throws IOException {
    Preconditions.checkNotNull(volume);
    Preconditions.checkNotNull(userAcl);
    metadataManager.getLock().acquireVolumeLock(volume);
    try {
      String dbVolumeKey = metadataManager.getVolumeKey(volume);
      OmVolumeArgs volumeArgs =
          metadataManager.getVolumeTable().get(dbVolumeKey);
      if (volumeArgs == null) {
        LOG.debug("volume:{} does not exist", volume);
        throw new OMException("Volume " + volume + " is not found",
            ResultCodes.VOLUME_NOT_FOUND);
      }

      Preconditions.checkState(volume.equals(volumeArgs.getVolume()));
      return volumeArgs.getAclMap().hasAccess(userAcl);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Check volume access failed for volume:{} user:{} rights:{}",
            volume, userAcl.getName(), userAcl.getRights(), ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseVolumeLock(volume);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<OmVolumeArgs> listVolumes(String userName,
      String prefix, String startKey, int maxKeys) throws IOException {
    metadataManager.getLock().acquireUserLock(userName);
    try {
      return metadataManager.listVolumes(
          userName, prefix, startKey, maxKeys);
    } finally {
      metadataManager.getLock().releaseUserLock(userName);
    }
  }
}
