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
import org.apache.hadoop.ozone.om.helpers.OmDeleteVolumeResponse;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeOwnerChangeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeList;
import org.apache.hadoop.utils.db.BatchOperation;

import com.google.common.base.Preconditions;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USER_MAX_VOLUME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USER_MAX_VOLUME_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;

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
  private final boolean isRatisEnabled;

  /**
   * Constructor.
   * @param conf - Ozone configuration.
   * @throws IOException
   */
  public VolumeManagerImpl(OMMetadataManager metadataManager,
      OzoneConfiguration conf) {
    this.metadataManager = metadataManager;
    this.maxUserVolumeCount = conf.getInt(OZONE_OM_USER_MAX_VOLUME,
        OZONE_OM_USER_MAX_VOLUME_DEFAULT);
    isRatisEnabled = conf.getBoolean(
        OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_ENABLE_DEFAULT);
  }

  // Helpers to add and delete volume from user list
  private VolumeList addVolumeToOwnerList(String volume, String owner)
      throws IOException {
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
      throw new OMException("Too many volumes for user:" + owner,
          ResultCodes.USER_TOO_MANY_VOLUMES);
    }

    // Add the new volume to the list
    prevVolList.add(volume);
    VolumeList newVolList = VolumeList.newBuilder()
        .addAllVolumeNames(prevVolList).build();

    return newVolList;
  }

  private VolumeList delVolumeFromOwnerList(String volume, String owner)
      throws IOException {
    // Get the volume list
    VolumeList volumeList = metadataManager.getUserTable().get(owner);
    List<String> prevVolList = new ArrayList<>();
    if (volumeList != null) {
      prevVolList.addAll(volumeList.getVolumeNamesList());
    } else {
      LOG.debug("volume:{} not found for user:{}");
      throw new OMException(ResultCodes.USER_NOT_FOUND);
    }

    // Remove the volume from the list
    prevVolList.remove(volume);
    VolumeList newVolList = VolumeList.newBuilder()
        .addAllVolumeNames(prevVolList).build();
    return newVolList;
  }

  /**
   * Creates a volume.
   * @param omVolumeArgs - OmVolumeArgs.
   * @return VolumeList
   */
  @Override
  public VolumeList createVolume(OmVolumeArgs omVolumeArgs) throws IOException {
    Preconditions.checkNotNull(omVolumeArgs);
    metadataManager.getLock().acquireUserLock(omVolumeArgs.getOwnerName());
    metadataManager.getLock().acquireVolumeLock(omVolumeArgs.getVolume());
    try {
      String dbVolumeKey = metadataManager.getVolumeKey(
          omVolumeArgs.getVolume());
      String dbUserKey = metadataManager.getUserKey(
          omVolumeArgs.getOwnerName());
      OmVolumeArgs volumeInfo =
          metadataManager.getVolumeTable().get(dbVolumeKey);

      // Check of the volume already exists
      if (volumeInfo != null) {
        LOG.debug("volume:{} already exists", omVolumeArgs.getVolume());
        throw new OMException(ResultCodes.VOLUME_ALREADY_EXISTS);
      }

      VolumeList volumeList = addVolumeToOwnerList(omVolumeArgs.getVolume(),
          omVolumeArgs.getOwnerName());

      // Set creation time
      omVolumeArgs.setCreationTime(System.currentTimeMillis());

      if (!isRatisEnabled) {
        createVolumeCommitToDB(omVolumeArgs, volumeList, dbVolumeKey,
            dbUserKey);
      }
      LOG.debug("created volume:{} user:{}", omVolumeArgs.getVolume(),
          omVolumeArgs.getOwnerName());
      return volumeList;
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Volume creation failed for user:{} volume:{}",
            omVolumeArgs.getOwnerName(), omVolumeArgs.getVolume(), ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseVolumeLock(omVolumeArgs.getVolume());
      metadataManager.getLock().releaseUserLock(omVolumeArgs.getOwnerName());
    }
  }


  @Override
  public void applyCreateVolume(OmVolumeArgs omVolumeArgs,
      VolumeList volumeList) throws IOException {
    // Do we need to hold lock in apply Transactions requests?
    String dbVolumeKey = metadataManager.getVolumeKey(omVolumeArgs.getVolume());
    String dbUserKey = metadataManager.getUserKey(omVolumeArgs.getOwnerName());
    try {
      createVolumeCommitToDB(omVolumeArgs, volumeList, dbVolumeKey, dbUserKey);
    } catch (IOException ex) {
      LOG.error("Volume creation failed for user:{} volume:{}",
          omVolumeArgs.getOwnerName(), omVolumeArgs.getVolume(), ex);
      throw ex;
    }
  }

  private void createVolumeCommitToDB(OmVolumeArgs omVolumeArgs,
      VolumeList volumeList, String dbVolumeKey, String dbUserKey)
      throws IOException {
    try (BatchOperation batch = metadataManager.getStore()
        .initBatchOperation()) {
      // Write the vol info
      metadataManager.getVolumeTable().putWithBatch(batch, dbVolumeKey,
          omVolumeArgs);
      metadataManager.getUserTable().putWithBatch(batch, dbUserKey,
          volumeList);
      // Add volume to user list
      metadataManager.getStore().commitBatchOperation(batch);
    } catch (IOException ex) {
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
  public OmVolumeOwnerChangeResponse setOwner(String volume, String owner)
      throws IOException {
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

      String originalOwner =
          metadataManager.getUserKey(volumeArgs.getOwnerName());
      VolumeList oldOwnerVolumeList = delVolumeFromOwnerList(volume,
          originalOwner);

      String newOwner =  metadataManager.getUserKey(owner);
      VolumeList newOwnerVolumeList = addVolumeToOwnerList(volume, newOwner);

      volumeArgs.setOwnerName(owner);
      if (!isRatisEnabled) {
        setOwnerCommitToDB(oldOwnerVolumeList, newOwnerVolumeList,
            volumeArgs, owner);
      }
      return new OmVolumeOwnerChangeResponse(oldOwnerVolumeList,
          newOwnerVolumeList, volumeArgs, originalOwner);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Changing volume ownership failed for user:{} volume:{}",
            owner, volume, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseVolumeLock(volume);
      metadataManager.getLock().releaseUserLock(owner);
    }
  }

  @Override
  public void applySetOwner(String oldOwner, VolumeList oldOwnerVolumeList,
      VolumeList newOwnerVolumeList, OmVolumeArgs newOwnerVolumeArgs)
      throws IOException {
    try {
      setOwnerCommitToDB(oldOwnerVolumeList, newOwnerVolumeList,
          newOwnerVolumeArgs, oldOwner);
    } catch (IOException ex) {
      LOG.error("Changing volume ownership failed for user:{} volume:{}",
          newOwnerVolumeArgs.getOwnerName(), newOwnerVolumeArgs.getVolume(),
          ex);
      throw ex;
    }
  }


  private void setOwnerCommitToDB(VolumeList oldOwnerVolumeList,
      VolumeList newOwnerVolumeList, OmVolumeArgs newOwnerVolumeArgs,
      String oldOwner) throws IOException {
    try (BatchOperation batch = metadataManager.getStore()
        .initBatchOperation()) {
      if (oldOwnerVolumeList.getVolumeNamesList().size() == 0) {
        metadataManager.getUserTable().deleteWithBatch(batch, oldOwner);
      } else {
        metadataManager.getUserTable().putWithBatch(batch, oldOwner,
            oldOwnerVolumeList);
      }
      metadataManager.getUserTable().putWithBatch(batch,
          newOwnerVolumeArgs.getOwnerName(),
          newOwnerVolumeList);

      String dbVolumeKey =
          metadataManager.getVolumeKey(newOwnerVolumeArgs.getVolume());
      metadataManager.getVolumeTable().putWithBatch(batch,
          dbVolumeKey, newOwnerVolumeArgs);
      metadataManager.getStore().commitBatchOperation(batch);
    }
  }


  /**
   * Changes the Quota on a volume.
   *
   * @param volume - Name of the volume.
   * @param quota - Quota in bytes.
   *
   * @return OmVolumeArgs
   * @throws IOException
   */
  @Override
  public OmVolumeArgs setQuota(String volume, long quota) throws IOException {
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


      volumeArgs.setQuotaInBytes(quota);

      if (!isRatisEnabled) {
        metadataManager.getVolumeTable().put(dbVolumeKey, volumeArgs);
      }
      return volumeArgs;
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

  @Override
  public void applySetQuota(OmVolumeArgs omVolumeArgs) throws IOException {
    try {
      String dbVolumeKey = metadataManager.getVolumeKey(
          omVolumeArgs.getVolume());
      metadataManager.getVolumeTable().put(dbVolumeKey, omVolumeArgs);
    } catch (IOException ex) {
      LOG.error("Changing volume quota failed for volume:{} quota:{}",
          omVolumeArgs.getVolume(), omVolumeArgs.getQuotaInBytes(), ex);
      throw ex;
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
   *
   * @return OmDeleteVolumeResponse
   * @throws IOException
   */
  @Override
  public OmDeleteVolumeResponse deleteVolume(String volume) throws IOException {
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
      VolumeList newVolumeList = delVolumeFromOwnerList(volume,
          volumeArgs.getOwnerName());

      if (!isRatisEnabled) {
        deleteVolumeCommitToDB(newVolumeList,
            volume, owner);
      }
      return new OmDeleteVolumeResponse(volume, owner, newVolumeList);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Delete volume failed for volume:{}", volume, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseVolumeLock(volume);
      metadataManager.getLock().releaseUserLock(owner);
    }
  }

  @Override
  public void applyDeleteVolume(String volume, String owner,
      VolumeList newVolumeList) throws IOException {
    try {
      deleteVolumeCommitToDB(newVolumeList, volume, owner);
    } catch (IOException ex) {
      LOG.error("Delete volume failed for volume:{}", volume,
          ex);
      throw ex;
    }
  }

  private void deleteVolumeCommitToDB(VolumeList newVolumeList,
      String volume, String owner) throws IOException {
    try (BatchOperation batch = metadataManager.getStore()
        .initBatchOperation()) {
      String dbUserKey = metadataManager.getUserKey(owner);
      if (newVolumeList.getVolumeNamesList().size() == 0) {
        metadataManager.getUserTable().deleteWithBatch(batch, dbUserKey);
      } else {
        metadataManager.getUserTable().putWithBatch(batch, dbUserKey,
            newVolumeList);
      }
      metadataManager.getVolumeTable().deleteWithBatch(batch,
          metadataManager.getVolumeKey(volume));
      metadataManager.getStore().commitBatchOperation(batch);
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
