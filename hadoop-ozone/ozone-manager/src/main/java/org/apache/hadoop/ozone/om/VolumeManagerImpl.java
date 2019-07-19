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
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeList;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.utils.db.BatchOperation;

import com.google.common.base.Preconditions;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USER_MAX_VOLUME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USER_MAX_VOLUME_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.USER_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

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
  private final boolean aclEnabled;


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
    aclEnabled = conf.getBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED,
        OzoneConfigKeys.OZONE_ACL_ENABLED_DEFAULT);
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
   */
  @Override
  public void createVolume(OmVolumeArgs omVolumeArgs) throws IOException {
    Preconditions.checkNotNull(omVolumeArgs);

    boolean acquiredUserLock = false;
    metadataManager.getLock().acquireLock(VOLUME_LOCK,
        omVolumeArgs.getVolume());
    try {
      acquiredUserLock = metadataManager.getLock().acquireLock(USER_LOCK,
          omVolumeArgs.getOwnerName());
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


      createVolumeCommitToDB(omVolumeArgs, volumeList, dbVolumeKey,
            dbUserKey);

      LOG.debug("created volume:{} user:{}", omVolumeArgs.getVolume(),
          omVolumeArgs.getOwnerName());
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Volume creation failed for user:{} volume:{}",
            omVolumeArgs.getOwnerName(), omVolumeArgs.getVolume(), ex);
      }
      throw ex;
    } finally {
      if (acquiredUserLock) {
        metadataManager.getLock().releaseLock(USER_LOCK,
            omVolumeArgs.getOwnerName());
      }
      metadataManager.getLock().releaseLock(VOLUME_LOCK,
          omVolumeArgs.getVolume());
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
  public void setOwner(String volume, String owner)
      throws IOException {
    Preconditions.checkNotNull(volume);
    Preconditions.checkNotNull(owner);
    boolean acquiredUsersLock = false;
    String actualOwner = null;
    metadataManager.getLock().acquireLock(VOLUME_LOCK, volume);
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

      actualOwner = volumeArgs.getOwnerName();
      String originalOwner = metadataManager.getUserKey(actualOwner);

      acquiredUsersLock = metadataManager.getLock().acquireMultiUserLock(owner,
          originalOwner);
      VolumeList oldOwnerVolumeList = delVolumeFromOwnerList(volume,
          originalOwner);

      String newOwner =  metadataManager.getUserKey(owner);
      VolumeList newOwnerVolumeList = addVolumeToOwnerList(volume, newOwner);

      volumeArgs.setOwnerName(owner);
      setOwnerCommitToDB(oldOwnerVolumeList, newOwnerVolumeList,
          volumeArgs, owner);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Changing volume ownership failed for user:{} volume:{}",
            owner, volume, ex);
      }
      throw ex;
    } finally {
      if (acquiredUsersLock) {
        metadataManager.getLock().releaseMultiUserLock(owner, actualOwner);
      }
      metadataManager.getLock().releaseLock(VOLUME_LOCK, volume);
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
   * @throws IOException
   */
  @Override
  public void setQuota(String volume, long quota) throws IOException {
    Preconditions.checkNotNull(volume);
    metadataManager.getLock().acquireLock(VOLUME_LOCK, volume);
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

      metadataManager.getVolumeTable().put(dbVolumeKey, volumeArgs);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Changing volume quota failed for volume:{} quota:{}", volume,
            quota, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(VOLUME_LOCK, volume);
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
    metadataManager.getLock().acquireLock(VOLUME_LOCK, volume);
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
      metadataManager.getLock().releaseLock(VOLUME_LOCK, volume);
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
    String owner = null;
    boolean acquiredUserLock = false;
    metadataManager.getLock().acquireLock(VOLUME_LOCK, volume);
    try {
      owner = getVolumeInfo(volume).getOwnerName();
      acquiredUserLock = metadataManager.getLock().acquireLock(USER_LOCK,
          owner);
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


      deleteVolumeCommitToDB(newVolumeList, volume, owner);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Delete volume failed for volume:{}", volume, ex);
      }
      throw ex;
    } finally {
      if (acquiredUserLock) {
        metadataManager.getLock().releaseLock(USER_LOCK, owner);
      }
      metadataManager.getLock().releaseLock(VOLUME_LOCK, volume);

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
    metadataManager.getLock().acquireLock(VOLUME_LOCK, volume);
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
            volume, userAcl.getName(), userAcl.getRights().toString(), ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(VOLUME_LOCK, volume);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<OmVolumeArgs> listVolumes(String userName,
      String prefix, String startKey, int maxKeys) throws IOException {
    metadataManager.getLock().acquireLock(USER_LOCK, userName);
    try {
      List<OmVolumeArgs> volumes = metadataManager.listVolumes(
          userName, prefix, startKey, maxKeys);
      UserGroupInformation userUgi = ProtobufRpcEngine.Server.
          getRemoteUser();
      if (userUgi == null || !aclEnabled) {
        return volumes;
      }

      List<OmVolumeArgs> filteredVolumes = volumes.stream().
          filter(v -> v.getAclMap().
              hasAccess(IAccessAuthorizer.ACLType.LIST, userUgi))
          .collect(Collectors.toList());
      return filteredVolumes;
    } finally {
      metadataManager.getLock().releaseLock(USER_LOCK, userName);
    }
  }

  /**
   * Add acl for Ozone object. Return true if acl is added successfully else
   * false.
   *
   * @param obj Ozone object for which acl should be added.
   * @param acl ozone acl top be added.
   * @throws IOException if there is error.
   */
  @Override
  public boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    Objects.requireNonNull(obj);
    Objects.requireNonNull(acl);
    if (!obj.getResourceType().equals(OzoneObj.ResourceType.VOLUME)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "VolumeManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    metadataManager.getLock().acquireLock(VOLUME_LOCK, volume);
    try {
      String dbVolumeKey = metadataManager.getVolumeKey(volume);
      OmVolumeArgs volumeArgs =
          metadataManager.getVolumeTable().get(dbVolumeKey);
      if (volumeArgs == null) {
        LOG.debug("volume:{} does not exist", volume);
        throw new OMException("Volume " + volume + " is not found",
            ResultCodes.VOLUME_NOT_FOUND);
      }
      try {
        volumeArgs.addAcl(acl);
      } catch (OMException ex) {
        LOG.debug("Add acl failed.", ex);
        return false;
      }
      metadataManager.getVolumeTable().put(dbVolumeKey, volumeArgs);

      Preconditions.checkState(volume.equals(volumeArgs.getVolume()));
      //return volumeArgs.getAclMap().hasAccess(userAcl);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Add acl operation failed for volume:{} acl:{}",
            volume, acl, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(VOLUME_LOCK, volume);
    }

    return true;
  }

  /**
   * Remove acl for Ozone object. Return true if acl is removed successfully
   * else false.
   *
   * @param obj Ozone object.
   * @param acl Ozone acl to be removed.
   * @throws IOException if there is error.
   */
  @Override
  public boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    Objects.requireNonNull(obj);
    Objects.requireNonNull(acl);
    if (!obj.getResourceType().equals(OzoneObj.ResourceType.VOLUME)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "VolumeManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    metadataManager.getLock().acquireLock(VOLUME_LOCK, volume);
    try {
      String dbVolumeKey = metadataManager.getVolumeKey(volume);
      OmVolumeArgs volumeArgs =
          metadataManager.getVolumeTable().get(dbVolumeKey);
      if (volumeArgs == null) {
        LOG.debug("volume:{} does not exist", volume);
        throw new OMException("Volume " + volume + " is not found",
            ResultCodes.VOLUME_NOT_FOUND);
      }
      try {
        volumeArgs.removeAcl(acl);
      } catch (OMException ex) {
        LOG.debug("Remove acl failed.", ex);
        return false;
      }
      metadataManager.getVolumeTable().put(dbVolumeKey, volumeArgs);

      Preconditions.checkState(volume.equals(volumeArgs.getVolume()));
      //return volumeArgs.getAclMap().hasAccess(userAcl);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Remove acl operation failed for volume:{} acl:{}",
            volume, acl, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(VOLUME_LOCK, volume);
    }

    return true;
  }

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for given
   * object to list of ACLs provided in argument.
   *
   * @param obj Ozone object.
   * @param acls List of acls.
   * @throws IOException if there is error.
   */
  @Override
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    Objects.requireNonNull(obj);
    Objects.requireNonNull(acls);

    if (!obj.getResourceType().equals(OzoneObj.ResourceType.VOLUME)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "VolumeManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    metadataManager.getLock().acquireLock(VOLUME_LOCK, volume);
    try {
      String dbVolumeKey = metadataManager.getVolumeKey(volume);
      OmVolumeArgs volumeArgs =
          metadataManager.getVolumeTable().get(dbVolumeKey);
      if (volumeArgs == null) {
        LOG.debug("volume:{} does not exist", volume);
        throw new OMException("Volume " + volume + " is not found",
            ResultCodes.VOLUME_NOT_FOUND);
      }
      volumeArgs.setAcls(acls);
      metadataManager.getVolumeTable().put(dbVolumeKey, volumeArgs);

      Preconditions.checkState(volume.equals(volumeArgs.getVolume()));
      //return volumeArgs.getAclMap().hasAccess(userAcl);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Set acl operation failed for volume:{} acls:{}",
            volume, acls, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(VOLUME_LOCK, volume);
    }

    return true;
  }

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @throws IOException if there is error.
   */
  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    Objects.requireNonNull(obj);

    if (!obj.getResourceType().equals(OzoneObj.ResourceType.VOLUME)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "VolumeManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    metadataManager.getLock().acquireLock(VOLUME_LOCK, volume);
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
      return volumeArgs.getAclMap().getAcl();
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Get acl operation failed for volume:{}", volume, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseLock(VOLUME_LOCK, volume);
    }
  }

  /**
   * Check access for given ozoneObject.
   *
   * @param ozObject object for which access needs to be checked.
   * @param context Context object encapsulating all user related information.
   * @return true if user has access else false.
   */
  @Override
  public boolean checkAccess(OzoneObj ozObject, RequestContext context)
      throws OMException {
    Objects.requireNonNull(ozObject);
    Objects.requireNonNull(context);

    String volume = ozObject.getVolumeName();
    metadataManager.getLock().acquireLock(VOLUME_LOCK, volume);
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
      boolean hasAccess = volumeArgs.getAclMap().hasAccess(
          context.getAclRights(), context.getClientUgi());
      LOG.debug("user:{} has access rights for volume:{} :{} ",
          context.getClientUgi(), ozObject.getVolumeName(), hasAccess);
      return hasAccess;
    } catch (IOException ex) {
      LOG.error("Check access operation failed for volume:{}", volume, ex);
      throw new OMException("Check access operation failed for " +
          "volume:" + volume, ex, ResultCodes.INTERNAL_ERROR);
    } finally {
      metadataManager.getLock().releaseLock(VOLUME_LOCK, volume);
    }
  }
}
