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

import com.google.common.base.Preconditions;
import org.apache.hadoop.ksm.helpers.KsmVolumeArgs;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.VolumeList;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.VolumeInfo;
import org.iq80.leveldb.DBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.ksm
    .KSMConfigKeys.OZONE_KSM_USER_MAX_VOLUME_DEFAULT;
import static org.apache.hadoop.ozone.ksm
    .KSMConfigKeys.OZONE_KSM_USER_MAX_VOLUME;
import static org.apache.hadoop.ozone.ksm.exceptions
    .KSMException.ResultCodes;

/**
 * KSM volume management code.
 */
public class VolumeManagerImpl implements VolumeManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(VolumeManagerImpl.class);

  private final MetadataManager metadataManager;
  private final int maxUserVolumeCount;

  /**
   * Constructor.
   * @param conf - Ozone configuration.
   * @throws IOException
   */
  public VolumeManagerImpl(MetadataManager metadataManager,
      OzoneConfiguration conf) throws IOException {
    this.metadataManager = metadataManager;
    this.maxUserVolumeCount = conf.getInt(OZONE_KSM_USER_MAX_VOLUME,
        OZONE_KSM_USER_MAX_VOLUME_DEFAULT);
  }

  // Helpers to add and delete volume from user list
  private void addVolumeToOwnerList(String volume, String owner,
                                    List<Map.Entry<byte[], byte[]>> putBatch)
      throws IOException {
    // Get the volume list
    byte[] dbUserKey = metadataManager.getUserKey(owner);
    byte[] volumeList  = metadataManager.get(dbUserKey);
    List<String> prevVolList = new LinkedList<>();
    if (volumeList != null) {
      VolumeList vlist = VolumeList.parseFrom(volumeList);
      prevVolList.addAll(vlist.getVolumeNamesList());
    }

    // Check the volume count
    if (prevVolList.size() >= maxUserVolumeCount) {
      LOG.error("Too many volumes for user:{}", owner);
      throw new KSMException(ResultCodes.FAILED_TOO_MANY_USER_VOLUMES);
    }

    // Add the new volume to the list
    prevVolList.add(volume);
    VolumeList newVolList = VolumeList.newBuilder()
        .addAllVolumeNames(prevVolList).build();
    putBatch.add(batchEntry(dbUserKey, newVolList.toByteArray()));
  }

  private void delVolumeFromOwnerList(String volume, String owner,
                                      List<Map.Entry<byte[], byte[]>> putBatch,
                                      List<byte[]> deleteBatch)
      throws IOException {
    // Get the volume list
    byte[] dbUserKey = metadataManager.getUserKey(owner);
    byte[] volumeList  = metadataManager.get(dbUserKey);
    List<String> prevVolList = new LinkedList<>();
    if (volumeList != null) {
      VolumeList vlist = VolumeList.parseFrom(volumeList);
      prevVolList.addAll(vlist.getVolumeNamesList());
    } else {
      throw new KSMException(ResultCodes.FAILED_USER_NOT_FOUND);
    }

    // Remove the volume from the list
    prevVolList.remove(volume);
    if (prevVolList.size() == 0) {
      deleteBatch.add(dbUserKey);
    } else {
      VolumeList newVolList = VolumeList.newBuilder()
          .addAllVolumeNames(prevVolList).build();
      putBatch.add(batchEntry(dbUserKey, newVolList.toByteArray()));
    }
  }

  private Map.Entry<byte[], byte[]> batchEntry(byte[] key, byte[] value) {
    return new AbstractMap.SimpleEntry<>(key, value);
  }

  /**
   * Creates a volume.
   * @param args - KsmVolumeArgs.
   */
  @Override
  public void createVolume(KsmVolumeArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    metadataManager.writeLock().lock();
    List<Map.Entry<byte[], byte[]>> batch = new LinkedList<>();
    try {
      byte[] dbVolumeKey = metadataManager.getVolumeKey(args.getVolume());
      byte[] volumeInfo = metadataManager.get(dbVolumeKey);

      // Check of the volume already exists
      if (volumeInfo != null) {
        LOG.error("volume:{} already exists", args.getVolume());
        throw new KSMException(ResultCodes.FAILED_VOLUME_ALREADY_EXISTS);
      }

      // Write the vol info
      VolumeInfo newVolumeInfo = args.getProtobuf();
      batch.add(batchEntry(dbVolumeKey, newVolumeInfo.toByteArray()));

      // Add volume to user list
      addVolumeToOwnerList(args.getVolume(), args.getOwnerName(), batch);
      metadataManager.batchPut(batch);
      LOG.info("created volume:{} user:{}",
                                  args.getVolume(), args.getOwnerName());
    } catch (IOException | DBException ex) {
      LOG.error("Volume creation failed for user:{} volname:{}",
                                args.getOwnerName(), args.getVolume(), ex);
      throw ex;
    } finally {
      metadataManager.writeLock().unlock();
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
    List<Map.Entry<byte[], byte[]>> putBatch = new LinkedList<>();
    List<byte[]> deleteBatch = new LinkedList<>();
    metadataManager.writeLock().lock();
    try {
      byte[] dbVolumeKey = metadataManager.getVolumeKey(volume);
      byte[] volInfo = metadataManager.get(dbVolumeKey);
      if (volInfo == null) {
        throw  new KSMException(ResultCodes.FAILED_VOLUME_NOT_FOUND);
      }

      VolumeInfo volumeInfo = VolumeInfo.parseFrom(volInfo);
      KsmVolumeArgs volumeArgs = KsmVolumeArgs.getFromProtobuf(volumeInfo);
      Preconditions.checkState(volume.equals(volumeInfo.getVolume()));

      delVolumeFromOwnerList(volume, volumeArgs.getOwnerName(),
          putBatch, deleteBatch);
      addVolumeToOwnerList(volume, owner, putBatch);

      KsmVolumeArgs newVolumeArgs =
          KsmVolumeArgs.newBuilder().setVolume(volumeArgs.getVolume())
              .setAdminName(volumeArgs.getAdminName())
              .setOwnerName(owner)
              .setQuotaInBytes(volumeArgs.getQuotaInBytes())
              .build();

      VolumeInfo newVolumeInfo = newVolumeArgs.getProtobuf();
      putBatch.add(batchEntry(dbVolumeKey, newVolumeInfo.toByteArray()));

      metadataManager.batchPutDelete(putBatch, deleteBatch);
    } catch (IOException ex) {
      LOG.error("Changing volume ownership failed for user:{} volume:{}",
          owner, volume, ex);
      throw ex;
    } finally {
      metadataManager.writeLock().unlock();
    }
  }

  /**
   * Changes the Quota on a volume.
   *
   * @param volume - Name of the volume.
   * @param quota - Quota in bytes.
   * @throws IOException
   */
  public void setQuota(String volume, long quota) throws IOException {
    Preconditions.checkNotNull(volume);
    metadataManager.writeLock().lock();
    try {
      byte[] dbVolumeKey = metadataManager.getVolumeKey(volume);
      byte[] volInfo = metadataManager.get(dbVolumeKey);
      if (volInfo == null) {
        throw  new KSMException(ResultCodes.FAILED_VOLUME_NOT_FOUND);
      }

      VolumeInfo volumeInfo = VolumeInfo.parseFrom(volInfo);
      KsmVolumeArgs volumeArgs = KsmVolumeArgs.getFromProtobuf(volumeInfo);
      Preconditions.checkState(volume.equals(volumeInfo.getVolume()));

      KsmVolumeArgs newVolumeArgs =
          KsmVolumeArgs.newBuilder().setVolume(volumeArgs.getVolume())
              .setAdminName(volumeArgs.getAdminName())
              .setOwnerName(volumeArgs.getOwnerName())
              .setQuotaInBytes(quota)
              .build();

      VolumeInfo newVolumeInfo = newVolumeArgs.getProtobuf();
      metadataManager.put(dbVolumeKey, newVolumeInfo.toByteArray());
    } catch (IOException ex) {
      LOG.error("Changing volume quota failed for volume:{} quota:{}",
          volume, quota, ex);
      throw ex;
    } finally {
      metadataManager.writeLock().unlock();
    }
  }

  /**
   * Gets the volume information.
   * @param volume - Volume name.
   * @return VolumeArgs or exception is thrown.
   * @throws IOException
   */
  public KsmVolumeArgs getVolumeInfo(String volume) throws IOException {
    Preconditions.checkNotNull(volume);
    metadataManager.readLock().lock();
    try {
      byte[] dbVolumeKey = metadataManager.getVolumeKey(volume);
      byte[] volInfo = metadataManager.get(dbVolumeKey);
      if (volInfo == null) {
        throw  new KSMException(ResultCodes.FAILED_VOLUME_NOT_FOUND);
      }

      VolumeInfo volumeInfo = VolumeInfo.parseFrom(volInfo);
      KsmVolumeArgs volumeArgs = KsmVolumeArgs.getFromProtobuf(volumeInfo);
      Preconditions.checkState(volume.equals(volumeInfo.getVolume()));
      return volumeArgs;
    } catch (IOException ex) {
      LOG.error("Info volume failed for volume:{}", volume, ex);
      throw ex;
    } finally {
      metadataManager.readLock().unlock();
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
    metadataManager.writeLock().lock();
    try {
      List<Map.Entry<byte[], byte[]>> putBatch = new LinkedList<>();
      List<byte[]> deleteBatch = new LinkedList<>();
      byte[] dbVolumeKey = metadataManager.getVolumeKey(volume);
      byte[] volInfo = metadataManager.get(dbVolumeKey);
      if (volInfo == null) {
        throw new KSMException(ResultCodes.FAILED_VOLUME_NOT_FOUND);
      }

      if (!metadataManager.isVolumeEmpty(volume)) {
        throw new KSMException(ResultCodes.FAILED_VOLUME_NOT_EMPTY);
      }

      VolumeInfo volumeInfo = VolumeInfo.parseFrom(volInfo);
      Preconditions.checkState(volume.equals(volumeInfo.getVolume()));
      // delete the volume from the owner list
      // as well as delete the volume entry
      delVolumeFromOwnerList(volume, volumeInfo.getOwnerName(),
          putBatch, deleteBatch);
      deleteBatch.add(dbVolumeKey);
      metadataManager.batchPutDelete(putBatch, deleteBatch);
    } catch (IOException ex) {
      LOG.error("Delete volume failed for volume:{}", volume, ex);
      throw ex;
    } finally {
      metadataManager.writeLock().unlock();
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
  public boolean checkVolumeAccess(String volume, OzoneAclInfo userAcl)
      throws IOException {
    Preconditions.checkNotNull(volume);
    Preconditions.checkNotNull(userAcl);
    metadataManager.readLock().lock();
    try {
      byte[] dbVolumeKey = metadataManager.getVolumeKey(volume);
      byte[] volInfo = metadataManager.get(dbVolumeKey);
      if (volInfo == null) {
        throw  new KSMException(ResultCodes.FAILED_VOLUME_NOT_FOUND);
      }

      VolumeInfo volumeInfo = VolumeInfo.parseFrom(volInfo);
      KsmVolumeArgs volumeArgs = KsmVolumeArgs.getFromProtobuf(volumeInfo);
      Preconditions.checkState(volume.equals(volumeInfo.getVolume()));
      return volumeArgs.getAclMap().hasAccess(userAcl);
    } catch (IOException ex) {
      LOG.error("Check volume access failed for volume:{} user:{} rights:{}",
          volume, userAcl.getName(), userAcl.getRights(), ex);
      throw ex;
    } finally {
      metadataManager.readLock().unlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<KsmVolumeArgs> listVolumes(String userName,
      String prefix, String startKey, int maxKeys) throws IOException {
    metadataManager.readLock().lock();
    try {
      return metadataManager.listVolumes(
          userName, prefix, startKey, maxKeys);
    } finally {
      metadataManager.readLock().unlock();
    }
  }
}
