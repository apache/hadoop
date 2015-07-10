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
package org.apache.hadoop.ozone.web.localstorage;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.StorageContainerConfiguration;
import org.apache.hadoop.ozone.web.exceptions.ErrorTable;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.response.ListVolumes;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.response.VolumeOwner;
import org.apache.hadoop.ozone.web.utils.OzoneConsts;
import org.iq80.leveldb.DBException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A stand alone Ozone implementation that allows us to run
 * Ozone tests in local mode. This acts as the
 * ozone backend when using MiniDFSCluster for testing.
 */
public final class OzoneMetadataManager {
  static final Log LOG = LogFactory.getLog(OzoneMetadataManager.class);
  private static OzoneMetadataManager bm = null;

  /*
    OzoneMetadataManager manages volume/bucket/object metadata and
    data.

    Metadata is maintained in 2 level DB files, UserDB and MetadataDB.

    UserDB contains a Name and a List. For example volumes owned by the user
    bilbo, would be maintained in UserDB as {bilbo}->{shire, rings}

    This list part of mapping is context sensitive.  That is, if you use {user
    name} as the key, the list you get is a list of volumes. if you use
    {user/volume} as the key the list you get is list of buckets. if you use
    {user/volume/bucket} as key the list you get is the list of objects.

    All keys in the UserDB starts with the UserName.

    We also need to maintain a flat namespace for volumes. This is
    maintained by the MetadataDB. MetadataDB contains the name of an
    object(volume, bucket or key) and its associated metadata.
    The keys in the Metadata DB are {volume}, {volume/bucket} or
    {volume/bucket/key}. User name is absent, so we have a common root name
    space for the volume.

    The value of part of metadataDB points to corresponding *Info structures.
    {volume] -> volumeInfo
    {volume/bucket} -> bucketInfo
    {volume/bucket/key} -> keyInfo


    Here are various work flows :

    CreateVolume -> Check if Volume exists in metadataDB, if not update UserDB
    with a list of volumes and update metadataDB with VolumeInfo.

    DeleteVolume -> Check the Volume, and check the VolumeInfo->bucketCount.
    if bucketCount == 0, delete volume from userDB->{List of volumes} and
    metadataDB.

    Very similar work flows exist for CreateBucket and DeleteBucket.

      // Please note : These database operations are *not* transactional,
      // which means that failure can lead to inconsistencies.
      // Only way to recover is to reset to a clean state, or
      // use rm -rf /tmp/ozone :)

    We have very simple locking policy. We have a ReaderWriter lock that is
    taken for each action, this lock is aptly named "lock".

    All actions *must* be performed with a lock held, either a read
    lock or a write lock. Violation of these locking policies can be harmful.


      // // IMPORTANT :
      // //  This is a simulation layer, this is NOT how the real
      // //  OZONE functions. This is written to so that we can write
      // //  stand-alone tests for the protocol and client code.

*/

  private OzoneLevelDBStore userDB;
  private OzoneLevelDBStore metadataDB;

  private static final  String USER_DB = "/user.db";
  private static final  String META_DB = "/metadata.db";

  private ReadWriteLock lock;
  private Charset encoding = Charset.forName("UTF-8");

  /**
   * Constructs OzoneMetadataManager.
   */
  private OzoneMetadataManager() {

    lock = new ReentrantReadWriteLock();
    StorageContainerConfiguration conf = new StorageContainerConfiguration();

    String storageRoot =
        conf.getTrimmed(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT,
                    OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT_DEFAULT);

    File file = new File(storageRoot);

    if (!file.exists() && !file.mkdirs()) {
      LOG.fatal("Creation of Ozone root failed. " + file.toString());
    }

    try {
      userDB = new OzoneLevelDBStore(new File(storageRoot + USER_DB), true);
      metadataDB = new OzoneLevelDBStore(new File(storageRoot + META_DB), true);
    } catch (IOException ex) {
      LOG.fatal("Cannot open db :" + ex.getMessage());
    }
  }

  /**
   * Gets Ozone Manager.
   * @return OzoneMetadataManager
   */
  public static synchronized  OzoneMetadataManager getOzoneMetadataManager() {
    if (bm == null) {
      bm = new OzoneMetadataManager();
    }
    return bm;
  }

  /**
   * Creates a volume.
   *
   * @param args - VolumeArgs
   *
   * @throws OzoneException
   */
  public void createVolume(VolumeArgs args) throws OzoneException {
    try {
      SimpleDateFormat format =
          new SimpleDateFormat(OzoneConsts.OZONE_DATE_FORMAT, Locale.US);
      format.setTimeZone(TimeZone.getTimeZone(OzoneConsts.OZONE_TIME_ZONE));

      lock.writeLock().lock();
      byte[] volumeName =
          metadataDB.get(args.getVolumeName().getBytes(encoding));

      if (volumeName != null) {
        LOG.debug("Volume already exists.");
        throw ErrorTable.newError(ErrorTable.VOLUME_ALREADY_EXISTS, args);
      }

      VolumeInfo newVInfo = new VolumeInfo(args.getVolumeName(), format
          .format(new Date(System.currentTimeMillis())), args.getAdminName());

      newVInfo.setQuota(args.getQuota());
      VolumeOwner owner = new VolumeOwner(args.getUserName());
      newVInfo.setOwner(owner);

      ListVolumes volumeList;
      byte[] userVolumes = userDB.get(args.getUserName().getBytes(encoding));
      if (userVolumes == null) {
        volumeList = new ListVolumes();
      } else {
        volumeList = ListVolumes.parse(new String(userVolumes, encoding));
      }

      volumeList.addVolume(newVInfo);
      volumeList.sort();

      // Please note : These database operations are *not* transactional,
      // which means that failure can lead to inconsistencies.
      // Only way to recover is to reset to a clean state, or
      // use rm -rf /tmp/ozone :)


      userDB.put(args.getUserName().getBytes(encoding),
                 volumeList.toDBString().getBytes(encoding));

      metadataDB.put(args.getVolumeName().getBytes(encoding),
                     newVInfo.toDBString().getBytes(encoding));

    } catch (IOException | DBException ex) {
      throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args, ex);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Updates the Volume properties like Owner Name and Quota.
   *
   * @param args - Volume Args
   * @param property - Flag which tells us what property to upgrade
   *
   * @throws OzoneException
   */
  public void setVolumeProperty(VolumeArgs args, VolumeProperty property)
      throws OzoneException {
    VolumeInfo info;
    try {
      lock.writeLock().lock();
      byte[] volumeInfo =
          metadataDB.get(args.getVolumeName().getBytes(encoding));
      if (volumeInfo == null) {
        throw ErrorTable.newError(ErrorTable.VOLUME_NOT_FOUND, args);
      }
      info = VolumeInfo.parse(new String(volumeInfo, encoding));

      byte[] userBytes = userDB.get(args.getResourceName().getBytes(encoding));
      ListVolumes volumeList;
      if (userBytes == null) {
        volumeList = new ListVolumes();
      } else {
        volumeList = ListVolumes.parse(new String(userBytes, encoding));
      }

      switch (property) {
      case OWNER:
        // needs new owner, we delete the volume object from the
        // old user's volume list
        removeOldOwner(info);
        VolumeOwner owner = new VolumeOwner(args.getUserName());
        // set the new owner
        info.setOwner(owner);
        break;
      case QUOTA:
        // if this is quota update we just remove the old object from the
        // current users list and update the same object later.
        volumeList.getVolumes().remove(info);
        info.setQuota(args.getQuota());
        break;
      default:
        OzoneException ozEx =
            ErrorTable.newError(ErrorTable.SERVER_ERROR, args);
        ozEx.setMessage("Volume property is not recognized");
        throw ozEx;
      }

      volumeList.addVolume(info);

      metadataDB.put(args.getVolumeName().getBytes(encoding),
                     info.toDBString().getBytes(encoding));

      // if this is an owner change this put will create a new owner or update
      // the owner's volume list.
      userDB.put(args.getResourceName().getBytes(encoding),
                 volumeList.toDBString().getBytes(encoding));

    } catch (IOException | DBException ex) {
      throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args, ex);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Removes the old owner from the volume.
   *
   * @param info - VolumeInfo
   *
   * @throws IOException
   */
  private void removeOldOwner(VolumeInfo info) throws IOException {
    // We need to look the owner that we know is the current owner
    byte[] volumeBytes =
        userDB.get(info.getOwner().getName().getBytes(encoding));
    ListVolumes volumeList =
        ListVolumes.parse(new String(volumeBytes, encoding));
    volumeList.getVolumes().remove(info);

    // Write the new list info to the old user data
    userDB.put(info.getOwner().getName().getBytes(encoding),
               volumeList.toDBString().getBytes(encoding));
  }

  /**
   * Checks if you are the owner of a specific volume.
   *
   * @param args - VolumeArgs
   *
   * @return - True if you are the owner, false otherwise
   *
   * @throws OzoneException
   */
  public boolean checkVolumeAccess(VolumeArgs args) throws OzoneException {
    VolumeInfo info;
    try {
      lock.readLock().lock();
      byte[] volumeInfo =
          metadataDB.get(args.getVolumeName().getBytes(encoding));
      if (volumeInfo == null) {
        throw ErrorTable.newError(ErrorTable.VOLUME_NOT_FOUND, args);
      }
      info = VolumeInfo.parse(new String(volumeInfo, encoding));
      return info.getOwner().getName().equals(args.getUserName());
    } catch (IOException | DBException ex) {
      throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args, ex);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * getVolumeInfo returns the Volume Info of a specific volume.
   *
   * @param args - Volume args
   *
   * @return VolumeInfo
   *
   * @throws OzoneException
   */
  public VolumeInfo getVolumeInfo(VolumeArgs args) throws OzoneException {
    try {
      lock.readLock().lock();
      byte[] volumeInfo =
          metadataDB.get(args.getVolumeName().getBytes(encoding));
      if (volumeInfo == null) {
        throw ErrorTable.newError(ErrorTable.VOLUME_NOT_FOUND, args);
      }

      return VolumeInfo.parse(new String(volumeInfo, encoding));
    } catch (IOException | DBException ex) {
      throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args, ex);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns all the volumes owned by a specific user.
   *
   * @param args - User Args
   *
   * @return - ListVolumes
   *
   * @throws OzoneException
   */
  public ListVolumes listVolumes(UserArgs args) throws OzoneException {
    try {
      lock.readLock().lock();
      byte[] volumeList = userDB.get(args.getUserName().getBytes(encoding));
      if (volumeList == null) {
        throw ErrorTable.newError(ErrorTable.USER_NOT_FOUND, args);
      }
      return ListVolumes.parse(new String(volumeList, encoding));
    } catch (IOException | DBException ex) {
      throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args, ex);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Deletes a volume if it exists and is empty.
   *
   * @param args - volume args
   *
   * @throws OzoneException
   */
  public void deleteVolume(VolumeArgs args) throws OzoneException {
    try {
      lock.writeLock().lock();
      byte[] volumeName =
          metadataDB.get(args.getVolumeName().getBytes(encoding));
      if (volumeName == null) {
        throw ErrorTable.newError(ErrorTable.VOLUME_NOT_FOUND, args);
      }

      VolumeInfo vInfo = VolumeInfo.parse(new String(volumeName, encoding));

      // Only remove volumes if they are empty.
      if (vInfo.getBucketCount() > 0) {
        throw ErrorTable.newError(ErrorTable.VOLUME_NOT_EMPTY, args);
      }

      ListVolumes volumeList;
      String user = vInfo.getOwner().getName();
      byte[] userVolumes = userDB.get(user.getBytes(encoding));
      if (userVolumes == null) {
        throw ErrorTable.newError(ErrorTable.VOLUME_NOT_FOUND, args);
      }

      volumeList = ListVolumes.parse(new String(userVolumes, encoding));
      volumeList.getVolumes().remove(vInfo);

      metadataDB.delete(args.getVolumeName().getBytes(encoding));
      userDB.put(user.getBytes(encoding),
                 volumeList.toDBString().getBytes(encoding));
    } catch (IOException | DBException ex) {
      throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args, ex);
    } finally {
      lock.writeLock().unlock();
    }
  }


  /**
   * This is used in updates to volume metadata.
   */
  public enum VolumeProperty {
    OWNER, QUOTA
  }
}
