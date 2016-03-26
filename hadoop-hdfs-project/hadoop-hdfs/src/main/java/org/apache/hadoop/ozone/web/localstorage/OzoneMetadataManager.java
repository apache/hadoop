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

import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.ozone.web.handlers.ListArgs;
import org.apache.hadoop.ozone.web.response.KeyInfo;
import org.apache.hadoop.ozone.web.response.ListKeys;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.utils.LevelDBStore;
import org.apache.hadoop.ozone.web.exceptions.ErrorTable;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.request.OzoneAcl;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.ListBuckets;
import org.apache.hadoop.ozone.web.response.ListVolumes;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.response.VolumeOwner;
import org.apache.hadoop.ozone.OzoneConsts;
import org.iq80.leveldb.DBException;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A stand alone Ozone implementation that allows us to run Ozone tests in local
 * mode. This acts as the ozone backend when using MiniDFSCluster for testing.
 */
public final class OzoneMetadataManager {

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
  static final Logger LOG = LoggerFactory.getLogger(OzoneMetadataManager.class);
  private static final String USER_DB = "/user.db";
  private static final String META_DB = "/metadata.db";
  private static OzoneMetadataManager bm = null;
  private LevelDBStore userDB;
  private LevelDBStore metadataDB;
  private ReadWriteLock lock;
  private Charset encoding = Charset.forName("UTF-8");
  private String storageRoot;
  private static final String OBJECT_DIR = "/_objects/";

  // This table keeps a pointer to objects whose operations
  // are in progress but not yet committed to persistent store
  private ConcurrentHashMap<OutputStream, String> inProgressObjects;

  /**
   * Constructs OzoneMetadataManager.
   */
  private OzoneMetadataManager(Configuration conf) throws IOException {

    lock = new ReentrantReadWriteLock();
    storageRoot =
        conf.getTrimmed(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT,
            OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT_DEFAULT);

    File file = new File(storageRoot + OBJECT_DIR);

    if (!file.exists() && !file.mkdirs()) {
      LOG.error("Creation of Ozone root failed. " + file.toString());
      throw new IOException("Creation of Ozone root failed.");
    }

    try {
      userDB = new LevelDBStore(new File(storageRoot + USER_DB), true);
      metadataDB = new LevelDBStore(new File(storageRoot + META_DB), true);
      inProgressObjects = new ConcurrentHashMap<>();
    } catch (IOException ex) {
      LOG.error("Cannot open db :" + ex.getMessage());
      throw ex;
    }
  }

  /**
   * Gets Ozone Manager.
   *
   * @return OzoneMetadataManager
   */
  public static synchronized OzoneMetadataManager
      getOzoneMetadataManager(Configuration conf) throws IOException {
    if (bm == null) {
      bm = new OzoneMetadataManager(conf);
    }
    return bm;
  }

  /**
   * Creates a volume.
   *
   * @param args - VolumeArgs
   * @throws OzoneException
   */
  public void createVolume(VolumeArgs args) throws OzoneException {
    lock.writeLock().lock();
    try {
      SimpleDateFormat format =
          new SimpleDateFormat(OzoneConsts.OZONE_DATE_FORMAT, Locale.US);
      format.setTimeZone(TimeZone.getTimeZone(OzoneConsts.OZONE_TIME_ZONE));

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
   * @param args     - Volume Args
   * @param property - Flag which tells us what property to upgrade
   * @throws OzoneException
   */
  public void setVolumeProperty(VolumeArgs args, VolumeProperty property)
      throws OzoneException {
    lock.writeLock().lock();
    try {
      byte[] volumeInfo =
          metadataDB.get(args.getVolumeName().getBytes(encoding));
      if (volumeInfo == null) {
        throw ErrorTable.newError(ErrorTable.VOLUME_NOT_FOUND, args);
      }
      VolumeInfo info = VolumeInfo.parse(new String(volumeInfo, encoding));

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
            ErrorTable.newError(ErrorTable.BAD_PROPERTY, args);
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
   * @return - True if you are the owner, false otherwise
   * @throws OzoneException
   */
  public boolean checkVolumeAccess(VolumeArgs args) throws OzoneException {
    lock.readLock().lock();
    try {
      byte[] volumeInfo =
          metadataDB.get(args.getVolumeName().getBytes(encoding));
      if (volumeInfo == null) {
        throw ErrorTable.newError(ErrorTable.VOLUME_NOT_FOUND, args);
      }

      VolumeInfo info = VolumeInfo.parse(new String(volumeInfo, encoding));
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
   * @return VolumeInfo
   * @throws OzoneException
   */
  public VolumeInfo getVolumeInfo(VolumeArgs args) throws OzoneException {
    lock.readLock().lock();
    try {
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
   * @return - ListVolumes
   * @throws OzoneException
   */
  public ListVolumes listVolumes(UserArgs args) throws OzoneException {
    lock.readLock().lock();
    try {
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
   * @throws OzoneException
   */
  public void deleteVolume(VolumeArgs args) throws OzoneException {
    lock.writeLock().lock();
    try {
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
   * Create a bucket if it does not exist.
   *
   * @param args - BucketArgs
   * @throws OzoneException
   */
  public void createBucket(BucketArgs args) throws OzoneException {
    lock.writeLock().lock();
    try {
      // check if volume exists, buckets cannot be created without volumes
      byte[] volumeName = metadataDB.get(args.getVolumeName()
          .getBytes(encoding));
      if (volumeName == null) {
        throw ErrorTable.newError(ErrorTable.VOLUME_NOT_FOUND, args);
      }

      // A resource name is volume/bucket -- That is the key in metadata table
      byte[] bucketName = metadataDB.get(args.getResourceName()
          .getBytes(encoding));
      if (bucketName != null) {
        throw ErrorTable.newError(ErrorTable.BUCKET_ALREADY_EXISTS, args);
      }

      BucketInfo bucketInfo =
          new BucketInfo(args.getVolumeName(), args.getBucketName());

      if (args.getRemoveAcls() != null) {
        OzoneException ex = ErrorTable.newError(ErrorTable.MALFORMED_ACL, args);
        ex.setMessage("Remove ACLs specified in bucket create. Please remove "
            + "them and retry.");
        throw ex;
      }

      VolumeInfo volInfo = VolumeInfo.parse(new String(volumeName, encoding));
      volInfo.setBucketCount(volInfo.getBucketCount() + 1);

      bucketInfo.setAcls(args.getAddAcls());
      bucketInfo.setStorageType(args.getStorageType());
      bucketInfo.setVersioning(args.getVersioning());
      ListBuckets bucketList;

      // get bucket list from user/volume -> bucketList
      byte[] volumeBuckets = userDB.get(args.getParentName()
          .getBytes(encoding));
      if (volumeBuckets == null) {
        bucketList = new ListBuckets();
      } else {
        bucketList = ListBuckets.parse(new String(volumeBuckets, encoding));
      }

      bucketList.addBucket(bucketInfo);
      bucketList.sort();

      // Update Volume->bucketCount
      userDB.put(args.getVolumeName().getBytes(encoding),
          volInfo.toDBString().getBytes(encoding));

      // Now update the userDB with user/volume -> bucketList
      userDB.put(args.getParentName().getBytes(encoding),
          bucketList.toDBString().getBytes(encoding));

      // and update the metadataDB with volume/bucket->BucketInfo
      metadataDB.put(args.getResourceName().getBytes(encoding),
          bucketInfo.toDBString().getBytes(encoding));

    } catch (IOException | DBException ex) {
      throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args, ex);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Updates the Bucket properties like ACls and Storagetype.
   *
   * @param args     - Bucket Args
   * @param property - Flag which tells us what property to upgrade
   * @throws OzoneException
   */
  public void setBucketProperty(BucketArgs args, BucketProperty property)
      throws OzoneException {

    lock.writeLock().lock();
    try {
      // volume/bucket-> bucketInfo
      byte[] bucketInfo = metadataDB.get(args.getResourceName().
          getBytes(encoding));
      if (bucketInfo == null) {
        throw ErrorTable.newError(ErrorTable.INVALID_BUCKET_NAME, args);
      }

      BucketInfo info = BucketInfo.parse(new String(bucketInfo, encoding));
      byte[] volumeBuckets = userDB.get(args.getParentName()
          .getBytes(encoding));
      ListBuckets bucketList = ListBuckets.parse(new String(volumeBuckets,
          encoding));
      bucketList.getBuckets().remove(info);

      switch (property) {
      case ACLS:
        processRemoveAcls(args, info);
        processAddAcls(args, info);
        break;
      case STORAGETYPE:
        info.setStorageType(args.getStorageType());
        break;
      case VERSIONING:
        info.setVersioning(args.getVersioning());
        break;
      default:
        OzoneException ozEx =
            ErrorTable.newError(ErrorTable.BAD_PROPERTY, args);
        ozEx.setMessage("Bucket property is not recognized.");
        throw ozEx;
      }

      bucketList.addBucket(info);
      metadataDB.put(args.getResourceName().getBytes(encoding),
          info.toDBString().getBytes(encoding));

      userDB.put(args.getParentName().getBytes(encoding),
          bucketList.toDBString().getBytes(encoding));
    } catch (IOException | DBException ex) {
      throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args, ex);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Process Remove Acls and remove them from the bucket.
   *
   * @param args - BucketArgs
   * @param info - BucketInfo
   */
  private void processRemoveAcls(BucketArgs args, BucketInfo info) {
    List<OzoneAcl> removeAcls = args.getRemoveAcls();
    if ((removeAcls == null) || (info.getAcls() == null)) {
      return;
    }
    for (OzoneAcl racl : args.getRemoveAcls()) {
      ListIterator<OzoneAcl> aclIter = info.getAcls().listIterator();
      while (aclIter.hasNext()) {
        if (racl.equals(aclIter.next())) {
          aclIter.remove();
          break;
        }
      }
    }
  }

  /**
   * Process Add Acls and Add them to the bucket.
   *
   * @param args - BucketArgs
   * @param info - BucketInfo
   */
  private void processAddAcls(BucketArgs args, BucketInfo info) {
    List<OzoneAcl> addAcls = args.getAddAcls();
    if ((addAcls == null)) {
      return;
    }

    if (info.getAcls() == null) {
      info.setAcls(addAcls);
      return;
    }

    for (OzoneAcl newacl : addAcls) {
      ListIterator<OzoneAcl> aclIter = info.getAcls().listIterator();
      while (aclIter.hasNext()) {
        if (newacl.equals(aclIter.next())) {
          continue;
        }
      }
      info.getAcls().add(newacl);
    }
  }

  /**
   * Deletes a given bucket.
   *
   * @param args - BucketArgs
   * @throws OzoneException
   */
  public void deleteBucket(BucketArgs args) throws OzoneException {
    lock.writeLock().lock();
    try {
      byte[] bucketInfo = metadataDB.get(args.getResourceName()
          .getBytes(encoding));
      if (bucketInfo == null) {
        throw ErrorTable.newError(ErrorTable.INVALID_BUCKET_NAME, args);
      }

      BucketInfo bInfo = BucketInfo.parse(new String(bucketInfo, encoding));

      // Only remove buckets if they are empty.
      if (bInfo.getKeyCount() > 0) {
        throw ErrorTable.newError(ErrorTable.BUCKET_NOT_EMPTY, args);
      }

      byte[] bucketBytes = userDB.get(args.getParentName().getBytes(encoding));
      if (bucketBytes == null) {
        throw ErrorTable.newError(ErrorTable.INVALID_BUCKET_NAME, args);
      }

      ListBuckets bucketList =
          ListBuckets.parse(new String(bucketBytes, encoding));
      bucketList.getBuckets().remove(bInfo);

      metadataDB.delete(args.getResourceName().getBytes(encoding));
      userDB.put(args.getParentName().getBytes(encoding),
          bucketList.toDBString().getBytes(encoding));
    } catch (IOException | DBException ex) {
      throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args, ex);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns the Bucket info for a given bucket.
   *
   * @param args - Bucket Args
   * @return BucketInfo   -  Bucket Information
   * @throws OzoneException
   */
  public BucketInfo getBucketInfo(BucketArgs args) throws OzoneException {
    lock.readLock().lock();
    try {
      byte[] bucketBytes = metadataDB.get(args.getResourceName()
          .getBytes(encoding));
      if (bucketBytes == null) {
        throw ErrorTable.newError(ErrorTable.INVALID_BUCKET_NAME, args);
      }

      return BucketInfo.parse(new String(bucketBytes, encoding));
    } catch (IOException ex) {
      throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args, ex);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns a list of buckets for a given volume.
   *
   * @param args - volume args
   * @return List of buckets
   * @throws OzoneException
   */
  public ListBuckets listBuckets(VolumeArgs args) throws OzoneException {
    lock.readLock().lock();
    try {
      String userVolKey = args.getUserName() + "/" + args.getVolumeName();

      byte[] bucketBytes = userDB.get(userVolKey.getBytes(encoding));
      if (bucketBytes == null) {
        throw ErrorTable.newError(ErrorTable.INVALID_VOLUME_NAME, args);
      }
      return ListBuckets.parse(new String(bucketBytes, encoding));
    } catch (IOException ex) {
      throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args, ex);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   *  Creates a key and returns a stream to which this key can be written to.
   * @param args  KeyArgs
   * @return - A stream into which key can be written to.
   * @throws OzoneException
   */
  public OutputStream createKey(KeyArgs args) throws OzoneException {
    lock.writeLock().lock();
    try {
      String fileNameHash = DigestUtils.sha256Hex(args.getResourceName());

      // Please don't try trillion objects unless the physical file system
      // is capable of doing that in a single directory.

      String fullPath = storageRoot + OBJECT_DIR + fileNameHash;
      File f = new File(fullPath);

      // In real ozone it would not be this way, a file will be overwritten
      // only if the upload is successful.
      if (f.exists()) {
        LOG.debug("we are overwriting a file. This is by design.");
        if(!f.delete()) {
          LOG.error("Unable to delete the file: {}", fullPath);
          throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args);
        }
      }

     // f.createNewFile();
      FileOutputStream fsStream = new FileOutputStream(f);
      inProgressObjects.put(fsStream, fullPath);

      return fsStream;
    } catch (IOException e) {
      throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args, e);
    } finally {
      lock.writeLock().unlock();
    }
  }


  /**
   * commit keys moves an In progress object into the metadata store
   * so that key is visible in the metadata operations from that point
   * onwards.
   *
   * @param args Object args
   *
   * @throws OzoneException
   */
  public void commitKey(KeyArgs args, OutputStream stream)
      throws OzoneException {
    SimpleDateFormat format =
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss ZZZ", Locale.US);
    lock.writeLock().lock();

    try {
      byte[] bucketInfo = metadataDB.get(args.getParentName()
          .getBytes(encoding));
      if (bucketInfo == null) {
        throw ErrorTable.newError(ErrorTable.INVALID_BUCKET_NAME, args);
      }
      BucketInfo bInfo = BucketInfo.parse(new String(bucketInfo, encoding));
      bInfo.setKeyCount(bInfo.getKeyCount() + 1);

      String fileNameHash = inProgressObjects.get(stream);
      inProgressObjects.remove(stream);
      if (fileNameHash == null) {
        throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args);
      }

      ListKeys keyList;
      byte[] bucketListBytes = userDB.get(args.getParentName()
          .getBytes(encoding));
      if (bucketListBytes == null) {
        keyList = new ListKeys();
      } else {
        keyList = ListKeys.parse(new String(bucketListBytes, encoding));
      }

      KeyInfo keyInfo;

      byte[] objectBytes = metadataDB.get(args.getResourceName()
          .getBytes(encoding));

      if (objectBytes != null) {
        // we are overwriting an existing object.
        // TODO : Emit info for Accounting
        keyInfo = KeyInfo.parse(new String(objectBytes, encoding));
        keyList.getKeyList().remove(keyInfo);
      } else {
        keyInfo = new KeyInfo();
      }

      keyInfo.setCreatedOn(format.format(new Date(System.currentTimeMillis())));

      // TODO : support version, we need to check if versioning
      // is switched on the bucket and make appropriate calls.
      keyInfo.setVersion(0);

      keyInfo.setDataFileName(fileNameHash);
      keyInfo.setKeyName(args.getKeyName());
      keyInfo.setMd5hash(args.getHash());
      keyInfo.setSize(args.getSize());

      keyList.getKeyList().add(keyInfo);

      // if the key exists, we overwrite happily :). since the
      // earlier call - createObject -  has overwritten the data.

      metadataDB.put(args.getResourceName().getBytes(encoding),
          keyInfo.toDBString().getBytes(encoding));

      metadataDB.put(args.getParentName().getBytes(encoding),
              bInfo.toDBString().getBytes(encoding));

      userDB.put(args.getParentName().getBytes(encoding),
          keyList.toDBString().getBytes(encoding));

    } catch (IOException e) {
      throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args, e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * deletes an key from a given bucket.
   *
   * @param args - ObjectArgs
   *
   * @throws OzoneException
   */
  public void deleteKey(KeyArgs args) throws OzoneException {
    lock.writeLock().lock();
    try {
      byte[] bucketInfo = metadataDB.get(args.getParentName()
          .getBytes(encoding));
      if (bucketInfo == null) {
        throw ErrorTable.newError(ErrorTable.INVALID_BUCKET_NAME, args);
      }
      BucketInfo bInfo = BucketInfo.parse(new String(bucketInfo, encoding));
      bInfo.setKeyCount(bInfo.getKeyCount() - 1);


      byte[] bucketListBytes = userDB.get(args.getParentName()
          .getBytes(encoding));
      if (bucketListBytes == null) {
        throw ErrorTable.newError(ErrorTable.INVALID_BUCKET_NAME, args);
      }
      ListKeys keyList = ListKeys.parse(new String(bucketListBytes, encoding));


      byte[] objectBytes = metadataDB.get(args.getResourceName()
          .getBytes(encoding));
      if (objectBytes == null) {
        throw ErrorTable.newError(ErrorTable.INVALID_KEY, args);
      }

      KeyInfo oInfo = KeyInfo.parse(new String(objectBytes, encoding));
      keyList.getKeyList().remove(oInfo);

      String fileNameHash = DigestUtils.sha256Hex(args.getResourceName());

      String fullPath = storageRoot + OBJECT_DIR + fileNameHash;
      File f = new File(fullPath);

      if (f.exists()) {
        if(!f.delete()) {
          throw ErrorTable.newError(ErrorTable.KEY_OPERATION_CONFLICT, args);
        }
      } else {
        throw ErrorTable.newError(ErrorTable.INVALID_KEY, args);
      }


      metadataDB.delete(args.getResourceName().getBytes(encoding));
      metadataDB.put(args.getParentName().getBytes(encoding),
              bInfo.toDBString().getBytes(encoding));
      userDB.put(args.getParentName().getBytes(encoding),
          keyList.toDBString().getBytes(encoding));
    } catch (IOException e) {
      throw ErrorTable.newError(ErrorTable.SERVER_ERROR, args, e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns a Stream for the file.
   *
   * @param args - Object args
   *
   * @return Stream
   *
   * @throws IOException
   * @throws OzoneException
   */
  public LengthInputStream newKeyReader(KeyArgs args)
      throws IOException, OzoneException {
    lock.readLock().lock();
    try {
      String fileNameHash = DigestUtils.sha256Hex(args.getResourceName());
      String fullPath = storageRoot + OBJECT_DIR + fileNameHash;
      File f = new File(fullPath);
      if (!f.exists()) {
        throw ErrorTable.newError(ErrorTable.INVALID_KEY, args);
      }
      long size = f.length();

      FileInputStream fileStream = new FileInputStream(f);
      return new LengthInputStream(fileStream, size);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns keys in a bucket.
   * @param args
   * @return  List of keys.
   * @throws IOException
   * @throws OzoneException
   */
  public ListKeys listKeys(ListArgs args) throws IOException, OzoneException {
    lock.readLock().lock();
    try {
      byte[] bucketInfo = metadataDB.get(args.getResourceName()
          .getBytes(encoding));
      if (bucketInfo == null) {
        throw ErrorTable.newError(ErrorTable.INVALID_BUCKET_NAME, args);
      }

      byte[] bucketListBytes = userDB.get(args.getResourceName()
          .getBytes(encoding));
      if (bucketListBytes == null) {
        throw ErrorTable.newError(ErrorTable.INVALID_BUCKET_NAME, args);
      }
      return ListKeys.parse(new String(bucketListBytes, encoding));
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * This is used in updates to volume metadata.
   */
  public enum VolumeProperty {
    OWNER, QUOTA
  }

  /**
   * Bucket Properties.
   */
  public enum BucketProperty {
    ACLS, STORAGETYPE, VERSIONING
  }
}
