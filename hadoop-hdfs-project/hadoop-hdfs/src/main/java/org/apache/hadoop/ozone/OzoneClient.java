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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.ozone.ksm.KSMConfigKeys;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

 /**
  * OzoneClient can connect to a Ozone Object Store and
  * perform basic operations.  It uses StorageHandler to
  * connect to KSM.
  */
public class OzoneClient implements Closeable {

  private final StorageHandler storageHandler;
  private final UserGroupInformation ugi;
  private final String hostName;
  private final OzoneAcl.OzoneACLRights userAclRights;

  public OzoneClient() throws IOException {
    this(new OzoneConfiguration());
  }

   /**
    * Creates OzoneClient object with the given configuration.
    * @param conf
    * @throws IOException
    */
  public OzoneClient(Configuration conf) throws IOException {
    this.storageHandler = new ObjectStoreHandler(conf).getStorageHandler();
    this.ugi = UserGroupInformation.getCurrentUser();
    this.hostName = OzoneUtils.getHostName();
    this.userAclRights = conf.getEnum(KSMConfigKeys.OZONE_KSM_USER_RIGHTS,
        KSMConfigKeys.OZONE_KSM_USER_RIGHTS_DEFAULT);
  }

   /**
    * Creates a new Volume.
    *
    * @param volumeName Name of the Volume
    * @throws IOException
    * @throws OzoneException
    */
  public void createVolume(String volumeName)
      throws IOException, OzoneException {
    createVolume(volumeName, ugi.getUserName());
  }

   /**
    * Creates a new Volume.
    *
    * @param volumeName Name of the Volume
    * @param owner Owner to be set for Volume
    * @throws IOException
    * @throws OzoneException
    */
  public void createVolume(String volumeName, String owner)
      throws IOException, OzoneException {
    createVolume(volumeName, owner, null);
  }

   /**
    * Creates a new Volume.
    *
    * @param volumeName Name of the Volume
    * @param owner Owner to be set for Volume
    * @param quota Volume Quota
    * @throws IOException
    * @throws OzoneException
    */
  public void createVolume(String volumeName, String owner, String quota)
      throws IOException, OzoneException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(owner);
    OzoneUtils.verifyResourceName(volumeName);

    String requestId = OzoneUtils.getRequestID();
    //since we are reusing UserArgs which is used for REST call
    // request, info, headers are null.
    UserArgs userArgs = new UserArgs(owner, requestId, hostName,
        null, null, null);
    userArgs.setGroups(ugi.getGroupNames());

    VolumeArgs volumeArgs = new VolumeArgs(volumeName, userArgs);
    //current user is set as admin for this volume
    volumeArgs.setAdminName(ugi.getUserName());
    if (quota != null) {
      volumeArgs.setQuota(quota);
    }
    storageHandler.createVolume(volumeArgs);
  }

   /**
    * Creates a new Bucket in the Volume.
    *
    * @param volumeName Name of the Volume
    * @param bucketName Name of the Bucket
    * @throws IOException
    * @throws OzoneException
    */
  public void createBucket(String volumeName, String bucketName)
      throws IOException, OzoneException {
    createBucket(volumeName, bucketName,
        OzoneConsts.Versioning.NOT_DEFINED, StorageType.DEFAULT);
  }

   /**
    * Creates a new Bucket in the Volume.
    *
    * @param volumeName
    * @param bucketName
    * @param versioning
    * @throws IOException
    * @throws OzoneException
    */
  public void createBucket(String volumeName, String bucketName,
                           OzoneConsts.Versioning versioning)
      throws IOException, OzoneException {
    createBucket(volumeName, bucketName, versioning,
        StorageType.DEFAULT);
  }

   /**
    * Creates a new Bucket in the Volume.
    *
    * @param volumeName Name of the Volume
    * @param bucketName Name of the Bucket
    * @param storageType StorageType for the Bucket
    * @throws IOException
    * @throws OzoneException
    */
  public void createBucket(String volumeName, String bucketName,
                           StorageType storageType)
      throws IOException, OzoneException {
    createBucket(volumeName, bucketName, OzoneConsts.Versioning.NOT_DEFINED,
        storageType);
  }

  public void createBucket(String volumeName, String bucketName,
                           OzoneAcl... acls)
      throws IOException, OzoneException {
    createBucket(volumeName, bucketName, OzoneConsts.Versioning.NOT_DEFINED,
        StorageType.DEFAULT, acls);
  }

  public void createBucket(String volumeName, String bucketName,
                           OzoneConsts.Versioning versioning,
                           StorageType storageType, OzoneAcl... acls)
      throws IOException, OzoneException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    OzoneUtils.verifyResourceName(bucketName);

    List<OzoneAcl> listOfAcls = new ArrayList<>();

    String userName = ugi.getUserName();
    String requestId = OzoneUtils.getRequestID();
    String[] groups = ugi.getGroupNames();

    UserArgs userArgs = new UserArgs(userName, requestId, hostName,
        null, null, null);
    userArgs.setGroups(groups);

    BucketArgs bucketArgs = new BucketArgs(volumeName, bucketName, userArgs);
    bucketArgs.setVersioning(versioning);
    bucketArgs.setStorageType(storageType);

    //Adding current user's ACL to the ACL list, for now this doesn't check
    //whether the "acls" argument passed to this method already has ACL for
    //current user. This has to be fixed.
    OzoneAcl userAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER, userName,
        userAclRights);
    listOfAcls.add(userAcl);
    //Should we also add ACL of current user's groups?
    if(acls != null && acls.length > 0) {
      listOfAcls.addAll(Arrays.asList(acls));
    }

    bucketArgs.setAddAcls(listOfAcls);
    storageHandler.createBucket(bucketArgs);
  }

   /**
    * Adds a new Key to the Volume/Bucket.
    *
    * @param volumeName Name of the Volume
    * @param bucketName Name of the Bucket
    * @param keyName Key name
    * @param value The Value
    * @throws IOException
    * @throws OzoneException
    */
  public void putKey(String volumeName, String bucketName,
                     String keyName, byte[] value)
      throws IOException, OzoneException {

    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(keyName);
    String requestId = OzoneUtils.getRequestID();
    UserArgs userArgs = new UserArgs(ugi.getUserName(), requestId, hostName,
        null, null, null);
    KeyArgs keyArgs = new KeyArgs(volumeName, bucketName, keyName, userArgs);
    keyArgs.setSize(value.length);
    OutputStream outStream = storageHandler.newKeyWriter(keyArgs);
    outStream.write(value);
    outStream.close();
  }

   /**
    * Close and release the resources.
    */
  @Override
  public void close() {
    storageHandler.close();
  }
}
