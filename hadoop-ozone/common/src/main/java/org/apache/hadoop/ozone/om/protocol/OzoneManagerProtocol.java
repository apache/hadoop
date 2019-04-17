/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.protocol;

import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;

import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.ozone.security.OzoneDelegationTokenSelector;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.TokenInfo;

/**
 * Protocol to talk to OM.
 */
@KerberosInfo(
    serverPrincipal = OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY)
@TokenInfo(OzoneDelegationTokenSelector.class)
public interface OzoneManagerProtocol
    extends OzoneManagerSecurityProtocol, Closeable {

  @SuppressWarnings("checkstyle:ConstantName")
  /**
   * Version 1: Initial version.
   */
  long versionID = 1L;

  /**
   * Creates a volume.
   * @param args - Arguments to create Volume.
   * @throws IOException
   */
  void createVolume(OmVolumeArgs args) throws IOException;

  /**
   * Changes the owner of a volume.
   * @param volume  - Name of the volume.
   * @param owner - Name of the owner.
   * @throws IOException
   */
  void setOwner(String volume, String owner) throws IOException;

  /**
   * Changes the Quota on a volume.
   * @param volume - Name of the volume.
   * @param quota - Quota in bytes.
   * @throws IOException
   */
  void setQuota(String volume, long quota) throws IOException;

  /**
   * Checks if the specified user can access this volume.
   * @param volume - volume
   * @param userAcl - user acls which needs to be checked for access
   * @return true if the user has required access for the volume,
   *         false otherwise
   * @throws IOException
   */
  boolean checkVolumeAccess(String volume, OzoneAclInfo userAcl)
      throws IOException;

  /**
   * Gets the volume information.
   * @param volume - Volume name.
   * @return VolumeArgs or exception is thrown.
   * @throws IOException
   */
  OmVolumeArgs getVolumeInfo(String volume) throws IOException;

  /**
   * Deletes an existing empty volume.
   * @param volume - Name of the volume.
   * @throws IOException
   */
  void deleteVolume(String volume) throws IOException;

  /**
   * Lists volume owned by a specific user.
   * @param userName - user name
   * @param prefix  - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  List<OmVolumeArgs> listVolumeByUser(String userName, String prefix, String
      prevKey, int maxKeys) throws IOException;

  /**
   * Lists volume all volumes in the cluster.
   * @param prefix  - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  List<OmVolumeArgs> listAllVolumes(String prefix, String
      prevKey, int maxKeys) throws IOException;

  /**
   * Creates a bucket.
   * @param bucketInfo - BucketInfo to create Bucket.
   * @throws IOException
   */
  void createBucket(OmBucketInfo bucketInfo) throws IOException;

  /**
   * Gets the bucket information.
   * @param volumeName - Volume name.
   * @param bucketName - Bucket name.
   * @return OmBucketInfo or exception is thrown.
   * @throws IOException
   */
  OmBucketInfo getBucketInfo(String volumeName, String bucketName)
      throws IOException;

  /**
   * Sets bucket property from args.
   * @param args - BucketArgs.
   * @throws IOException
   */
  void setBucketProperty(OmBucketArgs args) throws IOException;

  /**
   * Open the given key and return an open key session.
   *
   * @param args the args of the key.
   * @return OpenKeySession instance that client uses to talk to container.
   * @throws IOException
   */
  OpenKeySession openKey(OmKeyArgs args) throws IOException;

  /**
   * Commit a key. This will make the change from the client visible. The client
   * is identified by the clientID.
   *
   * @param args the key to commit
   * @param clientID the client identification
   * @throws IOException
   */
  void commitKey(OmKeyArgs args, long clientID) throws IOException;

  /**
   * Allocate a new block, it is assumed that the client is having an open key
   * session going on. This block will be appended to this open key session.
   *
   * @param args the key to append
   * @param clientID the client identification
   * @param excludeList List of datanodes/containers to exclude during block
   *                    allocation
   * @return an allocated block
   * @throws IOException
   */
  OmKeyLocationInfo allocateBlock(OmKeyArgs args, long clientID,
      ExcludeList excludeList) throws IOException;


  /**
   * Look up for the container of an existing key.
   *
   * @param args the args of the key.
   * @return OmKeyInfo instance that client uses to talk to container.
   * @throws IOException
   */
  OmKeyInfo lookupKey(OmKeyArgs args) throws IOException;

  /**
   * Rename an existing key within a bucket.
   * @param args the args of the key.
   * @param toKeyName New name to be used for the Key
   * @throws IOException
   */
  void renameKey(OmKeyArgs args, String toKeyName) throws IOException;

  /**
   * Deletes an existing key.
   *
   * @param args the args of the key.
   * @throws IOException
   */
  void deleteKey(OmKeyArgs args) throws IOException;

  /**
   * Deletes an existing empty bucket from volume.
   * @param volume - Name of the volume.
   * @param bucket - Name of the bucket.
   * @throws IOException
   */
  void deleteBucket(String volume, String bucket) throws IOException;

  /**
   * Returns a list of buckets represented by {@link OmBucketInfo}
   * in the given volume. Argument volumeName is required, others
   * are optional.
   *
   * @param volumeName
   *   the name of the volume.
   * @param startBucketName
   *   the start bucket name, only the buckets whose name is
   *   after this value will be included in the result.
   * @param bucketPrefix
   *   bucket name prefix, only the buckets whose name has
   *   this prefix will be included in the result.
   * @param maxNumOfBuckets
   *   the maximum number of buckets to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of buckets.
   * @throws IOException
   */
  List<OmBucketInfo> listBuckets(String volumeName,
      String startBucketName, String bucketPrefix, int maxNumOfBuckets)
      throws IOException;

  /**
   * Returns a list of keys represented by {@link OmKeyInfo}
   * in the given bucket. Argument volumeName, bucketName is required,
   * others are optional.
   *
   * @param volumeName
   *   the name of the volume.
   * @param bucketName
   *   the name of the bucket.
   * @param startKeyName
   *   the start key name, only the keys whose name is
   *   after this value will be included in the result.
   * @param keyPrefix
   *   key name prefix, only the keys whose name has
   *   this prefix will be included in the result.
   * @param maxKeys
   *   the maximum number of keys to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of keys.
   * @throws IOException
   */
  List<OmKeyInfo> listKeys(String volumeName,
      String bucketName, String startKeyName, String keyPrefix, int maxKeys)
      throws IOException;

  /**
   * Returns list of Ozone services with its configuration details.
   *
   * @return list of Ozone services
   * @throws IOException
   */
  List<ServiceInfo> getServiceList() throws IOException;

  /*
   * S3 Specific functionality that is supported by Ozone Manager.
   */

  /**
   * Creates an S3 bucket inside Ozone manager and creates the mapping needed
   * to access via both S3 and Ozone.
   * @param userName - S3 user name.
   * @param s3BucketName - S3 bucket Name.
   * @throws IOException - On failure, throws an exception like Bucket exists.
   */
  void createS3Bucket(String userName, String s3BucketName) throws IOException;

  /**
   * Delets an S3 bucket inside Ozone manager and deletes the mapping.
   * @param s3BucketName - S3 bucket Name.
   * @throws IOException in case the bucket cannot be deleted.
   */
  void deleteS3Bucket(String s3BucketName) throws IOException;

  /**
   * Returns the Ozone Namespace for the S3Bucket. It will return the
   * OzoneVolume/OzoneBucketName.
   * @param s3BucketName  - S3 Bucket Name.
   * @return String - The Ozone canonical name for this s3 bucket. This
   * string is useful for mounting an OzoneFS.
   * @throws IOException - Error is throw if the s3bucket does not exist.
   */
  String getOzoneBucketMapping(String s3BucketName) throws IOException;

  /**
   * Returns a list of buckets represented by {@link OmBucketInfo}
   * for the given user. Argument username is required, others
   * are optional.
   *
   * @param userName
   *   user Name.
   * @param startBucketName
   *   the start bucket name, only the buckets whose name is
   *   after this value will be included in the result.
   * @param bucketPrefix
   *   bucket name prefix, only the buckets whose name has
   *   this prefix will be included in the result.
   * @param maxNumOfBuckets
   *   the maximum number of buckets to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of buckets.
   * @throws IOException
   */
  List<OmBucketInfo> listS3Buckets(String userName, String startBucketName,
                                   String bucketPrefix, int maxNumOfBuckets)
      throws IOException;

  /**
   * Initiate multipart upload for the specified key.
   * @param keyArgs
   * @return MultipartInfo
   * @throws IOException
   */
  OmMultipartInfo initiateMultipartUpload(OmKeyArgs keyArgs) throws IOException;


  /**
   * Commit Multipart upload part file.
   * @param omKeyArgs
   * @param clientID
   * @return OmMultipartCommitUploadPartInfo
   * @throws IOException
   */
  OmMultipartCommitUploadPartInfo commitMultipartUploadPart(
      OmKeyArgs omKeyArgs, long clientID) throws IOException;

  /**
   * Complete Multipart upload Request.
   * @param omKeyArgs
   * @param multipartUploadList
   * @return OmMultipartUploadCompleteInfo
   * @throws IOException
   */
  OmMultipartUploadCompleteInfo completeMultipartUpload(
      OmKeyArgs omKeyArgs, OmMultipartUploadList multipartUploadList)
      throws IOException;

  /**
   * Abort multipart upload.
   * @param omKeyArgs
   * @throws IOException
   */
  void abortMultipartUpload(OmKeyArgs omKeyArgs) throws IOException;

  /**
   * Returns list of parts of a multipart upload key.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param uploadID
   * @param partNumberMarker
   * @param maxParts
   * @return OmMultipartUploadListParts
   */
  OmMultipartUploadListParts listParts(String volumeName, String bucketName,
      String keyName, String uploadID, int partNumberMarker,
      int maxParts)  throws IOException;

  /**
   * Gets s3Secret for given kerberos user.
   * @param kerberosID
   * @return S3SecretValue
   * @throws IOException
   */
  S3SecretValue getS3Secret(String kerberosID) throws IOException;

  /**
   * Get the OM Client's Retry and Failover Proxy provider.
   * @return OMFailoverProxyProvider
   */
  OMFailoverProxyProvider getOMFailoverProxyProvider();

  /**
   * OzoneFS api to get file status for an entry.
   *
   * @param keyArgs Key args
   * @throws OMException if file does not exist
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  OzoneFileStatus getFileStatus(OmKeyArgs keyArgs) throws IOException;

  /**
   * Ozone FS api to create a directory. Parent directories if do not exist
   * are created for the input directory.
   *
   * @param args Key args
   * @throws OMException if any entry in the path exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  void createDirectory(OmKeyArgs args) throws IOException;

  /**
   * OzoneFS api to creates an output stream for a file.
   *
   * @param keyArgs   Key args
   * @param overWrite if true existing file at the location will be overwritten
   * @param recursive if true file would be created even if parent directories
   *                  do not exist
   * @throws OMException if given key is a directory
   *                     if file exists and isOverwrite flag is false
   *                     if an ancestor exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  OpenKeySession createFile(OmKeyArgs keyArgs, boolean overWrite,
      boolean recursive) throws IOException;

  /**
   * OzoneFS api to lookup for a file.
   *
   * @param keyArgs Key args
   * @throws OMException if given key is not found or it is not a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  OmKeyInfo lookupFile(OmKeyArgs keyArgs) throws IOException;
}

