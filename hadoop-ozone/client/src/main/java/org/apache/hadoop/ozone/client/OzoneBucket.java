/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.WithMetadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A class that encapsulates OzoneBucket.
 */
public class OzoneBucket extends WithMetadata {

  /**
   * The proxy used for connecting to the cluster and perform
   * client operations.
   */
  private final ClientProtocol proxy;
  /**
   * Name of the volume in which the bucket belongs to.
   */
  private final String volumeName;
  /**
   * Name of the bucket.
   */
  private final String name;
  /**
   * Default replication factor to be used while creating keys.
   */
  private final ReplicationFactor defaultReplication;

  /**
   * Default replication type to be used while creating keys.
   */
  private final ReplicationType defaultReplicationType;
  /**
   * Bucket ACLs.
   */
  private List<OzoneAcl> acls;

  /**
   * Type of storage to be used for this bucket.
   * [RAM_DISK, SSD, DISK, ARCHIVE]
   */
  private StorageType storageType;

  /**
   * Bucket Version flag.
   */
  private Boolean versioning;

  /**
   * Cache size to be used for listKey calls.
   */
  private int listCacheSize;

  /**
   * Creation time of the bucket.
   */
  private long creationTime;

  /**
   * Bucket Encryption key name if bucket encryption is enabled.
   */
  private String encryptionKeyName;

  @SuppressWarnings("parameternumber")
  public OzoneBucket(Configuration conf, ClientProtocol proxy,
                     String volumeName, String bucketName,
                     List<OzoneAcl> acls, StorageType storageType,
                     Boolean versioning, long creationTime,
                     Map<String, String> metadata,
                     String encryptionKeyName) {
    Preconditions.checkNotNull(proxy, "Client proxy is not set.");
    this.proxy = proxy;
    this.volumeName = volumeName;
    this.name = bucketName;
    this.acls = acls;
    this.storageType = storageType;
    this.versioning = versioning;
    this.listCacheSize = HddsClientUtils.getListCacheSize(conf);
    this.creationTime = creationTime;
    this.defaultReplication = ReplicationFactor.valueOf(conf.getInt(
        OzoneConfigKeys.OZONE_REPLICATION,
        OzoneConfigKeys.OZONE_REPLICATION_DEFAULT));
    this.defaultReplicationType = ReplicationType.valueOf(conf.get(
        OzoneConfigKeys.OZONE_REPLICATION_TYPE,
        OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT));
    this.metadata = metadata;
    this.encryptionKeyName = encryptionKeyName;
  }

  /**
   * Constructs OzoneBucket instance.
   * @param conf Configuration object.
   * @param proxy ClientProtocol proxy.
   * @param volumeName Name of the volume the bucket belongs to.
   * @param bucketName Name of the bucket.
   * @param acls ACLs associated with the bucket.
   * @param storageType StorageType of the bucket.
   * @param versioning versioning status of the bucket.
   * @param creationTime creation time of the bucket.
   */
  @SuppressWarnings("parameternumber")
  public OzoneBucket(Configuration conf, ClientProtocol proxy,
                     String volumeName, String bucketName,
                     List<OzoneAcl> acls, StorageType storageType,
                     Boolean versioning, long creationTime,
                     Map<String, String> metadata) {
    Preconditions.checkNotNull(proxy, "Client proxy is not set.");
    this.proxy = proxy;
    this.volumeName = volumeName;
    this.name = bucketName;
    this.acls = acls;
    this.storageType = storageType;
    this.versioning = versioning;
    this.listCacheSize = HddsClientUtils.getListCacheSize(conf);
    this.creationTime = creationTime;
    this.defaultReplication = ReplicationFactor.valueOf(conf.getInt(
        OzoneConfigKeys.OZONE_REPLICATION,
        OzoneConfigKeys.OZONE_REPLICATION_DEFAULT));
    this.defaultReplicationType = ReplicationType.valueOf(conf.get(
        OzoneConfigKeys.OZONE_REPLICATION_TYPE,
        OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT));
    this.metadata = metadata;
  }

  @VisibleForTesting
  @SuppressWarnings("parameternumber")
  OzoneBucket(String volumeName, String name,
      ReplicationFactor defaultReplication,
      ReplicationType defaultReplicationType,
      List<OzoneAcl> acls, StorageType storageType, Boolean versioning,
      long creationTime) {
    this.proxy = null;
    this.volumeName = volumeName;
    this.name = name;
    this.defaultReplication = defaultReplication;
    this.defaultReplicationType = defaultReplicationType;
    this.acls = acls;
    this.storageType = storageType;
    this.versioning = versioning;
    this.creationTime = creationTime;
  }

  /**
   * Returns Volume Name.
   *
   * @return volumeName
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Returns Bucket Name.
   *
   * @return bucketName
   */
  public String getName() {
    return name;
  }

  /**
   * Returns ACL's associated with the Bucket.
   *
   * @return acls
   */
  public List<OzoneAcl> getAcls() {
    return acls;
  }

  /**
   * Returns StorageType of the Bucket.
   *
   * @return storageType
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Returns Versioning associated with the Bucket.
   *
   * @return versioning
   */
  public Boolean getVersioning() {
    return versioning;
  }

  /**
   * Returns creation time of the Bucket.
   *
   * @return creation time of the bucket
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * Return the bucket encryption key name.
   * @return the bucket encryption key name
   */
  public String getEncryptionKeyName() {
    return encryptionKeyName;
  }

  /**
   * Adds ACLs to the Bucket.
   * @param addAcls ACLs to be added
   * @throws IOException
   */
  public void addAcls(List<OzoneAcl> addAcls) throws IOException {
    proxy.addBucketAcls(volumeName, name, addAcls);
    addAcls.stream().filter(acl -> !acls.contains(acl)).forEach(
        acls::add);
  }

  /**
   * Removes ACLs from the bucket.
   * @param removeAcls ACLs to be removed
   * @throws IOException
   */
  public void removeAcls(List<OzoneAcl> removeAcls) throws IOException {
    proxy.removeBucketAcls(volumeName, name, removeAcls);
    acls.removeAll(removeAcls);
  }

  /**
   * Sets/Changes the storage type of the bucket.
   * @param newStorageType Storage type to be set
   * @throws IOException
   */
  public void setStorageType(StorageType newStorageType) throws IOException {
    proxy.setBucketStorageType(volumeName, name, newStorageType);
    storageType = newStorageType;
  }

  /**
   * Enable/Disable versioning of the bucket.
   * @param newVersioning
   * @throws IOException
   */
  public void setVersioning(Boolean newVersioning) throws IOException {
    proxy.setBucketVersioning(volumeName, name, newVersioning);
    versioning = newVersioning;
  }

  /**
   * Creates a new key in the bucket, with default replication type RATIS and
   * with replication factor THREE.
   * @param key Name of the key to be created.
   * @param size Size of the data the key will point to.
   * @return OzoneOutputStream to which the data has to be written.
   * @throws IOException
   */
  public OzoneOutputStream createKey(String key, long size)
      throws IOException {
    return createKey(key, size, defaultReplicationType, defaultReplication,
        new HashMap<>());
  }

  /**
   * Creates a new key in the bucket.
   * @param key Name of the key to be created.
   * @param size Size of the data the key will point to.
   * @param type Replication type to be used.
   * @param factor Replication factor of the key.
   * @return OzoneOutputStream to which the data has to be written.
   * @throws IOException
   */
  public OzoneOutputStream createKey(String key, long size,
                                     ReplicationType type,
                                     ReplicationFactor factor,
                                     Map<String, String> keyMetadata)
      throws IOException {
    return proxy
        .createKey(volumeName, name, key, size, type, factor, keyMetadata);
  }

  /**
   * Reads an existing key from the bucket.
   * @param key Name of the key to be read.
   * @return OzoneInputStream the stream using which the data can be read.
   * @throws IOException
   */
  public OzoneInputStream readKey(String key) throws IOException {
    return proxy.getKey(volumeName, name, key);
  }

  /**
   * Returns information about the key.
   * @param key Name of the key.
   * @return OzoneKeyDetails Information about the key.
   * @throws IOException
   */
  public OzoneKeyDetails getKey(String key) throws IOException {
    return proxy.getKeyDetails(volumeName, name, key);
  }

  /**
   * Returns Iterator to iterate over all keys in the bucket.
   * The result can be restricted using key prefix, will return all
   * keys if key prefix is null.
   *
   * @param keyPrefix Bucket prefix to match
   * @return {@code Iterator<OzoneKey>}
   */
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix) {
    return listKeys(keyPrefix, null);
  }

  /**
   * Returns Iterator to iterate over all keys after prevKey in the bucket.
   * If prevKey is null it iterates from the first key in the bucket.
   * The result can be restricted using key prefix, will return all
   * keys if key prefix is null.
   *
   * @param keyPrefix Bucket prefix to match
   * @param prevKey Keys will be listed after this key name
   * @return {@code Iterator<OzoneKey>}
   */
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix,
      String prevKey) {
    return new KeyIterator(keyPrefix, prevKey);
  }

  /**
   * Deletes key from the bucket.
   * @param key Name of the key to be deleted.
   * @throws IOException
   */
  public void deleteKey(String key) throws IOException {
    proxy.deleteKey(volumeName, name, key);
  }

  public void renameKey(String fromKeyName, String toKeyName)
      throws IOException {
    proxy.renameKey(volumeName, name, fromKeyName, toKeyName);
  }

  /**
   * Initiate multipart upload for a specified key.
   * @param keyName
   * @param type
   * @param factor
   * @return OmMultipartInfo
   * @throws IOException
   */
  public OmMultipartInfo initiateMultipartUpload(String keyName,
                                                 ReplicationType type,
                                                 ReplicationFactor factor)
      throws IOException {
    return  proxy.initiateMultipartUpload(volumeName, name, keyName, type,
        factor);
  }

  /**
   * Initiate multipart upload for a specified key, with default replication
   * type RATIS and with replication factor THREE.
   * @param key Name of the key to be created.
   * @return OmMultipartInfo.
   * @throws IOException
   */
  public OmMultipartInfo initiateMultipartUpload(String key)
      throws IOException {
    return initiateMultipartUpload(key, defaultReplicationType,
        defaultReplication);
  }

  /**
   * Create a part key for a multipart upload key.
   * @param key
   * @param size
   * @param partNumber
   * @param uploadID
   * @return OzoneOutputStream
   * @throws IOException
   */
  public OzoneOutputStream createMultipartKey(String key, long size,
                                              int partNumber, String uploadID)
      throws IOException {
    return proxy.createMultipartKey(volumeName, name, key, size, partNumber,
        uploadID);
  }

  /**
   * Complete Multipart upload. This will combine all the parts and make the
   * key visible in ozone.
   * @param key
   * @param uploadID
   * @param partsMap
   * @return OmMultipartUploadCompleteInfo
   * @throws IOException
   */
  public OmMultipartUploadCompleteInfo completeMultipartUpload(String key,
      String uploadID, Map<Integer, String> partsMap) throws IOException {
    return proxy.completeMultipartUpload(volumeName, name, key, uploadID,
        partsMap);
  }

  /**
   * Abort multipart upload request.
   * @param keyName
   * @param uploadID
   * @throws IOException
   */
  public void abortMultipartUpload(String keyName, String uploadID) throws
      IOException {
    proxy.abortMultipartUpload(volumeName, name, keyName, uploadID);
  }

  /**
   * Returns list of parts of a multipart upload key.
   * @param keyName
   * @param uploadID
   * @param partNumberMarker
   * @param maxParts
   * @return OzoneMultipartUploadPartListParts
   */
  public OzoneMultipartUploadPartListParts listParts(String keyName,
      String uploadID, int partNumberMarker, int maxParts)  throws IOException {
    // As at most we  can have 10000 parts for a key, not using iterator. If
    // needed, it can be done later. So, if we send 10000 as max parts at
    // most in a single rpc call, we return 0.6 mb, by assuming each part
    // size as 60 bytes (ignored the replication type size during calculation)

    return proxy.listParts(volumeName, name, keyName, uploadID,
              partNumberMarker, maxParts);
  }

  /**
   * OzoneFS api to get file status for an entry.
   *
   * @param keyName Key name
   * @throws OMException if file does not exist
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  public OzoneFileStatus getFileStatus(String keyName) throws IOException {
    return proxy.getOzoneFileStatus(volumeName, name, keyName);
  }

  /**
   * Ozone FS api to create a directory. Parent directories if do not exist
   * are created for the input directory.
   *
   * @param keyName Key name
   * @throws OMException if any entry in the path exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  public void createDirectory(String keyName) throws IOException {
    proxy.createDirectory(volumeName, name, keyName);
  }

  /**
   * OzoneFS api to creates an input stream for a file.
   *
   * @param keyName Key name
   * @throws OMException if given key is not found or it is not a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  public OzoneInputStream readFile(String keyName) throws IOException {
    return proxy.readFile(volumeName, name, keyName);
  }

  /**
   * OzoneFS api to creates an output stream for a file.
   *
   * @param keyName   Key name
   * @param overWrite if true existing file at the location will be overwritten
   * @param recursive if true file would be created even if parent directories
   *                    do not exist
   * @throws OMException if given key is a directory
   *                     if file exists and isOverwrite flag is false
   *                     if an ancestor exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  public OzoneOutputStream createFile(String keyName, long size,
      ReplicationType type, ReplicationFactor factor, boolean overWrite,
      boolean recursive) throws IOException {
    return proxy
        .createFile(volumeName, name, keyName, size, type, factor, overWrite,
            recursive);
  }

  /**
   * An Iterator to iterate over {@link OzoneKey} list.
   */
  private class KeyIterator implements Iterator<OzoneKey> {

    private String keyPrefix = null;

    private Iterator<OzoneKey> currentIterator;
    private OzoneKey currentValue;


    /**
     * Creates an Iterator to iterate over all keys after prevKey in the bucket.
     * If prevKey is null it iterates from the first key in the bucket.
     * The returned keys match key prefix.
     * @param keyPrefix
     */
    KeyIterator(String keyPrefix, String prevKey) {
      this.keyPrefix = keyPrefix;
      this.currentValue = null;
      this.currentIterator = getNextListOfKeys(prevKey).iterator();
    }

    @Override
    public boolean hasNext() {
      if(!currentIterator.hasNext()) {
        currentIterator = getNextListOfKeys(
            currentValue != null ? currentValue.getName() : null)
            .iterator();
      }
      return currentIterator.hasNext();
    }

    @Override
    public OzoneKey next() {
      if(hasNext()) {
        currentValue = currentIterator.next();
        return currentValue;
      }
      throw new NoSuchElementException();
    }

    /**
     * Gets the next set of key list using proxy.
     * @param prevKey
     * @return {@code List<OzoneVolume>}
     */
    private List<OzoneKey> getNextListOfKeys(String prevKey) {
      try {
        return proxy.listKeys(volumeName, name, keyPrefix, prevKey,
            listCacheSize);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
