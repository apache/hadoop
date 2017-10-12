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


import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A class that encapsulates OzoneBucket.
 */
public class OzoneBucket {

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
  public OzoneBucket(Configuration conf, ClientProtocol proxy,
                     String volumeName, String bucketName,
                     List<OzoneAcl> acls, StorageType storageType,
                     Boolean versioning, long creationTime) {
    this.proxy = proxy;
    this.volumeName = volumeName;
    this.name = bucketName;
    this.acls = acls;
    this.storageType = storageType;
    this.versioning = versioning;
    this.listCacheSize = OzoneClientUtils.getListCacheSize(conf);
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
   * Adds ACLs to the Bucket.
   * @param addAcls ACLs to be added
   * @throws IOException
   */
  public void addAcls(List<OzoneAcl> addAcls) throws IOException {
    Preconditions.checkNotNull(proxy, "Client proxy is not set.");
    Preconditions.checkNotNull(addAcls);
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
    Preconditions.checkNotNull(proxy, "Client proxy is not set.");
    Preconditions.checkNotNull(removeAcls);
    proxy.removeBucketAcls(volumeName, name, removeAcls);
    acls.removeAll(removeAcls);
  }

  /**
   * Sets/Changes the storage type of the bucket.
   * @param newStorageType Storage type to be set
   * @throws IOException
   */
  public void setStorageType(StorageType newStorageType) throws IOException {
    Preconditions.checkNotNull(proxy, "Client proxy is not set.");
    Preconditions.checkNotNull(newStorageType);
    proxy.setBucketStorageType(volumeName, name, newStorageType);
    storageType = newStorageType;
  }

  /**
   * Enable/Disable versioning of the bucket.
   * @param newVersioning
   * @throws IOException
   */
  public void setVersioning(Boolean newVersioning) throws IOException {
    Preconditions.checkNotNull(proxy, "Client proxy is not set.");
    Preconditions.checkNotNull(newVersioning);
    proxy.setBucketVersioning(volumeName, name, newVersioning);
    versioning = newVersioning;
  }

  /**
   * Creates a new key in the bucket.
   * @param key Name of the key to be created.
   * @param size Size of the data the key will point to.
   * @return OzoneOutputStream to which the data has to be written.
   * @throws IOException
   */
  public OzoneOutputStream createKey(String key, long size, OzoneProtos
      .ReplicationType type, OzoneProtos.ReplicationFactor factor)
      throws IOException {
    Preconditions.checkNotNull(proxy, "Client proxy is not set.");
    Preconditions.checkNotNull(key);
    return proxy.createKey(volumeName, name, key, size, type, factor);
  }

  /**
   * Reads an existing key from the bucket.
   * @param key Name of the key to be read.
   * @return OzoneInputStream the stream using which the data can be read.
   * @throws IOException
   */
  public OzoneInputStream readKey(String key) throws IOException {
    Preconditions.checkNotNull(proxy, "Client proxy is not set.");
    Preconditions.checkNotNull(key);
    return proxy.getKey(volumeName, name, key);
  }

  /**
   * Returns information about the key.
   * @param key Name of the key.
   * @return OzoneKey Information about the key.
   * @throws IOException
   */
  public OzoneKey getKey(String key) throws IOException {
    Preconditions.checkNotNull(proxy, "Client proxy is not set.");
    Preconditions.checkNotNull(key);
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
  public Iterator<OzoneKey> listKeys(String keyPrefix) {
    return new KeyIterator(keyPrefix);
  }

  /**
   * Deletes key from the bucket.
   * @param key Name of the key to be deleted.
   * @throws IOException
   */
  public void deleteKey(String key) throws IOException {
    Preconditions.checkNotNull(proxy, "Client proxy is not set.");
    Preconditions.checkNotNull(key);
    proxy.deleteKey(volumeName, name, key);
  }

  /**
   * An Iterator to iterate over {@link OzoneKey} list.
   */
  private class KeyIterator implements Iterator<OzoneKey> {

    private String keyPrefix = null;

    private Iterator<OzoneKey> currentIterator;
    private OzoneKey currentValue;


    /**
     * Creates an Iterator to iterate over all keys in the bucket,
     * which matches volume prefix.
     * @param keyPrefix
     */
    KeyIterator(String keyPrefix) {
      this.keyPrefix = keyPrefix;
      this.currentValue = null;
      this.currentIterator = getNextListOfKeys(null).iterator();
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
