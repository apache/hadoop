/**
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
package org.apache.hadoop.ozone.client.rest.response;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Preconditions;

/**
 * BucketInfo class is used used for parsing json response
 * when BucketInfo Call is made.
 */
public class BucketInfo implements Comparable<BucketInfo> {

  private static final ObjectReader READER =
      new ObjectMapper().readerFor(BucketInfo.class);

  private String volumeName;
  private String bucketName;
  private String createdOn;
  private List<OzoneAcl> acls;
  private OzoneConsts.Versioning versioning;
  private StorageType storageType;
  private String bekName;

  /**
   * Constructor for BucketInfo.
   *
   * @param volumeName
   * @param bucketName
   */
  public BucketInfo(String volumeName, String bucketName) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
  }


  /**
   * Default constructor for BucketInfo.
   */
  public BucketInfo() {
    acls = new LinkedList<>();
  }

  /**
   * Parse a JSON string into BucketInfo Object.
   *
   * @param jsonString Json String
   * @return BucketInfo
   * @throws IOException
   */
  public static BucketInfo parse(String jsonString) throws IOException {
    return READER.readValue(jsonString);
  }

  /**
   * Returns a List of ACLs set on the Bucket.
   *
   * @return List of Acl
   */
  public List<OzoneAcl> getAcls() {
    return acls;
  }

  /**
   * Sets ACls.
   *
   * @param acls Acl list
   */
  public void setAcls(List<OzoneAcl> acls) {
    this.acls = acls;
  }

  /**
   * Returns Storage Type info.
   *
   * @return Storage Type of the bucket
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Sets the Storage Type.
   *
   * @param storageType Storage Type
   */
  public void setStorageType(StorageType storageType) {
    this.storageType = storageType;
  }

  /**
   * Returns versioning.
   *
   * @return versioning Enum
   */
  public OzoneConsts.Versioning getVersioning() {
    return versioning;
  }

  /**
   * Sets Versioning.
   *
   * @param versioning
   */
  public void setVersioning(OzoneConsts.Versioning versioning) {
    this.versioning = versioning;
  }


  /**
   * Gets bucket Name.
   *
   * @return String
   */
  public String getBucketName() {
    return bucketName;
  }

  /**
   * Sets bucket Name.
   *
   * @param bucketName Name of the bucket
   */
  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  /**
   * Sets creation time of the bucket.
   *
   * @param creationTime Date String
   */
  public void setCreatedOn(String creationTime) {
    this.createdOn = creationTime;
  }

  /**
   * Returns creation time.
   *
   * @return creation time of bucket.
   */
  public String getCreatedOn() {
    return createdOn;
  }

  /**
   * Returns Volume Name.
   *
   * @return String volume name
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Sets the Volume Name of bucket.
   *
   * @param volumeName volumeName
   */
  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  /**
   * Return bucket encryption key name.
   * @return bucket encryption key name
   */
  public String getEncryptionKeyName() {
    return bekName;
  }

  /**
   * Sets the bucket encryption key name.
   * @param name bucket encryption key name
   */
  public void setEncryptionKeyName(String name) {
    this.bekName = name;
  }

  /**
   * Compares this object with the specified object for order.  Returns a
   * negative integer, zero, or a positive integer as this object is less
   * than, equal to, or greater than the specified object.
   *
   * Please note : BucketInfo compare functions are used only within the
   * context of a volume, hence volume name is purposefully ignored in
   * compareTo, equal and hashcode functions of this class.
   */
  @Override
  public int compareTo(BucketInfo o) {
    Preconditions.checkState(o.getVolumeName().equals(this.getVolumeName()));
    return this.bucketName.compareTo(o.getBucketName());
  }

  /**
   * Checks if two bucketInfo's are equal.
   * @param o Object BucketInfo
   * @return  True or False
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BucketInfo)) {
      return false;
    }

    BucketInfo that = (BucketInfo) o;
    Preconditions.checkState(that.getVolumeName().equals(this.getVolumeName()));
    return bucketName.equals(that.bucketName);

  }

  /**
   * Hash Code for this object.
   * @return int
   */
  @Override
  public int hashCode() {
    return bucketName.hashCode();
  }

}
