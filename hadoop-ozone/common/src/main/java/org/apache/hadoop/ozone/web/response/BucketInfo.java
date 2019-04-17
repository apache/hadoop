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
package org.apache.hadoop.ozone.web.response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.web.utils.JsonUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.google.common.base.Preconditions;

/**
 * BucketInfo class, this is used as response class to send
 * Json info about a bucket back to a client.
 */
public class BucketInfo implements Comparable<BucketInfo> {
  static final String BUCKET_INFO = "BUCKET_INFO_FILTER";
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(BucketInfo.class);
  private static final ObjectWriter WRITER;

  static {
    ObjectMapper mapper = new ObjectMapper();
    String[] ignorableFieldNames = {"bytesUsed", "keyCount"};

    FilterProvider filters = new SimpleFilterProvider().addFilter(BUCKET_INFO,
        SimpleBeanPropertyFilter.serializeAllExcept(ignorableFieldNames));
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.addMixIn(Object.class, MixIn.class);

    mapper.setFilterProvider(filters);
    WRITER = mapper.writerWithDefaultPrettyPrinter();
  }

  private String volumeName;
  private String bucketName;
  private String createdOn;
  private List<OzoneAcl> acls;
  private OzoneConsts.Versioning versioning;
  private StorageType storageType;
  private long bytesUsed;
  private long keyCount;
  private String encryptionKeyName;

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
    acls = new ArrayList<>();
  }

  /**
   * Parse a JSON string into BucketInfo Object.
   *
   * @param jsonString - Json String
   *
   * @return - BucketInfo
   *
   * @throws IOException
   */
  public static BucketInfo parse(String jsonString) throws IOException {
    return READER.readValue(jsonString);
  }

  /**
   * Returns a List of ACL on the Bucket.
   *
   * @return List of Acls
   */
  public List<OzoneAcl> getAcls() {
    return acls;
  }

  /**
   * Sets ACls.
   *
   * @param acls - Acls list
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
   * @param storageType - Storage Type
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
   * @param bucketName - Name of the bucket
   */
  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  /**
   * Sets creation time of the bucket.
   *
   * @param creationTime - Date String
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


  public void setEncryptionKeyName(String encryptionKeyName) {
    this.encryptionKeyName = encryptionKeyName;
  }

  public String getEncryptionKeyName() {
    return encryptionKeyName;
  }

  /**
   * Returns a JSON string of this object.
   * After stripping out bytesUsed and keyCount
   *
   * @return String
   */
  public String toJsonString() throws IOException {
    return WRITER.writeValueAsString(this);
  }

  /**
   * Returns the Object as a Json String.
   *
   * The reason why both toJSONString exists and toDBString exists
   * is because toJSONString supports an external facing contract with
   * REST clients. However server internally would want to add more
   * fields to this class. The distinction helps in serializing all
   * fields vs. only fields that are part of REST protocol.
   */
  public String toDBString() throws IOException {
    return JsonUtils.toJsonString(this);
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
   * Sets the Volume Name of the bucket.
   *
   * @param volumeName - volumeName
   */
  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
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

  /**
   * Get the number of bytes used by this bucket.
   *
   * @return long
   */
  public long getBytesUsed() {
    return bytesUsed;
  }

  /**
   * Set bytes Used.
   *
   * @param bytesUsed - bytesUsed
   */
  public void setBytesUsed(long bytesUsed) {
    this.bytesUsed = bytesUsed;
  }

  /**
   * Get Key Count  inside this bucket.
   *
   * @return - KeyCount
   */
  public long getKeyCount() {
    return keyCount;
  }

  /**
   * Set Key Count inside this bucket.
   *
   * @param keyCount - Sets the Key Count
   */
  public void setKeyCount(long keyCount) {
    this.keyCount = keyCount;
  }

  /**
   * This class allows us to create custom filters
   * for the Json serialization.
   */
  @JsonFilter(BUCKET_INFO)
  static class MixIn {

  }

}
