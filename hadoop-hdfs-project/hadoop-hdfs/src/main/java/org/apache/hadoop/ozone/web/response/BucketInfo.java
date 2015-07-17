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

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.web.request.OzoneAcl;
import org.apache.hadoop.ozone.web.utils.OzoneConsts;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.map.annotate.JsonFilter;
import org.codehaus.jackson.map.ser.FilterProvider;
import org.codehaus.jackson.map.ser.impl.SimpleBeanPropertyFilter;
import org.codehaus.jackson.map.ser.impl.SimpleFilterProvider;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * BucketInfo class, this is used as response class to send
 * Json info about a bucket back to a client.
 */
public class BucketInfo implements Comparable<BucketInfo> {
  static final String BUCKET_INFO = "BUCKET_INFO_FILTER";
  private String volumeName;
  private String bucketName;
  private List<OzoneAcl> acls;
  private OzoneConsts.Versioning versioning;
  private StorageType storageType;
  private long bytesUsed;
  private long keyCount;

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
    acls = new LinkedList<OzoneAcl>();
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
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(jsonString, BucketInfo.class);
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
   * Returns a JSON string of this object.
   * After stripping out bytesUsed and keyCount
   *
   * @return String
   */
  public String toJsonString() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    String[] ignorableFieldNames = {"bytesUsed", "keyCount"};

    FilterProvider filters = new SimpleFilterProvider().addFilter(
        BUCKET_INFO,
        SimpleBeanPropertyFilter.serializeAllExcept(ignorableFieldNames));

    mapper.setVisibility(JsonMethod.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.getSerializationConfig()
        .addMixInAnnotations(Object.class, MixIn.class);
    ObjectWriter writer = mapper.writer(filters);
    return writer.writeValueAsString(this);
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
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(this);
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
  class MixIn {

  }

}
