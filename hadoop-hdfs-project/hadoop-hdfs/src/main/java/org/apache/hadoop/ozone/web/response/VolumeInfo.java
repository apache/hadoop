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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
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

/**
 * VolumeInfo Class is the Java class that represents
 * Json when VolumeInfo Call is made.
 */
@InterfaceAudience.Private
public class VolumeInfo implements Comparable<VolumeInfo> {

  static final String VOLUME_INFO = "VOLUME_INFO_FILTER";
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(VolumeInfo.class);
  private static final ObjectWriter WRITER;

  static {
    ObjectMapper mapper = new ObjectMapper();
    String[] ignorableFieldNames = {"bytesUsed", "bucketCount"};

    FilterProvider filters = new SimpleFilterProvider()
        .addFilter(VOLUME_INFO, SimpleBeanPropertyFilter
            .serializeAllExcept(ignorableFieldNames));
    mapper.setVisibility(PropertyAccessor.FIELD,
        JsonAutoDetect.Visibility.ANY);
    mapper.addMixIn(Object.class, MixIn.class);

    mapper.setFilterProvider(filters);
    WRITER = mapper.writerWithDefaultPrettyPrinter();
  }

  /**
   * Custom Json Filter Class.
   */
  @JsonFilter(VOLUME_INFO)
  class MixIn {
  }
  private VolumeOwner owner;
  private OzoneQuota quota;
  private String volumeName;
  private String createdOn;
  private String createdBy;

  private long bytesUsed;
  private long bucketCount;


  /**
   * Constructor for VolumeInfo.
   *
   * @param volumeName - Name of the Volume
   * @param createdOn _ Date String
   * @param createdBy - Person who created it
   */
  public VolumeInfo(String volumeName, String createdOn, String createdBy) {
    this.createdOn = createdOn;
    this.volumeName = volumeName;
    this.createdBy = createdBy;
  }

  /**
   * Constructor for VolumeInfo.
   */
  public VolumeInfo() {
  }

  /**
   * Returns the name of the person who created this volume.
   *
   * @return Name of Admin who created this
   */
  public String getCreatedBy() {
    return createdBy;
  }

  /**
   * Sets the user name of the person who created this volume.
   *
   * @param createdBy - UserName
   */
  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }

  /**
   * Gets the date on which this volume was created.
   *
   * @return - Date String
   */
  public String getCreatedOn() {
    return createdOn;
  }

  /**
   * Sets the date string.
   *
   * @param createdOn - Date String
   */
  public void setCreatedOn(String createdOn) {
    this.createdOn = createdOn;
  }

  /**
   * Returns the owner info.
   *
   * @return - OwnerInfo
   */
  public VolumeOwner getOwner() {
    return owner;
  }

  /**
   * Sets the owner.
   *
   * @param owner - OwnerInfo
   */
  public void setOwner(VolumeOwner owner) {
    this.owner = owner;
  }

  /**
   * Returns the quota information on a volume.
   *
   * @return Quota
   */
  public OzoneQuota getQuota() {
    return quota;
  }

  /**
   * Sets the quota info.
   *
   * @param quota - Quota Info
   */
  public void setQuota(OzoneQuota quota) {
    this.quota = quota;
  }

  /**
   * gets the volume name.
   *
   * @return - Volume Name
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Sets the volume name.
   *
   * @param volumeName - Volume Name
   */
  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  /**
   * Returns a JSON string of this object.
   * After stripping out bytesUsed and bucketCount
   *
   * @return String - json string
   * @throws IOException
   */
  public String toJsonString() throws IOException {
    return WRITER.writeValueAsString(this);
  }

  /**
   * When we serialize a volumeInfo to our database
   * we will use all fields. However the toJsonString
   * will strip out bytesUsed and bucketCount from the
   * volume Info
   *
   * @return Json String
   *
   * @throws IOException
   */
  public String toDBString() throws IOException {
    return JsonUtils.toJsonString(this);
  }


  /**
   * Comparable Interface.
   * @param o VolumeInfo Object.
   * @return Result of comparison
   */
  @Override
  public int compareTo(VolumeInfo o) {
    return this.volumeName.compareTo(o.getVolumeName());
  }

  /**
   * Gets the number of bytesUsed by this volume.
   *
   * @return long - Bytes used
   */
  public long getBytesUsed() {
    return bytesUsed;
  }

  /**
   * Sets number of bytesUsed by this volume.
   *
   * @param bytesUsed - Number of bytesUsed
   */
  public void setBytesUsed(long bytesUsed) {
    this.bytesUsed = bytesUsed;
  }

  /**
   * Returns VolumeInfo class from json string.
   *
   * @param data - Json String
   *
   * @return VolumeInfo
   *
   * @throws IOException
   */
  public static VolumeInfo parse(String data) throws IOException {
    return READER.readValue(data);
  }

  /**
   * Indicates whether some other object is "equal to" this one.
   *
   * @param obj the reference object with which to compare.
   *
   * @return {@code true} if this object is the same as the obj
   * argument; {@code false} otherwise.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    VolumeInfo otherInfo = (VolumeInfo) obj;
    return otherInfo.getVolumeName().equals(this.getVolumeName());
  }

  /**
   * Returns a hash code value for the object. This method is
   * supported for the benefit of hash tables such as those provided by
   * HashMap.
   * @return a hash code value for this object.
   *
   * @see Object#equals(Object)
   * @see System#identityHashCode
   */
  @Override
  public int hashCode() {
    return getVolumeName().hashCode();
  }

  /**
   * Total number of buckets under this volume.
   *
   * @return - bucketCount
   */
  public long getBucketCount() {
    return bucketCount;
  }

  /**
   * Sets the buckets count.
   *
   * @param bucketCount - Bucket Count
   */
  public void setBucketCount(long bucketCount) {
    this.bucketCount = bucketCount;
  }
}
