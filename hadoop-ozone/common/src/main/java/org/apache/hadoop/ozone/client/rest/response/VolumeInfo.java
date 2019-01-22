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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.client.OzoneQuota;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * VolumeInfo Class is used for parsing json response
 * when VolumeInfo Call is made.
 */
@InterfaceAudience.Private
public class VolumeInfo implements Comparable<VolumeInfo> {


  private static final ObjectReader READER =
      new ObjectMapper().readerFor(VolumeInfo.class);

  private VolumeOwner owner;
  private OzoneQuota quota;
  private String volumeName;
  private String createdOn;
  private String createdBy;


  /**
   * Constructor for VolumeInfo.
   *
   * @param volumeName - Name of the Volume
   * @param createdOn _ Date String
   * @param createdBy - Person who created it
   */
  public VolumeInfo(String volumeName, String createdOn,
                    String createdBy) {
    this.volumeName = volumeName;
    this.createdOn = createdOn;
    this.createdBy = createdBy;
  }

  /**
   * Constructor for VolumeInfo.
   */
  public VolumeInfo() {
  }

  /**
   * gets the volume name.
   *
   * @return Volume Name
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Sets the volume name.
   *
   * @param volumeName Volume Name
   */
  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
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
   * @param createdBy UserName
   */
  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }

  /**
   * Gets the date on which this volume was created.
   *
   * @return Date String
   */
  public String getCreatedOn() {
    return createdOn;
  }

  /**
   * Sets the date string.
   *
   * @param createdOn Date String
   */
  public void setCreatedOn(String createdOn) {
    this.createdOn = createdOn;
  }

  /**
   * Returns the owner info.
   *
   * @return OwnerInfo
   */
  public VolumeOwner getOwner() {
    return owner;
  }

  /**
   * Sets the owner.
   *
   * @param owner OwnerInfo
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
   * @param quota Quota Info
   */
  public void setQuota(OzoneQuota quota) {
    this.quota = quota;
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
   * Returns VolumeInfo class from json string.
   *
   * @param data Json String
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

}
