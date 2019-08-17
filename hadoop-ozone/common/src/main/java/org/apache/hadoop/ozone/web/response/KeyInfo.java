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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdds.client.ReplicationType;
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
 * Represents an Ozone key Object.
 */
public class KeyInfo implements Comparable<KeyInfo> {
  static final String OBJECT_INFO = "OBJECT_INFO_FILTER";

  private static final ObjectReader READER =
      new ObjectMapper().readerFor(KeyInfo.class);
  private static final ObjectWriter WRITER;

  static {
    ObjectMapper mapper = new ObjectMapper();
    String[] ignorableFieldNames = {"dataFileName"};

    FilterProvider filters = new SimpleFilterProvider()
        .addFilter(OBJECT_INFO, SimpleBeanPropertyFilter
            .serializeAllExcept(ignorableFieldNames));
    mapper.setVisibility(PropertyAccessor.FIELD,
        JsonAutoDetect.Visibility.ANY);
    mapper.addMixIn(Object.class, MixIn.class);

    mapper.setFilterProvider(filters);
    WRITER = mapper.writerWithDefaultPrettyPrinter();
  }

  /**
   * This class allows us to create custom filters
   * for the Json serialization.
   */
  @JsonFilter(OBJECT_INFO)
  class MixIn {

  }
  private long version;
  private String md5hash;
  private String createdOn;
  private String modifiedOn;
  private long size;
  private String keyName;

  private String dataFileName;

  private ReplicationType type;

  /**
   * Return replication type of the key.
   *
   * @return replication type
   */
  public ReplicationType getType() {
    return type;
  }

  /**
   * Set replication type of the key.
   *
   * @param replicationType
   */
  public void setType(ReplicationType replicationType) {
    this.type = replicationType;
  }

  /**
   * When this key was created.
   *
   * @return Date String
   */
  public String getCreatedOn() {
    return createdOn;
  }

  /**
   * When this key was modified.
   *
   * @return Date String
   */
  public String getModifiedOn() {
    return modifiedOn;
  }

  /**
   * When this key was created.
   *
   * @param createdOn - Date String
   */
  public void setCreatedOn(String createdOn) {
    this.createdOn = createdOn;
  }

  /**
   * When this key was modified.
   *
   * @param modifiedOn - Date String
   */
  public void setModifiedOn(String modifiedOn) {
    this.modifiedOn = modifiedOn;
  }

  /**
   * Full path to where the actual data for this key is stored.
   *
   * @return String
   */
  public String getDataFileName() {
    return dataFileName;
  }

  /**
   * Sets up where the file path is stored.
   *
   * @param dataFileName - Data File Name
   */
  public void setDataFileName(String dataFileName) {
    this.dataFileName = dataFileName;
  }

  /**
   * Gets the Keyname of this object.
   *
   * @return String
   */
  public String getKeyName() {
    return keyName;
  }

  /**
   * Sets the Key name of this object.
   *
   * @param keyName - String
   */
  public void setKeyName(String keyName) {
    this.keyName = keyName;
  }

  /**
   * Returns the MD5 Hash for the data of this key.
   *
   * @return String MD5
   */
  public String getMd5hash() {
    return md5hash;
  }

  /**
   * Sets the MD5 of this file.
   *
   * @param md5hash - Md5 of this file
   */
  public void setMd5hash(String md5hash) {
    this.md5hash = md5hash;
  }

  /**
   * Number of bytes stored in the data part of this key.
   *
   * @return long size of the data file
   */
  public long getSize() {
    return size;
  }

  /**
   * Sets the size of the Data part of this key.
   *
   * @param size - Size in long
   */
  public void setSize(long size) {
    this.size = size;
  }

  /**
   * Version of this key.
   *
   * @return - returns the version of this key.
   */
  public long getVersion() {
    return version;
  }

  /**
   * Sets the version of this key.
   *
   * @param version - Version String
   */
  public void setVersion(long version) {
    this.version = version;
  }

  /**
   * Compares this object with the specified object for order.  Returns a
   * negative integer, zero, or a positive integer as this object is less
   * than, equal to, or greater than the specified object.
   *
   * @param o the object to be compared.
   *
   * @return a negative integer, zero, or a positive integer as this object
   * is less than, equal to, or greater than the specified object.
   *
   * @throws NullPointerException if the specified object is null
   * @throws ClassCastException if the specified object's type prevents it
   * from being compared to this object.
   */
  @Override
  public int compareTo(KeyInfo o) {
    if (this.keyName.compareTo(o.getKeyName()) != 0) {
      return this.keyName.compareTo(o.getKeyName());
    }

    if (this.getVersion() == o.getVersion()) {
      return 0;
    }
    if (this.getVersion() < o.getVersion()) {
      return -1;
    }
    return 1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KeyInfo keyInfo = (KeyInfo) o;

    return new EqualsBuilder()
        .append(version, keyInfo.version)
        .append(keyName, keyInfo.keyName)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(version)
        .append(keyName)
        .toHashCode();
  }

  /**

   * Parse a string to retuen BucketInfo Object.
   *
   * @param jsonString - Json String
   *
   * @return - BucketInfo
   *
   * @throws IOException
   */
  public static KeyInfo parse(String jsonString) throws IOException {
    return READER.readValue(jsonString);
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
   */
  public String toDBString() throws IOException {
    return JsonUtils.toJsonString(this);
  }
}
