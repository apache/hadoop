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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.map.annotate.JsonFilter;
import org.codehaus.jackson.map.ser.FilterProvider;
import org.codehaus.jackson.map.ser.impl.SimpleBeanPropertyFilter;
import org.codehaus.jackson.map.ser.impl.SimpleFilterProvider;

import java.io.IOException;

/**
 * Represents an Ozone key Object.
 */
public class KeyInfo implements Comparable<KeyInfo> {
  static final String OBJECT_INFO = "OBJECT_INFO_FILTER";
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
  private long size;
  private String keyName;

  private String dataFileName;

  /**
   * When this key was created.
   *
   * @return Date String
   */
  public String getCreatedOn() {
    return createdOn;
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
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(jsonString, KeyInfo.class);
  }


  /**
   * Returns a JSON string of this object.
   * After stripping out bytesUsed and keyCount
   *
   * @return String
   */
  public String toJsonString() throws IOException {
    String[] ignorableFieldNames = {"dataFileName"};

    FilterProvider filters = new SimpleFilterProvider()
        .addFilter(OBJECT_INFO, SimpleBeanPropertyFilter
            .serializeAllExcept(ignorableFieldNames));

    ObjectMapper mapper = new ObjectMapper()
        .setVisibility(JsonMethod.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.getSerializationConfig()
        .addMixInAnnotations(Object.class, MixIn.class);
    ObjectWriter writer = mapper.writer(filters);
    return writer.writeValueAsString(this);
  }

  /**
   * Returns the Object as a Json String.
   */
  public String toDBString() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(this);
  }
}
