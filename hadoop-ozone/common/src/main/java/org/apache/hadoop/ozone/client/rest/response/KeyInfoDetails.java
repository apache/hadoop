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
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.fs.FileEncryptionInfo;

/**
 * KeyInfoDetails class is used for parsing json response
 * when KeyInfoDetails Call is made.
 */
public class KeyInfoDetails extends KeyInfo {

  private static final ObjectReader READER =
      new ObjectMapper().readerFor(KeyInfoDetails.class);

  /**
   * a list of Map which maps localID to ContainerID
   * to specify replica locations.
   */
  private List<KeyLocation> keyLocations;

  private FileEncryptionInfo feInfo;

  /**
   * Constructor needed for json serialization.
   */
  public KeyInfoDetails() {
  }

  /**
   * Set details of key location.
   *
   * @param locations - details of key location
   */
  public void setKeyLocation(List<KeyLocation> locations) {
    this.keyLocations = locations;
  }

  /**
   * Returns details of key location.
   *
   * @return volumeName
   */
  public List<KeyLocation> getKeyLocations() {
    return keyLocations;
  }

  public void setFileEncryptionInfo(FileEncryptionInfo info) {
    this.feInfo = info;
  }

  public FileEncryptionInfo getFileEncryptionInfo() {
    return feInfo;
  }

  /**
   * Parse a string to return KeyInfoDetails Object.
   *
   * @param jsonString Json String
   * @return KeyInfoDetails
   * @throws IOException
   */
  public static KeyInfoDetails parse(String jsonString) throws IOException {
    return READER.readValue(jsonString);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KeyInfoDetails that = (KeyInfoDetails) o;

    return new EqualsBuilder()
        .append(getVersion(), that.getVersion())
        .append(getKeyName(), that.getKeyName())
        .append(keyLocations, that.keyLocations)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(21, 33)
        .append(getVersion())
        .append(getKeyName())
        .append(keyLocations)
        .toHashCode();
  }
}

