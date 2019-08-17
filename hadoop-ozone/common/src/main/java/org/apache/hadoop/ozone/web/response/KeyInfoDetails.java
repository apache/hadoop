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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;

/**
 * Represents an Ozone key Object with detail information of location.
 */
public class KeyInfoDetails extends KeyInfo {
  /**
   * a list of Map which maps localID to ContainerID
   * to specify replica locations.
   */
  private List<KeyLocation> keyLocations;

  /**
   * Set details of key location.
   *
   * @param keyLocations - details of key location
   */
  public void setKeyLocations(List<KeyLocation> keyLocations) {
    this.keyLocations = keyLocations;
  }

  /**
   * Returns details of key location.
   *
   * @return volumeName
   */
  public List<KeyLocation> getKeyLocations() {
    return keyLocations;
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
        .append(keyLocations, that.getKeyLocations())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(getVersion())
        .append(getKeyName())
        .append(keyLocations)
        .toHashCode();
  }
}
