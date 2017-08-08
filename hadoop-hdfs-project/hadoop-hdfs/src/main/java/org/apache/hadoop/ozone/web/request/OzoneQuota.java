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

package org.apache.hadoop.ozone.web.request;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.rest.headers.Header;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * represents an OzoneQuota Object that can be applied to
 * a storage volume.
 */
@InterfaceAudience.Private
public class OzoneQuota {

  private Units unit;
  private int size;

  /** Quota Units.*/
  public enum Units {UNDEFINED, BYTES, MB, GB, TB}

  /**
   * Returns size.
   *
   * @return int
   */
  public int getSize() {
    return size;
  }

  /**
   * Returns Units.
   *
   * @return Unit in MB, GB or TB
   */
  public Units getUnit() {
    return unit;
  }

  /**
   * Constructs a default Quota object.
   */
  public OzoneQuota() {
    this.size = 0;
    this.unit = Units.UNDEFINED;
  }

  /**
   * Constructor for Ozone Quota.
   *
   * @param size - Integer Size
   * @param unit MB, GB  or TB
   */
  public OzoneQuota(int size, Units unit) {
    this.size = size;
    this.unit = unit;
  }

  /**
   * Formats a quota as a string.
   *
   * @param quota the quota to format
   * @return string representation of quota
   */
  public static String formatQuota(OzoneQuota quota) {
    return String.valueOf(quota.size) + quota.unit;
  }

  /**
   * Parses a user provided string and returns the
   * Quota Object.
   *
   * @param quotaString Quota String
   *
   * @return OzoneQuota object
   *
   * @throws IllegalArgumentException
   */
  public static OzoneQuota parseQuota(String quotaString)
      throws IllegalArgumentException {

    if ((quotaString == null) || (quotaString.isEmpty())) {
      throw new IllegalArgumentException(
          "Quota string cannot be null or empty.");
    }

    if (isRemove(quotaString)) {
      throw new IllegalArgumentException("Remove is invalid in this context.");
    }

    String uppercase = quotaString.toUpperCase().replaceAll("\\s+", "");
    String size = "";
    int nSize;
    Units currUnit = Units.MB;
    Boolean found = false;
    if (uppercase.endsWith(Header.OZONE_QUOTA_MB)) {
      size = uppercase
          .substring(0, uppercase.length() - Header.OZONE_QUOTA_MB.length());
      currUnit = Units.MB;
      found = true;
    }

    if (uppercase.endsWith(Header.OZONE_QUOTA_GB)) {
      size = uppercase
          .substring(0, uppercase.length() - Header.OZONE_QUOTA_GB.length());
      currUnit = Units.GB;
      found = true;
    }

    if (uppercase.endsWith(Header.OZONE_QUOTA_TB)) {
      size = uppercase
          .substring(0, uppercase.length() - Header.OZONE_QUOTA_TB.length());
      currUnit = Units.TB;
      found = true;
    }

    if (uppercase.endsWith(Header.OZONE_QUOTA_BYTES)) {
      size = uppercase
          .substring(0, uppercase.length() - Header.OZONE_QUOTA_BYTES.length());
      currUnit = Units.BYTES;
      found = true;
    }

    if (!found) {
      throw new IllegalArgumentException(
          "Quota unit not recognized. Supported values are BYTES, MB, GB and " +
              "TB.");
    }

    nSize = Integer.parseInt(size);
    if (nSize < 0) {
      throw new IllegalArgumentException("Quota cannot be negative.");
    }

    return new OzoneQuota(nSize, currUnit);
  }


  /**
   * Checks if Quota String is just as remove operation.
   *
   * @param quotaString User provided quota String
   *
   * @return True if it is Remove, false otherwise
   */
  public static boolean isRemove(String quotaString) {

    return (quotaString != null) &&
        (quotaString.compareToIgnoreCase(Header.OZONE_QUOTA_REMOVE) == 0);
  }

  /**
   * Returns size in Bytes or -1 if there is no Quota.
   */
  @JsonIgnore
  public long sizeInBytes() {
    switch (this.unit) {
    case BYTES:
      return this.getSize();
    case MB:
      return this.getSize() * OzoneConsts.MB;
    case GB:
      return this.getSize() * OzoneConsts.GB;
    case TB:
      return this.getSize() * OzoneConsts.TB;
    case UNDEFINED:
    default:
      return -1;
    }
  }

  /**
   * Returns OzoneQuota corresponding to size in bytes.
   *
   * @param sizeInBytes size in bytes to be converted
   *
   * @return OzoneQuota object
   */
  public static OzoneQuota getOzoneQuota(long sizeInBytes) {
    long size;
    Units unit;
    if (sizeInBytes % OzoneConsts.TB == 0) {
      size = sizeInBytes / OzoneConsts.TB;
      unit = Units.TB;
    } else if (sizeInBytes % OzoneConsts.GB == 0) {
      size = sizeInBytes / OzoneConsts.GB;
      unit = Units.GB;
    } else if (sizeInBytes % OzoneConsts.MB == 0) {
      size = sizeInBytes / OzoneConsts.MB;
      unit = Units.MB;
    } else {
      size = sizeInBytes;
      unit = Units.BYTES;
    }
    return new OzoneQuota((int)size, unit);
  }
}
