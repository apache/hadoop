/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga;

import java.io.Serializable;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/** A class that represents an FPGA card. */
public class FpgaDevice implements Serializable {
  private static final long serialVersionUID = -4678487141824092751L;
  private final String type;
  private final int major;
  private final int minor;

  // the alias device name. Intel use acl number acl0 to acl31
  private final String aliasDevName;

  // IP file identifier. matrix multiplication for instance (mutable)
  private String IPID;
  // SHA-256 hash of the uploaded aocx file (mutable)
  private String aocxHash;

  // cached hash value
  private Integer hashCode;

  public String getType() {
    return type;
  }

  public int getMajor() {
    return major;
  }

  public int getMinor() {
    return minor;
  }

  public String getIPID() {
    return IPID;
  }

  public String getAocxHash() {
    return aocxHash;
  }

  public void setAocxHash(String hash) {
    this.aocxHash = hash;
  }

  public void setIPID(String IPID) {
    this.IPID = IPID;
  }

  public String getAliasDevName() {
    return aliasDevName;
  }

  public FpgaDevice(String type, int major, int minor, String aliasDevName) {
    this.type = Preconditions.checkNotNull(type, "type must not be null");
    this.major = major;
    this.minor = minor;
    this.aliasDevName = Preconditions.checkNotNull(aliasDevName,
        "aliasDevName must not be null");
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    FpgaDevice other = (FpgaDevice) obj;
    if (aliasDevName == null) {
      if (other.aliasDevName != null) {
        return false;
      }
    } else if (!aliasDevName.equals(other.aliasDevName)) {
      return false;
    }
    if (major != other.major) {
      return false;
    }
    if (minor != other.minor) {
      return false;
    }
    if (type == null) {
      if (other.type != null) {
        return false;
      }
    } else if (!type.equals(other.type)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    if (hashCode == null) {
      final int prime = 31;
      int result = 1;

      result = prime * result + major;
      result = prime * result + type.hashCode();
      result = prime * result + minor;
      result = prime * result + aliasDevName.hashCode();

      hashCode = result;
    }

    return hashCode;
  }

  @Override
  public String toString() {
    return "FPGA Device:(Type: " + this.type + ", Major: " + this.major
        + ", Minor: " + this.minor + ", IPID: " + this.IPID + ", Hash: "
        + this.aocxHash + ")";
  }

}
