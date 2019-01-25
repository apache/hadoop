/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.ozone.OzoneAcl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class encapsulates the arguments that are
 * required for creating a volume.
 */
public final class VolumeArgs {

  private final String admin;
  private final String owner;
  private final String quota;
  private final List<OzoneAcl> acls;
  private Map<String, String> metadata;

  /**
   * Private constructor, constructed via builder.
   * @param admin Administrator's name.
   * @param owner Volume owner's name
   * @param quota Volume Quota.
   * @param acls User to access rights map.
   */
  private VolumeArgs(String admin,
      String owner,
      String quota,
      List<OzoneAcl> acls,
      Map<String, String> metadata) {
    this.admin = admin;
    this.owner = owner;
    this.quota = quota;
    this.acls = acls;
    this.metadata = metadata;
  }

  /**
   * Returns the Admin Name.
   * @return String.
   */
  public String getAdmin() {
    return admin;
  }

  /**
   * Returns the owner Name.
   * @return String
   */
  public String getOwner() {
    return owner;
  }

  /**
   * Returns Volume Quota.
   * @return Quota.
   */
  public String getQuota() {
    return quota;
  }

  /**
   * Return custom key value map.
   *
   * @return metadata
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  public List<OzoneAcl> getAcls() {
    return acls;
  }
  /**
   * Returns new builder class that builds a OmVolumeArgs.
   *
   * @return Builder
   */
  public static VolumeArgs.Builder newBuilder() {
    return new VolumeArgs.Builder();
  }

  /**
   * Builder for OmVolumeArgs.
   */
  public static class Builder {
    private String adminName;
    private String ownerName;
    private String volumeQuota;
    private List<OzoneAcl> listOfAcls;
    private Map<String, String> metadata = new HashMap<>();


    public VolumeArgs.Builder setAdmin(String admin) {
      this.adminName = admin;
      return this;
    }

    public VolumeArgs.Builder setOwner(String owner) {
      this.ownerName = owner;
      return this;
    }

    public VolumeArgs.Builder setQuota(String quota) {
      this.volumeQuota = quota;
      return this;
    }

    public VolumeArgs.Builder addMetadata(String key, String value) {
      metadata.put(key, value);
      return this;
    }
    public VolumeArgs.Builder setAcls(List<OzoneAcl> acls)
        throws IOException {
      this.listOfAcls = acls;
      return this;
    }

    /**
     * Constructs a CreateVolumeArgument.
     * @return CreateVolumeArgs.
     */
    public VolumeArgs build() {
      return new VolumeArgs(adminName, ownerName, volumeQuota, listOfAcls,
          metadata);
    }
  }

}
