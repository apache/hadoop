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
package org.apache.hadoop.ozone.container.common.impl;


import com.google.common.base.Preconditions;

/**
 * Defines layout versions for the Chunks.
 */

public final class ChunkLayOutVersion {

  private final static ChunkLayOutVersion[] CHUNK_LAYOUT_VERSION_INFOS =
      {new ChunkLayOutVersion(1, "Data without checksums.")};

  private int version;
  private String description;


  /**
   * Never created outside this class.
   *
   * @param description -- description
   * @param version     -- version number
   */
  private ChunkLayOutVersion(int version, String description) {
    this.version = version;
    this.description = description;
  }

  /**
   * Return ChunkLayOutVersion object for the chunkVersion.
   * @param chunkVersion
   * @return ChunkLayOutVersion
   */
  public static ChunkLayOutVersion getChunkLayOutVersion(int chunkVersion) {
    Preconditions.checkArgument((chunkVersion <= ChunkLayOutVersion
        .getLatestVersion().getVersion()));
    for(ChunkLayOutVersion chunkLayOutVersion : CHUNK_LAYOUT_VERSION_INFOS) {
      if(chunkLayOutVersion.getVersion() == chunkVersion) {
        return chunkLayOutVersion;
      }
    }
    return null;
  }

  /**
   * Returns all versions.
   *
   * @return Version info array.
   */
  public static ChunkLayOutVersion[] getAllVersions() {
    return CHUNK_LAYOUT_VERSION_INFOS.clone();
  }

  /**
   * Returns the latest version.
   *
   * @return versionInfo
   */
  public static ChunkLayOutVersion getLatestVersion() {
    return CHUNK_LAYOUT_VERSION_INFOS[CHUNK_LAYOUT_VERSION_INFOS.length - 1];
  }

  /**
   * Return version.
   *
   * @return int
   */
  public int getVersion() {
    return version;
  }

  /**
   * Returns description.
   * @return String
   */
  public String getDescription() {
    return description;
  }

}
