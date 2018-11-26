/*
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

package org.apache.hadoop.fs.s3a.s3guard;

import javax.annotation.Nullable;

/**
 * LocalMetadataEntry is used to store entries in the cache of
 * LocalMetadataStore. PathMetadata or dirListingMetadata can be null. The
 * entry is not immutable.
 */
public final class LocalMetadataEntry {
  @Nullable
  private PathMetadata pathMetadata;
  @Nullable
  private DirListingMetadata dirListingMetadata;

  LocalMetadataEntry() {
  }

  LocalMetadataEntry(PathMetadata pmd){
    pathMetadata = pmd;
    dirListingMetadata = null;
  }

  LocalMetadataEntry(DirListingMetadata dlm){
    pathMetadata = null;
    dirListingMetadata = dlm;
  }

  public PathMetadata getFileMeta() {
    return pathMetadata;
  }

  public DirListingMetadata getDirListingMeta() {
    return dirListingMetadata;
  }


  public boolean hasPathMeta() {
    return this.pathMetadata != null;
  }

  public boolean hasDirMeta() {
    return this.dirListingMetadata != null;
  }

  public void setPathMetadata(PathMetadata pathMetadata) {
    this.pathMetadata = pathMetadata;
  }

  public void setDirListingMetadata(DirListingMetadata dirListingMetadata) {
    this.dirListingMetadata = dirListingMetadata;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("LocalMetadataEntry{");
    if(pathMetadata != null) {
      sb.append("pathMetadata=" + pathMetadata.getFileStatus().getPath());
    }
    if(dirListingMetadata != null){
      sb.append("; dirListingMetadata=" + dirListingMetadata.getPath());
    }
    sb.append("}");
    return sb.toString();
  }
}
