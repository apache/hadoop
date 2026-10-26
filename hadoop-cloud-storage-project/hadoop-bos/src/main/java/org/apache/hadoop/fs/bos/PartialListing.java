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

package org.apache.hadoop.fs.bos;

/**
 * Holds information on a directory listing for a
 * {@link BosNativeFileSystemStore}.
 * This includes the {@link FileMetadata files} and
 * directories (their names) contained in a directory.
 *
 * <p>This listing may be returned in chunks, so a
 * {@code priorLastKey} is provided so that the next chunk
 * may be requested.
 *
 * @see BosNativeFileSystemStore#list(String, int, String)
 */
class PartialListing {

  private final String priorLastKey;
  private final FileMetadata[] files;
  private final FileMetadata[] directories;

  /**
   * Constructs a PartialListing.
   *
   * @param priorLastKey the marker for requesting the next
   *                     chunk
   * @param files        the file entries in this listing
   * @param commonPrefixesWithExtMeta the directory entries
   *                     in this listing
   */
  PartialListing(
      String priorLastKey, FileMetadata[] files,
      FileMetadata[] commonPrefixesWithExtMeta) {
    this.priorLastKey = priorLastKey;
    this.files = files;
    this.directories = commonPrefixesWithExtMeta;
  }

  /**
   * Returns the file entries in this listing.
   *
   * @return the array of file metadata
   */
  public FileMetadata[] getFiles() {
    return files;
  }

  /**
   * Returns the marker for requesting the next listing
   * chunk.
   *
   * @return the prior last key, or null if this is the
   *         last chunk
   */
  public String getPriorLastKey() {
    return priorLastKey;
  }

  /**
   * Returns the directory entries in this listing.
   *
   * @return the array of directory metadata
   */
  public FileMetadata[] getDirectories() {
    return directories;
  }
}
