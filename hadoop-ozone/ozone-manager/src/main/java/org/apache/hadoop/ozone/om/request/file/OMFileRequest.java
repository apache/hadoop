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

package org.apache.hadoop.ozone.om.request.file;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.hadoop.ozone.om.OMMetadataManager;

import javax.annotation.Nonnull;

/**
 * Base class for file requests.
 */
public final class OMFileRequest {

  private OMFileRequest() {
  }
  /**
   * Verify any files exist in the given path in the specified volume/bucket.
   * @param omMetadataManager
   * @param volumeName
   * @param bucketName
   * @param keyPath
   * @return true - if file exist in the given path, else false.
   * @throws IOException
   */
  public static OMDirectoryResult verifyFilesInPath(
      @Nonnull OMMetadataManager omMetadataManager,
      @Nonnull String volumeName,
      @Nonnull String bucketName, @Nonnull String keyName,
      @Nonnull Path keyPath) throws IOException {

    String fileNameFromDetails = omMetadataManager.getOzoneKey(volumeName,
        bucketName, keyName);
    String dirNameFromDetails = omMetadataManager.getOzoneDirKey(volumeName,
        bucketName, keyName);

    while (keyPath != null) {
      String pathName = keyPath.toString();

      String dbKeyName = omMetadataManager.getOzoneKey(volumeName,
          bucketName, pathName);
      String dbDirKeyName = omMetadataManager.getOzoneDirKey(volumeName,
          bucketName, pathName);

      if (omMetadataManager.getKeyTable().get(dbKeyName) != null) {
        // Found a file in the given path.
        // Check if this is actual file or a file in the given path
        if (dbKeyName.equals(fileNameFromDetails)) {
          return OMDirectoryResult.FILE_EXISTS;
        } else {
          return OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;
        }
      } else if (omMetadataManager.getKeyTable().get(dbDirKeyName) != null) {
        // Found a directory in the given path.
        // Check if this is actual directory or a directory in the given path
        if (dbDirKeyName.equals(dirNameFromDetails)) {
          return OMDirectoryResult.DIRECTORY_EXISTS;
        } else {
          return OMDirectoryResult.DIRECTORY_EXISTS_IN_GIVENPATH;
        }
      }
      keyPath = keyPath.getParent();
    }

    // Found no files/ directories in the given path.
    return OMDirectoryResult.NONE;
  }

  /**
   * Return codes used by verifyFilesInPath method.
   */
  enum OMDirectoryResult {

    // In below examples path is assumed as "a/b/c" in volume volume1 and
    // bucket b1.

    // When a directory exists in given path.
    // If we have a directory with name "a/b" we return this enum value.
    DIRECTORY_EXISTS_IN_GIVENPATH,

    // When a file exists in given path.
    // If we have a file with name "a/b" we return this enum value.
    FILE_EXISTS_IN_GIVENPATH,

    // When file already exists with the given path.
    // If we have a file with name "a/b/c" we return this enum value.
    FILE_EXISTS,

    // When directory exists with the given path.
    // If we have a file with name "a/b/c" we return this enum value.
    DIRECTORY_EXISTS,

    // If no file/directory exists with the given path.
    // If we don't have any file/directory name with "a/b/c" or any
    // sub-directory or file name from the given path we return this enum value.
    NONE
  }
}
