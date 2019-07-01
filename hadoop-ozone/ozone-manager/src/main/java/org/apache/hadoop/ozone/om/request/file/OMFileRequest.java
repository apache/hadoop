package org.apache.hadoop.ozone.om.request.file;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.hadoop.ozone.om.OMMetadataManager;

import javax.annotation.Nonnull;

/**
 * Base class for file requests.
 */
public interface OMFileRequest {
  /**
   * Verify any files exist in the given path in the specified volume/bucket.
   * @param omMetadataManager
   * @param volumeName
   * @param bucketName
   * @param keyPath
   * @return true - if file exist in the given path, else false.
   * @throws IOException
   */
  default OMDirectoryResult verifyFilesInPath(
      @Nonnull OMMetadataManager omMetadataManager, @Nonnull String volumeName,
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
    DIRECTORY_EXISTS_IN_GIVENPATH,
    FILE_EXISTS_IN_GIVENPATH,
    FILE_EXISTS,
    DIRECTORY_EXISTS,
    NONE
  }
}
