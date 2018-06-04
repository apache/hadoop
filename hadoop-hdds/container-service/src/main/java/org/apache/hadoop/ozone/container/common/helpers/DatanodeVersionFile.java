/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.helpers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.DataNodeLayoutVersion;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Properties;

/**
 * This is a utility class which helps to create the version file on datanode
 * and also validate the content of the version file.
 */
public class DatanodeVersionFile {

  private final String scmUuid;
  private final long cTime;
  private final int layOutVersion;

  public DatanodeVersionFile(String scmUuid, long cTime, int layOutVersion) {
    this.scmUuid = scmUuid;
    this.cTime = cTime;
    this.layOutVersion = layOutVersion;
  }

  private Properties createProperties() {
    Properties properties = new Properties();
    properties.setProperty(OzoneConsts.SCM_ID, scmUuid);
    properties.setProperty(OzoneConsts.CTIME, String.valueOf(cTime));
    properties.setProperty(OzoneConsts.LAYOUTVERSION, String.valueOf(
        layOutVersion));
    return properties;
  }

  /**
   * Creates a version File in specified path.
   * @param path
   * @throws IOException
   */
  public void createVersionFile(File path) throws
      IOException {
    try (RandomAccessFile file = new RandomAccessFile(path, "rws");
         FileOutputStream out = new FileOutputStream(file.getFD())) {
      file.getChannel().truncate(0);
      Properties properties = createProperties();
      /*
       * If server is interrupted before this line,
       * the version file will remain unchanged.
       */
      properties.store(out, null);
      /*
       * Now the new fields are flushed to the head of the file, but file
       * length can still be larger then required and therefore the file can
       * contain whole or corrupted fields from its old contents in the end.
       * If server is interrupted here and restarted later these extra fields
       * either should not effect server behavior or should be handled
       * by the server correctly.
       */
      file.getChannel().truncate(file.getChannel().size());
    }
  }


  /**
   * Creates a property object from the specified file content.
   * @param  versionFile
   * @return Properties
   * @throws IOException
   */
  public static Properties readFrom(File versionFile) throws IOException {
    try (RandomAccessFile file = new RandomAccessFile(versionFile, "rws");
         FileInputStream in = new FileInputStream(file.getFD())) {
      Properties props = new Properties();
      props.load(in);
      return props;
    }
  }

  /**
   * Verifies scmUuid is valid or not.
   * @param scmIdVersionFile
   * @param scmId
   * @throws InconsistentStorageStateException
   */
  @VisibleForTesting
  public static void verifyScmUuid(String scmIdVersionFile, String scmId) throws
      InconsistentStorageStateException {
    Preconditions.checkState(StringUtils.isNotBlank(scmIdVersionFile),
        "Invalid scmUuid from Version File.");
    Preconditions.checkState(StringUtils.isNotBlank(scmId),
        "Invalid scmUuid from SCM version request response");
    if(!scmIdVersionFile.equals(scmId)) {
      throw new InconsistentStorageStateException("MisMatch of ScmUuid " +
          "scmUuid from version File is: " + scmIdVersionFile + "SCM " +
          "version response scmUuid is" + scmId);
    }
  }

  /**
   * Verifies creationTime is valid or not.
   * @param creationTime
   */
  @VisibleForTesting
  public static void verifyCreationTime(String creationTime) {
    Preconditions.checkState(!StringUtils.isBlank(creationTime),
        "Invalid creation Time.");
  }

  /**
   * Verifies layOutVersion is valid or not.
   * @param lv
   * @throws InconsistentStorageStateException
   */
  @VisibleForTesting
  public static void verifyLayOutVersion(String lv) throws
      InconsistentStorageStateException {
    Preconditions.checkState(!StringUtils.isBlank(lv),
        "Invalid layOutVersion.");
    int version = Integer.parseInt(lv);
    if(DataNodeLayoutVersion.getLatestVersion().getVersion() != version) {
      throw new InconsistentStorageStateException("Incorrect layOutVersion");
    }
  }

  /**
   * Returns the versionFile path for the StorageLocation.
   * @param location
   * @param scmUuid
   * @return versionFile - File
   */
  @VisibleForTesting
  public static File getVersionFile(StorageLocation location, String scmUuid) {
    if (location != null) {
      String path = location.getUri().getPath();
      File parentPath = new File(path + File.separator + Storage
          .STORAGE_DIR_HDDS + File.separator +  scmUuid + File.separator +
          Storage.STORAGE_DIR_CURRENT + File.separator);
      File versionFile = new File(parentPath, Storage.STORAGE_FILE_VERSION);
      return versionFile;
    } else {
      return null;
    }
  }

}
