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
package org.apache.hadoop.fs.viewfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation for Apache Hadoop compatible file system based mount-table
 * file loading.
 */
public class HCFSMountTableConfigLoader implements MountTableConfigLoader {
  private static final String REGEX_DOT = "[.]";
  private static final Logger LOGGER =
      LoggerFactory.getLogger(HCFSMountTableConfigLoader.class);
  private Path mountTablePath = null;

  /**
   * Loads the mount-table configuration from hadoop compatible file system and
   * add the configuration items to given configuration. Mount-table
   * configuration format should be suffixed with version number. Format:
   * mount-table.<versionNumber>.xml Example: mount-table.1.xml When user wants
   * to update mount table, the expectation is to upload new configuration file
   * with incremented version number. This API loads the highest version number
   * file. We can also configure single file path directly.
   *
   * @param mountTableConfigPath : A directory path where mount-table files
   *          stored or a mount-table file path. We recommend to configure
   *          directory with the mount-table version files.
   * @param conf : to add the mount table as resource.
   */
  @Override
  public void load(String mountTableConfigPath, Configuration conf)
      throws IOException {
    this.mountTablePath = new Path(mountTableConfigPath);
    String scheme = mountTablePath.toUri().getScheme();
    ViewFileSystem.FsGetter fsGetter =
        new ViewFileSystemOverloadScheme.ChildFsGetter(scheme);
    FileSystem fs = fsGetter.getNewInstance(mountTablePath.toUri(), conf);

    RemoteIterator<LocatedFileStatus> listFiles =
        fs.listFiles(mountTablePath, false);
    LocatedFileStatus lfs = null;
    int higheirVersion = -1;
    while (listFiles.hasNext()) {
      LocatedFileStatus curLfs = listFiles.next();
      String cur = curLfs.getPath().getName();
      String[] nameParts = cur.split(REGEX_DOT);
      if (nameParts.length < 2) {
        logInvalidFileNameFormat(cur);
        continue; // invalid file name
      }
      int curVersion = higheirVersion;
      try {
        curVersion = Integer.parseInt(nameParts[nameParts.length - 2]);
      } catch (NumberFormatException nfe) {
        logInvalidFileNameFormat(cur);
        continue;
      }

      if (curVersion > higheirVersion) {
        higheirVersion = curVersion;
        lfs = curLfs;
      }
    }

    if (lfs == null) {
      // No valid mount table file found.
      // TODO: Should we fail? Currently viewfs init will fail if no mount
      // links anyway.
      LOGGER.warn("No valid mount-table file exist at: " + mountTableConfigPath
          + ". At least one mount-table file should present with the name "
          + "format: mount-table.<versionNumber>.xml");
      return;
    }
    // Latest version file.
    Path letestVersionMountTable = lfs.getPath();
    // We don't need to close this stream as it would have cached in
    // ChildFsGetter.
    conf.addResource(fs.open(letestVersionMountTable));
  }

  private void logInvalidFileNameFormat(String cur) {
    LOGGER.warn("Invalid file name format for mount-table version file: " + cur
        + ". The valid file name format is "
        + "mount-table-name.<versionNumber>.xml");
  }

}
