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
import org.apache.hadoop.fs.FSDataInputStream;
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
  private Path mountTable = null;

  /**
   * Loads the mount-table configuration from hadoop compatible file system and
   * add the configuration items to given configuration. Mount-table
   * configuration format should be suffixed with version number.
   * Format: mount-table.<versionNumber>.xml
   * Example: mount-table.1.xml
   * When user wants to update mount-table, the expectation is to upload new
   * mount-table configuration file with monotonically increasing integer as
   * version number. This API loads the highest version number file. We can
   * also configure single file path directly.
   *
   * @param mountTableConfigPath : A directory path where mount-table files
   *          stored or a mount-table file path. We recommend to configure
   *          directory with the mount-table version files.
   * @param conf : to add the mount table as resource.
   */
  @Override
  public void load(String mountTableConfigPath, Configuration conf)
      throws IOException {
    this.mountTable = new Path(mountTableConfigPath);
    String scheme = mountTable.toUri().getScheme();
    FsGetter fsGetter = new ViewFileSystemOverloadScheme.ChildFsGetter(scheme);
    try (FileSystem fs = fsGetter.getNewInstance(mountTable.toUri(), conf)) {
      RemoteIterator<LocatedFileStatus> listFiles =
          fs.listFiles(mountTable, false);
      LocatedFileStatus lfs = null;
      int higherVersion = -1;
      while (listFiles.hasNext()) {
        LocatedFileStatus curLfs = listFiles.next();
        String cur = curLfs.getPath().getName();
        String[] nameParts = cur.split(REGEX_DOT);
        if (nameParts.length < 2) {
          logInvalidFileNameFormat(cur);
          continue; // invalid file name
        }
        int curVersion = higherVersion;
        try {
          curVersion = Integer.parseInt(nameParts[nameParts.length - 2]);
        } catch (NumberFormatException nfe) {
          logInvalidFileNameFormat(cur);
          continue;
        }

        if (curVersion > higherVersion) {
          higherVersion = curVersion;
          lfs = curLfs;
        }
      }

      if (lfs == null) {
        // No valid mount table file found.
        // TODO: Should we fail? Currently viewfs init will fail if no mount
        // links anyway.
        LOGGER.warn("No valid mount-table file exist at: {}. At least one "
            + "mount-table file should present with the name format: "
            + "mount-table.<versionNumber>.xml", mountTableConfigPath);
        return;
      }
      // Latest version file.
      Path latestVersionMountTable = lfs.getPath();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Loading the mount-table {} into configuration.",
            latestVersionMountTable);
      }
      try (FSDataInputStream open = fs.open(latestVersionMountTable)) {
        Configuration newConf = new Configuration(false);
        newConf.addResource(open);
        // This will add configuration props as resource, instead of stream
        // itself. So, that stream can be closed now.
        conf.addResource(newConf);
      }
    }
  }

  private void logInvalidFileNameFormat(String cur) {
    LOGGER.warn("Invalid file name format for mount-table version file: {}. "
        + "The valid file name format is mount-table-name.<versionNumber>.xml",
        cur);
  }

}
