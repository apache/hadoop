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

package org.apache.hadoop.fs.slive;

import java.util.Random;

import org.apache.hadoop.fs.Path;

/**
 * Class which generates a file or directory path using a simple random
 * generation algorithm stated in http://issues.apache.org/jira/browse/HDFS-708
 */
class PathFinder {

  private enum Type {
    FILE, DIRECTORY
  }

  private static final String DIR_PREFIX = "sl_dir_";
  private static final String FILE_PREFIX = "sl_file_";

  private Path basePath;
  private ConfigExtractor config;
  private Random rnd;

  PathFinder(ConfigExtractor cfg, Random rnd) {
    this.basePath = cfg.getDataPath();
    this.config = cfg;
    this.rnd = rnd;
  }

  /**
   * This function uses a simple recursive algorithm to generate a path name
   * using the current id % limitPerDir and using current id / limitPerDir to
   * form the rest of the tree segments
   * 
   * @param curId
   *          the current id to use for determining the current directory id %
   *          per directory limit and then used for determining the next segment
   *          of the path to use, if <= zero this will return the base path
   * @param limitPerDir
   *          the per directory file limit used in modulo and division
   *          operations to calculate the file name and path tree
   * @param type
   *          directory or file enumeration
   * @return Path
   */
  private Path getPath(int curId, int limitPerDir, Type type) {
    if (curId <= 0) {
      return basePath;
    }
    String name = "";
    switch (type) {
    case FILE:
      name = FILE_PREFIX + new Integer(curId % limitPerDir).toString();
      break;
    case DIRECTORY:
      name = DIR_PREFIX + new Integer(curId % limitPerDir).toString();
      break;
    }
    Path base = getPath((curId / limitPerDir), limitPerDir, Type.DIRECTORY);
    return new Path(base, name);
  }

  /**
   * Gets a file path using the given configuration provided total files and
   * files per directory
   * 
   * @return path
   */
  Path getFile() {
    int fileLimit = config.getTotalFiles();
    int dirLimit = config.getDirSize();
    int startPoint = 1 + rnd.nextInt(fileLimit);
    return getPath(startPoint, dirLimit, Type.FILE);
  }

  /**
   * Gets a directory path using the given configuration provided total files
   * and files per directory
   * 
   * @return path
   */
  Path getDirectory() {
    int fileLimit = config.getTotalFiles();
    int dirLimit = config.getDirSize();
    int startPoint = rnd.nextInt(fileLimit);
    return getPath(startPoint, dirLimit, Type.DIRECTORY);
  }

}
