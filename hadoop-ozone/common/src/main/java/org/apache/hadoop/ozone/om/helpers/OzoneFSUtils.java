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
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.fs.Path;

import java.nio.file.Paths;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

/**
 * Utility class for OzoneFileSystem.
 */
public final class OzoneFSUtils {

  private OzoneFSUtils() {}

  /**
   * Returns string representation of path after removing the leading slash.
   */
  public static String pathToKey(Path path) {
    return path.toString().substring(1);
  }

  /**
   * Returns string representation of the input path parent. The function adds
   * a trailing slash if it does not exist and returns an empty string if the
   * parent is root.
   */
  public static String getParent(String keyName) {
    java.nio.file.Path parentDir = Paths.get(keyName).getParent();
    if (parentDir == null) {
      return "";
    }
    return addTrailingSlashIfNeeded(parentDir.toString());
  }

  /**
   * The function returns immediate child of given ancestor in a particular
   * descendant. For example if ancestor is /a/b and descendant is /a/b/c/d/e
   * the function should return /a/b/c/. If the descendant itself is the
   * immediate child then it is returned as is without adding a trailing slash.
   * This is done to distinguish files from a directory as in ozone files do
   * not carry a trailing slash.
   */
  public static String getImmediateChild(String descendant, String ancestor) {
    ancestor =
        !ancestor.isEmpty() ? addTrailingSlashIfNeeded(ancestor) : ancestor;
    if (!descendant.startsWith(ancestor)) {
      return null;
    }
    java.nio.file.Path descendantPath = Paths.get(descendant);
    java.nio.file.Path ancestorPath = Paths.get(ancestor);
    int ancestorPathNameCount =
        ancestor.isEmpty() ? 0 : ancestorPath.getNameCount();
    if (descendantPath.getNameCount() - ancestorPathNameCount > 1) {
      return addTrailingSlashIfNeeded(
          ancestor + descendantPath.getName(ancestorPathNameCount));
    }
    return descendant;
  }

  public static String addTrailingSlashIfNeeded(String key) {
    if (!key.endsWith(OZONE_URI_DELIMITER)) {
      return key + OZONE_URI_DELIMITER;
    } else {
      return key;
    }
  }

  public static boolean isFile(String keyName) {
    return !keyName.endsWith(OZONE_URI_DELIMITER);
  }
}
