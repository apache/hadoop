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
package org.apache.hadoop.hdfs.server.federation.resolver.order;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;

/**
 * Variation of HashResolver that only uses the first level of the path.
 */
public class HashFirstResolver extends HashResolver {

  @Override
  public String getFirstNamespace(final String path, final PathLocation loc) {
    String srcPath = loc.getSourcePath();
    String trimmedPath = trimPathToChild(path, srcPath);
    LOG.debug("Only using the first part of the path: {} -> {}",
        path, trimmedPath);
    return super.getFirstNamespace(trimmedPath, loc);
  }

  /**
   * Hash only up to the immediate child of the mount point. This prevents the
   * need to create/maintain subtrees under each multi-destination mount point.
   * Each child of a multi-destination mount is mapped to only one hash
   * location.
   * <p>
   * Trims a path to at most the immediate child of a parent path. For example:
   * <ul>
   * <li>path = /a/b/c, parent = /a will be trimmed to /a/b.
   * <li>path = /a/b, parent = /a/b will be trimmed to /a/b
   * </ul>
   *
   * @param path The path to trim.
   * @param parent The parent used to find the immediate child.
   * @return Trimmed path.
   */
  private static String trimPathToChild(String path, String parent) {
    // Path is invalid or equal to the parent
    if (path.length() <= parent.length()) {
      return parent;
    }
    String remainder = path.substring(parent.length());
    String[] components =
        remainder.replaceFirst("^/", "").split(Path.SEPARATOR);
    if (components.length > 0 && components[0].length() > 0) {
      if (parent.endsWith(Path.SEPARATOR)) {
        return parent + components[0];
      } else {
        return parent + Path.SEPARATOR + components[0];
      }
    } else {
      return parent;
    }
  }
}