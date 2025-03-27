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
package org.apache.hadoop.fs;

import java.util.Collection;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Utility class for bulk delete operations.
 */
public final class BulkDeleteUtils {

  private BulkDeleteUtils() {
  }

  /**
   * Preconditions for bulk delete paths.
   * @param paths paths to delete.
   * @param pageSize maximum number of paths to delete in a single operation.
   * @param basePath base path for the delete operation.
   */
  public static void validateBulkDeletePaths(Collection<Path> paths, int pageSize, Path basePath) {
    requireNonNull(paths);
    checkArgument(paths.size() <= pageSize,
            "Number of paths (%d) is larger than the page size (%d)", paths.size(), pageSize);
    paths.forEach(p -> {
      checkArgument(p.isAbsolute(), "Path %s is not absolute", p);
      checkArgument(validatePathIsUnderParent(p, basePath),
              "Path %s is not under the base path %s", p, basePath);
    });
  }

  /**
   * Check if a given path is the base path or under the base path.
   * @param p path to check.
   * @param basePath base path.
   * @return true if the given path is the base path or under the base path.
   */
  public static boolean validatePathIsUnderParent(Path p, Path basePath) {
    while (p != null) {
      if (p.equals(basePath)) {
        return true;
      }
      p = p.getParent();
    }
    return false;
  }

}
