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

package org.apache.hadoop.yarn.server.sharedcache;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class that contains helper methods for dealing with the internal
 * shared cache structure.
 */
@Private
@Unstable
public class SharedCacheUtil {

  private static final Logger LOG =
      LoggerFactory.getLogger(SharedCacheUtil.class);

  @Private
  public static int getCacheDepth(Configuration conf) {
    int cacheDepth =
        conf.getInt(YarnConfiguration.SHARED_CACHE_NESTED_LEVEL,
            YarnConfiguration.DEFAULT_SHARED_CACHE_NESTED_LEVEL);

    if (cacheDepth <= 0) {
      LOG.warn("Specified cache depth was less than or equal to zero."
          + " Using default value instead. Default: {}, Specified: {}",
          YarnConfiguration.DEFAULT_SHARED_CACHE_NESTED_LEVEL, cacheDepth);
      cacheDepth = YarnConfiguration.DEFAULT_SHARED_CACHE_NESTED_LEVEL;
    }

    return cacheDepth;
  }

  @Private
  public static String getCacheEntryPath(int cacheDepth, String cacheRoot,
      String checksum) {

    if (cacheDepth <= 0) {
      throw new IllegalArgumentException(
          "The cache depth must be greater than 0. Passed value: " + cacheDepth);
    }
    if (checksum.length() < cacheDepth) {
      throw new IllegalArgumentException("The checksum passed was too short: "
          + checksum);
    }

    // Build the cache entry path to the specified depth. For example, if the
    // depth is 3 and the checksum is 3c4f, the path would be:
    // SHARED_CACHE_ROOT/3/c/4/3c4f
    StringBuilder sb = new StringBuilder(cacheRoot);
    for (int i = 0; i < cacheDepth; i++) {
      sb.append(Path.SEPARATOR_CHAR);
      sb.append(checksum.charAt(i));
    }
    sb.append(Path.SEPARATOR_CHAR).append(checksum);

    return sb.toString();
  }

  @Private
  public static String getCacheEntryGlobPattern(int depth) {
    StringBuilder pattern = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      pattern.append("*/");
    }
    pattern.append("*");
    return pattern.toString();
  }
}
