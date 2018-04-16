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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.utils.ConsistentHashRing;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Order the destinations based on consistent hashing.
 */
public class HashResolver implements OrderedResolver {

  protected static final Logger LOG =
      LoggerFactory.getLogger(HashResolver.class);


  /** Namespace set hash -> Locator. */
  private final Map<Integer, ConsistentHashRing> hashResolverMap;

  /** Patterns for temporary files. */
  private static final String HEX_PATTERN = "\\p{XDigit}";
  private static final String UUID_PATTERN = HEX_PATTERN + "{8}-" +
      HEX_PATTERN + "{4}-" + HEX_PATTERN + "{4}-" + HEX_PATTERN + "{4}-" +
      HEX_PATTERN + "{12}";
  private static final String ATTEMPT_PATTERN =
      "attempt_\\d+_\\d{4}_._\\d{6}_\\d{2}";
  private static final String[] TEMP_FILE_PATTERNS = {
      "(.+)\\.COPYING$",
      "(.+)\\._COPYING_.*$",
      "(.+)\\.tmp$",
      "_temp/(.+)$",
      "_temporary/(.+)\\." + UUID_PATTERN + "$",
      "(.*)_temporary/\\d/_temporary/" + ATTEMPT_PATTERN + "/(.+)$" };
  /** Pattern for temporary files (or of the individual patterns). */
  private static final Pattern TEMP_FILE_PATTERN =
      Pattern.compile(StringUtils.join("|", TEMP_FILE_PATTERNS));


  public HashResolver() {
    this.hashResolverMap = new ConcurrentHashMap<>();
  }

  /**
   * Use the result from consistent hashing locator to prioritize the locations
   * for a path.
   *
   * @param path Path to check.
   * @param loc Federated location with multiple destinations.
   * @return First namespace based on hash.
   */
  @Override
  public String getFirstNamespace(final String path, final PathLocation loc) {
    String finalPath = extractTempFileName(path);
    Set<String> namespaces = loc.getNamespaces();
    ConsistentHashRing locator = getHashResolver(namespaces);
    String hashedSubcluster = locator.getLocation(finalPath);
    if (hashedSubcluster == null) {
      String srcPath = loc.getSourcePath();
      LOG.error("Cannot find subcluster for {} ({} -> {})",
          srcPath, path, finalPath);
    }
    LOG.debug("Namespace for {} ({}) is {}", path, finalPath, hashedSubcluster);
    return hashedSubcluster;
  }

  /**
   * Get the cached (if available) or generate a new hash resolver for this
   * particular set of unique namespace identifiers.
   *
   * @param namespaces A set of unique namespace identifiers.
   * @return A hash resolver configured to consistently resolve paths to
   *         namespaces using the provided set of namespace identifiers.
   */
  private ConsistentHashRing getHashResolver(final Set<String> namespaces) {
    int hash = namespaces.hashCode();
    ConsistentHashRing resolver = this.hashResolverMap.get(hash);
    if (resolver == null) {
      resolver = new ConsistentHashRing(namespaces);
      this.hashResolverMap.put(hash, resolver);
    }
    return resolver;
  }

  /**
   * Some files use a temporary naming pattern. Extract the final name from the
   * temporary name. For example, files *._COPYING_ will be renamed, so we
   * remove that chunk.
   *
   * @param input Input string.
   * @return Final file name.
   */
  @VisibleForTesting
  public static String extractTempFileName(final String input) {
    StringBuilder sb = new StringBuilder();
    Matcher matcher = TEMP_FILE_PATTERN.matcher(input);
    if (matcher.find()) {
      for (int i=1; i <= matcher.groupCount(); i++) {
        String match = matcher.group(i);
        if (match != null) {
          sb.append(match);
        }
      }
    }
    if (sb.length() > 0) {
      String ret = sb.toString();
      LOG.debug("Extracted {} from {}", ret, input);
      return ret;
    }
    return input;
  }
}