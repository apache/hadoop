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

package org.apache.hadoop.fs.s3a.s3guard;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.Constants.AUTHORITATIVE_PATH;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_AUTHORITATIVE_PATH;

/**
 * Logic for integrating MetadataStore with S3A.
 * Most of the methods here were deleted when the S3Guard feature was removed.
 */
@SuppressWarnings("deprecation")
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class S3Guard {
  private static final Logger LOG = LoggerFactory.getLogger(S3Guard.class);

  // Utility class.  All static functions.
  private S3Guard() {
  }

  /**
   * Assert that there is no S3Guard.
   * @param fsURI FileSystem URI
   * @param conf configuration
   * @throws PathIOException if the metastore is enabled.
   */
  public static void checkNoS3Guard(URI fsURI, Configuration conf) throws PathIOException {
    final String classname = conf.getTrimmed(Constants.S3_METADATA_STORE_IMPL, "");
    if (!"org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore".equals(classname)
        && !classname.isEmpty()) {
      LOG.error("Metastore option source {}",
          (Object) conf.getPropertySources(Constants.S3_METADATA_STORE_IMPL));
      throw new PathIOException(fsURI.toString(),
          "Filesystem is configured to use Unsupported S3Guard store " + classname);
    }
  }

  public static Collection<String> getAuthoritativePaths(S3AFileSystem fs) {
    return getAuthoritativePaths(
        fs.getUri(),
        fs.getConf(),
        p -> fs.maybeAddTrailingSlash(fs.qualify(p).toString()));
  }

  /**
   * Get the authoritative paths of a filesystem.
   *
   * @param uri FS URI
   * @param conf configuration
   * @param qualifyToDir a qualification operation
   * @return list of URIs valid for this FS.
   */
  @VisibleForTesting
  static Collection<String> getAuthoritativePaths(
      final URI uri,
      final Configuration conf,
      final Function<Path, String> qualifyToDir) {
    String[] rawAuthoritativePaths =
        conf.getTrimmedStrings(AUTHORITATIVE_PATH, DEFAULT_AUTHORITATIVE_PATH);
    Collection<String> authoritativePaths = new ArrayList<>();
    if (rawAuthoritativePaths.length > 0) {
      for (int i = 0; i < rawAuthoritativePaths.length; i++) {
        Path path = new Path(rawAuthoritativePaths[i]);
        URI pathURI = path.toUri();
        if (pathURI.getAuthority() != null &&
            !pathURI.getAuthority().equals(uri.getAuthority())) {
          // skip on auth
          continue;
        }
        if (pathURI.getScheme() != null &&
            !pathURI.getScheme().equals(uri.getScheme())) {
          // skip on auth
          continue;
        }
        authoritativePaths.add(qualifyToDir.apply(path));
      }
    }
    return authoritativePaths;
  }

  /**
   * Is the path for the given FS instance authoritative?
   * @param p path
   * @param fs filesystem
   * @param authPaths possibly empty list of authoritative paths
   * @return true iff the path is authoritative
   */
  public static boolean allowAuthoritative(Path p, S3AFileSystem fs,
      Collection<String> authPaths) {
    String haystack = fs.maybeAddTrailingSlash(fs.qualify(p).toString());
    if (!authPaths.isEmpty()) {
      for (String needle : authPaths) {
        if (haystack.startsWith(needle)) {
          return true;
        }
      }
    }
    return false;
  }

}
