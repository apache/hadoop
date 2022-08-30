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
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.Constants.AUTHORITATIVE_PATH;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_AUTHORITATIVE_PATH;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_METASTORE_DYNAMO;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_METASTORE_LOCAL;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;

/**
 * Logic for integrating MetadataStore with S3A.
 * Most of the methods here were deleted when the S3Guard feature was removed.
 */
@SuppressWarnings("deprecation")
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class S3Guard {
  private static final Logger LOG = LoggerFactory.getLogger(S3Guard.class);

  static final String NULL_METADATA_STORE
      = "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore";

  // Utility class.  All static functions.
  private S3Guard() {
  }

  /**
   * Assert that the FS is not configured to use an unsupported S3Guard
   * option.
   * the empty/null option is preferred.
   * if the local store or null store is requested, the configuration is
   * allowed.
   * request for a DynamoDB or other store class will raise an exception.
   * @param fsURI FileSystem URI
   * @param conf configuration
   * @return true if an option was set but ignored
   * @throws PathIOException if an unsupported metastore was found.
   */
  public static boolean checkNoS3Guard(URI fsURI, Configuration conf) throws PathIOException {
    final String classname = conf.getTrimmed(S3_METADATA_STORE_IMPL, "");

    if (classname.isEmpty()) {
      // all good. declare nothing was found.
      return false;
    }
    // there is a s3guard configuration option
    // ignore if harmless; reject if DDB or unknown
    final String[] sources = conf.getPropertySources(S3_METADATA_STORE_IMPL);
    final String origin = sources == null
        ? "unknown"
        : sources[0];
    final String fsPath = fsURI.toString();
    switch (classname) {
    case NULL_METADATA_STORE:
      // harmless
      LOG.debug("Ignoring S3Guard store option of {} -no longer needed " +
              "Origin {}",
          NULL_METADATA_STORE, origin);
      break;
    case S3GUARD_METASTORE_LOCAL:
      // used in some libraries (e.g. hboss) to force a consistent s3 in a test
      // run.
      // print a message and continue
      LOG.warn("Ignoring S3Guard store option of {} -no longer needed or supported. "
              + "Origin {}",
          S3GUARD_METASTORE_LOCAL, origin);
      break;
    case S3GUARD_METASTORE_DYNAMO:
      // this is the dangerous one, as it is a sign that a config is in use where
      // older releases will use DDB for listing metadata, yet this
      // client will not update it.
      final String message = String.format("S3Guard is no longer needed/supported,"
              + " yet %s is configured to use DynamoDB as the S3Guard metadata store."
              + " This is no longer needed or supported. " +
              "Origin of setting is %s",
          fsPath, origin);
      LOG.error(message);
      throw new PathIOException(fsPath, message);

    default:
      // an unknown store entirely.
      throw new PathIOException(fsPath,
          "Filesystem is configured to use unknown S3Guard store " + classname
              + " origin " + origin);
    }

    // an option was set, but it was harmless
    return true;
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
