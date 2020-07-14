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

package org.apache.hadoop.fs.s3a.impl;


import java.util.Locale;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_DIRECTORY_MARKER_POLICY;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_KEEP;

/**
 * Implementation of directory policy.
 */
public final class DirectoryPolicyImpl
    implements DirectoryPolicy {

  /**
   * Error string when unable to parse the marker policy option.
   */
  public static final String UNKNOWN_MARKER_POLICY = "Unknown value of "
      + DIRECTORY_MARKER_POLICY + ": ";

  private static final Logger LOG = LoggerFactory.getLogger(
      DirectoryPolicyImpl.class);

  /**
   * Chosen marker policy.
   */
  private final MarkerPolicy markerPolicy;

  /**
   * Callback to evaluate authoritativeness of a
   * path.
   */
  private final Predicate<Path> authoritativeness;

  /**
   * Constructor.
   * @param conf config
   * @param authoritativeness Callback to evaluate authoritativeness of a
   * path.
   */
  public DirectoryPolicyImpl(
      final Configuration conf,
      final Predicate<Path> authoritativeness) {
    this.authoritativeness = authoritativeness;
    String option = conf.getTrimmed(DIRECTORY_MARKER_POLICY,
        DEFAULT_DIRECTORY_MARKER_POLICY);
    MarkerPolicy p;
    switch (option.toLowerCase(Locale.ENGLISH)) {
    case DIRECTORY_MARKER_POLICY_DELETE:
      // backwards compatible.
      p = MarkerPolicy.Delete;
      LOG.debug("Directory markers will be deleted");
      break;
    case DIRECTORY_MARKER_POLICY_KEEP:
      p = MarkerPolicy.Keep;
      LOG.info("Directory markers will be kept");
      break;
    case DIRECTORY_MARKER_POLICY_AUTHORITATIVE:
      p = MarkerPolicy.Authoritative;
      LOG.info("Directory markers will be kept on authoritative"
          + " paths");
      break;
    default:
      throw new IllegalArgumentException(UNKNOWN_MARKER_POLICY + option);
    }
    this.markerPolicy = p;
  }

  @Override
  public boolean keepDirectoryMarkers(final Path path) {
    switch (markerPolicy) {
    case Keep:
      return true;
    case Authoritative:
      return authoritativeness.test(path);
    case Delete:
    default:   // which cannot happen
      return false;
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "DirectoryMarkerRetention{");
    sb.append("policy='").append(markerPolicy).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
