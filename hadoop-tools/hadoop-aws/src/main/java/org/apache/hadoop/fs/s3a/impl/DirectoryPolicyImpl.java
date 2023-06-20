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


import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;
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
import static org.apache.hadoop.fs.s3a.Constants.STORE_CAPABILITY_DIRECTORY_MARKER_AWARE;
import static org.apache.hadoop.fs.s3a.Constants.STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_KEEP;
import static org.apache.hadoop.fs.s3a.Constants.STORE_CAPABILITY_DIRECTORY_MARKER_POLICY_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.STORE_CAPABILITY_DIRECTORY_MARKER_POLICY_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.STORE_CAPABILITY_DIRECTORY_MARKER_POLICY_KEEP;

/**
 * Implementation of directory policy.
 */
public final class DirectoryPolicyImpl
    implements DirectoryPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(
      DirectoryPolicyImpl.class);

  /**
   * Error string when unable to parse the marker policy option.
   */
  public static final String UNKNOWN_MARKER_POLICY =
      "Unknown policy in "
      + DIRECTORY_MARKER_POLICY + ": ";

  /**
   * All available policies.
   */
  private static final Set<MarkerPolicy> AVAILABLE_POLICIES =
      EnumSet.allOf(MarkerPolicy.class);

  /**
   * Keep all markers.
   */
  public static final DirectoryPolicy KEEP = new DirectoryPolicyImpl(
      MarkerPolicy.Keep, (p) -> false);

  /**
   * Delete all markers.
   */
  public static final DirectoryPolicy DELETE = new DirectoryPolicyImpl(
      MarkerPolicy.Delete, (p) -> false);

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
   * @param markerPolicy marker policy
   * @param authoritativeness function for authoritativeness
   */
  public DirectoryPolicyImpl(final MarkerPolicy markerPolicy,
      final Predicate<Path> authoritativeness) {
    this.markerPolicy = markerPolicy;
    this.authoritativeness = authoritativeness;
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
  public MarkerPolicy getMarkerPolicy() {
    return markerPolicy;
  }

  @Override
  public String describe() {
    return markerPolicy.getOptionName();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "DirectoryMarkerRetention{");
    sb.append("policy='").append(markerPolicy.getOptionName()).append('\'');
    sb.append('}');
    return sb.toString();
  }

  /**
   * Return path policy for store and paths.
   * @param path path
   * @param capability capability
   * @return true if a capability is active
   */
  @Override
  public boolean hasPathCapability(final Path path, final String capability) {

    switch (capability) {
    /*
     * Marker policy is dynamically determined for the given path.
     */
    case STORE_CAPABILITY_DIRECTORY_MARKER_AWARE:
      return true;

    case STORE_CAPABILITY_DIRECTORY_MARKER_POLICY_KEEP:
      return markerPolicy == MarkerPolicy.Keep;

    case STORE_CAPABILITY_DIRECTORY_MARKER_POLICY_DELETE:
      return markerPolicy == MarkerPolicy.Delete;

    case STORE_CAPABILITY_DIRECTORY_MARKER_POLICY_AUTHORITATIVE:
      return markerPolicy == MarkerPolicy.Authoritative;

    case STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_KEEP:
      return keepDirectoryMarkers(path);

    case STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_DELETE:
      return !keepDirectoryMarkers(path);

    default:
      throw new IllegalArgumentException("Unknown capability " + capability);
    }
  }

  /**
   * Create/Get the policy for this configuration.
   * @param conf config
   * @param authoritativeness Callback to evaluate authoritativeness of a
   * path.
   * @return a policy
   */
  public static DirectoryPolicy getDirectoryPolicy(
      final Configuration conf,
      final Predicate<Path> authoritativeness) {
    DirectoryPolicy policy;
    String option = conf.getTrimmed(DIRECTORY_MARKER_POLICY,
        DEFAULT_DIRECTORY_MARKER_POLICY);
    switch (option.toLowerCase(Locale.ENGLISH)) {
    case DIRECTORY_MARKER_POLICY_DELETE:
      // backwards compatible.
      LOG.debug("Directory markers will be deleted");
      policy = DELETE;
      break;
    case DIRECTORY_MARKER_POLICY_KEEP:
      LOG.debug("Directory markers will be kept");
      policy = KEEP;
      break;
    case DIRECTORY_MARKER_POLICY_AUTHORITATIVE:
      LOG.debug("Directory markers will be kept on authoritative"
          + " paths");
      policy = new DirectoryPolicyImpl(MarkerPolicy.Authoritative,
          authoritativeness);
      break;
    default:
      throw new IllegalArgumentException(UNKNOWN_MARKER_POLICY + option);
    }
    return policy;
  }

  /**
   * Enumerate all available policies.
   * @return set of the policies.
   */
  public static Set<MarkerPolicy> availablePolicies() {
    return AVAILABLE_POLICIES;
  }

}
