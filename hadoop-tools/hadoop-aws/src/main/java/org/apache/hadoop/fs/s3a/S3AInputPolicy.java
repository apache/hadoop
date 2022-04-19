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

package org.apache.hadoop.fs.s3a;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Locale;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_DEFAULT;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_RANDOM;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_VECTOR;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE;

/**
 * Stream input policy.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum S3AInputPolicy {

  Normal(FS_OPTION_OPENFILE_READ_POLICY_DEFAULT, false, true),
  Random(FS_OPTION_OPENFILE_READ_POLICY_RANDOM, true, false),
  Sequential(FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL, false, false);

  /** Policy name. */
  private final String policy;

  /** Is this random IO? */
  private final boolean randomIO;

  /** Is this an adaptive policy? */
  private final boolean adaptive;

  S3AInputPolicy(String policy,
      boolean randomIO,
      boolean adaptive) {
    this.policy = policy;
    this.randomIO = randomIO;
    this.adaptive = adaptive;
  }

  @Override
  public String toString() {
    return policy;
  }

  String getPolicy() {
    return policy;
  }

  boolean isRandomIO() {
    return randomIO;
  }

  boolean isAdaptive() {
    return adaptive;
  }

  /**
   * Choose an access policy.
   * @param name strategy name from a configuration option, etc.
   * @param defaultPolicy default policy to fall back to.
   * @return the chosen strategy
   */
  public static S3AInputPolicy getPolicy(
      String name,
      @Nullable S3AInputPolicy defaultPolicy) {
    String trimmed = name.trim().toLowerCase(Locale.ENGLISH);
    switch (trimmed) {
    case FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE:
    case FS_OPTION_OPENFILE_READ_POLICY_DEFAULT:
    case Constants.INPUT_FADV_NORMAL:
      return Normal;

    // all these options currently map to random IO.
    case FS_OPTION_OPENFILE_READ_POLICY_RANDOM:
    case FS_OPTION_OPENFILE_READ_POLICY_VECTOR:
      return Random;

    case FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL:
    case FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE:
      return Sequential;
    default:
      return defaultPolicy;
    }
  }

  /**
   * Scan the list of input policies, returning the first one supported.
   * @param policies list of policies.
   * @param defaultPolicy fallback
   * @return a policy or the defaultPolicy, which may be null
   */
  public static S3AInputPolicy getFirstSupportedPolicy(
      Collection<String> policies,
      @Nullable S3AInputPolicy defaultPolicy) {
    for (String s : policies) {
      S3AInputPolicy nextPolicy = S3AInputPolicy.getPolicy(s, null);
      if (nextPolicy != null) {
        return nextPolicy;
      }
    }
    return defaultPolicy;
  }

}
