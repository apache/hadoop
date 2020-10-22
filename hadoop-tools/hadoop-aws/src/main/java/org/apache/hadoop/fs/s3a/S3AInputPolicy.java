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

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_FADVISE_ADAPTIVE;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADV_NORMAL;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADV_RANDOM;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADV_SEQUENTIAL;

/**
 * Filesystem input policy.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum S3AInputPolicy {

  Normal(INPUT_FADV_NORMAL),
  Sequential(INPUT_FADV_SEQUENTIAL),
  Random(INPUT_FADV_RANDOM);

  private final String policy;

  S3AInputPolicy(String policy) {
    this.policy = policy;
  }

  @Override
  public String toString() {
    return policy;
  }

  /**
   * Choose an FS access policy.
   * @param name strategy name from a configuration option, etc.
   * @param defaultPolicy default policy to fall back to.
   * @return the chosen strategy
   */
  public static S3AInputPolicy getPolicy(
      String name,
      @Nullable S3AInputPolicy defaultPolicy) {
    String trimmed = name.trim().toLowerCase(Locale.ENGLISH);
    switch (trimmed) {
    case INPUT_FADV_NORMAL:
    case FS_OPTION_OPENFILE_FADVISE_ADAPTIVE:
      return Normal;
    case INPUT_FADV_RANDOM:
      return Random;
    case INPUT_FADV_SEQUENTIAL:
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
