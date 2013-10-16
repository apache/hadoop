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
package org.apache.hadoop.http;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

/**
 * Singleton to get access to Http related configuration.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HttpConfig {
  private static Policy policy;
  public enum Policy {
    HTTP_ONLY,
    HTTPS_ONLY;

    public static Policy fromString(String value) {
      if (value.equalsIgnoreCase(CommonConfigurationKeysPublic
              .HTTP_POLICY_HTTPS_ONLY)) {
        return HTTPS_ONLY;
      }
      return HTTP_ONLY;
    }
  }

  static {
    Configuration conf = new Configuration();
    boolean sslEnabled = conf.getBoolean(
            CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_KEY,
            CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_DEFAULT);
    policy = sslEnabled ? Policy.HTTPS_ONLY : Policy.HTTP_ONLY;
  }

  public static void setPolicy(Policy policy) {
    HttpConfig.policy = policy;
  }

  public static boolean isSecure() {
    return policy == Policy.HTTPS_ONLY;
  }

  public static String getSchemePrefix() {
    return (isSecure()) ? "https://" : "http://";
  }

  public static String getScheme(Policy policy) {
    return policy == Policy.HTTPS_ONLY ? "https://" : "http://";
  }
}
