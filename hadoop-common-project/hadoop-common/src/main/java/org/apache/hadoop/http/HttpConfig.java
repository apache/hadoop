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

import com.google.common.annotations.VisibleForTesting;
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
  private static boolean sslEnabled;

  static {
    Configuration conf = new Configuration();
    sslEnabled = conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_KEY,
        CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_DEFAULT);
  }

  @VisibleForTesting
  static void setSecure(boolean secure) {
    sslEnabled = secure;
  }

  public static boolean isSecure() {
    return sslEnabled;
  }

  public static String getSchemePrefix() {
    return (isSecure()) ? "https://" : "http://";
  }

}
