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

package org.apache.hadoop.fs.s3native;

import java.io.IOException;
import java.net.URI;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3native.S3NativeFileSystemConfigKeys.S3_NATIVE_AWS_ACCESS_KEY_ID;
import static org.apache.hadoop.fs.s3native.S3NativeFileSystemConfigKeys.S3_NATIVE_AWS_SECRET_ACCESS_KEY;

/**
 * <p>
 * Extracts AWS credentials from the filesystem URI or configuration.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class S3Credentials {

  private String accessKey;
  private String secretAccessKey;

  /**
   * @param uri bucket URI optionally containing username and password.
   * @param conf configuration
   * @throws IllegalArgumentException if credentials for S3 cannot be
   * determined.
   * @throws IOException if credential providers are misconfigured and we have
   *                     to talk to them.
   */
  public void initialize(URI uri, Configuration conf) throws IOException {
    Preconditions.checkArgument(uri.getHost() != null,
        "Invalid hostname in URI " + uri);

    String userInfo = uri.getUserInfo();
    if (userInfo != null) {
      int index = userInfo.indexOf(':');
      if (index != -1) {
        accessKey = userInfo.substring(0, index);
        secretAccessKey = userInfo.substring(index + 1);
      } else {
        accessKey = userInfo;
      }
    }

    if (accessKey == null) {
      accessKey = conf.getTrimmed(S3_NATIVE_AWS_ACCESS_KEY_ID);
    }
    if (secretAccessKey == null) {
      final char[] pass = conf.getPassword(S3_NATIVE_AWS_SECRET_ACCESS_KEY);
      if (pass != null) {
        secretAccessKey = (new String(pass)).trim();
      }
    }

    final String scheme = uri.getScheme();
    Preconditions.checkArgument(!(accessKey == null && secretAccessKey == null),
        "AWS Access Key ID and Secret Access Key must be specified as the " +
            "username or password (respectively) of a " + scheme + " URL, or " +
            "by setting the " + S3_NATIVE_AWS_ACCESS_KEY_ID + " or " +
            S3_NATIVE_AWS_SECRET_ACCESS_KEY + " properties (respectively).");
    Preconditions.checkArgument(accessKey != null,
        "AWS Access Key ID must be specified as the username of a " + scheme +
            " URL, or by setting the " + S3_NATIVE_AWS_ACCESS_KEY_ID +
            " property.");
    Preconditions.checkArgument(secretAccessKey != null,
        "AWS Secret Access Key must be specified as the password of a " + scheme
            + " URL, or by setting the " + S3_NATIVE_AWS_SECRET_ACCESS_KEY +
            " property.");
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getSecretAccessKey() {
    return secretAccessKey;
  }
}
