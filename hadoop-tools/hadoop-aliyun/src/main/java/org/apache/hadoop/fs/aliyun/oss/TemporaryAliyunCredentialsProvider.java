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

package org.apache.hadoop.fs.aliyun.oss;

import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentials;
import com.aliyun.oss.common.auth.InvalidCredentialsException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

import static org.apache.hadoop.fs.aliyun.oss.Constants.*;

/**
 * Support session credentials for authenticating with ALiyun.
 */
public class TemporaryAliyunCredentialsProvider implements CredentialsProvider {
  public static final String NAME
      = "org.apache.hadoop.fs.aliyun.oss.TemporaryAliyunCredentialsProvider";
  private final String accessKeyId;
  private final String accessKeySecret;
  private final String securityToken;

  public TemporaryAliyunCredentialsProvider(URI uri, Configuration conf) {
    this.accessKeyId = conf.get(ACCESS_KEY, null);
    this.accessKeySecret = conf.get(SECRET_KEY, null);
    this.securityToken = conf.get(SECURITY_TOKEN, null);
  }

  @Override
  public void setCredentials(Credentials creds) {

  }

  @Override
  public Credentials getCredentials() {
    if (!StringUtils.isEmpty(accessKeyId)
        && !StringUtils.isEmpty(accessKeySecret)
        && !StringUtils.isEmpty(securityToken)) {
      return new DefaultCredentials(accessKeyId, accessKeySecret,
          securityToken);
    }
    throw new InvalidCredentialsException(
        "AccessKeyId, AccessKeySecret or SecurityToken is unset");
  }
}
