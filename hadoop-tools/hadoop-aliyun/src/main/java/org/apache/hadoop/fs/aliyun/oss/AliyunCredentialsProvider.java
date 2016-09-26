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

import java.io.IOException;

import static org.apache.hadoop.fs.aliyun.oss.Constants.*;

/**
 * Support session credentials for authenticating with Aliyun.
 */
public class AliyunCredentialsProvider implements CredentialsProvider {
  private Credentials credentials = null;

  public AliyunCredentialsProvider(Configuration conf)
      throws IOException {
    String accessKeyId;
    String accessKeySecret;
    String securityToken;
    try {
      accessKeyId = AliyunOSSUtils.getValueWithKey(conf, ACCESS_KEY_ID);
      accessKeySecret = AliyunOSSUtils.getValueWithKey(conf, ACCESS_KEY_SECRET);
    } catch (IOException e) {
      throw new InvalidCredentialsException(e);
    }

    try {
      securityToken = AliyunOSSUtils.getValueWithKey(conf, SECURITY_TOKEN);
    } catch (IOException e) {
      securityToken = null;
    }

    if (StringUtils.isEmpty(accessKeyId)
        || StringUtils.isEmpty(accessKeySecret)) {
      throw new InvalidCredentialsException(
          "AccessKeyId and AccessKeySecret should not be null or empty.");
    }

    if (StringUtils.isNotEmpty(securityToken)) {
      credentials = new DefaultCredentials(accessKeyId, accessKeySecret,
          securityToken);
    } else {
      credentials = new DefaultCredentials(accessKeyId, accessKeySecret);
    }
  }

  @Override
  public void setCredentials(Credentials creds) {
    if (creds == null) {
      throw new InvalidCredentialsException("Credentials should not be null.");
    }

    credentials = creds;
  }

  @Override
  public Credentials getCredentials() {
    if (credentials == null) {
      throw new InvalidCredentialsException("Invalid credentials");
    }

    return credentials;
  }
}
