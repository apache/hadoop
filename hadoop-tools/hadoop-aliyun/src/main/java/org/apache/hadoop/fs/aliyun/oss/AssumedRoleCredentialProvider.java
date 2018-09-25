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
import com.aliyun.oss.common.auth.InvalidCredentialsException;
import com.aliyun.oss.common.auth.STSAssumeRoleSessionCredentialsProvider;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_ID;
import static org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_SECRET;

/**
 * Support assumed role credentials for authenticating with Aliyun.
 * roleArn is configured in core-site.xml
 */
public class AssumedRoleCredentialProvider implements CredentialsProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(AssumedRoleCredentialProvider.class);
  public static final String NAME
      = "org.apache.hadoop.fs.aliyun.oss.AssumedRoleCredentialProvider";
  private Credentials credentials;
  private String roleArn;
  private long duration;
  private String stsEndpoint;
  private String sessionName;
  private double expiredFactor;
  private STSAssumeRoleSessionCredentialsProvider stsCredentialsProvider;

  public AssumedRoleCredentialProvider(URI uri, Configuration conf) {
    roleArn = conf.getTrimmed(Constants.ROLE_ARN, "");
    if (StringUtils.isEmpty(roleArn)) {
      throw new InvalidCredentialsException(
          "fs.oss.assumed.role.arn is empty");
    }

    duration = conf.getLong(Constants.ASSUMED_ROLE_DURATION,
        Constants.ASSUMED_ROLE_DURATION_DEFAULT);

    expiredFactor = conf.getDouble(Constants.ASSUMED_ROLE_STS_EXPIRED_FACTOR,
        Constants.ASSUMED_ROLE_STS_EXPIRED_FACTOR_DEFAULT);

    stsEndpoint = conf.getTrimmed(Constants.ASSUMED_ROLE_STS_ENDPOINT, "");
    if (StringUtils.isEmpty(stsEndpoint)) {
      throw new InvalidCredentialsException(
          "fs.oss.assumed.role.sts.endpoint is empty");
    }

    sessionName = conf.getTrimmed(Constants.ASSUMED_ROLE_SESSION_NAME, "");

    String accessKeyId;
    String accessKeySecret;
    try {
      accessKeyId = AliyunOSSUtils.getValueWithKey(conf, ACCESS_KEY_ID);
      accessKeySecret = AliyunOSSUtils.getValueWithKey(conf, ACCESS_KEY_SECRET);
    } catch (IOException e) {
      throw new InvalidCredentialsException(e);
    }

    try {
      DefaultProfile.addEndpoint("", "", "Sts", stsEndpoint);
    } catch (ClientException e) {
      throw new InvalidCredentialsException(e);
    }

    stsCredentialsProvider = new STSAssumeRoleSessionCredentialsProvider(
        new com.aliyuncs.auth.BasicCredentials(accessKeyId, accessKeySecret),
        roleArn, DefaultProfile.getProfile("", accessKeyId, accessKeySecret))
            .withExpiredDuration(duration).withExpiredFactor(expiredFactor);

    if (!StringUtils.isEmpty(sessionName)) {
      stsCredentialsProvider.withRoleSessionName(sessionName);
    }
  }

  @Override
  public void setCredentials(Credentials creds) {
    throw new InvalidCredentialsException(
        "Should not set credentials from external call");
  }

  @Override
  public Credentials getCredentials() {
    credentials = stsCredentialsProvider.getCredentials();
    if (credentials == null) {
      throw new InvalidCredentialsException("Invalid credentials");
    }
    return credentials;
  }
}
