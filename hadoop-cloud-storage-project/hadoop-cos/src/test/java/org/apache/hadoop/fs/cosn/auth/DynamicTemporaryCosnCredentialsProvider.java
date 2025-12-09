/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.cosn.auth;

import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.CosNConfigKeys;

import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.profile.ClientProfile;
import com.tencentcloudapi.common.profile.HttpProfile;
import com.tencentcloudapi.sts.v20180813.StsClient;
import com.tencentcloudapi.sts.v20180813.models.GetFederationTokenRequest;
import com.tencentcloudapi.sts.v20180813.models.GetFederationTokenResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A COSCredentialsProvider that generates temporary credentials from Tencent Cloud STS.
 * This provider requires a long-term secret ID and key with permission to call
 * the STS GetFederationToken action.
 */
public class DynamicTemporaryCosnCredentialsProvider implements COSCredentialsProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(DynamicTemporaryCosnCredentialsProvider.class);

  public static final String STS_SECRET_ID_KEY = "fs.cosn.auth.sts.secret.id";
  public static final String STS_SECRET_KEY_KEY = "fs.cosn.auth.sts.secret.key";
  public static final String STS_ENDPOINT_KEY = "fs.cosn.auth.sts.endpoint";
  public static final String DEFAULT_STS_ENDPOINT = "sts.tencentcloudapi.com";
  public static final String TOKEN_DURATION_SECONDS_KEY = "fs.cosn.auth.sts.token.duration.seconds";
  public static final int DEFAULT_TOKEN_DURATION_SECONDS = 900; // 15 minutes

  private final String longTermSecretId;
  private final String longTermSecretKey;
  private final String stsEndpoint;
  private final String region;
  private final String bucketName;
  private final long durationSeconds;

  private final AtomicReference<ExpiringCredentials> expiringCredentialsRef =
      new AtomicReference<>();

  public DynamicTemporaryCosnCredentialsProvider(Configuration conf) throws IOException {
    this.longTermSecretId = conf.get(STS_SECRET_ID_KEY);
    this.longTermSecretKey = conf.get(STS_SECRET_KEY_KEY);
    this.stsEndpoint = conf.get(STS_ENDPOINT_KEY, DEFAULT_STS_ENDPOINT);
    this.region = conf.get(CosNConfigKeys.COSN_REGION_KEY);
    this.bucketName = conf.get("fs.defaultFS").replace("cosn://", "");
    this.durationSeconds = conf.getLong(TOKEN_DURATION_SECONDS_KEY, DEFAULT_TOKEN_DURATION_SECONDS);

    if (this.longTermSecretId == null || this.longTermSecretKey == null) {
      throw new IOException(
          "Long-term STS credentials not provided in configuration. Please set " + STS_SECRET_ID_KEY
              + " and " + STS_SECRET_KEY_KEY);
    }
    if (this.region == null || this.bucketName == null) {
      throw new IOException("Bucket region or name not configured.");
    }
  }

  @Override
  public COSCredentials getCredentials() {
    ExpiringCredentials current = expiringCredentialsRef.get();
    // Refresh if credentials are not present, or are within 60 seconds of expiry.
    if (current == null
        || System.currentTimeMillis() >= current.getExpirationTimeMillis() - 60000) {
      LOG.info("STS credentials expired or not found, requesting new token.");
      refresh();
    }
    return expiringCredentialsRef.get().getCredentials();
  }

  @Override
  public void refresh() {
    try {
      Credential cred = new Credential(this.longTermSecretId, this.longTermSecretKey);
      HttpProfile httpProfile = new HttpProfile();
      httpProfile.setEndpoint(this.stsEndpoint);
      ClientProfile clientProfile = new ClientProfile();
      clientProfile.setHttpProfile(httpProfile);

      StsClient client = new StsClient(cred, this.region, clientProfile);
      GetFederationTokenRequest req = new GetFederationTokenRequest();

      String policyTemplate = "{\"version\":\"2.0\",\"statement\":[{\"action\":[\"cos:*\"],"
          + "\"effect\":\"allow\",\"resource\":[\"qcs::cos:%s:uid/%s:%s/*\"]}]}";
      String policy =
          String.format(policyTemplate, this.region, getAppIdFromBucket(this.bucketName),
              this.bucketName);
      req.setPolicy(policy);

      req.setDurationSeconds(this.durationSeconds);
      req.setName("HadoopCosNContractTest");

      GetFederationTokenResponse resp = client.GetFederationToken(req);

      long expirationTimeMillis = (resp.getExpiredTime() * 1000);
      BasicSessionCredentials credentials =
          new BasicSessionCredentials(resp.getCredentials().getTmpSecretId(),
              resp.getCredentials().getTmpSecretKey(), resp.getCredentials().getToken());

      expiringCredentialsRef.set(new ExpiringCredentials(credentials, expirationTimeMillis));
      LOG.info("Successfully refreshed STS credentials. Expiration: {}",
          new java.util.Date(expirationTimeMillis));

    } catch (Exception e) {
      LOG.error("Failed to get token from STS: {}", e.toString());
      throw new RuntimeException("Failed to get token from STS", e);
    }
  }

  private String getAppIdFromBucket(String bucket) {
    int lastDash = bucket.lastIndexOf('-');
    if (lastDash != -1 && lastDash < bucket.length() - 1) {
      return bucket.substring(lastDash + 1);
    }
    throw new IllegalArgumentException("Could not determine AppID from bucket name: " + bucket);
  }

  /**
   * Helper class to hold credentials and their expiration time.
   */
  private static class ExpiringCredentials {
    private final BasicSessionCredentials credentials;
    private final long expirationTimeMillis;

    ExpiringCredentials(BasicSessionCredentials credentials, long expirationTimeMillis) {
      this.credentials = credentials;
      this.expirationTimeMillis = expirationTimeMillis;
    }

    BasicSessionCredentials getCredentials() {
      return credentials;
    }

    long getExpirationTimeMillis() {
      return expirationTimeMillis;
    }
  }
}