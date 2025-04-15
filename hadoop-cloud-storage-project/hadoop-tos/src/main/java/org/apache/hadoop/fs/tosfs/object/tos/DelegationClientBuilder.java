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

package org.apache.hadoop.fs.tosfs.object.tos;

import com.volcengine.tos.TOSClientConfiguration;
import com.volcengine.tos.TosException;
import com.volcengine.tos.transport.TransportConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;
import org.apache.hadoop.fs.tosfs.object.Constants;
import org.apache.hadoop.fs.tosfs.object.tos.auth.CredentialsProvider;
import org.apache.hadoop.fs.tosfs.util.ParseUtils;
import org.apache.hadoop.fs.tosfs.util.TOSClientContextUtils;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.VersionInfo;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.fs.tosfs.object.tos.TOS.TOS_SCHEME;

public class DelegationClientBuilder {

  public static final int DISABLE_TOS_RETRY_VALUE = -1;
  private static final String TOS_ENDPOINT_KEY =
      ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key(TOS_SCHEME);
  private static final String TOS_REGION_KEY = ConfKeys.FS_OBJECT_STORAGE_REGION.key(TOS_SCHEME);

  @VisibleForTesting
  static final Map<String, DelegationClient> CACHE = new ConcurrentHashMap<>();

  private String bucket;
  private Configuration conf;

  public DelegationClientBuilder bucket(String bucketInput) {
    this.bucket = bucketInput;
    return this;
  }

  public DelegationClientBuilder conf(Configuration confInput) {
    this.conf = confInput;
    return this;
  }

  public DelegationClient build() throws TosException {
    Preconditions.checkNotNull(bucket, "Bucket cannot be null");
    Preconditions.checkNotNull(conf, "Conf cannot be null");
    String endpoint = getAndCheckEndpoint(conf);
    String region = getAndCheckRegion(conf, endpoint);

    if (conf.getBoolean(TosKeys.FS_TOS_DISABLE_CLIENT_CACHE,
        TosKeys.FS_TOS_DISABLE_CLIENT_CACHE_DEFAULT)) {
      return createNewClient(conf, endpoint, region, bucket, false);
    }
    return CACHE.computeIfAbsent(bucket,
        client -> createNewClient(conf, endpoint, region, bucket, true));
  }

  private DelegationClient createNewClient(Configuration config, String endpoint, String region,
      String bucketName, boolean cached) {
    CredentialsProvider provider = createProvider(config, bucketName);
    TOSClientConfiguration clientConfiguration = TOSClientConfiguration.builder()
        .region(region)
        .endpoint(endpoint)
        .credentials(provider)
        .enableCrc(config.getBoolean(
            TosKeys.FS_TOS_CRC_CHECK_ENABLED, TosKeys.FS_TOS_CRC_CHECK_ENABLED_DEFAULT))
        .transportConfig(createTransportConfig(config))
        .userAgentProductName(config.get(
            TosKeys.FS_TOS_USER_AGENT_PREFIX, TosKeys.FS_TOS_USER_AGENT_PREFIX_DEFAULT))
        .userAgentSoftName(Constants.TOS_FS)
        .userAgentSoftVersion(VersionInfo.getVersion())
        .build();

    int maxRetryTimes = config.getInt(TosKeys.FS_TOS_REQUEST_MAX_RETRY_TIMES,
        TosKeys.FS_TOS_REQUEST_MAX_RETRY_TIMES_DEFAULT);
    List<String> nonRetryable409ErrorCodes = Arrays.asList(
        config.getTrimmedStrings(TosKeys.FS_TOS_FAST_FAILURE_409_ERROR_CODES,
            TosKeys.FS_TOS_FAST_FAILURE_409_ERROR_CODES_DEFAULT));

    if (cached) {
      return new CachedClient(clientConfiguration, maxRetryTimes, nonRetryable409ErrorCodes);
    } else {
      return new DelegationClient(clientConfiguration, maxRetryTimes, nonRetryable409ErrorCodes);
    }
  }

  private CredentialsProvider createProvider(Configuration config, String bucketName) {
    try {
      CredentialsProvider provider = (CredentialsProvider) Class.forName(
              config.get(TosKeys.FS_TOS_CREDENTIALS_PROVIDER,
                  TosKeys.FS_TOS_CREDENTIALS_PROVIDER_DEFAULT))
              .getDeclaredConstructor()
              .newInstance();
      provider.initialize(config, bucketName);
      return provider;
    } catch (ClassNotFoundException |
             InstantiationException |
             IllegalAccessException |
             InvocationTargetException |
             NoSuchMethodException e) {
      throw new TosException(e);
    }
  }

  private String getAndCheckEndpoint(Configuration config) {
    String endpoint = config.get(TOS_ENDPOINT_KEY);
    if (StringUtils.isBlank(endpoint)) {
      endpoint = ParseUtils.envAsString(TOS.ENV_TOS_ENDPOINT);
    }
    Preconditions.checkNotNull(endpoint, "%s cannot be null", TOS_ENDPOINT_KEY);
    return endpoint.trim();
  }

  private String getAndCheckRegion(Configuration config, String endpoint) {
    String region = config.get(TOS_REGION_KEY);
    if (StringUtils.isNotBlank(region)) {
      return region.trim();
    }
    region = TOSClientContextUtils.parseRegion(endpoint);
    Preconditions.checkNotNull(region, "%s cannot be null", TOS_REGION_KEY);
    return region.trim();
  }

  private TransportConfig createTransportConfig(Configuration config) {
    TransportConfig.TransportConfigBuilder builder = TransportConfig.builder();
    // Disable tos sdk retry with negative number since we have set retry strategy above TOS SDK,
    // which cannot support retry all input streams via mark & reset API.
    // It's hard to use it as there are some restrictions.
    // the TOS SDK will reset the max retry count with 3 if the configured count equal to 0.
    builder.maxRetryCount(DISABLE_TOS_RETRY_VALUE);

    builder.maxConnections(config.getInt(TosKeys.FS_TOS_HTTP_MAX_CONNECTIONS,
        TosKeys.FS_TOS_HTTP_MAX_CONNECTIONS_DEFAULT));
    builder.idleConnectionTimeMills(config.getInt(TosKeys.FS_TOS_HTTP_IDLE_CONNECTION_TIME_MILLS,
        TosKeys.FS_TOS_HTTP_IDLE_CONNECTION_TIME_MILLS_DEFAULT));
    builder.connectTimeoutMills(config.getInt(TosKeys.FS_TOS_HTTP_CONNECT_TIMEOUT_MILLS,
        TosKeys.FS_TOS_HTTP_CONNECT_TIMEOUT_MILLS_DEFAULT));
    builder.readTimeoutMills(config.getInt(TosKeys.FS_TOS_HTTP_READ_TIMEOUT_MILLS,
        TosKeys.FS_TOS_HTTP_READ_TIMEOUT_MILLS_DEFAULT));
    builder.writeTimeoutMills(config.getInt(TosKeys.FS_TOS_HTTP_WRITE_TIMEOUT_MILLS,
        TosKeys.FS_TOS_HTTP_WRITE_TIMEOUT_MILLS_DEFAULT));
    builder.enableVerifySSL(config.getBoolean(TosKeys.FS_TOS_HTTP_ENABLE_VERIFY_SSL,
        TosKeys.FS_TOS_HTTP_ENABLE_VERIFY_SSL_DEFAULT));
    builder.dnsCacheTimeMinutes(config.getInt(TosKeys.FS_TOS_HTTP_DNS_CACHE_TIME_MINUTES,
        TosKeys.FS_TOS_HTTP_DNS_CACHE_TIME_MINUTES_DEFAULT));

    return builder.build();
  }

  static class CachedClient extends DelegationClient {

    protected CachedClient(TOSClientConfiguration configuration, int maxRetryTimes,
        List<String> nonRetryable409ErrorCodes) {
      super(configuration, maxRetryTimes, nonRetryable409ErrorCodes);
    }

    @Override
    public void close() {
      // do nothing as this client may be shared by multiple upper-layer instances
    }
  }
}
