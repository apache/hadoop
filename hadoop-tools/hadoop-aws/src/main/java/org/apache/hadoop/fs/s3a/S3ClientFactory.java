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

package org.apache.hadoop.fs.s3a;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.monitoring.MonitoringListener;
import com.amazonaws.services.s3.AmazonS3;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.s3a.statistics.StatisticsFromAwsSdk;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_ENDPOINT;

/**
 * Factory for creation of {@link AmazonS3} client instances.
 * Important: HBase's HBoss module implements this interface in its
 * tests.
 * Take care when updating this interface to ensure that a client
 * implementing only the deprecated method will work.
 * See https://github.com/apache/hbase-filesystem
 *
 */
@InterfaceAudience.LimitedPrivate("HBoss")
@InterfaceStability.Evolving
public interface S3ClientFactory {

  /**
   * Creates a new {@link AmazonS3} client.
   *
   * @param uri S3A file system URI
   * @param parameters parameter object
   * @return S3 client
   * @throws IOException IO problem
   */
  AmazonS3 createS3Client(URI uri,
      S3ClientCreationParameters parameters) throws IOException;

  /**
   * Settings for the S3 Client.
   * Implemented as a class to pass in so that adding
   * new parameters does not break the binding of
   * external implementations of the factory.
   */
  final class S3ClientCreationParameters {

    /**
     * Credentials.
     */
    private AWSCredentialsProvider credentialSet;

    /**
     * Endpoint.
     */
    private String endpoint = DEFAULT_ENDPOINT;

    /**
     * Custom Headers.
     */
    private final Map<String, String> headers = new HashMap<>();

    /**
     * Monitoring listener.
     */
    private MonitoringListener monitoringListener;

    /**
     * RequestMetricCollector metrics...if not-null will be wrapped
     * with an {@code AwsStatisticsCollector} and passed to
     * the client.
     */
    private StatisticsFromAwsSdk metrics;

    /**
     * Use (deprecated) path style access.
     */
    private boolean pathStyleAccess;

    /**
     * This is in the settings awaiting wiring up and testing.
     */
    private boolean requesterPays;

    /**
     * Request handlers; used for auditing, X-Ray etc.
     */
    private List<RequestHandler2> requestHandlers;

    /**
     * Suffix to UA.
     */
    private String userAgentSuffix = "";

    public List<RequestHandler2> getRequestHandlers() {
      return requestHandlers;
    }

    /**
     * List of request handlers.
     * @param handlers handler list.
     * @return this object
     */
    public S3ClientCreationParameters withRequestHandlers(
        @Nullable final List<RequestHandler2> handlers) {
      requestHandlers = handlers;
      return this;
    }

    public MonitoringListener getMonitoringListener() {
      return monitoringListener;
    }

    /**
     * listener for AWS monitoring events.
     * @param listener listener
     * @return this object
     */
    public S3ClientCreationParameters withMonitoringListener(
        @Nullable final MonitoringListener listener) {
      monitoringListener = listener;
      return this;
    }

    public StatisticsFromAwsSdk getMetrics() {
      return metrics;
    }

    /**
     * Metrics binding. This is the S3A-level
     * statistics interface, which will be wired
     * up to the AWS callbacks.
     * @param statistics statistics implementation
     * @return this object
     */
    public S3ClientCreationParameters withMetrics(
        @Nullable final StatisticsFromAwsSdk statistics) {
      metrics = statistics;
      return this;
    }

    /**
     * Requester pays option. Not yet wired up.
     * @param value new value
     * @return the builder
     */
    public S3ClientCreationParameters withRequesterPays(
        final boolean value) {
      requesterPays = value;
      return this;
    }

    public boolean isRequesterPays() {
      return requesterPays;
    }

    public AWSCredentialsProvider getCredentialSet() {
      return credentialSet;
    }

    /**
     * Set credentials.
     * @param value new value
     * @return the builder
     */

    public S3ClientCreationParameters withCredentialSet(
        final AWSCredentialsProvider value) {
      credentialSet = value;
      return this;
    }

    public String getUserAgentSuffix() {
      return userAgentSuffix;
    }

    /**
     * Set UA suffix.
     * @param value new value
     * @return the builder
     */

    public S3ClientCreationParameters withUserAgentSuffix(
        final String value) {
      userAgentSuffix = value;
      return this;
    }

    public String getEndpoint() {
      return endpoint;
    }

    /**
     * Set endpoint.
     * @param value new value
     * @return the builder
     */

    public S3ClientCreationParameters withEndpoint(
        final String value) {
      endpoint = value;
      return this;
    }

    public boolean isPathStyleAccess() {
      return pathStyleAccess;
    }

    /**
     * Set path access option.
     * @param value new value
     * @return the builder
     */
    public S3ClientCreationParameters withPathStyleAccess(
        final boolean value) {
      pathStyleAccess = value;
      return this;
    }

    /**
     * Add a custom header.
     * @param header header name
     * @param value new value
     * @return the builder
     */
    public S3ClientCreationParameters withHeader(
        String header, String value) {
      headers.put(header, value);
      return this;
    }

    /**
     * Get the map of headers.
     * @return (mutable) header map
     */
    public Map<String, String> getHeaders() {
      return headers;
    }
  }
}
