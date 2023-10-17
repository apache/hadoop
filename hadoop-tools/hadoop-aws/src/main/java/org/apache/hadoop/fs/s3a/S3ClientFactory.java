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
import java.util.concurrent.Executor;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.s3a.statistics.StatisticsFromAwsSdk;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_ENDPOINT;

/**
 * Factory for creation of {@link S3Client} client instances.
 * Important: HBase's HBoss module implements this interface in its
 * tests.
 * Take care when updating this interface to ensure that a client
 * implementing only the deprecated method will work.
 * See https://github.com/apache/hbase-filesystem
 *
 * @deprecated This interface will be replaced by one which uses the AWS SDK V2 S3 client as part of
 * upgrading S3A to SDK V2. See HADOOP-18073.
 */
@InterfaceAudience.LimitedPrivate("HBoss")
@InterfaceStability.Evolving
public interface S3ClientFactory {

  /**
   * Creates a new {@link S3Client}.
   * The client returned supports synchronous operations. For
   * asynchronous operations, use
   * {@link #createS3AsyncClient(URI, S3ClientCreationParameters)}.
   *
   * @param uri S3A file system URI
   * @param parameters parameter object
   * @return S3 client
   * @throws IOException on any IO problem
   */
  S3Client createS3Client(URI uri,
      S3ClientCreationParameters parameters) throws IOException;

  /**
   * Creates a new {@link S3AsyncClient}.
   * The client returned supports asynchronous operations. For
   * synchronous operations, use
   * {@link #createS3Client(URI, S3ClientCreationParameters)}.
   *
   * @param uri S3A file system URI
   * @param parameters parameter object
   * @return Async S3 client
   * @throws IOException on any IO problem
   */
  S3AsyncClient createS3AsyncClient(URI uri,
      S3ClientCreationParameters parameters) throws IOException;

  /**
   * Creates a new {@link S3TransferManager}.
   *
   * @param s3AsyncClient the async client to be used by the TM.
   * @return S3 transfer manager
   */
  S3TransferManager createS3TransferManager(S3AsyncClient s3AsyncClient);

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
    private AwsCredentialsProvider credentialSet;

    /**
     * Endpoint.
     */
    private String endpoint = DEFAULT_ENDPOINT;

    /**
     * Custom Headers.
     */
    private final Map<String, String> headers = new HashMap<>();

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
     * Permit requests to requester pays buckets.
     */
    private boolean requesterPays;

    /**
     * Execution interceptors; used for auditing, X-Ray etc.
     * */
    private List<ExecutionInterceptor> executionInterceptors;

    /**
     * Suffix to UA.
     */
    private String userAgentSuffix = "";

    /**
     * S3A path.
     * added in HADOOP-18330
     */
    private URI pathUri;

    /**
     * Minimum part size for transfer parts.
     */
    private long minimumPartSize;

    /**
     * Threshold for multipart operations.
     */
    private long multiPartThreshold;

    /**
     * Multipart upload enabled.
     */
    private boolean multipartCopy = true;

    /**
     * Executor that the transfer manager will use to execute background tasks.
     */
    private Executor transferManagerExecutor;

    /**
     * Region of the S3 bucket.
     */
    private String region;


    /**
     * List of execution interceptors to include in the chain
     * of interceptors in the SDK.
     * @return the interceptors list
     */
    public List<ExecutionInterceptor> getExecutionInterceptors() {
      return executionInterceptors;
    }

    /**
     * List of execution interceptors.
     * @param interceptors interceptors list.
     * @return this object
     */
    public S3ClientCreationParameters withExecutionInterceptors(
        @Nullable final List<ExecutionInterceptor> interceptors) {
      executionInterceptors = interceptors;
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
     * Set requester pays option.
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

    public AwsCredentialsProvider getCredentialSet() {
      return credentialSet;
    }

    /**
     * Set credentials.
     * @param value new value
     * @return the builder
     */

    public S3ClientCreationParameters withCredentialSet(
        final AwsCredentialsProvider value) {
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

    /**
     * Get the full s3 path.
     * added in HADOOP-18330
     * @return path URI
     */
    public URI getPathUri() {
      return pathUri;
    }

    /**
     * Set full s3a path.
     * added in HADOOP-18330
     * @param value new value
     * @return the builder
     */
    public S3ClientCreationParameters withPathUri(
        final URI value) {
      pathUri = value;
      return this;
    }

    /**
     * Get the minimum part size for transfer parts.
     * @return part size
     */
    public long getMinimumPartSize() {
      return minimumPartSize;
    }

    /**
     * Set the minimum part size for transfer parts.
     * @param value new value
     * @return the builder
     */
    public S3ClientCreationParameters withMinimumPartSize(
        final long value) {
      minimumPartSize = value;
      return this;
    }

    /**
     * Get the threshold for multipart operations.
     * @return multipart threshold
     */
    public long getMultiPartThreshold() {
      return multiPartThreshold;
    }

    /**
     * Set the threshold for multipart operations.
     * @param value new value
     * @return the builder
     */
    public S3ClientCreationParameters withMultipartThreshold(
        final long value) {
      multiPartThreshold = value;
      return this;
    }

    /**
     * Get the executor that the transfer manager will use to execute background tasks.
     * @return part size
     */
    public Executor getTransferManagerExecutor() {
      return transferManagerExecutor;
    }

    /**
     * Set the executor that the transfer manager will use to execute background tasks.
     * @param value new value
     * @return the builder
     */
    public S3ClientCreationParameters withTransferManagerExecutor(
        final Executor value) {
      transferManagerExecutor = value;
      return this;
    }

    /**
     * Set the multipart flag..
     *
     * @param value new value
     * @return the builder
     */
    public S3ClientCreationParameters withMultipartCopyEnabled(final boolean value) {
      this.multipartCopy = value;
      return this;
    }

    /**
     * Get the multipart flag.
     * @return multipart flag
     */
    public boolean isMultipartCopy() {
      return multipartCopy;
    }

    /**
     * Set region.
     *
     * @param value new value
     * @return the builder
     */
    public S3ClientCreationParameters withRegion(
        final String value) {
      region = value;
      return this;
    }

    /**
     * Get the region.
     * @return invoker
     */
    public String getRegion() {
      return region;
    }
  }
}
