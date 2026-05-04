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

package org.apache.hadoop.fs.azurebfs.constants;

import org.apache.hadoop.fs.azurebfs.utils.MetricFormat;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.OpenFileOptions;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DOT;

/**
 * Responsible to keep all the Azure Blob File System configurations keys in Hadoop configuration file.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ConfigurationKeys {

  /**
   * Config to specify if the configured account is HNS enabled or not. If
   * this config is not set, getacl call is made on account filesystem root
   * path on DFS Endpoint to determine HNS status.
   */
  public static final String FS_AZURE_ACCOUNT_IS_HNS_ENABLED = "fs.azure.account.hns.enabled";

  /**
   * Config to specify which {@link  AbfsServiceType} to use with HNS-Disabled Account type.
   * Default value will be identified from URL used to initialize filesystem.
   * This will allow an override to choose service endpoint in cases where any
   * local DNS resolution is set for account and driver is unable to detect
   * intended endpoint from the url used to initialize filesystem.
   * If configured Blob for HNS-Enabled account, FS init will fail.
   * Value {@value} case-insensitive "DFS" or "BLOB"
   */
  public static final String FS_AZURE_FNS_ACCOUNT_SERVICE_TYPE = "fs.azure.fns.account.service.type";

  /**
   * Config to specify which {@link AbfsServiceType} to use only for Ingress Operations.
   * Other operations will continue to move to the FS configured service endpoint.
   * Value {@value} case-insensitive "DFS" or "BLOB"
   */
  public static final String FS_AZURE_INGRESS_SERVICE_TYPE = "fs.azure.ingress.service.type";

  /**
   * Config to be set only for cases where traffic over dfs endpoint is
   * experiencing compatibility issues and need to move to blob for mitigation.
   * Value {@value} case-insensitive "True" or "False"
   */
  public static final String FS_AZURE_ENABLE_DFSTOBLOB_FALLBACK = "fs.azure.enable.dfstoblob.fallback";

  /**
   * Enable or disable expect hundred continue header.
   * Value: {@value}.
   */
  public static final String FS_AZURE_ACCOUNT_IS_EXPECT_HEADER_ENABLED = "fs.azure.account.expect.header.enabled";
  public static final String FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME = "fs.azure.account.key";

  /**
   * Config to set separate metrics account in case user don't want to use
   * existing storage account for metrics collection.
   * Value: {@value}.
   */
  public static final String FS_AZURE_METRICS_ACCOUNT_NAME = "fs.azure.metrics.account.name";
  /**
   * Config to set metrics account key for @FS_AZURE_METRICS_ACCOUNT_NAME.
   * Value: {@value}.
   */
  public static final String FS_AZURE_METRICS_ACCOUNT_KEY = "fs.azure.metrics.account.key";
  /**
   * Config to set metrics format. Possible values are {@link MetricFormat}
   * Value: {@value}.
   */
  public static final String FS_AZURE_METRICS_FORMAT = "fs.azure.metrics.format";
  /**
   * Config to enable or disable metrics collection.
   * Value: {@value}.
   */
  public static final String FS_AZURE_METRICS_COLLECTION_ENABLED = "fs.azure.metrics.collection.enabled";
  /**
   * Config to enable or disable emitting metrics when idle time exceeds threshold.
   * Value: {@value}.
   */
  public static final String FS_AZURE_METRICS_SHOULD_EMIT_ON_IDLE_TIME = "fs.azure.metrics.should.emit.on.idle.time";
  /**
   * Config to set threshold for emitting metrics when number of operations exceeds threshold.
   * Value: {@value}.
   */
  public static final String FS_AZURE_METRICS_EMIT_THRESHOLD = "fs.azure.metrics.emit.threshold";
  /**
   * Config to set interval in seconds to check for threshold breach for emitting metrics.
   * If the number of operations exceed threshold within this interval, metrics will be emitted.
   * Value: {@value}.
   */
  public static final String FS_AZURE_METRICS_EMIT_THRESHOLD_INTERVAL_SECS = "fs.azure.metrics.emit.threshold.interval.secs";
  /**
   * Config to set interval in minutes for emitting metrics in regular time intervals.
   * Value: {@value}.
   */
  public static final String FS_AZURE_METRICS_EMIT_INTERVAL_MINS = "fs.azure.metrics.emit.interval.mins";
  /**
   * Config to set maximum metrics calls per second.
   * Value: {@value}.
   */
  public static final String FS_AZURE_METRICS_MAX_CALLS_PER_SECOND =  "fs.azure.metrics.max.calls.per.second";
  /**
   * Config to enable or disable backoff retry metrics collection.
   * Value: {@value}.
   */
  public static final String FS_AZURE_METRICS_BACKOFF_RETRY_ENABLED = "fs.azure.metrics.backoff.retry.enabled";

  public static final String FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME_REGX = "fs\\.azure\\.account\\.key\\.(.*)";
  public static final String FS_AZURE_SECURE_MODE = "fs.azure.secure.mode";
  public static final String FS_AZURE_ACCOUNT_LEVEL_THROTTLING_ENABLED = "fs.azure.account.throttling.enabled";

  // Retry strategy defined by the user
  public static final String AZURE_MIN_BACKOFF_INTERVAL = "fs.azure.io.retry.min.backoff.interval";
  public static final String AZURE_MAX_BACKOFF_INTERVAL = "fs.azure.io.retry.max.backoff.interval";
  public static final String AZURE_STATIC_RETRY_FOR_CONNECTION_TIMEOUT_ENABLED = "fs.azure.static.retry.for.connection.timeout.enabled";
  public static final String AZURE_STATIC_RETRY_INTERVAL = "fs.azure.static.retry.interval";
  public static final String AZURE_BACKOFF_INTERVAL = "fs.azure.io.retry.backoff.interval";
  public static final String AZURE_MAX_IO_RETRIES = "fs.azure.io.retry.max.retries";
  public static final String AZURE_CUSTOM_TOKEN_FETCH_RETRY_COUNT = "fs.azure.custom.token.fetch.retry.count";

  /**
   * Config to set HTTP Connection Timeout Value for Rest Operations.
   * Value: {@value}.
   */
  public static final String AZURE_HTTP_CONNECTION_TIMEOUT = "fs.azure.http.connection.timeout";
  /**
   * Config to set HTTP Read Timeout Value for Rest Operations.
   * Value: {@value}.
   */
  public static final String AZURE_HTTP_READ_TIMEOUT = "fs.azure.http.read.timeout";

  /**
   * Config to set HTTP Expect100-Continue Wait Timeout Value for Rest Operations.
   * Value: {@value}.
   */
  public static final String AZURE_EXPECT_100CONTINUE_WAIT_TIMEOUT
      = "fs.azure.http.expect.100continue.wait.timeout";

  //  Retry strategy for getToken calls
  public static final String AZURE_OAUTH_TOKEN_FETCH_RETRY_COUNT = "fs.azure.oauth.token.fetch.retry.max.retries";
  public static final String AZURE_OAUTH_TOKEN_FETCH_RETRY_MIN_BACKOFF = "fs.azure.oauth.token.fetch.retry.min.backoff.interval";
  public static final String AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_BACKOFF = "fs.azure.oauth.token.fetch.retry.max.backoff.interval";
  public static final String AZURE_OAUTH_TOKEN_FETCH_RETRY_DELTA_BACKOFF = "fs.azure.oauth.token.fetch.retry.delta.backoff";

  // Read and write buffer sizes defined by the user
  public static final String AZURE_WRITE_MAX_CONCURRENT_REQUESTS = "fs.azure.write.max.concurrent.requests";
  public static final String AZURE_WRITE_MAX_REQUESTS_TO_QUEUE = "fs.azure.write.max.requests.to.queue";
  public static final String AZURE_WRITE_BUFFER_SIZE = "fs.azure.write.request.size";

  /**
   * Maximum Number of blocks a single output stream can have
   * active (uploading, or queued to the central FileSystem
   * instance's pool of queued operations.
   * This stops a single stream overloading the shared thread pool.
   * {@value}
   * <p>
   * Default is {@link FileSystemConfigurations#BLOCK_UPLOAD_ACTIVE_BLOCKS_DEFAULT}
   */
  public static final String FS_AZURE_BLOCK_UPLOAD_ACTIVE_BLOCKS =
      "fs.azure.block.upload.active.blocks";

  /**
   * Buffer directory path for uploading AbfsOutputStream data blocks.
   * Value: {@value}
   */
  public static final String FS_AZURE_BLOCK_UPLOAD_BUFFER_DIR =
      "fs.azure.buffer.dir";

  /**
   * What data block buffer to use.
   * <br>
   * Options include: "disk", "array", and "bytebuffer"(Default).
   * <br>
   * Default is {@link FileSystemConfigurations#DATA_BLOCKS_BUFFER_DEFAULT}.
   * Value: {@value}
   */
  public static final String DATA_BLOCKS_BUFFER =
      "fs.azure.data.blocks.buffer";

  /** If the data size written by Hadoop app is small, i.e. data size :
   *  (a) before any of HFlush/HSync call is made or
   *  (b) between 2 HFlush/Hsync API calls
   *  is less than write buffer size, 2 separate calls, one for append and
   *  another for flush are made.
   *  By enabling the small write optimization, a single call will be made to
   *  perform both append and flush operations and hence reduce request count.
   */
  public static final String AZURE_ENABLE_SMALL_WRITE_OPTIMIZATION = "fs.azure.write.enableappendwithflush";
  public static final String AZURE_READ_BUFFER_SIZE = "fs.azure.read.request.size";
  public static final String AZURE_READ_SMALL_FILES_COMPLETELY = "fs.azure.read.smallfilescompletely";
  /**
   * When parquet files are read, first few read are metadata reads before
   * reading the actual data. First the read is done of last 8 bytes of parquet
   * file to get the postion of metadta and next read is done for reading that
   * metadata. With this optimization these two reads can be combined into 1.
   * Value: {@value}
   */
  public static final String AZURE_READ_OPTIMIZE_FOOTER_READ = "fs.azure.read.optimizefooterread";
  /**
   * In case of footer reads it was not required to read full buffer size.
   * Most of the metadata information required was within 256 KB and it will be
   * more performant to read less. 512 KB is a sweet spot.
   * This config is used to define how much footer length the user wants to read.
   * Value: {@value}
   */
  public static final String AZURE_FOOTER_READ_BUFFER_SIZE = "fs.azure.footer.read.request.size";

  /**
   * Read ahead range parameter which can be set by user.
   * Default value is {@link FileSystemConfigurations#DEFAULT_READ_AHEAD_RANGE}.
   * This might reduce number of calls to remote as next requested
   * data could already be present in buffer {@value}.
   */
  public static final String AZURE_READ_AHEAD_RANGE = "fs.azure.readahead.range";
  public static final String AZURE_BLOCK_SIZE_PROPERTY_NAME = "fs.azure.block.size";
  public static final String AZURE_BLOCK_LOCATION_HOST_PROPERTY_NAME = "fs.azure.block.location.impersonatedhost";
  public static final String AZURE_CONCURRENT_CONNECTION_VALUE_OUT = "fs.azure.concurrentRequestCount.out";
  public static final String AZURE_CONCURRENT_CONNECTION_VALUE_IN = "fs.azure.concurrentRequestCount.in";
  public static final String AZURE_TOLERATE_CONCURRENT_APPEND = "fs.azure.io.read.tolerate.concurrent.append";
  public static final String AZURE_LIST_MAX_RESULTS = "fs.azure.list.max.results";
  public static final String AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION = "fs.azure.createRemoteFileSystemDuringInitialization";
  public static final String AZURE_SKIP_USER_GROUP_METADATA_DURING_INITIALIZATION = "fs.azure.skipUserGroupMetadataDuringInitialization";
  public static final String FS_AZURE_ENABLE_AUTOTHROTTLING = "fs.azure.enable.autothrottling";
  public static final String FS_AZURE_METRIC_IDLE_TIMEOUT = "fs.azure.metric.idle.timeout";
  public static final String FS_AZURE_METRIC_ANALYSIS_TIMEOUT = "fs.azure.metric.analysis.timeout";
  public static final String FS_AZURE_ACCOUNT_OPERATION_IDLE_TIMEOUT = "fs.azure.account.operation.idle.timeout";
  public static final String FS_AZURE_ANALYSIS_PERIOD = "fs.azure.analysis.period";
  public static final String FS_AZURE_ALWAYS_USE_HTTPS = "fs.azure.always.use.https";
  public static final String FS_AZURE_ATOMIC_RENAME_KEY = "fs.azure.atomic.rename.key";
  /** This config ensures that during create overwrite an existing file will be
   *  overwritten only if there is a match on the eTag of existing file.
   */
  public static final String FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE = "fs.azure.enable.conditional.create.overwrite";
  public static final String FS_AZURE_ENABLE_MKDIR_OVERWRITE = "fs.azure.enable.mkdir.overwrite";
  /** Provides a config to provide comma separated path prefixes on which Appendblob based files are created
   *  Default is empty. **/
  public static final String FS_AZURE_APPEND_BLOB_KEY = "fs.azure.appendblob.directories";
  /** Provides a config to provide comma separated path prefixes which support infinite leases.
   *  Files under these paths will be leased when created or opened for writing and the lease will
   *  be released when the file is closed. The lease may be broken with the breakLease method on
   *  AzureBlobFileSystem. Default is empty.
   * **/
  public static final String FS_AZURE_INFINITE_LEASE_KEY = "fs.azure.infinite-lease.directories";
  /** Provides a number of threads to use for lease operations for infinite lease directories.
   *  Must be set to a minimum of 1 if infinite lease directories are to be used. Default is 0. **/
  public static final String FS_AZURE_LEASE_THREADS = "fs.azure.lease.threads";
  public static final String FS_AZURE_READ_AHEAD_QUEUE_DEPTH = "fs.azure.readaheadqueue.depth";
  public static final String FS_AZURE_ALWAYS_READ_BUFFER_SIZE = "fs.azure.read.alwaysReadBufferSize";
  public static final String FS_AZURE_READ_AHEAD_BLOCK_SIZE = "fs.azure.read.readahead.blocksize";
  /**
   * Provides hint for the read workload pattern.
   * Possible Values Exposed in {@link OpenFileOptions#FS_OPTION_OPENFILE_READ_POLICIES}
   */
  public static final String FS_AZURE_READ_POLICY = "fs.azure.read.policy";

  /** Provides a config control to enable or disable ABFS Flush operations -
   *  HFlush and HSync. Default is true. **/
  public static final String FS_AZURE_ENABLE_FLUSH = "fs.azure.enable.flush";
  /** Provides a config control to disable or enable OutputStream Flush API
   *  operations in AbfsOutputStream. Flush() will trigger actions that
   *  guarantee that buffered data is persistent with a perf cost while the API
   *  documentation does not have such expectations of data being persisted.
   *  Default value of this config is true. **/
  public static final String FS_AZURE_DISABLE_OUTPUTSTREAM_FLUSH = "fs.azure.disable.outputstream.flush";
  public static final String FS_AZURE_USER_AGENT_PREFIX_KEY = "fs.azure.user.agent.prefix";
  /**
   * The client correlation ID provided over config that will be added to
   * x-ms-client-request-Id header. Defaults to empty string if the length and
   * character constraints are not satisfied. **/
  public static final String FS_AZURE_CLIENT_CORRELATIONID = "fs.azure.client.correlationid";
  public static final String FS_AZURE_TRACINGHEADER_FORMAT = "fs.azure.tracingheader.format";
  public static final String FS_AZURE_CLUSTER_NAME = "fs.azure.cluster.name";
  public static final String FS_AZURE_CLUSTER_TYPE = "fs.azure.cluster.type";
  public static final String FS_AZURE_SSL_CHANNEL_MODE_KEY = "fs.azure.ssl.channel.mode";
  /** Provides a config to enable/disable the checkAccess API.
   *  By default this will be
   *  FileSystemConfigurations.DEFAULT_ENABLE_CHECK_ACCESS. **/
  public static final String FS_AZURE_ENABLE_CHECK_ACCESS = "fs.azure.enable.check.access";
  public static final String FS_AZURE_USE_UPN = "fs.azure.use.upn";
  /** User principal names (UPNs) have the format “{alias}@{domain}”. If true,
   *  only {alias} is included when a UPN would otherwise appear in the output
   *  of APIs like getFileStatus, getOwner, getAclStatus, etc. Default is false. **/
  public static final String FS_AZURE_FILE_OWNER_ENABLE_SHORTNAME = "fs.azure.identity.transformer.enable.short.name";
  /** If the domain name is specified and “fs.azure.identity.transformer.enable.short.name”
   *  is true, then the {alias} part of a UPN can be specified as input to APIs like setOwner and
   *  setAcl and it will be transformed to a UPN by appending @ and the domain specified by
   *  this configuration property. **/
  public static final String FS_AZURE_FILE_OWNER_DOMAINNAME = "fs.azure.identity.transformer.domain.name";
  /** An Azure Active Directory object ID (oid) used as the replacement for names contained in the
   * list specified by “fs.azure.identity.transformer.service.principal.substitution.list.
   * Notice that instead of setting oid, you can also set $superuser.**/
  public static final String FS_AZURE_OVERRIDE_OWNER_SP = "fs.azure.identity.transformer.service.principal.id";
  /** A comma separated list of names to be replaced with the service principal ID specified by
   * “fs.default.identity.transformer.service.principal.id”. This substitution occurs
   * when setOwner, setAcl, modifyAclEntries, or removeAclEntries are invoked with identities
   * contained in the substitution list. Notice that when in non-secure cluster, asterisk symbol "*"
   * can be used to match all user/group. **/
  public static final String FS_AZURE_OVERRIDE_OWNER_SP_LIST = "fs.azure.identity.transformer.service.principal.substitution.list";
  /** By default this is set as false, so “$superuser” is replaced with the current user when it appears as the owner
   * or owning group of a file or directory. To disable it, set it as true. **/
  public static final String FS_AZURE_SKIP_SUPER_USER_REPLACEMENT = "fs.azure.identity.transformer.skip.superuser.replacement";
  public static final String AZURE_KEY_ACCOUNT_KEYPROVIDER = "fs.azure.account.keyprovider";
  public static final String AZURE_KEY_ACCOUNT_SHELLKEYPROVIDER_SCRIPT = "fs.azure.shellkeyprovider.script";

  /**
   * Enable or disable readahead V1 in AbfsInputStream.
   * Value: {@value}.
   */
  public static final String FS_AZURE_ENABLE_READAHEAD = "fs.azure.enable.readahead";
  /**
   * Enable or disable readahead V2 in AbfsInputStream. This will work independent of V1.
   * Value: {@value}.
   */
  public static final String FS_AZURE_ENABLE_READAHEAD_V2 = "fs.azure.enable.readahead.v2";

  /**
   * Enable or disable dynamic scaling of thread pool and buffer pool of readahead V2.
   * Value: {@value}.
   */
  public static final String FS_AZURE_ENABLE_READAHEAD_V2_DYNAMIC_SCALING = "fs.azure.enable.readahead.v2.dynamic.scaling";

  /**
   * Enable or disable request priority for prefetch requests
   * Value: {@value}.
   */
  public static final String FS_AZURE_ENABLE_PREFETCH_REQUEST_PRIORITY = "fs.azure.enable.prefetch.request.priority";

  /**
   * Request priority value for prefetch requests
   * Value: {@value}.
   */
  public static final String FS_AZURE_PREFETCH_REQUEST_PRIORITY_VALUE = "fs.azure.prefetch.request.priority.value";

  /**
   * Minimum number of prefetch threads in the thread pool for readahead V2.
   * {@value }
   */
  public static final String FS_AZURE_READAHEAD_V2_MIN_THREAD_POOL_SIZE = "fs.azure.readahead.v2.min.thread.pool.size";
  /**
   * Maximum number of prefetch threads in the thread pool for readahead V2.
   * {@value }
   */
  public static final String FS_AZURE_READAHEAD_V2_MAX_THREAD_POOL_SIZE = "fs.azure.readahead.v2.max.thread.pool.size";
  /**
   * Minimum size of the buffer pool for caching prefetched data for readahead V2.
   * {@value }
   */
  public static final String FS_AZURE_READAHEAD_V2_MIN_BUFFER_POOL_SIZE = "fs.azure.readahead.v2.min.buffer.pool.size";
  /**
   * Maximum size of the buffer pool for caching prefetched data for readahead V2.
   * {@value }
   */
  public static final String FS_AZURE_READAHEAD_V2_MAX_BUFFER_POOL_SIZE = "fs.azure.readahead.v2.max.buffer.pool.size";

  /**
   * Interval in milliseconds for periodic monitoring of CPU usage and up/down scaling thread pool size accordingly.
   * {@value }
   */
  public static final String FS_AZURE_READAHEAD_V2_CPU_MONITORING_INTERVAL_MILLIS = "fs.azure.readahead.v2.cpu.monitoring.interval.millis";

  /**
   * Percentage by which the thread pool size should be upscaled when CPU usage is low.
   */
  public static final String FS_AZURE_READAHEAD_V2_THREAD_POOL_UPSCALE_PERCENTAGE = "fs.azure.readahead.v2.thread.pool.upscale.percentage";

  /**
   * Percentage by which the thread pool size should be downscaled when CPU usage is high.
   */
  public static final String FS_AZURE_READAHEAD_V2_THREAD_POOL_DOWNSCALE_PERCENTAGE = "fs.azure.readahead.v2.thread.pool.downscale.percentage";

  /**
   * Interval in milliseconds for periodic monitoring of memory usage and up/down scaling buffer pool size accordingly.
   * {@value }
   */
  public static final String FS_AZURE_READAHEAD_V2_MEMORY_MONITORING_INTERVAL_MILLIS = "fs.azure.readahead.v2.memory.monitoring.interval.millis";

  /**
   * TTL in milliseconds for the idle threads in executor service used by read ahead v2.
   */
  public static final String FS_AZURE_READAHEAD_V2_EXECUTOR_SERVICE_TTL_MILLIS = "fs.azure.readahead.v2.executor.service.ttl.millis";

  /**
   * TTL in milliseconds for the cached buffers in buffer pool used by read ahead v2.
   */
  public static final String FS_AZURE_READAHEAD_V2_CACHED_BUFFER_TTL_MILLIS = "fs.azure.readahead.v2.cached.buffer.ttl.millis";

  /**
   * Threshold percentage for CPU usage to scale up/down the thread pool size in read ahead v2.
   */
  public static final String FS_AZURE_READAHEAD_V2_CPU_USAGE_THRESHOLD_PERCENT = "fs.azure.readahead.v2.cpu.usage.threshold.percent";

  /**
   * Threshold percentage for memory usage to scale up/down the buffer pool size in read ahead v2.
   */
  public static final String FS_AZURE_READAHEAD_V2_MEMORY_USAGE_THRESHOLD_PERCENT = "fs.azure.readahead.v2.memory.usage.threshold.percent";

  /** Setting this true will make the driver use it's own RemoteIterator implementation */
  public static final String FS_AZURE_ENABLE_ABFS_LIST_ITERATOR = "fs.azure.enable.abfslistiterator";
  /** Server side encryption key encoded in Base6format {@value}.*/
  public static final String FS_AZURE_ENCRYPTION_ENCODED_CLIENT_PROVIDED_KEY =
      "fs.azure.encryption.encoded.client-provided-key";
  /** SHA256 hash of encryption key encoded in Base64format */
  public static final String FS_AZURE_ENCRYPTION_ENCODED_CLIENT_PROVIDED_KEY_SHA =
      "fs.azure.encryption.encoded.client-provided-key-sha";
  /** Custom EncryptionContextProvider type */
  public static final String FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE = "fs.azure.encryption.context.provider.type";

  /** End point of ABFS account: {@value}. */
  public static final String AZURE_ABFS_ENDPOINT = "fs.azure.abfs.endpoint";
  /** Key for auth type properties: {@value}. */
  public static final String FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME = "fs.azure.account.auth.type";
  /** Key for oauth token provider type: {@value}. */
  public static final String FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME = "fs.azure.account.oauth.provider.type";
  /** Key for oauth AAD client id: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID = "fs.azure.account.oauth2.client.id";
  /** Key for oauth AAD client secret: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET = "fs.azure.account.oauth2.client.secret";
  /** Key for oauth AAD client endpoint: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT = "fs.azure.account.oauth2.client.endpoint";
  /** Key for oauth msi tenant id: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT = "fs.azure.account.oauth2.msi.tenant";
  /** Key for oauth msi endpoint: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT = "fs.azure.account.oauth2.msi.endpoint";
  /** Key for oauth msi Authority: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY = "fs.azure.account.oauth2.msi.authority";
  /** Key for oauth user name: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_USER_NAME = "fs.azure.account.oauth2.user.name";
  /** Key for oauth user password: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_USER_PASSWORD = "fs.azure.account.oauth2.user.password";
  /** Key for oauth refresh token: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_REFRESH_TOKEN = "fs.azure.account.oauth2.refresh.token";
  /** Key for oauth AAD refresh token endpoint: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_REFRESH_TOKEN_ENDPOINT = "fs.azure.account.oauth2.refresh.token.endpoint";
  /** Key for oauth AAD workload identity token file path: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_TOKEN_FILE = "fs.azure.account.oauth2.token.file";
  /** Key for custom client assertion provider class for WorkloadIdentityTokenProvider */
  public static final String FS_AZURE_ACCOUNT_OAUTH_CLIENT_ASSERTION_PROVIDER_TYPE = "fs.azure.account.oauth2.client.assertion.provider.type";
  /** Key for enabling the tracking of ABFS API latency and sending the latency numbers to the ABFS API service */
  public static final String FS_AZURE_ABFS_LATENCY_TRACK = "fs.azure.abfs.latency.track";

  /** Key for rate limit capacity, as used by IO operations which try to throttle themselves. */
  public static final String FS_AZURE_ABFS_IO_RATE_LIMIT = "fs.azure.io.rate.limit";

  /** Add extra resilience to rename failures, at the expense of performance. */
  public static final String FS_AZURE_ABFS_RENAME_RESILIENCE = "fs.azure.enable.rename.resilience";

  /**
   * Specify whether paginated behavior is to be expected or not in delete path. {@value}
   */
  public static final String FS_AZURE_ENABLE_PAGINATED_DELETE = "fs.azure.enable.paginated.delete";

  /** Add extra layer of verification of the integrity of the request content during transport: {@value}. */
  public static final String FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION = "fs.azure.enable.checksum.validation";

  /** Add extra layer of verification of the integrity of the full blob request content during transport: {@value}. */
  public static final String FS_AZURE_ENABLE_FULL_BLOB_CHECKSUM_VALIDATION = "fs.azure.enable.full.blob.checksum.validation";

  public static String accountProperty(String property, String account) {
    return property + DOT + account;
  }

  public static String containerProperty(String property, String fsName, String account) {
    return property + DOT + fsName + DOT + account;
  }

  public static final String FS_AZURE_ENABLE_DELEGATION_TOKEN = "fs.azure.enable.delegation.token";
  public static final String FS_AZURE_DELEGATION_TOKEN_PROVIDER_TYPE = "fs.azure.delegation.token.provider.type";

  /** Key for fixed SAS token: {@value}. **/
  public static final String FS_AZURE_SAS_FIXED_TOKEN = "fs.azure.sas.fixed.token";

  /** Key for SAS token provider: {@value}. **/
  public static final String FS_AZURE_SAS_TOKEN_PROVIDER_TYPE = "fs.azure.sas.token.provider.type";

  /** For performance, AbfsInputStream/AbfsOutputStream re-use SAS tokens until the expiry is within this number of seconds. **/
  public static final String FS_AZURE_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS = "fs.azure.sas.token.renew.period.for.streams";

  /** Key to enable custom identity transformation. */
  public static final String FS_AZURE_IDENTITY_TRANSFORM_CLASS = "fs.azure.identity.transformer.class";
  /** Key for Local User to Service Principal file location. */
  public static final String FS_AZURE_LOCAL_USER_SP_MAPPING_FILE_PATH = "fs.azure.identity.transformer.local.service.principal.mapping.file.path";
  /** Key for Local Group to Service Group file location. */
  public static final String FS_AZURE_LOCAL_GROUP_SG_MAPPING_FILE_PATH = "fs.azure.identity.transformer.local.service.group.mapping.file.path";
  /**
   * Optional config to enable a lock free pread which will bypass buffer in AbfsInputStream.
   * This is not a config which can be set at cluster level. It can be used as
   * an option on FutureDataInputStreamBuilder.
   * @see FileSystem#openFile(org.apache.hadoop.fs.Path)
   */
  public static final String FS_AZURE_BUFFERED_PREAD_DISABLE = "fs.azure.buffered.pread.disable";
  /**Defines what network library to use for server IO calls: {@value}*/
  public static final String FS_AZURE_NETWORKING_LIBRARY = "fs.azure.networking.library";
  /**
   * Maximum number of IOExceptions retries for a single server call on ApacheHttpClient.
   * Breach of this count would turn off future uses of the ApacheHttpClient library
   * in the JVM lifecycle: {@value}
   */
  public static final String FS_AZURE_APACHE_HTTP_CLIENT_MAX_IO_EXCEPTION_RETRIES = "fs.azure.apache.http.client.max.io.exception.retries";
  /**Maximum ApacheHttpClient-connection cache size at filesystem level: {@value}*/
  public static final String FS_AZURE_APACHE_HTTP_CLIENT_MAX_CACHE_SIZE = "fs.azure.apache.http.client.max.cache.size";
  /**
   * Defines number of connections to establish during warmup phase
   * of ApacheHttpClient connection cache: {@value}
   */
  public static final String FS_AZURE_APACHE_HTTP_CLIENT_CACHE_WARMUP_COUNT = "fs.azure.apache.http.client.cache.warmup.count";
  /**
   * Defines number of connections to establish during refresh phase
   * of ApacheHttpClient connection cache: {@value}
   */
  public static final String FS_AZURE_APACHE_HTTP_CLIENT_CACHE_REFRESH_COUNT = "fs.azure.apache.http.client.cache.refresh.count";
  /**
   * Defines time duration to wait for ApacheHttpClient connection
   * cache to warmup/ refresh: {@value}
   */
  public static final String FS_AZURE_APACHE_HTTP_CLIENT_MAX_REFRESH_WAIT_TIME_MILLIS = "fs.azure.apache.http.client.max.refresh.wait.time.millis";
  /**
   * Minimum number of cached connections in ApacheHttpClient cache
   * below which refresh will be triggered. {@value}
   */
  public static final String FS_AZURE_APACHE_HTTP_CLIENT_MIN_TRIGGER_REFRESH_COUNT = "fs.azure.apache.http.client.min.trigger.refresh.count";
  /**
   * Time duration to wait for ApacheHttpClient connection cache to warmup/refresh: {@value}
   */
  public static final String FS_AZURE_APACHE_HTTP_CLIENT_WARMUP_CACHE_TIMEOUT_MILLIS = "fs.azure.apache.http.client.warmup.cache.timeout.millis";
  /**
   * Blob copy API is an async API, this configuration defines polling duration
   * for checking copy status: {@value}
   */
  public static final String FS_AZURE_BLOB_COPY_PROGRESS_WAIT_MILLIS = "fs.azure.blob.copy.progress.wait.millis";
  /**
   * Maximum time to wait for a blob copy operation to complete: {@value}
   */
  public static final String FS_AZURE_BLOB_COPY_MAX_WAIT_MILLIS = "fs.azure.blob.copy.max.wait.millis";
  /**Blob rename lease refresh duration: {@value}*/
  public static final String FS_AZURE_BLOB_ATOMIC_RENAME_LEASE_REFRESH_DURATION
          = "fs.azure.blob.atomic.rename.lease.refresh.duration";
  /**Maximum number of blob information enqueued in memory for rename or delete orchestration: {@value}*/
  public static final String FS_AZURE_PRODUCER_QUEUE_MAX_SIZE = "fs.azure.blob.dir.list.producer.queue.max.size";
  /**
   * Maximum consumer lag (count of blob information which is yet to be taken for operation)
   * in blob listing which can be tolerated before making producer to wait for
   * consumer lag to become tolerable: {@value}.
   */
  public static final String FS_AZURE_CONSUMER_MAX_LAG = "fs.azure.blob.dir.list.consumer.max.lag";
  /**Maximum number of thread per blob-rename orchestration: {@value}*/
  public static final String FS_AZURE_BLOB_DIR_RENAME_MAX_THREAD = "fs.azure.blob.dir.rename.max.thread";
  /**Maximum number of thread per blob-delete orchestration: {@value}*/
  public static final String FS_AZURE_BLOB_DIR_DELETE_MAX_THREAD = "fs.azure.blob.dir.delete.max.thread";

  /**Flag to enable/disable sending client transactional ID during create/rename operations: {@value}*/
  public static final String FS_AZURE_ENABLE_CLIENT_TRANSACTION_ID = "fs.azure.enable.client.transaction.id";
  /**Flag to enable/disable create idempotency during create operation: {@value}*/
  public static final String FS_AZURE_ENABLE_CREATE_BLOB_IDEMPOTENCY = "fs.azure.enable.create.blob.idempotency";

  /**
   * Flag to enable/disable tail latency tracker for AbfsRestOperation.
   * When enabled, Client observed E2E latency will be tracked by a histogram.
   * Regular p50, p99 and configured percentile latencies will be reported.
   */
  public static final String FS_AZURE_ENABLE_TAIL_LATENCY_TRACKER = "fs.azure.enable.tail.latency.tracker";

  /**
   * Flag to enable/disable tail latency based timeout for AbfsRestOperation.
   * When enabled, if an operation's latency exceeds the currently reported tail
   * latency by the tracker, the ongoing socket connection will be closed and
   * the operation will be retried, up to the configured max retry count: {@value}
   */
  public static final String FS_AZURE_ENABLE_TAIL_LATENCY_REQUEST_TIMEOUT = "fs.azure.enable.tail.latency.timeout";

  /**
   * The percentile value to be considered as tail latency value.
   * Default is 99.0 (99th percentile): {@value}
   */
  public static final String FS_AZURE_TAIL_LATENCY_PERCENTILE = "fs.azure.tail.latency.percentile";

  /**
   * The minimum deviation (in percentage) between p50 and tail latency
   * percentile to trigger tail latency based request timeout: {@value}
   */
  public static final String FS_AZURE_TAIL_LATENCY_MIN_DEVIATION = "fs.azure.tail.latency.min.deviation";

  /**
   * The minimum sample size required before the histogram starts reporting latency data: {@value}
   */
  public static final String FS_AZURE_TAIL_LATENCY_MIN_SAMPLE_SIZE = "fs.azure.tail.latency.min.sample.size";

  /**
   * The time window (in milliseconds) over which the tail latency analysis is performed.
   * Until the whole window is filled, the histogram will not report any latency data: {@value}
   */
  public static final String FS_AZURE_TAIL_LATENCY_ANALYSIS_WINDOW_MILLIS = "fs.azure.tail.latency.analysis.window.millis";

  /**
   * The granularity (in milliseconds) at which the tail latency analysis window is divided.
   * This is to make sliding window calculations efficient and robust: {@value}
   */
  public static final String FS_AZURE_TAIL_LATENCY_ANALYSIS_WINDOW_GRANULARITY = "fs.azure.tail.latency.analysis.window.granularity";

  /**
   * Interval (in milliseconds) at which the tail latency percentile is computed
   * and updated by the background thread for each operation type: {@value}
   */
  public static final String FS_AZURE_TAIL_LATENCY_PERCENTILE_COMPUTATION_INTERVAL_MILLIS = "fs.azure.tail.latency.percentile.computation.interval.millis";

  /**
   * Maximum number of retries for an operation when tail latency based timeout occur: {@value}
   */
  public static final String FS_AZURE_TAIL_LATENCY_MAX_RETRY_COUNT = "fs.azure.tail.latency.max.retry.count";

  /**
   * If true, restricts GPS (getPathStatus) calls on openFileforRead
   * Default: false
   */
  public static final String FS_AZURE_RESTRICT_GPS_ON_OPENFILE = "fs.azure.restrict.gps.on.openfile";

  private ConfigurationKeys() {}
}
