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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;

/**
 * Responsible to keep all the Azure Blob File System related configurations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class FileSystemConfigurations {

  public static final String DEFAULT_FS_AZURE_ACCOUNT_IS_HNS_ENABLED = "";
  public static final boolean DEFAULT_FS_AZURE_ENABLE_DFSTOBLOB_FALLBACK = false;
  public static final boolean DEFAULT_FS_AZURE_ACCOUNT_IS_EXPECT_HEADER_ENABLED = true;
  public static final String USER_HOME_DIRECTORY_PREFIX = "/user";

  private static final int SIXTY_SECONDS = 60_000;

  // Retry parameter defaults.
  public static final int DEFAULT_MIN_BACKOFF_INTERVAL = 500;  // 500ms
  public static final int DEFAULT_MAX_BACKOFF_INTERVAL = 25_000;  // 25s
  public static final boolean DEFAULT_STATIC_RETRY_FOR_CONNECTION_TIMEOUT_ENABLED = true;
  public static final int DEFAULT_STATIC_RETRY_INTERVAL = 1_000; // 1s
  public static final int DEFAULT_BACKOFF_INTERVAL = 500;  // 500ms
  public static final int DEFAULT_MAX_RETRY_ATTEMPTS = 30;
  public static final int DEFAULT_CUSTOM_TOKEN_FETCH_RETRY_COUNT = 3;

  /**
   * Default value of connection timeout to be used while setting up HTTP Connection.
   * Value: {@value}.
   */
  public static final int DEFAULT_HTTP_CONNECTION_TIMEOUT = 2_000; // 2s
  /**
   * Default value of read timeout to be used while setting up HTTP Connection.
   * Value: {@value}.
   */
  public static final int DEFAULT_HTTP_READ_TIMEOUT = 30_000; // 30 secs

  // Retry parameter defaults.
  public static final int DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_ATTEMPTS = 5;
  public static final int DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MIN_BACKOFF_INTERVAL = 0;
  public static final int DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_BACKOFF_INTERVAL = SIXTY_SECONDS;
  public static final int DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_DELTA_BACKOFF = 2;

  public static final int ONE_KB = 1024;
  public static final int ONE_MB = ONE_KB * ONE_KB;

  // Default upload and download buffer size
  public static final int DEFAULT_WRITE_BUFFER_SIZE = 8 * ONE_MB;  // 8 MB
  public static final int APPENDBLOB_MAX_WRITE_BUFFER_SIZE = 4 * ONE_MB;  // 4 MB
  public static final boolean DEFAULT_AZURE_ENABLE_SMALL_WRITE_OPTIMIZATION = false;
  public static final int DEFAULT_READ_BUFFER_SIZE = 4 * ONE_MB;  // 4 MB
  public static final boolean DEFAULT_READ_SMALL_FILES_COMPLETELY = false;
  public static final boolean DEFAULT_OPTIMIZE_FOOTER_READ = true;
  public static final int DEFAULT_FOOTER_READ_BUFFER_SIZE = 512 * ONE_KB;
  public static final boolean DEFAULT_ALWAYS_READ_BUFFER_SIZE = false;
  public static final int DEFAULT_READ_AHEAD_BLOCK_SIZE = 4 * ONE_MB;
  public static final int DEFAULT_READ_AHEAD_RANGE = 64 * ONE_KB; // 64 KB
  public static final int MIN_BUFFER_SIZE = 16 * ONE_KB;  // 16 KB
  public static final int MAX_BUFFER_SIZE = 100 * ONE_MB;  // 100 MB
  public static final long MAX_AZURE_BLOCK_SIZE = 256 * 1024 * 1024L; // changing default abfs blocksize to 256MB
  public static final String AZURE_BLOCK_LOCATION_HOST_DEFAULT = "localhost";
  public static final int DEFAULT_AZURE_LIST_MAX_RESULTS = 5000;

  public static final String SERVER_SIDE_ENCRYPTION_ALGORITHM = "AES256";

  public static final int MAX_CONCURRENT_READ_THREADS = 12;
  public static final int MAX_CONCURRENT_WRITE_THREADS = 8;
  public static final boolean DEFAULT_READ_TOLERATE_CONCURRENT_APPEND = false;
  public static final boolean DEFAULT_AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION = false;
  public static final boolean DEFAULT_AZURE_SKIP_USER_GROUP_METADATA_DURING_INITIALIZATION = false;

  public static final String DEFAULT_FS_AZURE_ATOMIC_RENAME_DIRECTORIES = "/hbase";
  public static final boolean DEFAULT_FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE = true;
  public static final boolean DEFAULT_FS_AZURE_ENABLE_MKDIR_OVERWRITE = true;
  public static final String DEFAULT_FS_AZURE_APPEND_BLOB_DIRECTORIES = "";
  public static final String DEFAULT_FS_AZURE_INFINITE_LEASE_DIRECTORIES = "";
  public static final int DEFAULT_LEASE_THREADS = 0;
  public static final int MIN_LEASE_THREADS = 0;
  public static final int DEFAULT_LEASE_DURATION = -1;
  public static final int INFINITE_LEASE_DURATION = -1;
  public static final int MIN_LEASE_DURATION = 15;
  public static final int MAX_LEASE_DURATION = 60;

  public static final int DEFAULT_READ_AHEAD_QUEUE_DEPTH = 2;

  public static final boolean DEFAULT_ENABLE_FLUSH = true;
  public static final boolean DEFAULT_DISABLE_OUTPUTSTREAM_FLUSH = true;
  public static final boolean DEFAULT_ENABLE_AUTOTHROTTLING = false;
  public static final int DEFAULT_METRIC_IDLE_TIMEOUT_MS = 60_000;
  public static final int DEFAULT_METRIC_ANALYSIS_TIMEOUT_MS = 60_000;
  public static final boolean DEFAULT_FS_AZURE_ACCOUNT_LEVEL_THROTTLING_ENABLED = true;
  public static final int DEFAULT_ACCOUNT_OPERATION_IDLE_TIMEOUT_MS = 60_000;
  public static final int DEFAULT_ANALYSIS_PERIOD_MS = 10_000;

  public static final DelegatingSSLSocketFactory.SSLChannelMode DEFAULT_FS_AZURE_SSL_CHANNEL_MODE
      = DelegatingSSLSocketFactory.SSLChannelMode.Default;

  public static final boolean DEFAULT_ENABLE_DELEGATION_TOKEN = false;
  public static final boolean DEFAULT_ENABLE_HTTPS = true;

  public static final boolean DEFAULT_USE_UPN = false;
  public static final boolean DEFAULT_ENABLE_CHECK_ACCESS = true;
  public static final boolean DEFAULT_ABFS_LATENCY_TRACK = false;
  public static final long DEFAULT_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS_IN_SECONDS = 120;

  public static final boolean DEFAULT_ENABLE_READAHEAD = true;
  public static final String DEFAULT_FS_AZURE_USER_AGENT_PREFIX = EMPTY_STRING;
  public static final String DEFAULT_VALUE_UNKNOWN = "UNKNOWN";

  public static final boolean DEFAULT_DELETE_CONSIDERED_IDEMPOTENT = true;
  public static final int DEFAULT_CLOCK_SKEW_WITH_SERVER_IN_MS = 5 * 60 * 1000; // 5 mins

  public static final int STREAM_ID_LEN = 12;
  public static final boolean DEFAULT_ENABLE_ABFS_LIST_ITERATOR = true;
  public static final boolean DEFAULT_ENABLE_ABFS_RENAME_RESILIENCE = true;
  public static final boolean DEFAULT_ENABLE_PAGINATED_DELETE = false;
  public static final boolean DEFAULT_ENABLE_ABFS_CHECKSUM_VALIDATION = false;

  /**
   * Limit of queued block upload operations before writes
   * block for an OutputStream. Value: {@value}
   */
  public static final int BLOCK_UPLOAD_ACTIVE_BLOCKS_DEFAULT = 20;

  /**
   * Length of the block ID used for appends.
   */
  public static final int BLOCK_ID_LENGTH = 60;

  /**
   * Buffer blocks to disk.
   * Capacity is limited to available disk space.
   */
  public static final String DATA_BLOCKS_BUFFER_DISK = "disk";

  public static final String DATA_BLOCKS_BYTEBUFFER = "bytebuffer";

  /**
   * Default buffer option: {@value}.
   */
  public static final String DATA_BLOCKS_BUFFER_DEFAULT =
          DATA_BLOCKS_BYTEBUFFER;

  /**
   * IO rate limit. Value: {@value}
   */
  public static final int RATE_LIMIT_DEFAULT = 1_000;

  public static final int ZERO = 0;
  public static final int HUNDRED = 100;
  public static final long THOUSAND = 1000L;

  public static final HttpOperationType DEFAULT_NETWORKING_LIBRARY
      = HttpOperationType.JDK_HTTP_URL_CONNECTION;

  public static final int DEFAULT_APACHE_HTTP_CLIENT_MAX_IO_EXCEPTION_RETRIES = 3;

  public static final long DEFAULT_HTTP_CLIENT_CONN_MAX_IDLE_TIME = 5_000L;

  public static final int DEFAULT_HTTP_CLIENT_CONN_MAX_CACHED_CONNECTIONS = 5;

  public static final long DEFAULT_AZURE_BLOB_COPY_PROGRESS_WAIT_MILLIS = 1_000L;

  public static final long DEFAULT_AZURE_BLOB_COPY_MAX_WAIT_MILLIS = 300_000L;

  public static final long DEFAULT_AZURE_BLOB_ATOMIC_RENAME_LEASE_REFRESH_DURATION = 60_000L;

  public static final int DEFAULT_FS_AZURE_PRODUCER_QUEUE_MAX_SIZE = 2 * DEFAULT_AZURE_LIST_MAX_RESULTS;

  public static final int DEFAULT_FS_AZURE_CONSUMER_MAX_LAG = DEFAULT_AZURE_LIST_MAX_RESULTS;

  public static final int DEFAULT_FS_AZURE_LISTING_ACTION_THREADS = 5;

  public static final int DEFAULT_FS_AZURE_BLOB_RENAME_THREAD = DEFAULT_FS_AZURE_LISTING_ACTION_THREADS;

  public static final int DEFAULT_FS_AZURE_BLOB_DELETE_THREAD = DEFAULT_FS_AZURE_LISTING_ACTION_THREADS;

  public static final boolean DEFAULT_FS_AZURE_ENABLE_CLIENT_TRANSACTION_ID = false;

  private FileSystemConfigurations() {}
}
