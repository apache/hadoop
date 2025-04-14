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

package org.apache.hadoop.fs.tosfs.conf;

import org.apache.hadoop.fs.tosfs.object.ChecksumType;
import org.apache.hadoop.fs.tosfs.object.tos.TOSErrorCodes;

public final class TosKeys {

  private TosKeys() {}

  /**
   * The accessKey key to access the tos object storage.
   */
  public static final String FS_TOS_ACCESS_KEY_ID = "fs.tos.access-key-id";

  /**
   * The secret access key to access the object storage.
   */
  public static final String FS_TOS_SECRET_ACCESS_KEY = "fs.tos.secret-access-key";

  /**
   * The session token to access the object storage.
   */
  public static final String FS_TOS_SESSION_TOKEN = "fs.tos.session-token";

  /**
   * The access key to access the object storage for the configured bucket, where %s is the bucket
   * name.
   */
  public static final ArgumentKey FS_TOS_BUCKET_ACCESS_KEY_ID =
      new ArgumentKey("fs.tos.bucket.%s.access-key-id");

  /**
   * The secret access key to access the object storage for the configured bucket, where %s is the
   * bucket name.
   */
  public static final ArgumentKey FS_TOS_BUCKET_SECRET_ACCESS_KEY =
      new ArgumentKey("fs.tos.bucket.%s.secret-access-key");

  /**
   * The session token to access the object storage for the configured bucket, where %s is the
   * bucket name.
   */
  public static final ArgumentKey FS_TOS_BUCKET_SESSION_TOKEN =
      new ArgumentKey("fs.tos.bucket.%s.session-token");

  // Credential
  /**
   * Default credentials provider chain that looks for credentials in this order:
   * SimpleCredentialsProvider,EnvironmentCredentialsProvider.
   */
  public static final String FS_TOS_CREDENTIALS_PROVIDER = "fs.tos.credentials.provider";
  public static final String FS_TOS_CREDENTIALS_PROVIDER_DEFAULT =
      "org.apache.hadoop.fs.tosfs.object.tos.auth.DefaultCredentialsProviderChain";

  /**
   * User customized credential provider classes, separate provider class name with comma if there
   * are multiple providers.
   */
  public static final String FS_TOS_CUSTOM_CREDENTIAL_PROVIDER_CLASSES =
      "fs.tos.credential.provider.custom.classes";

  public static final String[] FS_TOS_CUSTOM_CREDENTIAL_PROVIDER_CLASSES_DEFAULT =
      new String[] {"org.apache.hadoop.fs.tosfs.object.tos.auth.EnvironmentCredentialsProvider",
          "org.apache.hadoop.fs.tosfs.object.tos.auth.SimpleCredentialsProvider"};

  /**
   * Set a canned ACL for newly created and copied objects. Value may be 'private', 'public-read',
   * 'public-read-write', 'authenticated-read', 'bucket-owner-read', 'bucket-owner-full-control',
   * 'bucket-owner-entrusted'. If set, caller IAM role must have 'tos:PutObjectAcl' permission on
   * the bucket.
   */
  public static final String FS_TOS_ACL_DEFAULT = "fs.tos.acl.default";

  // TOS http client.
  /**
   * The maximum number of connections to the TOS service that a client can create.
   */
  public static final String FS_TOS_HTTP_MAX_CONNECTIONS = "fs.tos.http.maxConnections";
  public static final int FS_TOS_HTTP_MAX_CONNECTIONS_DEFAULT = 1024;

  /**
   * The time that a connection thread can be in idle state, larger than which the thread will be
   * terminated.
   */
  public static final String FS_TOS_HTTP_IDLE_CONNECTION_TIME_MILLS =
      "fs.tos.http.idleConnectionTimeMills";
  public static final int FS_TOS_HTTP_IDLE_CONNECTION_TIME_MILLS_DEFAULT = 60000;

  /**
   * The connect timeout that the tos client tries to connect to the TOS service.
   */
  public static final String FS_TOS_HTTP_CONNECT_TIMEOUT_MILLS = "fs.tos.http.connectTimeoutMills";
  public static final int FS_TOS_HTTP_CONNECT_TIMEOUT_MILLS_DEFAULT = 10000;

  /**
   * The reading timeout when reading data from tos. Note that it is configured for the tos client
   * sdk, not hadoop-tos.
   */
  public static final String FS_TOS_HTTP_READ_TIMEOUT_MILLS = "fs.tos.http.readTimeoutMills";
  public static final int FS_TOS_HTTP_READ_TIMEOUT_MILLS_DEFAULT = 30000;

  /**
   * The writing timeout when uploading data to tos. Note that it is configured for the tos client
   * sdk, not hadoop-tos.
   */
  public static final String FS_TOS_HTTP_WRITE_TIMEOUT_MILLS = "fs.tos.http.writeTimeoutMills";
  public static final int FS_TOS_HTTP_WRITE_TIMEOUT_MILLS_DEFAULT = 30000;

  /**
   * Enables SSL connections to TOS or not.
   */
  public static final String FS_TOS_HTTP_ENABLE_VERIFY_SSL = "fs.tos.http.enableVerifySSL";
  public static final boolean FS_TOS_HTTP_ENABLE_VERIFY_SSL_DEFAULT = true;

  /**
   * The timeout (in minutes) of the dns cache used in tos client.
   */
  public static final String FS_TOS_HTTP_DNS_CACHE_TIME_MINUTES = "fs.tos.http.dnsCacheTimeMinutes";
  public static final int FS_TOS_HTTP_DNS_CACHE_TIME_MINUTES_DEFAULT = 0;

  /**
   * Used for directory bucket, whether enable recursive delete capability in TOS server, which will
   * atomic delete all objects under given dir(inclusive), otherwise the client will list all sub
   * objects, and then send batch delete request to TOS to delete dir.
   */
  public static final String FS_TOS_RMR_SERVER_ENABLED = "fs.tos.rmr.server.enabled";
  public static final boolean FS_FS_TOS_RMR_SERVER_ENABLED_DEFAULT = false;

  /**
   * If fs.tos.rmr.client.enabled is true, client will list all objects under the given dir and
   * delete them by batch. Set value with true will use the recursive delete capability of TOS SDK,
   * otherwise will delete object one by one via preorder tree walk.
   */
  public static final String FS_TOS_RMR_CLIENT_ENABLE = "fs.tos.rmr.client.enabled";
  public static final boolean FS_TOS_RMR_CLIENT_ENABLE_DEFAULT = true;

  /**
   * The prefix will be used as the product name in TOS SDK. The final user agent pattern is
   * '{prefix}/TOS_FS/{hadoop tos version}'.
   */
  public static final String FS_TOS_USER_AGENT_PREFIX = "fs.tos.user.agent.prefix";
  public static final String FS_TOS_USER_AGENT_PREFIX_DEFAULT = "HADOOP-TOS";

  // TOS common keys.
  /**
   * The threshold indicates whether reuse the socket connection to optimize read performance during
   * closing tos object inputstream of get object. If the remaining bytes is less than max drain
   * bytes during closing the inputstream, will just skip the bytes instead of closing the socket
   * connection.
   */
  public static final String FS_TOS_MAX_DRAIN_BYTES = "fs.tos.max-drain-bytes";
  public static final long FS_TOS_MAX_DRAIN_BYTES_DEFAULT = 1024 * 1024L;

  /**
   * Whether disable the tos http client cache in the current JVM.
   */
  public static final String FS_TOS_DISABLE_CLIENT_CACHE = "fs.tos.client.disable.cache";
  public static final boolean FS_TOS_DISABLE_CLIENT_CACHE_DEFAULT = false;

  /**
   * The batch size when deleting the objects in batches.
   */
  public static final String FS_TOS_DELETE_OBJECTS_COUNT = "fs.tos.batch.delete.objects-count";
  public static final int FS_TOS_DELETE_OBJECTS_COUNT_DEFAULT = 1000;

  /**
   * The maximum retry times when deleting objects in batches failed.
   */
  public static final String FS_TOS_BATCH_DELETE_MAX_RETRIES = "fs.tos.batch.delete.max-retries";
  public static final int FS_TOS_BATCH_DELETE_MAX_RETRIES_DEFAULT = 20;

  /**
   * The codes from TOS deleteMultiObjects response, client will resend the batch delete request to
   * delete the failed keys again if the response only contains these codes, otherwise won't send
   * request anymore.
   */
  public static final String FS_TOS_BATCH_DELETE_RETRY_CODES = "fs.tos.batch.delete.retry-codes";
  public static final String[] FS_TOS_BATCH_DELETE_RETRY_CODES_DEFAULT =
      new String[] {"ExceedAccountQPSLimit", "ExceedAccountRateLimit", "ExceedBucketQPSLimit",
          "ExceedBucketRateLimit", "InternalError", "ServiceUnavailable", "SlowDown",
          "TooManyRequests"};

  /**
   * The retry interval (in milliseconds) when deleting objects in batches failed.
   */
  public static final String FS_TOS_BATCH_DELETE_RETRY_INTERVAL =
      "fs.tos.batch.delete.retry.interval";
  public static final long FS_TOS_BATCH_DELETE_RETRY_INTERVAL_DEFAULT = 1000L;

  /**
   * The batch size of listing object per request for the given object storage, such as listing a
   * directory, searching for all objects whose path starts with the directory path, and returning
   * them as a list.
   */
  public static final String FS_TOS_LIST_OBJECTS_COUNT = "fs.tos.list.objects-count";
  public static final int FS_TOS_LIST_OBJECTS_COUNT_DEFAULT = 1000;

  /**
   * The maximum retry times of sending request via TOS client, client will resend the request if
   * got retryable exceptions, e.g. SocketException, UnknownHostException, SSLException,
   * InterruptedException, SocketTimeoutException, or got TOO_MANY_REQUESTS, INTERNAL_SERVER_ERROR
   * http codes.
   */
  public static final String FS_TOS_REQUEST_MAX_RETRY_TIMES = "fs.tos.request.max.retry.times";
  public static final int FS_TOS_REQUEST_MAX_RETRY_TIMES_DEFAULT = 20;

  /**
   * The fast-fail error codes means the error cannot be solved by retrying the request. TOS client
   * won't retry the request if receiving a 409 http status code and if the error code is in the
   * configured non-retryable error code list.
   */
  public static final String FS_TOS_FAST_FAILURE_409_ERROR_CODES =
      "fs.tos.fast-fail-409-error-codes";
  public static final String FS_TOS_FAST_FAILURE_409_ERROR_CODES_DEFAULT =
      TOSErrorCodes.FAST_FAILURE_CONFLICT_ERROR_CODES;

  /**
   * The maximum retry times of reading object content via TOS client, client will resend the
   * request to create a new input stream if getting unexpected end of stream error during reading
   * the input stream.
   */
  public static final String FS_TOS_MAX_READ_OBJECT_RETRIES = "fs.tos.inputstream.max.retry.times";
  public static final int FS_TOS_MAX_READ_OBJECT_RETRIES_DEFAULT = 5;

  /**
   * Enable the crc check when uploading files to tos or not.
   */
  public static final String FS_TOS_CRC_CHECK_ENABLED = "fs.tos.crc.check.enable";
  public static final boolean FS_TOS_CRC_CHECK_ENABLED_DEFAULT = true;

  /**
   * Whether enable tos getFileStatus API or not, which returns the object info directly in one RPC
   * request, otherwise, might need to send three RPC requests to get object info.
   * For example, there is a key 'a/b/c' exists in TOS, and we want to get object status of 'a/b',
   * the GetFileStatus('a/b') will return the prefix 'a/b/' as a directory object directly. If this
   * property is disabled, we need to head('a/b') at first, and then head('a/b/'), and last call
   * list('a/b/', limit=1) to get object info. Using GetFileStatus API can reduce the RPC call
   * times.
   */
  public static final String FS_TOS_GET_FILE_STATUS_ENABLED = "fs.tos.get-file-status.enabled";
  public static final boolean FS_TOS_GET_FILE_STATUS_ENABLED_DEFAULT = true;

  /**
   * The key indicates the name of the tos checksum algorithm. Specify the algorithm name to compare
   * checksums between different storage systems. For example to compare checksums between hdfs and
   * tos, we need to configure the algorithm name to COMPOSITE-CRC32C.
   */
  public static final String FS_TOS_CHECKSUM_ALGORITHM = "fs.tos.checksum-algorithm";
  public static final String FS_TOS_CHECKSUM_ALGORITHM_DEFAULT = "TOS-CHECKSUM";

  /**
   * The key indicates how to retrieve file checksum from tos, error will be thrown if the
   * configured checksum type is not supported by tos. The supported checksum types are:
   * CRC32C, CRC64ECMA.
   */
  public static final String FS_TOS_CHECKSUM_TYPE = "fs.tos.checksum-type";
  public static final String FS_TOS_CHECKSUM_TYPE_DEFAULT = ChecksumType.CRC64ECMA.name();
}
