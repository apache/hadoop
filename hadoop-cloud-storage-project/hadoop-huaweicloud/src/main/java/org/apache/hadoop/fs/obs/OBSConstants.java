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

package org.apache.hadoop.fs.obs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * All constants used by {@link OBSFileSystem}.
 *
 * <p>Some of the strings are marked as {@code Unstable}. This means that they
 * may be unsupported in future; at which point they will be marked as
 * deprecated and simply ignored.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
final class OBSConstants {
  /**
   * Minimum multipart size which OBS supports.
   */
  static final int MULTIPART_MIN_SIZE = 5 * 1024 * 1024;

  /**
   * OBS access key.
   */
  static final String ACCESS_KEY = "fs.obs.access.key";

  /**
   * OBS secret key.
   */
  static final String SECRET_KEY = "fs.obs.secret.key";

  /**
   * OBS credentials provider.
   */
  static final String OBS_CREDENTIALS_PROVIDER
      = "fs.obs.credentials.provider";

  /**
   * OBS client security provider.
   */
  static final String OBS_SECURITY_PROVIDER = "fs.obs.security.provider";

  /**
   * Extra set of security credentials which will be prepended to that set in
   * {@code "hadoop.security.credential.provider.path"}. This extra option
   * allows for per-bucket overrides.
   */
  static final String OBS_SECURITY_CREDENTIAL_PROVIDER_PATH =
      "fs.obs.security.credential.provider.path";

  /**
   * Session token for when using TemporaryOBSCredentialsProvider.
   */
  static final String SESSION_TOKEN = "fs.obs.session.token";

  /**
   * Maximum number of simultaneous connections to obs.
   */
  static final String MAXIMUM_CONNECTIONS = "fs.obs.connection.maximum";

  /**
   * Default value of {@link #MAXIMUM_CONNECTIONS}.
   */
  static final int DEFAULT_MAXIMUM_CONNECTIONS = 1000;

  /**
   * Connect to obs over ssl.
   */
  static final String SECURE_CONNECTIONS = "fs.obs.connection.ssl.enabled";

  /**
   * Default value of {@link #SECURE_CONNECTIONS}.
   */
  static final boolean DEFAULT_SECURE_CONNECTIONS = false;

  /**
   * Use a custom endpoint.
   */
  static final String ENDPOINT = "fs.obs.endpoint";

  /**
   * Host for connecting to OBS through proxy server.
   */
  static final String PROXY_HOST = "fs.obs.proxy.host";

  /**
   * Port for connecting to OBS through proxy server.
   */
  static final String PROXY_PORT = "fs.obs.proxy.port";

  /**
   * User name for connecting to OBS through proxy server.
   */
  static final String PROXY_USERNAME = "fs.obs.proxy.username";

  /**
   * Password for connecting to OBS through proxy server.
   */
  static final String PROXY_PASSWORD = "fs.obs.proxy.password";

  /**
   * Default port for HTTPS.
   */
  static final int DEFAULT_HTTPS_PORT = 443;

  /**
   * Default port for HTTP.
   */
  static final int DEFAULT_HTTP_PORT = 80;

  /**
   * Number of times we should retry errors.
   */
  static final String MAX_ERROR_RETRIES = "fs.obs.attempts.maximum";

  /**
   * Default value of {@link #MAX_ERROR_RETRIES}.
   */
  static final int DEFAULT_MAX_ERROR_RETRIES = 3;

  /**
   * Seconds until we give up trying to establish a connection to obs.
   */
  static final String ESTABLISH_TIMEOUT
      = "fs.obs.connection.establish.timeout";

  /**
   * Default value of {@link #ESTABLISH_TIMEOUT}.
   */
  static final int DEFAULT_ESTABLISH_TIMEOUT = 120000;

  /**
   * Seconds until we give up on a connection to obs.
   */
  static final String SOCKET_TIMEOUT = "fs.obs.connection.timeout";

  /**
   * Default value of {@link #SOCKET_TIMEOUT}.
   */
  static final int DEFAULT_SOCKET_TIMEOUT = 120000;

  /**
   * Socket send buffer to be used in OBS SDK.
   */
  static final String SOCKET_SEND_BUFFER = "fs.obs.socket.send.buffer";

  /**
   * Default value of {@link #SOCKET_SEND_BUFFER}.
   */
  static final int DEFAULT_SOCKET_SEND_BUFFER = 256 * 1024;

  /**
   * Socket receive buffer to be used in OBS SDK.
   */
  static final String SOCKET_RECV_BUFFER = "fs.obs.socket.recv.buffer";

  /**
   * Default value of {@link #SOCKET_RECV_BUFFER}.
   */
  static final int DEFAULT_SOCKET_RECV_BUFFER = 256 * 1024;

  /**
   * Number of records to get while paging through a directory listing.
   */
  static final String MAX_PAGING_KEYS = "fs.obs.paging.maximum";

  /**
   * Default value of {@link #MAX_PAGING_KEYS}.
   */
  static final int DEFAULT_MAX_PAGING_KEYS = 1000;

  /**
   * Maximum number of threads to allow in the pool used by TransferManager.
   */
  static final String MAX_THREADS = "fs.obs.threads.max";

  /**
   * Default value of {@link #MAX_THREADS}.
   */
  static final int DEFAULT_MAX_THREADS = 20;

  /**
   * Maximum number of tasks cached if all threads are already uploading.
   */
  static final String MAX_TOTAL_TASKS = "fs.obs.max.total.tasks";

  /**
   * Default value of {@link #MAX_TOTAL_TASKS}.
   */
  static final int DEFAULT_MAX_TOTAL_TASKS = 20;

  /**
   * Max number of copy threads.
   */
  static final String MAX_COPY_THREADS = "fs.obs.copy.threads.max";

  /**
   * Default value of {@link #MAX_COPY_THREADS}.
   */
  static final int DEFAULT_MAX_COPY_THREADS = 40;

  /**
   * Max number of delete threads.
   */
  static final String MAX_DELETE_THREADS = "fs.obs.delete.threads.max";

  /**
   * Default value of {@link #MAX_DELETE_THREADS}.
   */
  static final int DEFAULT_MAX_DELETE_THREADS = 20;

  /**
   * Unused option: maintained for compile-time compatibility. If set, a warning
   * is logged in OBS during init.
   */
  @Deprecated
  static final String CORE_THREADS = "fs.obs.threads.core";

  /**
   * The time that an idle thread waits before terminating.
   */
  static final String KEEPALIVE_TIME = "fs.obs.threads.keepalivetime";

  /**
   * Default value of {@link #KEEPALIVE_TIME}.
   */
  static final int DEFAULT_KEEPALIVE_TIME = 60;

  /**
   * Size of each of or multipart pieces in bytes.
   */
  static final String MULTIPART_SIZE = "fs.obs.multipart.size";

  /**
   * Default value of {@link #MULTIPART_SIZE}.
   */
  static final long DEFAULT_MULTIPART_SIZE = 104857600; // 100 MB

  /**
   * Enable multi-object delete calls.
   */
  static final String ENABLE_MULTI_DELETE = "fs.obs.multiobjectdelete.enable";

  /**
   * Max number of objects in one multi-object delete call. This option takes
   * effect only when the option 'ENABLE_MULTI_DELETE' is set to 'true'.
   */
  static final String MULTI_DELETE_MAX_NUMBER
      = "fs.obs.multiobjectdelete.maximum";

  /**
   * Default value of {@link #MULTI_DELETE_MAX_NUMBER}.
   */
  static final int DEFAULT_MULTI_DELETE_MAX_NUMBER = 1000;

  /**
   * Delete recursively or not.
   */
  static final String MULTI_DELETE_RECURSION
      = "fs.obs.multiobjectdelete.recursion";

  /**
   * Minimum number of objects in one multi-object delete call.
   */
  static final String MULTI_DELETE_THRESHOLD
      = "fs.obs.multiobjectdelete.threshold";

  /**
   * Default value of {@link #MULTI_DELETE_THRESHOLD}.
   */
  static final int MULTI_DELETE_DEFAULT_THRESHOLD = 3;

  /**
   * Comma separated list of directories.
   */
  static final String BUFFER_DIR = "fs.obs.buffer.dir";

  /**
   * Switch to the fast block-by-block upload mechanism.
   */
  static final String FAST_UPLOAD = "fs.obs.fast.upload";

  /**
   * What buffer to use. Default is {@link #FAST_UPLOAD_BUFFER_DISK} Value:
   * {@value}
   */
  @InterfaceStability.Unstable
  static final String FAST_UPLOAD_BUFFER = "fs.obs.fast.upload.buffer";

  /**
   * Buffer blocks to disk: {@value}. Capacity is limited to available disk
   * space.
   */
  @InterfaceStability.Unstable
  static final String FAST_UPLOAD_BUFFER_DISK = "disk";

  /**
   * Use an in-memory array. Fast but will run of heap rapidly: {@value}.
   */
  @InterfaceStability.Unstable
  static final String FAST_UPLOAD_BUFFER_ARRAY = "array";

  /**
   * Use a byte buffer. May be more memory efficient than the {@link
   * #FAST_UPLOAD_BUFFER_ARRAY}: {@value}.
   */
  @InterfaceStability.Unstable
  static final String FAST_UPLOAD_BYTEBUFFER = "bytebuffer";

  /**
   * Maximum number of blocks a single output stream can have active (uploading,
   * or queued to the central FileSystem instance's pool of queued operations.
   * )This stops a single stream overloading the shared thread pool. {@value}
   *
   * <p>Default is {@link #DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS}
   */
  @InterfaceStability.Unstable
  static final String FAST_UPLOAD_ACTIVE_BLOCKS
      = "fs.obs.fast.upload.active.blocks";

  /**
   * Limit of queued block upload operations before writes block. Value:
   * {@value}
   */
  @InterfaceStability.Unstable
  static final int DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS = 4;

  /**
   * Canned acl options: Private | PublicRead | PublicReadWrite |
   * AuthenticatedRead | LogDeliveryWrite | BucketOwnerRead |
   * BucketOwnerFullControl.
   */
  static final String CANNED_ACL = "fs.obs.acl.default";

  /**
   * Default value of {@link #CANNED_ACL}.
   */
  static final String DEFAULT_CANNED_ACL = "";

  /**
   * Should we try to purge old multipart uploads when starting up.
   */
  static final String PURGE_EXISTING_MULTIPART = "fs.obs.multipart.purge";

  /**
   * Default value of {@link #PURGE_EXISTING_MULTIPART}.
   */
  static final boolean DEFAULT_PURGE_EXISTING_MULTIPART = false;

  /**
   * Purge any multipart uploads older than this number of seconds.
   */
  static final String PURGE_EXISTING_MULTIPART_AGE
      = "fs.obs.multipart.purge.age";

  /**
   * Default value of {@link #PURGE_EXISTING_MULTIPART_AGE}.
   */
  static final long DEFAULT_PURGE_EXISTING_MULTIPART_AGE = 86400;

  /**
   * OBS folder suffix.
   */
  static final String OBS_FOLDER_SUFFIX = "_$folder$";

  /**
   * Block size for
   * {@link org.apache.hadoop.fs.FileSystem#getDefaultBlockSize()}.
   */
  static final String FS_OBS_BLOCK_SIZE = "fs.obs.block.size";

  /**
   * Default value of {@link #FS_OBS_BLOCK_SIZE}.
   */
  static final int DEFAULT_FS_OBS_BLOCK_SIZE = 128 * 1024 * 1024;

  /**
   * OBS scheme.
   */
  static final String OBS_SCHEME = "obs";

  /**
   * Prefix for all OBS properties: {@value}.
   */
  static final String FS_OBS_PREFIX = "fs.obs.";

  /**
   * Prefix for OBS bucket-specific properties: {@value}.
   */
  static final String FS_OBS_BUCKET_PREFIX = "fs.obs.bucket.";

  /**
   * OBS default port.
   */
  static final int OBS_DEFAULT_PORT = -1;

  /**
   * User agent prefix.
   */
  static final String USER_AGENT_PREFIX = "fs.obs.user.agent.prefix";

  /**
   * Read ahead buffer size to prevent connection re-establishments.
   */
  static final String READAHEAD_RANGE = "fs.obs.readahead.range";

  /**
   * Default value of {@link #READAHEAD_RANGE}.
   */
  static final long DEFAULT_READAHEAD_RANGE = 1024 * 1024;

  /**
   * Flag indicating if {@link OBSInputStream#read(long, byte[], int, int)} will
   * use the implementation of
   * {@link org.apache.hadoop.fs.FSInputStream#read(long,
   * byte[], int, int)}.
   */
  static final String READ_TRANSFORM_ENABLE = "fs.obs.read.transform.enable";

  /**
   * OBS client factory implementation class.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static final String OBS_CLIENT_FACTORY_IMPL
      = "fs.obs.client.factory.impl";

  /**
   * Default value of {@link #OBS_CLIENT_FACTORY_IMPL}.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static final Class<? extends OBSClientFactory>
      DEFAULT_OBS_CLIENT_FACTORY_IMPL =
      DefaultOBSClientFactory.class;

  /**
   * Maximum number of partitions in a multipart upload: {@value}.
   */
  @InterfaceAudience.Private
  static final int MAX_MULTIPART_COUNT = 10000;

  // OBS Client configuration

  /**
   * Idle connection time.
   */
  static final String IDLE_CONNECTION_TIME = "fs.obs.idle.connection.time";

  /**
   * Default value of {@link #IDLE_CONNECTION_TIME}.
   */
  static final int DEFAULT_IDLE_CONNECTION_TIME = 30000;

  /**
   * Maximum number of idle connections.
   */
  static final String MAX_IDLE_CONNECTIONS = "fs.obs.max.idle.connections";

  /**
   * Default value of {@link #MAX_IDLE_CONNECTIONS}.
   */
  static final int DEFAULT_MAX_IDLE_CONNECTIONS = 1000;

  /**
   * Keep alive.
   */
  static final String KEEP_ALIVE = "fs.obs.keep.alive";

  /**
   * Default value of {@link #KEEP_ALIVE}.
   */
  static final boolean DEFAULT_KEEP_ALIVE = true;

  /**
   * Validate certificate.
   */
  static final String VALIDATE_CERTIFICATE = "fs.obs.validate.certificate";

  /**
   * Default value of {@link #VALIDATE_CERTIFICATE}.
   */
  static final boolean DEFAULT_VALIDATE_CERTIFICATE = false;

  /**
   * Verify response content type.
   */
  static final String VERIFY_RESPONSE_CONTENT_TYPE
      = "fs.obs.verify.response.content.type";

  /**
   * Default value of {@link #VERIFY_RESPONSE_CONTENT_TYPE}.
   */
  static final boolean DEFAULT_VERIFY_RESPONSE_CONTENT_TYPE = true;

  /**
   * UploadStreamRetryBufferSize.
   */
  static final String UPLOAD_STREAM_RETRY_SIZE
      = "fs.obs.upload.stream.retry.buffer.size";

  /**
   * Default value of {@link #UPLOAD_STREAM_RETRY_SIZE}.
   */
  static final int DEFAULT_UPLOAD_STREAM_RETRY_SIZE = 512 * 1024;

  /**
   * Read buffer size.
   */
  static final String READ_BUFFER_SIZE = "fs.obs.read.buffer.size";

  /**
   * Default value of {@link #READ_BUFFER_SIZE}.
   */
  static final int DEFAULT_READ_BUFFER_SIZE = 256 * 1024;

  /**
   * Write buffer size.
   */
  static final String WRITE_BUFFER_SIZE = "fs.obs.write.buffer.size";

  /**
   * Default value of {@link #WRITE_BUFFER_SIZE}.
   */
  static final int DEFAULT_WRITE_BUFFER_SIZE = 256 * 1024;

  /**
   * Canonical name.
   */
  static final String CNAME = "fs.obs.cname";

  /**
   * Default value of {@link #CNAME}.
   */
  static final boolean DEFAULT_CNAME = false;

  /**
   * Strict host name verification.
   */
  static final String STRICT_HOSTNAME_VERIFICATION
      = "fs.obs.strict.hostname.verification";

  /**
   * Default value of {@link #STRICT_HOSTNAME_VERIFICATION}.
   */
  static final boolean DEFAULT_STRICT_HOSTNAME_VERIFICATION = false;

  /**
   * Size of object copy part pieces in bytes.
   */
  static final String COPY_PART_SIZE = "fs.obs.copypart.size";

  /**
   * Maximum value of {@link #COPY_PART_SIZE}.
   */
  static final long MAX_COPY_PART_SIZE = 5368709120L; // 5GB

  /**
   * Default value of {@link #COPY_PART_SIZE}.
   */
  static final long DEFAULT_COPY_PART_SIZE = 104857600L; // 100MB

  /**
   * Maximum number of copy part threads.
   */
  static final String MAX_COPY_PART_THREADS = "fs.obs.copypart.threads.max";

  /**
   * Default value of {@link #MAX_COPY_PART_THREADS}.
   */
  static final int DEFAULT_MAX_COPY_PART_THREADS = 40;

  /**
   * Number of core list threads.
   */
  static final String CORE_LIST_THREADS = "fs.obs.list.threads.core";

  /**
   * Default value of {@link #CORE_LIST_THREADS}.
   */
  static final int DEFAULT_CORE_LIST_THREADS = 30;

  /**
   * Maximum number of list threads.
   */
  static final String MAX_LIST_THREADS = "fs.obs.list.threads.max";

  /**
   * Default value of {@link #MAX_LIST_THREADS}.
   */
  static final int DEFAULT_MAX_LIST_THREADS = 60;

  /**
   * Capacity of list work queue.
   */
  static final String LIST_WORK_QUEUE_CAPACITY
      = "fs.obs.list.workqueue.capacity";

  /**
   * Default value of {@link #LIST_WORK_QUEUE_CAPACITY}.
   */
  static final int DEFAULT_LIST_WORK_QUEUE_CAPACITY = 1024;

  /**
   * List parallel factor.
   */
  static final String LIST_PARALLEL_FACTOR = "fs.obs.list.parallel.factor";

  /**
   * Default value of {@link #LIST_PARALLEL_FACTOR}.
   */
  static final int DEFAULT_LIST_PARALLEL_FACTOR = 30;

  /**
   * Switch for the fast delete.
   */
  static final String TRASH_ENABLE = "fs.obs.trash.enable";

  /**
   * Enable obs content summary or not.
   */
  static final String OBS_CONTENT_SUMMARY_ENABLE
      = "fs.obs.content.summary.enable";

  /**
   * Enable obs client dfs list or not.
   */
  static final String OBS_CLIENT_DFS_LIST_ENABLE
      = "fs.obs.client.dfs.list.enable";

  /**
   * Default trash : false.
   */
  static final boolean DEFAULT_TRASH = false;

  /**
   * The fast delete recycle directory.
   */
  static final String TRASH_DIR = "fs.obs.trash.dir";

  /**
   * Encryption type is sse-kms or sse-c.
   */
  static final String SSE_TYPE = "fs.obs.server-side-encryption-type";

  /**
   * Kms key id for sse-kms, while key base64 encoded content for sse-c.
   */
  static final String SSE_KEY = "fs.obs.server-side-encryption-key";

  /**
   * Array first block size.
   */
  static final String FAST_UPLOAD_BUFFER_ARRAY_FIRST_BLOCK_SIZE
      = "fs.obs.fast.upload.array.first.buffer";

  /**
   * The fast upload buffer array first block default size.
   */
  static final int FAST_UPLOAD_BUFFER_ARRAY_FIRST_BLOCK_SIZE_DEFAULT = 1024
      * 1024;

  /**
   * Auth Type Negotiation Enable Switch.
   */
  static final String SDK_AUTH_TYPE_NEGOTIATION_ENABLE
      = "fs.obs.authtype.negotiation.enable";

  /**
   * Default value of {@link #SDK_AUTH_TYPE_NEGOTIATION_ENABLE}.
   */
  static final boolean DEFAULT_SDK_AUTH_TYPE_NEGOTIATION_ENABLE = false;

  /**
   * Okhttp retryOnConnectionFailure switch.
   */
  static final String SDK_RETRY_ON_CONNECTION_FAILURE_ENABLE
      = "fs.obs.connection.retry.enable";

  /**
   * Default value of {@link #SDK_RETRY_ON_CONNECTION_FAILURE_ENABLE}.
   */
  static final boolean DEFAULT_SDK_RETRY_ON_CONNECTION_FAILURE_ENABLE = true;

  /**
   * Sdk max retry times on unexpected end of stream. exception, default: -1,
   * don't retry
   */
  static final String SDK_RETRY_TIMES_ON_UNEXPECTED_END_EXCEPTION
      = "fs.obs.unexpectedend.retrytime";

  /**
   * Default value of {@link #SDK_RETRY_TIMES_ON_UNEXPECTED_END_EXCEPTION}.
   */
  static final int DEFAULT_SDK_RETRY_TIMES_ON_UNEXPECTED_END_EXCEPTION = -1;

  /**
   * Maximum sdk connection retry times, default : 2000.
   */
  static final int DEFAULT_MAX_SDK_CONNECTION_RETRY_TIMES = 2000;

  /**
   * Second to millisecond factor.
   */
  static final int SEC2MILLISEC_FACTOR = 1000;

  private OBSConstants() {
  }
}
