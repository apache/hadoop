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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;

import java.util.concurrent.TimeUnit;

/**
 * All the constants used with the {@link S3AFileSystem}.
 *
 * Some of the strings are marked as {@code Unstable}. This means
 * that they may be unsupported in future; at which point they will be marked
 * as deprecated and simply ignored.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class Constants {

  private Constants() {
  }

  /**
   * default hadoop temp dir on local system: {@value}.
   */
  public static final String HADOOP_TMP_DIR = "hadoop.tmp.dir";

  /** The minimum multipart size which S3 supports. */
  public static final int MULTIPART_MIN_SIZE = 5 * 1024 * 1024;

  // s3 access key
  public static final String ACCESS_KEY = "fs.s3a.access.key";

  // s3 secret key
  public static final String SECRET_KEY = "fs.s3a.secret.key";

  // aws credentials providers
  public static final String AWS_CREDENTIALS_PROVIDER =
      "fs.s3a.aws.credentials.provider";

  /**
   * Extra set of security credentials which will be prepended to that
   * set in {@code "hadoop.security.credential.provider.path"}.
   * This extra option allows for per-bucket overrides.
   */
  public static final String S3A_SECURITY_CREDENTIAL_PROVIDER_PATH =
      "fs.s3a.security.credential.provider.path";

  /**
   * session token for when using TemporaryAWSCredentialsProvider: : {@value}.
   */
  public static final String SESSION_TOKEN = "fs.s3a.session.token";

  /**
   * ARN of AWS Role to request: {@value}.
   */
  public static final String ASSUMED_ROLE_ARN =
      "fs.s3a.assumed.role.arn";

  /**
   * Session name for the assumed role, must be valid characters according
   * to the AWS APIs: {@value}.
   * If not set, one is generated from the current Hadoop/Kerberos username.
   */
  public static final String ASSUMED_ROLE_SESSION_NAME =
      "fs.s3a.assumed.role.session.name";

  /**
   * Duration of assumed roles before a refresh is attempted.
   */
  public static final String ASSUMED_ROLE_SESSION_DURATION =
      "fs.s3a.assumed.role.session.duration";

  /**
   * Security Token Service Endpoint: {@value}.
   * If unset, uses the default endpoint.
   */
  public static final String ASSUMED_ROLE_STS_ENDPOINT =
      "fs.s3a.assumed.role.sts.endpoint";

  /**
   * Default endpoint for session tokens: {@value}.
   * This is the central STS endpoint which, for v3 signing, can
   * issue STS tokens for any region.
   */
  public static final String DEFAULT_ASSUMED_ROLE_STS_ENDPOINT = "";

  /**
   * Region for the STS endpoint; needed if the endpoint
   * is set to anything other then the central one.: {@value}.
   */
  public static final String ASSUMED_ROLE_STS_ENDPOINT_REGION =
      "fs.s3a.assumed.role.sts.endpoint.region";

  /**
   * Default value for the STS endpoint region; needed for
   * v4 signing: {@value}.
   */
  public static final String ASSUMED_ROLE_STS_ENDPOINT_REGION_DEFAULT = "";

  /**
   * Default duration of an assumed role: {@value}.
   */
  public static final String ASSUMED_ROLE_SESSION_DURATION_DEFAULT = "1h";

  /**
   * List of providers to authenticate for the assumed role: {@value}.
   */
  public static final String ASSUMED_ROLE_CREDENTIALS_PROVIDER =
      "fs.s3a.assumed.role.credentials.provider";

  /**
   * JSON policy containing the policy to apply to the role: {@value}.
   * This is not used for delegation tokens, which generate the policy
   * automatically, and restrict it to the S3, KMS and S3Guard services
   * needed.
   */
  public static final String ASSUMED_ROLE_POLICY =
      "fs.s3a.assumed.role.policy";

  public static final String ASSUMED_ROLE_CREDENTIALS_DEFAULT =
      SimpleAWSCredentialsProvider.NAME;


  // the maximum number of tasks cached if all threads are already uploading
  public static final String MAX_TOTAL_TASKS = "fs.s3a.max.total.tasks";

  public static final int DEFAULT_MAX_TOTAL_TASKS = 32;

  // number of simultaneous connections to s3
  public static final String MAXIMUM_CONNECTIONS = "fs.s3a.connection.maximum";
  public static final int DEFAULT_MAXIMUM_CONNECTIONS = 48;

  // connect to s3 over ssl?
  public static final String SECURE_CONNECTIONS =
      "fs.s3a.connection.ssl.enabled";
  public static final boolean DEFAULT_SECURE_CONNECTIONS = true;

  // use OpenSSL or JSEE for secure connections
  public static final String SSL_CHANNEL_MODE =  "fs.s3a.ssl.channel.mode";
  public static final DelegatingSSLSocketFactory.SSLChannelMode
      DEFAULT_SSL_CHANNEL_MODE =
          DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE;

  /**
   * Endpoint. For v4 signing and/or better performance,
   * this should be the specific endpoint of the region
   * in which the bucket is hosted.
   */
  public static final String ENDPOINT = "fs.s3a.endpoint";

  /**
   * Default value of s3 endpoint: {@value}.
   * It tells the AWS client to work it out by asking the central
   * endpoint where the bucket lives; caching that
   * value in the client for the life of the process.
   * <p>
   * Note: previously this constant was defined as
   * {@link #CENTRAL_ENDPOINT}, however the actual
   * S3A client code used "" as the default when
   * {@link #ENDPOINT} was unset.
   * As core-default.xml also set the endpoint to "",
   * the empty string has long been the <i>real</i>
   * default value.
   */
  public static final String DEFAULT_ENDPOINT = "";

  /**
   * The central endpoint :{@value}.
   */
  public static final String CENTRAL_ENDPOINT = "s3.amazonaws.com";

  //Enable path style access? Overrides default virtual hosting
  public static final String PATH_STYLE_ACCESS = "fs.s3a.path.style.access";

  //connect to s3 through a proxy server?
  public static final String PROXY_HOST = "fs.s3a.proxy.host";
  public static final String PROXY_PORT = "fs.s3a.proxy.port";
  public static final String PROXY_USERNAME = "fs.s3a.proxy.username";
  public static final String PROXY_PASSWORD = "fs.s3a.proxy.password";
  public static final String PROXY_DOMAIN = "fs.s3a.proxy.domain";
  public static final String PROXY_WORKSTATION = "fs.s3a.proxy.workstation";

  /**
   * Number of times the AWS client library should retry errors before
   * escalating to the S3A code: {@value}.
   */
  public static final String MAX_ERROR_RETRIES = "fs.s3a.attempts.maximum";

  /**
   * Default number of times the AWS client library should retry errors before
   * escalating to the S3A code: {@value}.
   */
  public static final int DEFAULT_MAX_ERROR_RETRIES = 10;

  /**
   * Experimental/Unstable feature: should the AWS client library retry
   * throttle responses before escalating to the S3A code: {@value}.
   *
   * When set to false, the S3A connector sees all S3 throttle events,
   * And so can update it counters and the metrics, and use its own retry
   * policy.
   * However, this may have adverse effects on some operations where the S3A
   * code cannot retry as efficiently as the AWS client library.
   *
   * This only applies to S3 operations, not to DynamoDB or other services.
   */
  @InterfaceStability.Unstable
  public static final String EXPERIMENTAL_AWS_INTERNAL_THROTTLING =
      "fs.s3a.experimental.aws.s3.throttling";

  /**
   * Default value of {@link #EXPERIMENTAL_AWS_INTERNAL_THROTTLING},
   * value: {@value}.
   */
  @InterfaceStability.Unstable
  public static final boolean EXPERIMENTAL_AWS_INTERNAL_THROTTLING_DEFAULT =
      true;

  // seconds until we give up trying to establish a connection to s3
  public static final String ESTABLISH_TIMEOUT =
      "fs.s3a.connection.establish.timeout";
  public static final int DEFAULT_ESTABLISH_TIMEOUT = 50000;

  // seconds until we give up on a connection to s3
  public static final String SOCKET_TIMEOUT = "fs.s3a.connection.timeout";
  public static final int DEFAULT_SOCKET_TIMEOUT = 200000;

  // milliseconds until a request is timed-out
  public static final String REQUEST_TIMEOUT =
      "fs.s3a.connection.request.timeout";
  public static final int DEFAULT_REQUEST_TIMEOUT = 0;

  // socket send buffer to be used in Amazon client
  public static final String SOCKET_SEND_BUFFER = "fs.s3a.socket.send.buffer";
  public static final int DEFAULT_SOCKET_SEND_BUFFER = 8 * 1024;

  // socket send buffer to be used in Amazon client
  public static final String SOCKET_RECV_BUFFER = "fs.s3a.socket.recv.buffer";
  public static final int DEFAULT_SOCKET_RECV_BUFFER = 8 * 1024;

  // number of records to get while paging through a directory listing
  public static final String MAX_PAGING_KEYS = "fs.s3a.paging.maximum";
  public static final int DEFAULT_MAX_PAGING_KEYS = 5000;

  // the maximum number of threads to allow in the pool used by TransferManager
  public static final String MAX_THREADS = "fs.s3a.threads.max";
  public static final int DEFAULT_MAX_THREADS = 10;

  // the time an idle thread waits before terminating
  public static final String KEEPALIVE_TIME = "fs.s3a.threads.keepalivetime";
  public static final int DEFAULT_KEEPALIVE_TIME = 60;

  // size of each of or multipart pieces in bytes
  public static final String MULTIPART_SIZE = "fs.s3a.multipart.size";
  public static final long DEFAULT_MULTIPART_SIZE = 67108864; // 64M

  // minimum size in bytes before we start a multipart uploads or copy
  public static final String MIN_MULTIPART_THRESHOLD =
      "fs.s3a.multipart.threshold";
  public static final long DEFAULT_MIN_MULTIPART_THRESHOLD = 134217728; // 128M

  //enable multiobject-delete calls?
  public static final String ENABLE_MULTI_DELETE =
      "fs.s3a.multiobjectdelete.enable";

  /**
   * Number of objects to delete in a single multi-object delete {@value}.
   * Max: 1000.
   *
   * A bigger value it means fewer POST requests when deleting a directory
   * tree with many objects.
   * However, as you are limited to only a a few thousand requests per
   * second against a single partition of an S3 bucket,
   * a large page size can easily overload the bucket and so trigger
   * throttling.
   *
   * Furthermore, as the reaction to this request is being throttled
   * is simply to retry it -it can take a while for the situation to go away.
   * While a large value may give better numbers on tests and benchmarks
   * where only a single operations being executed, once multiple
   * applications start working with the same bucket these large
   * deletes can be highly disruptive.
   */
  public static final String BULK_DELETE_PAGE_SIZE =
      "fs.s3a.bulk.delete.page.size";

  /**
   * Default Number of objects to delete in a single multi-object
   * delete: {@value}.
   */
  public static final int BULK_DELETE_PAGE_SIZE_DEFAULT = 250;

  // comma separated list of directories
  public static final String BUFFER_DIR = "fs.s3a.buffer.dir";

  // switch to the fast block-by-block upload mechanism
  // this is the only supported upload mechanism
  @Deprecated
  public static final String FAST_UPLOAD = "fs.s3a.fast.upload";
  @Deprecated
  public static final boolean DEFAULT_FAST_UPLOAD = false;

  //initial size of memory buffer for a fast upload
  @Deprecated
  public static final String FAST_BUFFER_SIZE = "fs.s3a.fast.buffer.size";
  public static final int DEFAULT_FAST_BUFFER_SIZE = 1048576; //1MB

  /**
   * What buffer to use.
   * Default is {@link #FAST_UPLOAD_BUFFER_DISK}
   * Value: {@value}
   */
  public static final String FAST_UPLOAD_BUFFER =
      "fs.s3a.fast.upload.buffer";

  /**
   * Buffer blocks to disk: {@value}.
   * Capacity is limited to available disk space.
   */

  public static final String FAST_UPLOAD_BUFFER_DISK = "disk";

  /**
   * Use an in-memory array. Fast but will run of heap rapidly: {@value}.
   */
  public static final String FAST_UPLOAD_BUFFER_ARRAY = "array";

  /**
   * Use a byte buffer. May be more memory efficient than the
   * {@link #FAST_UPLOAD_BUFFER_ARRAY}: {@value}.
   */
  public static final String FAST_UPLOAD_BYTEBUFFER = "bytebuffer";

  /**
   * Default buffer option: {@value}.
   */
  public static final String DEFAULT_FAST_UPLOAD_BUFFER =
      FAST_UPLOAD_BUFFER_DISK;

  /**
   * Maximum Number of blocks a single output stream can have
   * active (uploading, or queued to the central FileSystem
   * instance's pool of queued operations.
   * This stops a single stream overloading the shared thread pool.
   * {@value}
   * <p>
   * Default is {@link #DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS}
   */
  public static final String FAST_UPLOAD_ACTIVE_BLOCKS =
      "fs.s3a.fast.upload.active.blocks";

  /**
   * Limit of queued block upload operations before writes
   * block. Value: {@value}
   */
  public static final int DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS = 4;

  /**
   * Rather than raise an exception when an attempt is
   * made to call the Syncable APIs, warn and downgrade.
   * Value: {@value}.
   */
  public static final String DOWNGRADE_SYNCABLE_EXCEPTIONS =
      "fs.s3a.downgrade.syncable.exceptions";

  /**
   * Default value for syncable invocation.
   * Value: {@value}.
   */
  public static final boolean DOWNGRADE_SYNCABLE_EXCEPTIONS_DEFAULT =
      false;

  /**
   * The capacity of executor queues for operations other than block
   * upload, where {@link #FAST_UPLOAD_ACTIVE_BLOCKS} is used instead.
   * This should be less than {@link #MAX_THREADS} for fair
   * submission.
   * Value: {@value}.
   */
  public static final String EXECUTOR_CAPACITY = "fs.s3a.executor.capacity";

  /**
   * The capacity of executor queues for operations other than block
   * upload, where {@link #FAST_UPLOAD_ACTIVE_BLOCKS} is used instead.
   * Value: {@value}
   */
  public static final int DEFAULT_EXECUTOR_CAPACITY = 16;

  // Private | PublicRead | PublicReadWrite | AuthenticatedRead |
  // LogDeliveryWrite | BucketOwnerRead | BucketOwnerFullControl
  public static final String CANNED_ACL = "fs.s3a.acl.default";
  public static final String DEFAULT_CANNED_ACL = "";

  // should we try to purge old multipart uploads when starting up
  public static final String PURGE_EXISTING_MULTIPART =
      "fs.s3a.multipart.purge";
  public static final boolean DEFAULT_PURGE_EXISTING_MULTIPART = false;

  // purge any multipart uploads older than this number of seconds
  public static final String PURGE_EXISTING_MULTIPART_AGE =
      "fs.s3a.multipart.purge.age";
  public static final long DEFAULT_PURGE_EXISTING_MULTIPART_AGE = 86400;

  // s3 server-side encryption, see S3AEncryptionMethods for valid options
  public static final String SERVER_SIDE_ENCRYPTION_ALGORITHM =
      "fs.s3a.server-side-encryption-algorithm";

  /**
   * The standard encryption algorithm AWS supports.
   * Different implementations may support others (or none).
   * Use the S3AEncryptionMethods instead when configuring
   * which Server Side Encryption to use.
   * Value: "{@value}".
   */
  @Deprecated
  public static final String SERVER_SIDE_ENCRYPTION_AES256 =
      "AES256";

  /**
   * Used to specify which AWS KMS key to use if
   * {@link #SERVER_SIDE_ENCRYPTION_ALGORITHM} is
   * {@code SSE-KMS} (will default to aws/s3
   * master key if left blank).
   * With with {@code SSE_C}, the base-64 encoded AES 256 key.
   * May be set within a JCEKS file.
   * Value: "{@value}".
   */
  public static final String SERVER_SIDE_ENCRYPTION_KEY =
      "fs.s3a.server-side-encryption.key";

  /**
   * List of custom Signers. The signer class will be loaded, and the signer
   * name will be associated with this signer class in the S3 SDK.
   * Examples
   * CustomSigner {@literal ->} 'CustomSigner:org.apache...CustomSignerClass'
   * CustomSigners {@literal ->} 'CSigner1:CSigner1Class,CSigner2:CSigner2Class'
   * Initializer {@literal ->} 'CSigner1:CSigner1Class:CSigner1InitializerClass'
   * With Existing {@literal ->} 'AWS4Signer,CSigner1,CSigner2:CSigner2Class'
   */
  public static final String CUSTOM_SIGNERS = "fs.s3a.custom.signers";

  /**
   * There's 3 parameters that can be used to specify a non-default signing
   * algorithm.<br>
   * fs.s3a.signing-algorithm - This property has existed for the longest time.
   * If specified, without either of the other 2 properties being specified,
   * this signing algorithm will be used for S3 and DDB (S3Guard). <br>
   * The other 2 properties override this value for S3 or DDB. <br>
   * fs.s3a.s3.signing-algorithm - Allows overriding the S3 Signing algorithm.
   * This does not affect DDB. Specifying this property without specifying
   * fs.s3a.signing-algorithm will only update the signing algorithm for S3
   * requests, and the default will be used for DDB.<br>
   * fs.s3a.ddb.signing-algorithm - Allows overriding the DDB Signing algorithm.
   * This does not affect S3. Specifying this property without specifying
   * fs.s3a.signing-algorithm will only update the signing algorithm for
   * DDB requests, and the default will be used for S3.
   */
  public static final String SIGNING_ALGORITHM = "fs.s3a.signing-algorithm";

  public static final String SIGNING_ALGORITHM_S3 =
      "fs.s3a." + Constants.AWS_SERVICE_IDENTIFIER_S3.toLowerCase()
          + ".signing-algorithm";

  public static final String SIGNING_ALGORITHM_DDB =
      "fs.s3a." + Constants.AWS_SERVICE_IDENTIFIER_DDB.toLowerCase()
          + "signing-algorithm";

  public static final String SIGNING_ALGORITHM_STS =
      "fs.s3a." + Constants.AWS_SERVICE_IDENTIFIER_STS.toLowerCase()
          + "signing-algorithm";

  public static final String S3N_FOLDER_SUFFIX = "_$folder$";
  public static final String FS_S3A_BLOCK_SIZE = "fs.s3a.block.size";
  public static final String FS_S3A = "s3a";

  /** Prefix for all S3A properties: {@value}. */
  public static final String FS_S3A_PREFIX = "fs.s3a.";

  /** Prefix for S3A bucket-specific properties: {@value}. */
  public static final String FS_S3A_BUCKET_PREFIX = "fs.s3a.bucket.";

  /**
   * Default port for this is 443: HTTPS.
   */
  public static final int S3A_DEFAULT_PORT = 443;

  public static final String USER_AGENT_PREFIX = "fs.s3a.user.agent.prefix";

  /** Whether or not to allow MetadataStore to be source of truth for a path prefix */
  public static final String AUTHORITATIVE_PATH = "fs.s3a.authoritative.path";
  public static final String[] DEFAULT_AUTHORITATIVE_PATH = {};

  /** Whether or not to allow MetadataStore to be source of truth. */
  public static final String METADATASTORE_AUTHORITATIVE =
      "fs.s3a.metadatastore.authoritative";
  public static final boolean DEFAULT_METADATASTORE_AUTHORITATIVE = false;

  /**
   * Bucket validation parameter which can be set by client. This will be
   * used in {@code S3AFileSystem.initialize(URI, Configuration)}.
   * Value: {@value}
   */
  public static final String S3A_BUCKET_PROBE = "fs.s3a.bucket.probe";

  /**
   * Default value of bucket validation parameter. An existence of bucket
   * will be validated using {@code S3AFileSystem.verifyBucketExistsV2()}.
   * Value: {@value}
   */
  public static final int S3A_BUCKET_PROBE_DEFAULT = 0;

  /**
   * How long a directory listing in the MS is considered as authoritative.
   */
  public static final String METADATASTORE_METADATA_TTL =
      "fs.s3a.metadatastore.metadata.ttl";

  /**
   * Default TTL in milliseconds: 15 minutes.
   */
  public static final long DEFAULT_METADATASTORE_METADATA_TTL =
      TimeUnit.MINUTES.toMillis(15);

  /** read ahead buffer size to prevent connection re-establishments. */
  public static final String READAHEAD_RANGE = "fs.s3a.readahead.range";
  public static final long DEFAULT_READAHEAD_RANGE = 64 * 1024;

  /**
   * Which input strategy to use for buffering, seeking and similar when
   * reading data.
   * Value: {@value}
   */
  @InterfaceStability.Unstable
  public static final String INPUT_FADVISE =
      "fs.s3a.experimental.input.fadvise";

  /**
   * General input. Some seeks, some reads.
   * Value: {@value}
   */
  @InterfaceStability.Unstable
  public static final String INPUT_FADV_NORMAL = "normal";

  /**
   * Optimized for sequential access.
   * Value: {@value}
   */
  @InterfaceStability.Unstable
  public static final String INPUT_FADV_SEQUENTIAL = "sequential";

  /**
   * Optimized purely for random seek+read/positionedRead operations;
   * The performance of sequential IO may be reduced in exchange for
   * more efficient {@code seek()} operations.
   * Value: {@value}
   */
  @InterfaceStability.Unstable
  public static final String INPUT_FADV_RANDOM = "random";

  /**
   * Gauge name for the input policy : {@value}.
   * This references an enum currently exclusive to the S3A stream.
   */
  public static final String STREAM_READ_GAUGE_INPUT_POLICY =
      "stream_read_gauge_input_policy";

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static final String S3_CLIENT_FACTORY_IMPL =
      "fs.s3a.s3.client.factory.impl";

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static final Class<? extends S3ClientFactory>
      DEFAULT_S3_CLIENT_FACTORY_IMPL =
          DefaultS3ClientFactory.class;

  /**
   * Maximum number of partitions in a multipart upload: {@value}.
   */
  @InterfaceAudience.Private
  public static final int MAX_MULTIPART_COUNT = 10000;

  /* Constants. */
  public static final String S3_METADATA_STORE_IMPL =
      "fs.s3a.metadatastore.impl";

  /**
   * Whether to fail when there is an error writing to the metadata store.
   */
  public static final String FAIL_ON_METADATA_WRITE_ERROR =
      "fs.s3a.metadatastore.fail.on.write.error";

  /**
   * Default value ({@value}) for FAIL_ON_METADATA_WRITE_ERROR.
   */
  public static final boolean FAIL_ON_METADATA_WRITE_ERROR_DEFAULT = true;

  /** Minimum period of time (in milliseconds) to keep metadata (may only be
   * applied when a prune command is manually run).
   */
  @InterfaceStability.Unstable
  public static final String S3GUARD_CLI_PRUNE_AGE =
      "fs.s3a.s3guard.cli.prune.age";

  /**
   * The region of the DynamoDB service.
   *
   * This config has no default value. If the user does not set this, the
   * S3Guard will operate table in the associated S3 bucket region.
   */
  public static final String S3GUARD_DDB_REGION_KEY =
      "fs.s3a.s3guard.ddb.region";

  /**
   * The DynamoDB table name to use.
   *
   * This config has no default value. If the user does not set this, the
   * S3Guard implementation will use the respective S3 bucket name.
   */
  public static final String S3GUARD_DDB_TABLE_NAME_KEY =
      "fs.s3a.s3guard.ddb.table";

  /**
   * A prefix for adding tags to the DDB Table upon creation.
   *
   * For example:
   * fs.s3a.s3guard.ddb.table.tag.mytag
   */
  public static final String S3GUARD_DDB_TABLE_TAG =
      "fs.s3a.s3guard.ddb.table.tag.";

  /**
   * Whether to create the DynamoDB table if the table does not exist.
   * Value: {@value}.
   */
  public static final String S3GUARD_DDB_TABLE_CREATE_KEY =
      "fs.s3a.s3guard.ddb.table.create";

  /**
   * Read capacity when creating a table.
   * When it and the write capacity are both "0", a per-request table is
   * created.
   * Value: {@value}.
   */
  public static final String S3GUARD_DDB_TABLE_CAPACITY_READ_KEY =
      "fs.s3a.s3guard.ddb.table.capacity.read";

  /**
   * Default read capacity when creating a table.
   * Value: {@value}.
   */
  public static final long S3GUARD_DDB_TABLE_CAPACITY_READ_DEFAULT = 0;

  /**
   * Write capacity when creating a table.
   * When it and the read capacity are both "0", a per-request table is
   * created.
   * Value: {@value}.
   */
  public static final String S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY =
      "fs.s3a.s3guard.ddb.table.capacity.write";

  /**
   * Default write capacity when creating a table.
   * Value: {@value}.
   */
  public static final long S3GUARD_DDB_TABLE_CAPACITY_WRITE_DEFAULT = 0;

  /**
   * Whether server-side encryption (SSE) is enabled or disabled on the table.
   * By default it's disabled, meaning SSE is set to AWS owned CMK.
   * @see com.amazonaws.services.dynamodbv2.model.SSESpecification#setEnabled
   */
  public static final String S3GUARD_DDB_TABLE_SSE_ENABLED =
      "fs.s3a.s3guard.ddb.table.sse.enabled";

  /**
   * The KMS Master Key (CMK) used for the KMS encryption on the table.
   *
   * To specify a CMK, this config value can be its key ID, Amazon Resource
   * Name (ARN), alias name, or alias ARN. Users only provide this config
   * if the key is different from the default DynamoDB KMS Master Key, which is
   * alias/aws/dynamodb.
   */
  public static final String S3GUARD_DDB_TABLE_SSE_CMK =
      "fs.s3a.s3guard.ddb.table.sse.cmk";

  /**
   * The maximum put or delete requests per BatchWriteItem request.
   *
   * Refer to Amazon API reference for this limit.
   */
  public static final int S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT = 25;

  public static final String S3GUARD_DDB_MAX_RETRIES =
      "fs.s3a.s3guard.ddb.max.retries";

  /**
   * Max retries on batched/throttled DynamoDB operations before giving up and
   * throwing an IOException.  Default is {@value}. See core-default.xml for
   * more detail.
   */
  public static final int S3GUARD_DDB_MAX_RETRIES_DEFAULT =
      DEFAULT_MAX_ERROR_RETRIES;

  public static final String S3GUARD_DDB_THROTTLE_RETRY_INTERVAL =
      "fs.s3a.s3guard.ddb.throttle.retry.interval";
  public static final String S3GUARD_DDB_THROTTLE_RETRY_INTERVAL_DEFAULT =
      "100ms";

  /**
   * Period of time (in milliseconds) to sleep between batches of writes.
   * Currently only applies to prune operations, as they are naturally a
   * lower priority than other operations.
   */
  @InterfaceStability.Unstable
  public static final String S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY =
      "fs.s3a.s3guard.ddb.background.sleep";
  public static final int S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_DEFAULT = 25;

  /**
   * The default "Null" metadata store: {@value}.
   */
  public static final String S3GUARD_METASTORE_NULL
      = "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore";

  /**
   * Use Local memory for the metadata: {@value}.
   * This is not coherent across processes and must be used for testing only.
   */
  @InterfaceStability.Unstable
  public static final String S3GUARD_METASTORE_LOCAL
      = "org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore";

  /**
   * Maximum number of records in LocalMetadataStore.
   */
  @InterfaceStability.Unstable
  public static final String S3GUARD_METASTORE_LOCAL_MAX_RECORDS =
      "fs.s3a.s3guard.local.max_records";
  public static final int DEFAULT_S3GUARD_METASTORE_LOCAL_MAX_RECORDS = 256;

  /**
   * Time to live in milliseconds in LocalMetadataStore.
   * If zero, time-based expiration is disabled.
   */
  @InterfaceStability.Unstable
  public static final String S3GUARD_METASTORE_LOCAL_ENTRY_TTL =
      "fs.s3a.s3guard.local.ttl";
  public static final int DEFAULT_S3GUARD_METASTORE_LOCAL_ENTRY_TTL
      = 60 * 1000;

  /**
   * Use DynamoDB for the metadata: {@value}.
   */
  public static final String S3GUARD_METASTORE_DYNAMO
      = "org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore";

  /**
   * The warn level if S3Guard is disabled.
   */
  public static final String S3GUARD_DISABLED_WARN_LEVEL
      = "fs.s3a.s3guard.disabled.warn.level";
  public static final String DEFAULT_S3GUARD_DISABLED_WARN_LEVEL =
      "SILENT";

  /**
   * Inconsistency (visibility delay) injection settings.
   */
  @InterfaceStability.Unstable
  public static final String FAIL_INJECT_INCONSISTENCY_KEY =
      "fs.s3a.failinject.inconsistency.key.substring";

  @InterfaceStability.Unstable
  public static final String FAIL_INJECT_INCONSISTENCY_MSEC =
      "fs.s3a.failinject.inconsistency.msec";

  @InterfaceStability.Unstable
  public static final String FAIL_INJECT_INCONSISTENCY_PROBABILITY =
      "fs.s3a.failinject.inconsistency.probability";

  /**
   * S3 API level parameters.
   */
  @InterfaceStability.Unstable
  public static final String LIST_VERSION = "fs.s3a.list.version";

  @InterfaceStability.Unstable
  public static final int DEFAULT_LIST_VERSION = 2;

  @InterfaceStability.Unstable
  public static final String FAIL_INJECT_THROTTLE_PROBABILITY =
      "fs.s3a.failinject.throttle.probability";

  @InterfaceStability.Unstable
  public static final String FAIL_INJECT_CLIENT_FACTORY =
      "org.apache.hadoop.fs.s3a.InconsistentS3ClientFactory";

  /**
   * Number of times to retry any repeatable S3 client request on failure,
   * excluding throttling requests: {@value}.
   */
  public static final String RETRY_LIMIT = "fs.s3a.retry.limit";

  /**
   * Default retry limit: {@value}.
   */
  public static final int RETRY_LIMIT_DEFAULT = 7;

  /**
   * Interval between retry attempts.: {@value}.
   */
  public static final String RETRY_INTERVAL = "fs.s3a.retry.interval";

  /**
   * Default retry interval: {@value}.
   */
  public static final String RETRY_INTERVAL_DEFAULT = "500ms";

  /**
   * Number of times to retry any throttled request: {@value}.
   */
  public static final String RETRY_THROTTLE_LIMIT =
      "fs.s3a.retry.throttle.limit";

  /**
   * Default throttled retry limit: {@value}.
   */
  public static final int RETRY_THROTTLE_LIMIT_DEFAULT = 20;

  /**
   * Interval between retry attempts on throttled requests: {@value}.
   */
  public static final String RETRY_THROTTLE_INTERVAL =
      "fs.s3a.retry.throttle.interval";

  /**
   * Default throttled retry interval: {@value}.
   */
  public static final String RETRY_THROTTLE_INTERVAL_DEFAULT = "500ms";

  /**
   * Should etags be exposed as checksums?
   */
  public static final String ETAG_CHECKSUM_ENABLED =
      "fs.s3a.etag.checksum.enabled";

  /**
   * Default value: false.
   */
  public static final boolean ETAG_CHECKSUM_ENABLED_DEFAULT = false;

  /**
   * Where to get the value to use in change detection.  E.g. eTag, or
   * versionId?
   */
  public static final String CHANGE_DETECT_SOURCE
      = "fs.s3a.change.detection.source";

  /**
   * eTag as the change detection mechanism.
   */
  public static final String CHANGE_DETECT_SOURCE_ETAG = "etag";

  /**
   * Object versionId as the change detection mechanism.
   */
  public static final String CHANGE_DETECT_SOURCE_VERSION_ID = "versionid";

  /**
   * Default change detection mechanism: eTag.
   */
  public static final String CHANGE_DETECT_SOURCE_DEFAULT =
      CHANGE_DETECT_SOURCE_ETAG;

  /**
   * Mode to run change detection in.  Server side comparison?  Client side
   * comparison? Client side compare and warn rather than exception?  Don't
   * bother at all?
   */
  public static final String CHANGE_DETECT_MODE =
      "fs.s3a.change.detection.mode";

  /**
   * Change is detected on the client side by comparing the returned id with the
   * expected id.  A difference results in {@link RemoteFileChangedException}.
   */
  public static final String CHANGE_DETECT_MODE_CLIENT = "client";

  /**
   * Change is detected by passing the expected value in the GetObject request.
   * If the expected value is unavailable, {@link RemoteFileChangedException} is
   * thrown.
   */
  public static final String CHANGE_DETECT_MODE_SERVER = "server";

  /**
   * Change is detected on the client side by comparing the returned id with the
   * expected id.  A difference results in a WARN level message being logged.
   */
  public static final String CHANGE_DETECT_MODE_WARN = "warn";

  /**
   * Change detection is turned off.  Readers may see inconsistent results due
   * to concurrent writes without any exception or warning messages.  May be
   * useful with third-party S3 API implementations that don't support one of
   * the change detection modes.
   */
  public static final String CHANGE_DETECT_MODE_NONE = "none";

  /**
   * Default change detection mode: server.
   */
  public static final String CHANGE_DETECT_MODE_DEFAULT =
      CHANGE_DETECT_MODE_SERVER;

  /**
   * If true, raises a {@link RemoteFileChangedException} exception when S3
   * doesn't provide the attribute defined by fs.s3a.change.detection.source.
   * For example, if source is versionId, but object versioning is not enabled
   * on the bucket, or alternatively if source is eTag and a third-party S3
   * implementation that doesn't return eTag is used.
   * <p>
   * When false, only a warning message will be logged for this condition.
   */
  public static final String CHANGE_DETECT_REQUIRE_VERSION =
      "fs.s3a.change.detection.version.required";

  /**
   * Default change detection require version: true.
   */
  public static final boolean CHANGE_DETECT_REQUIRE_VERSION_DEFAULT = true;

  /**
   * Number of times to retry any repeatable S3 client request on failure,
   * excluding throttling requests: {@value}.
   */
  public static final String S3GUARD_CONSISTENCY_RETRY_LIMIT =
      "fs.s3a.s3guard.consistency.retry.limit";

  /**
   * Default retry limit: {@value}.
   */
  public static final int S3GUARD_CONSISTENCY_RETRY_LIMIT_DEFAULT = 7;

  /**
   * Initial retry interval: {@value}.
   */
  public static final String S3GUARD_CONSISTENCY_RETRY_INTERVAL =
      "fs.s3a.s3guard.consistency.retry.interval";

  /**
   * Default initial retry interval: {@value}.
   * The consistency retry probe uses exponential backoff, because
   * each probe can cause the S3 load balancers to retain any 404 in
   * its cache for longer. See HADOOP-16490.
   */
  public static final String S3GUARD_CONSISTENCY_RETRY_INTERVAL_DEFAULT =
      "2s";

  public static final String AWS_SERVICE_IDENTIFIER_S3 = "S3";
  public static final String AWS_SERVICE_IDENTIFIER_DDB = "DDB";
  public static final String AWS_SERVICE_IDENTIFIER_STS = "STS";

  /**
   * How long to wait for the thread pool to terminate when cleaning up.
   * Value: {@value} seconds.
   */
  public static final int THREAD_POOL_SHUTDOWN_DELAY_SECONDS = 30;

  /**
   * Policy for directory markers.
   * This is a new feature of HADOOP-13230 which addresses
   * some scale, performance and permissions issues -but
   * at the risk of backwards compatibility.
   */
  public static final String DIRECTORY_MARKER_POLICY =
      "fs.s3a.directory.marker.retention";

  /**
   * Delete directory markers. This is the backwards compatible option.
   * Value: {@value}.
   */
  public static final String DIRECTORY_MARKER_POLICY_DELETE =
      "delete";

  /**
   * Retain directory markers.
   * Value: {@value}.
   */
  public static final String DIRECTORY_MARKER_POLICY_KEEP =
      "keep";

  /**
   * Retain directory markers in authoritative directory trees only.
   * Value: {@value}.
   */
  public static final String DIRECTORY_MARKER_POLICY_AUTHORITATIVE =
      "authoritative";

  /**
   * Default retention policy: {@value}.
   */
  public static final String DEFAULT_DIRECTORY_MARKER_POLICY =
      DIRECTORY_MARKER_POLICY_DELETE;


  /**
   * {@code PathCapabilities} probe to verify that an S3A Filesystem
   * has the changes needed to safely work with buckets where
   * directoy markers have not been deleted.
   * Value: {@value}.
   */
  public static final String STORE_CAPABILITY_DIRECTORY_MARKER_AWARE
      = "fs.s3a.capability.directory.marker.aware";

  /**
   * {@code PathCapabilities} probe to indicate that the filesystem
   * keeps directory markers.
   * Value: {@value}.
   */
  public static final String STORE_CAPABILITY_DIRECTORY_MARKER_POLICY_KEEP
      = "fs.s3a.capability.directory.marker.policy.keep";

  /**
   * {@code PathCapabilities} probe to indicate that the filesystem
   * deletes directory markers.
   * Value: {@value}.
   */
  public static final String STORE_CAPABILITY_DIRECTORY_MARKER_POLICY_DELETE
      = "fs.s3a.capability.directory.marker.policy.delete";

  /**
   * {@code PathCapabilities} probe to indicate that the filesystem
   * keeps directory markers in authoritative paths only.
   * Value: {@value}.
   */
  public static final String
      STORE_CAPABILITY_DIRECTORY_MARKER_POLICY_AUTHORITATIVE =
      "fs.s3a.capability.directory.marker.policy.authoritative";

  /**
   * {@code PathCapabilities} probe to indicate that a path
   * keeps directory markers.
   * Value: {@value}.
   */
  public static final String STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_KEEP
      = "fs.s3a.capability.directory.marker.action.keep";

  /**
   * {@code PathCapabilities} probe to indicate that a path
   * deletes directory markers.
   * Value: {@value}.
   */
  public static final String STORE_CAPABILITY_DIRECTORY_MARKER_ACTION_DELETE
      = "fs.s3a.capability.directory.marker.action.delete";

  /**
   * To comply with the XAttr rules, all headers of the object retrieved
   * through the getXAttr APIs have the prefix: {@value}.
   */
  public static final String XA_HEADER_PREFIX = "header.";

}
