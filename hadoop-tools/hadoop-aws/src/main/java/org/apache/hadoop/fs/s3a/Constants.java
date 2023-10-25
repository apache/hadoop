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
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;

import java.util.concurrent.TimeUnit;

/**
 * Constants used with the {@link S3AFileSystem}.
 *
 * Some of the strings are marked as {@code Unstable}. This means
 * that they may be Unsupported in future; at which point they will be marked
 * as deprecated and simply ignored.
 *
 * All S3Guard related constants are marked as Deprecated and either ignored (ddb config)
 * or rejected (setting the metastore to anything other than the null store)
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
   * automatically, and restrict it to the S3 and KMS services
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
  public static final int DEFAULT_MAXIMUM_CONNECTIONS = 96;

  /**
   * Configuration option to configure expiration time of
   * s3 http connection from the connection pool in milliseconds: {@value}.
   */
  public static final String CONNECTION_TTL = "fs.s3a.connection.ttl";

  /**
   * Default value for {@code CONNECTION_TTL}: {@value}.
   */
  public static final long DEFAULT_CONNECTION_TTL = 5 * 60_000;

  // connect to s3 over ssl?
  public static final String SECURE_CONNECTIONS =
      "fs.s3a.connection.ssl.enabled";
  public static final boolean DEFAULT_SECURE_CONNECTIONS = true;

  /**
   * Configuration option for S3 Requester Pays feature: {@value}.
   */
  public static final String ALLOW_REQUESTER_PAYS = "fs.s3a.requester.pays.enabled";
  /**
   * Default configuration for {@value ALLOW_REQUESTER_PAYS}: {@value}.
   */
  public static final boolean DEFAULT_ALLOW_REQUESTER_PAYS = false;

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
  /** Is the proxy secured(proxyProtocol = HTTPS)? */
  public static final String PROXY_SECURED = "fs.s3a.proxy.ssl.enabled";

  /**
   * Number of times the AWS client library should retry errors before
   * escalating to the S3A code: {@value}.
   * The S3A connector does its own selective retries; the only time the AWS
   * SDK operations are not wrapped is during multipart copy via the AWS SDK
   * transfer manager.
   */
  public static final String MAX_ERROR_RETRIES = "fs.s3a.attempts.maximum";

  /**
   * Default number of times the AWS client library should retry errors before
   * escalating to the S3A code: {@value}.
   */
  public static final int DEFAULT_MAX_ERROR_RETRIES = 5;

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

  // milliseconds until we give up trying to establish a connection to s3
  public static final String ESTABLISH_TIMEOUT =
      "fs.s3a.connection.establish.timeout";
  public static final int DEFAULT_ESTABLISH_TIMEOUT = 5000;

  // milliseconds until we give up on a connection to s3
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
      true;

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

  /**
   * Content encoding: gzip, deflate, compress, br, etc.
   * Value {@value}.
   */
  public static final String CONTENT_ENCODING = "fs.s3a.object.content.encoding";

  /**
   * S3 storage class: standard, reduced_redundancy, intelligent_tiering etc.
   * Value {@value }.
   */
  public static final String STORAGE_CLASS = "fs.s3a.create.storage.class";

  /**
   * S3 Storage option: {@value}.
   */
  public static final String STORAGE_CLASS_STANDARD = "standard";

  /**
   * S3 Storage option: {@value}.
   */
  public static final String STORAGE_CLASS_REDUCED_REDUNDANCY = "reduced_redundancy";

  /**
   * S3 Storage option: {@value}.
   */
  public static final String STORAGE_CLASS_GLACIER = "glacier";

  /**
   * S3 Storage option: {@value}.
   */
  public static final String STORAGE_CLASS_STANDARD_INFREQUENT_ACCESS = "standard_ia";

  /**
   * S3 Storage option: {@value}.
   */
  public static final String STORAGE_CLASS_ONEZONE_INFREQUENT_ACCESS = "onezone_ia";

  /**
   * S3 Storage option: {@value}.
   */
  public static final String STORAGE_CLASS_INTELLIGENT_TIERING = "intelligent_tiering";

  /**
   * S3 Storage option: {@value}.
   */
  public static final String STORAGE_CLASS_DEEP_ARCHIVE = "deep_archive";

  /**
   * S3 Storage option: {@value}.
   */
  public static final String STORAGE_CLASS_OUTPOSTS = "outposts";

  /**
   * S3 Storage option: {@value}.
   */
  public static final String STORAGE_CLASS_GLACIER_INSTANT_RETRIEVAL = "glacier_ir";

  // should we try to purge old multipart uploads when starting up
  public static final String PURGE_EXISTING_MULTIPART =
      "fs.s3a.multipart.purge";
  public static final boolean DEFAULT_PURGE_EXISTING_MULTIPART = false;

  // purge any multipart uploads older than this number of seconds
  public static final String PURGE_EXISTING_MULTIPART_AGE =
      "fs.s3a.multipart.purge.age";
  public static final long DEFAULT_PURGE_EXISTING_MULTIPART_AGE = 86400;

  /**
   * s3 server-side encryption, see
   * {@link S3AEncryptionMethods} for valid options.
   *
   * {@value}
   */
  @Deprecated
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
  @Deprecated
  public static final String SERVER_SIDE_ENCRYPTION_KEY =
      "fs.s3a.server-side-encryption.key";

  /**
   * Set S3-server side encryption(SSE) or S3-Client side encryption(CSE)
   * algorithm. Check {@link S3AEncryptionMethods} for valid options.
   * <br>
   * value: {@value}
   */
  public static final String S3_ENCRYPTION_ALGORITHM =
      "fs.s3a.encryption.algorithm";

  /**
   * Set S3-SSE or S3-CSE encryption Key if required.
   * <br>
   * <i>Note:</i>
   *   <ul>
   *     <li>In case of S3-CSE this value needs to be set for CSE to work.</li>
   *     <li>In case of S3-SSE follow {@link #SERVER_SIDE_ENCRYPTION_KEY}</li>
   *   </ul>
   * value:{@value}
   */
  public static final String S3_ENCRYPTION_KEY =
      "fs.s3a.encryption.key";

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
   * Multiple parameters can be used to specify a non-default signing
   * algorithm.<br>
   * fs.s3a.signing-algorithm - This property has existed for the longest time.
   * If specified, without other properties being specified,
   * this signing algorithm will be used for all services. <br>
   * Another property overrides this value for S3. <br>
   * fs.s3a.s3.signing-algorithm - Allows overriding the S3 Signing algorithm.
   * Specifying this property without specifying
   * fs.s3a.signing-algorithm will only update the signing algorithm for S3
   * requests.
   * {@code fs.s3a.sts.signing-algorithm}: algorithm to use for STS interaction.
   */
  public static final String SIGNING_ALGORITHM = "fs.s3a.signing-algorithm";

  public static final String SIGNING_ALGORITHM_S3 =
      "fs.s3a." + Constants.AWS_SERVICE_IDENTIFIER_S3.toLowerCase()
          + ".signing-algorithm";

  @Deprecated
  public static final String SIGNING_ALGORITHM_DDB =
      "fs.s3a." + Constants.AWS_SERVICE_IDENTIFIER_DDB.toLowerCase()
          + "signing-algorithm";

  public static final String SIGNING_ALGORITHM_STS =
      "fs.s3a." + Constants.AWS_SERVICE_IDENTIFIER_STS.toLowerCase()
          + ".signing-algorithm";

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

  /**
   * Paths considered "authoritative".
   * When S3guard was supported, this skipped checks to s3 on directory listings.
   * It is also use to optionally disable marker retentation purely on these
   * paths -a feature which is still retained/available.
   * */
  public static final String AUTHORITATIVE_PATH = "fs.s3a.authoritative.path";
  public static final String[] DEFAULT_AUTHORITATIVE_PATH = {};

  /**
   * Whether or not to allow MetadataStore to be source of truth.
   * @deprecated no longer supported
   */
  @Deprecated
  public static final String METADATASTORE_AUTHORITATIVE =
      "fs.s3a.metadatastore.authoritative";
  @Deprecated
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
   * @deprecated no longer supported
   */
  @Deprecated
  public static final String METADATASTORE_METADATA_TTL =
      "fs.s3a.metadatastore.metadata.ttl";

  /**
   * Default TTL in milliseconds: 15 minutes.
   */
  @Deprecated
  public static final long DEFAULT_METADATASTORE_METADATA_TTL =
      TimeUnit.MINUTES.toMillis(15);

  /** read ahead buffer size to prevent connection re-establishments. */
  public static final String READAHEAD_RANGE = "fs.s3a.readahead.range";
  public static final long DEFAULT_READAHEAD_RANGE = 64 * 1024;

  /**
   * The threshold at which drain operations switch
   * to being asynchronous with the schedule/wait overhead
   * compared to synchronous.
   * Value: {@value}
   */
  public static final String ASYNC_DRAIN_THRESHOLD = "fs.s3a.input.async.drain.threshold";

  /**
   * This is a number based purely on experimentation in
   * {@code ITestS3AInputStreamPerformance}.
   * Value: {@value}
   */
  public static final int DEFAULT_ASYNC_DRAIN_THRESHOLD = 16_000;

  /**
   * Which input strategy to use for buffering, seeking and similar when
   * reading data.
   * Value: {@value}
   */
  public static final String INPUT_FADVISE =
      "fs.s3a.experimental.input.fadvise";

  /**
   * The default value for this FS.
   * Which for S3A, is adaptive.
   * Value: {@value}
   * @deprecated use the {@link Options.OpenFileOptions} value
   * in code which only needs to be compiled against newer hadoop
   * releases.
   */
  public static final String INPUT_FADV_DEFAULT =
      Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_DEFAULT;

  /**
   * General input. Some seeks, some reads.
   * The policy name "default" is standard across different stores,
   * and should be preferred.
   * Value: {@value}
   */
  public static final String INPUT_FADV_NORMAL = "normal";

  /**
   * Optimized for sequential access.
   * Value: {@value}
   * @deprecated use the {@link Options.OpenFileOptions} value
   * in code which only needs to be compiled against newer hadoop
   * releases.
   */
  public static final String INPUT_FADV_SEQUENTIAL =
      Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL;

  /**
   * Optimized purely for random seek+read/positionedRead operations;
   * The performance of sequential IO may be reduced in exchange for
   * more efficient {@code seek()} operations.
   * Value: {@value}
   * @deprecated use the {@link Options.OpenFileOptions} value
   * in code which only needs to be compiled against newer hadoop
   * releases.
   */
  public static final String INPUT_FADV_RANDOM =
      Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_RANDOM;

  /**
   * Gauge name for the input policy : {@value}.
   * This references an enum currently exclusive to the S3A stream.
   */
  public static final String STREAM_READ_GAUGE_INPUT_POLICY =
      "stream_read_gauge_input_policy";

  /**
   * S3 Client Factory implementation class: {@value}.
   * Unstable and incompatible between v1 and v2 SDK versions.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static final String S3_CLIENT_FACTORY_IMPL =
      "fs.s3a.s3.client.factory.impl";

  /**
   * Default factory:
   * {@code org.apache.hadoop.fs.s3a.DefaultS3ClientFactory}.
   */
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

  /*
   * Obsolete S3Guard-related options, retained purely because this file
   * is @Public/@Evolving.
   */
  @Deprecated
  public static final String S3_METADATA_STORE_IMPL =
      "fs.s3a.metadatastore.impl";
  @Deprecated
  public static final String FAIL_ON_METADATA_WRITE_ERROR =
      "fs.s3a.metadatastore.fail.on.write.error";
  @Deprecated
  public static final boolean FAIL_ON_METADATA_WRITE_ERROR_DEFAULT = true;
  @InterfaceStability.Unstable
  @Deprecated
  public static final String S3GUARD_CLI_PRUNE_AGE =
      "fs.s3a.s3guard.cli.prune.age";
  @Deprecated
  public static final String S3GUARD_DDB_REGION_KEY =
      "fs.s3a.s3guard.ddb.region";
  @Deprecated
  public static final String S3GUARD_DDB_TABLE_NAME_KEY =
      "fs.s3a.s3guard.ddb.table";
  @Deprecated
  public static final String S3GUARD_DDB_TABLE_TAG =
      "fs.s3a.s3guard.ddb.table.tag.";
  @Deprecated
  public static final String S3GUARD_DDB_TABLE_CREATE_KEY =
      "fs.s3a.s3guard.ddb.table.create";
  @Deprecated
  public static final String S3GUARD_DDB_TABLE_CAPACITY_READ_KEY =
      "fs.s3a.s3guard.ddb.table.capacity.read";
  @Deprecated
  public static final long S3GUARD_DDB_TABLE_CAPACITY_READ_DEFAULT = 0;
  @Deprecated
  public static final String S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY =
      "fs.s3a.s3guard.ddb.table.capacity.write";
  @Deprecated
  public static final long S3GUARD_DDB_TABLE_CAPACITY_WRITE_DEFAULT = 0;
  @Deprecated
  public static final String S3GUARD_DDB_TABLE_SSE_ENABLED =
      "fs.s3a.s3guard.ddb.table.sse.enabled";
  @Deprecated
  public static final String S3GUARD_DDB_TABLE_SSE_CMK =
      "fs.s3a.s3guard.ddb.table.sse.cmk";
  @Deprecated
  public static final int S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT = 25;
  @Deprecated
  public static final String S3GUARD_DDB_MAX_RETRIES =
      "fs.s3a.s3guard.ddb.max.retries";
  @Deprecated
  public static final int S3GUARD_DDB_MAX_RETRIES_DEFAULT =
      DEFAULT_MAX_ERROR_RETRIES;
  @Deprecated
  public static final String S3GUARD_DDB_THROTTLE_RETRY_INTERVAL =
      "fs.s3a.s3guard.ddb.throttle.retry.interval";
  @Deprecated
  public static final String S3GUARD_DDB_THROTTLE_RETRY_INTERVAL_DEFAULT =
      "100ms";
  @Deprecated
  @InterfaceStability.Unstable
  public static final String S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY =
      "fs.s3a.s3guard.ddb.background.sleep";
  @Deprecated
  public static final int S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_DEFAULT = 25;

  /**
   * The default "Null" metadata store: {@value}.
   */
  @Deprecated
  public static final String S3GUARD_METASTORE_NULL
      = "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore";
  @Deprecated
  @InterfaceStability.Unstable
  public static final String S3GUARD_METASTORE_LOCAL
      = "org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore";
  @InterfaceStability.Unstable
  @Deprecated
  public static final String S3GUARD_METASTORE_LOCAL_MAX_RECORDS =
      "fs.s3a.s3guard.local.max_records";
  @Deprecated
  public static final int DEFAULT_S3GUARD_METASTORE_LOCAL_MAX_RECORDS = 256;
  @InterfaceStability.Unstable
  @Deprecated
  public static final String S3GUARD_METASTORE_LOCAL_ENTRY_TTL =
      "fs.s3a.s3guard.local.ttl";
  @Deprecated
  public static final int DEFAULT_S3GUARD_METASTORE_LOCAL_ENTRY_TTL
      = 60 * 1000;
  @Deprecated
  public static final String S3GUARD_METASTORE_DYNAMO
      = "org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore";
  @Deprecated
  public static final String S3GUARD_DISABLED_WARN_LEVEL
      = "fs.s3a.s3guard.disabled.warn.level";
  @Deprecated
  public static final String DEFAULT_S3GUARD_DISABLED_WARN_LEVEL =
      "SILENT";

  /**
   * Inconsistency (visibility delay) injection settings.
   * No longer used.
   */
  @Deprecated
  public static final String FAIL_INJECT_INCONSISTENCY_KEY =
      "fs.s3a.failinject.inconsistency.key.substring";

  @Deprecated
  public static final String FAIL_INJECT_INCONSISTENCY_MSEC =
      "fs.s3a.failinject.inconsistency.msec";

  @Deprecated
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
  @Deprecated
  public static final String S3GUARD_CONSISTENCY_RETRY_LIMIT =
      "fs.s3a.s3guard.consistency.retry.limit";

  /**
   * Default retry limit: {@value}.
   */
  @Deprecated
  public static final int S3GUARD_CONSISTENCY_RETRY_LIMIT_DEFAULT = 7;

  /**
   * Initial retry interval: {@value}.
   */
  @Deprecated
  public static final String S3GUARD_CONSISTENCY_RETRY_INTERVAL =
      "fs.s3a.s3guard.consistency.retry.interval";

  /**
   * Default initial retry interval: {@value}.
   * The consistency retry probe uses exponential backoff, because
   * each probe can cause the S3 load balancers to retain any 404 in
   * its cache for longer. See HADOOP-16490.
   */
  @Deprecated
  public static final String S3GUARD_CONSISTENCY_RETRY_INTERVAL_DEFAULT =
      "2s";

  public static final String AWS_SERVICE_IDENTIFIER_S3 = "S3";
  @Deprecated
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
      DIRECTORY_MARKER_POLICY_KEEP;


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

  /**
   * AWS S3 region for the bucket. When set bypasses the construction of
   * region through endpoint url.
   */
  public static final String AWS_REGION = "fs.s3a.endpoint.region";

  /**
   * The special S3 region which can be used to talk to any bucket.
   * Value {@value}.
   */
  public static final String AWS_S3_CENTRAL_REGION = "us-east-1";

  /**
   * The default S3 region when using cross region client.
   * Value {@value}.
   */
  public static final String AWS_S3_DEFAULT_REGION = "us-east-2";

  /**
   * Require that all S3 access is made through Access Points.
   */
  public static final String AWS_S3_ACCESSPOINT_REQUIRED = "fs.s3a.accesspoint.required";

  /**
   * Flag for create performance.
   * This is *not* a configuration option; it is for use in the
   * {code createFile()} builder.
   * Value {@value}.
   */
  public static final String FS_S3A_CREATE_PERFORMANCE = "fs.s3a.create.performance";

  /**
   * Prefix for adding a header to the object when created.
   * The actual value must have a "." suffix and then the actual header.
   * This is *not* a configuration option; it is only for use in the
   * {code createFile()} builder.
   * Value {@value}.
   */
  public static final String FS_S3A_CREATE_HEADER = "fs.s3a.create.header";

  /**
   * What is the smallest reasonable seek in bytes such
   * that we group ranges together during vectored read operation.
   * Value : {@value}.
   */
  public static final String AWS_S3_VECTOR_READS_MIN_SEEK_SIZE =
          "fs.s3a.vectored.read.min.seek.size";

  /**
   * What is the largest merged read size in bytes such
   * that we group ranges together during vectored read.
   * Setting this value to 0 will disable merging of ranges.
   * Value : {@value}.
   */
  public static final String AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE =
          "fs.s3a.vectored.read.max.merged.size";

  /**
   * Default minimum seek in bytes during vectored reads : {@value}.
   */
  public static final int DEFAULT_AWS_S3_VECTOR_READS_MIN_SEEK_SIZE = 4896; // 4K

  /**
   * Default maximum read size in bytes during vectored reads : {@value}.
   */
  public static final int DEFAULT_AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE = 1253376; //1M

  /**
   * Maximum number of range reads a single input stream can have
   * active (downloading, or queued) to the central FileSystem
   * instance's pool of queued operations.
   * This stops a single stream overloading the shared thread pool.
   * {@value}
   * <p>
   * Default is {@link #DEFAULT_AWS_S3_VECTOR_ACTIVE_RANGE_READS}
   */
  public static final String AWS_S3_VECTOR_ACTIVE_RANGE_READS =
          "fs.s3a.vectored.active.ranged.reads";

  /**
   * Limit of queued range data download operations during vectored
   * read. Value: {@value}
   */
  public static final int DEFAULT_AWS_S3_VECTOR_ACTIVE_RANGE_READS = 4;

  /**
   * Prefix of auth classes in AWS SDK V1.
   */
  public static final String AWS_AUTH_CLASS_PREFIX = "com.amazonaws.auth";

  /**
   * Controls whether the prefetching input stream is enabled.
   */
  public static final String PREFETCH_ENABLED_KEY = "fs.s3a.prefetch.enabled";

  /**
   * Default option as to whether the prefetching input stream is enabled.
   */
  public static final boolean  PREFETCH_ENABLED_DEFAULT = false;

  // If the default values are used, each file opened for reading will consume
  // 64 MB of heap space (8 blocks x 8 MB each).

  /**
   * The size of a single prefetched block in number of bytes.
   */
  public static final String PREFETCH_BLOCK_SIZE_KEY = "fs.s3a.prefetch.block.size";
  public static final int PREFETCH_BLOCK_DEFAULT_SIZE = 8 * 1024 * 1024;

  /**
   * Maximum number of blocks prefetched at any given time.
   */
  public static final String PREFETCH_BLOCK_COUNT_KEY = "fs.s3a.prefetch.block.count";
  public static final int PREFETCH_BLOCK_DEFAULT_COUNT = 8;

  /**
   * Option to enable or disable the multipart uploads.
   * Value: {@value}.
   * <p>
   * Default is {@link #DEFAULT_MULTIPART_UPLOAD_ENABLED}.
   */
  public static final String MULTIPART_UPLOADS_ENABLED = "fs.s3a.multipart.uploads.enabled";

  /**
   * Default value for multipart uploads.
   * {@value}
   */
  public static final boolean DEFAULT_MULTIPART_UPLOAD_ENABLED = true;

  /**
   * Stream supports multipart uploads to the given path.
   */
  public static final String STORE_CAPABILITY_DIRECTORY_MARKER_MULTIPART_UPLOAD_ENABLED =
      "fs.s3a.capability.multipart.uploads.enabled";

  /**
   * Prefetch max blocks count config.
   * Value = {@value}
   */
  public static final String PREFETCH_MAX_BLOCKS_COUNT = "fs.s3a.prefetch.max.blocks.count";

  /**
   * Default value for max blocks count config.
   * Value = {@value}
   */
  public static final int DEFAULT_PREFETCH_MAX_BLOCKS_COUNT = 4;

  /**
   * The bucket region header.
   */
  public static final String BUCKET_REGION_HEADER = "x-amz-bucket-region";
}
