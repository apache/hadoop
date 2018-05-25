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

package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;

/** 
 * This class contains constants for configuration keys used
 * in the common code.
 *
 * It includes all publicly documented configuration keys. In general
 * this class should not be used directly (use CommonConfigurationKeys
 * instead)
 *
 */

@InterfaceAudience.Public
public class CommonConfigurationKeysPublic {
  
  // The Keys
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IO_NATIVE_LIB_AVAILABLE_KEY =
    "io.native.lib.available";
  /** Default value for IO_NATIVE_LIB_AVAILABLE_KEY */
  public static final boolean IO_NATIVE_LIB_AVAILABLE_DEFAULT = true;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY =
    "net.topology.script.number.args";
  /** Default value for NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY */
  public static final int     NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_DEFAULT = 100;

  //FS keys
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  FS_DEFAULT_NAME_KEY = "fs.defaultFS";
  /** Default value for FS_DEFAULT_NAME_KEY */
  public static final String  FS_DEFAULT_NAME_DEFAULT = "file:///";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  FS_DF_INTERVAL_KEY = "fs.df.interval"; 
  /** Default value for FS_DF_INTERVAL_KEY */
  public static final long    FS_DF_INTERVAL_DEFAULT = 60000;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  FS_DU_INTERVAL_KEY = "fs.du.interval";
  /** Default value for FS_DU_INTERVAL_KEY */
  public static final long    FS_DU_INTERVAL_DEFAULT = 600000;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY =
    "fs.client.resolve.remote.symlinks";
  /** Default value for FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY */
  public static final boolean FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_DEFAULT = true;


  //Defaults are not specified for following keys
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY =
    "net.topology.script.file.name";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY =
    "net.topology.node.switch.mapping.impl";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  NET_TOPOLOGY_IMPL_KEY =
    "net.topology.impl";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY =
    "net.topology.table.file.name";
  public static final String NET_DEPENDENCY_SCRIPT_FILE_NAME_KEY = 
    "net.topology.dependency.script.file.name";

  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  FS_TRASH_CHECKPOINT_INTERVAL_KEY =
    "fs.trash.checkpoint.interval";
  /** Default value for FS_TRASH_CHECKPOINT_INTERVAL_KEY */
  public static final long    FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT = 0;

  // TBD: Code is still using hardcoded values (e.g. "fs.automatic.close")
  // instead of constant (e.g. FS_AUTOMATIC_CLOSE_KEY)
  //
  /** Not used anywhere, looks like default value for FS_LOCAL_BLOCK_SIZE */
  public static final long    FS_LOCAL_BLOCK_SIZE_DEFAULT = 32*1024*1024;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  FS_AUTOMATIC_CLOSE_KEY = "fs.automatic.close";
  /** Default value for FS_AUTOMATIC_CLOSE_KEY */
  public static final boolean FS_AUTOMATIC_CLOSE_DEFAULT = true;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  FS_FILE_IMPL_KEY = "fs.file.impl";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  FS_FTP_HOST_KEY = "fs.ftp.host";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  FS_FTP_HOST_PORT_KEY = "fs.ftp.host.port";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  FS_TRASH_INTERVAL_KEY = "fs.trash.interval";
  /** Default value for FS_TRASH_INTERVAL_KEY */
  public static final long    FS_TRASH_INTERVAL_DEFAULT = 0;

  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IO_MAPFILE_BLOOM_SIZE_KEY =
    "io.mapfile.bloom.size";
  /** Default value for IO_MAPFILE_BLOOM_SIZE_KEY */
  public static final int     IO_MAPFILE_BLOOM_SIZE_DEFAULT = 1024*1024;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IO_MAPFILE_BLOOM_ERROR_RATE_KEY =
    "io.mapfile.bloom.error.rate" ;
  /** Default value for IO_MAPFILE_BLOOM_ERROR_RATE_KEY */
  public static final float   IO_MAPFILE_BLOOM_ERROR_RATE_DEFAULT = 0.005f;
  /** Codec class that implements Lzo compression algorithm */
  public static final String  IO_COMPRESSION_CODEC_LZO_CLASS_KEY =
    "io.compression.codec.lzo.class";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IO_MAP_INDEX_INTERVAL_KEY =
    "io.map.index.interval";
  /** Default value for IO_MAP_INDEX_INTERVAL_DEFAULT */
  public static final int     IO_MAP_INDEX_INTERVAL_DEFAULT = 128;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IO_MAP_INDEX_SKIP_KEY = "io.map.index.skip";
  /** Default value for IO_MAP_INDEX_SKIP_KEY */
  public static final int     IO_MAP_INDEX_SKIP_DEFAULT = 0;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IO_SEQFILE_COMPRESS_BLOCKSIZE_KEY =
    "io.seqfile.compress.blocksize";
  /** Default value for IO_SEQFILE_COMPRESS_BLOCKSIZE_KEY */
  public static final int     IO_SEQFILE_COMPRESS_BLOCKSIZE_DEFAULT = 1000000;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IO_FILE_BUFFER_SIZE_KEY =
    "io.file.buffer.size";
  /** Default value for IO_FILE_BUFFER_SIZE_KEY */
  public static final int     IO_FILE_BUFFER_SIZE_DEFAULT = 4096;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IO_SKIP_CHECKSUM_ERRORS_KEY =
    "io.skip.checksum.errors";
  /** Default value for IO_SKIP_CHECKSUM_ERRORS_KEY */
  public static final boolean IO_SKIP_CHECKSUM_ERRORS_DEFAULT = false;
  /**
   * @deprecated Moved to mapreduce, see mapreduce.task.io.sort.mb
   * in mapred-default.xml
   * See https://issues.apache.org/jira/browse/HADOOP-6801
   */
  public static final String  IO_SORT_MB_KEY = "io.sort.mb";
  /** Default value for IO_SORT_MB_DEFAULT */
  public static final int     IO_SORT_MB_DEFAULT = 100;
  /**
   * @deprecated Moved to mapreduce, see mapreduce.task.io.sort.factor
   * in mapred-default.xml
   * See https://issues.apache.org/jira/browse/HADOOP-6801
   */
  public static final String  IO_SORT_FACTOR_KEY = "io.sort.factor";
  /** Default value for IO_SORT_FACTOR_DEFAULT */
  public static final int     IO_SORT_FACTOR_DEFAULT = 100;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IO_SERIALIZATIONS_KEY = "io.serializations";

  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  TFILE_IO_CHUNK_SIZE_KEY = "tfile.io.chunk.size";
  /** Default value for TFILE_IO_CHUNK_SIZE_DEFAULT */
  public static final int     TFILE_IO_CHUNK_SIZE_DEFAULT = 1024*1024;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  TFILE_FS_INPUT_BUFFER_SIZE_KEY =
    "tfile.fs.input.buffer.size";
  /** Default value for TFILE_FS_INPUT_BUFFER_SIZE_KEY */
  public static final int     TFILE_FS_INPUT_BUFFER_SIZE_DEFAULT = 256*1024;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  TFILE_FS_OUTPUT_BUFFER_SIZE_KEY =
    "tfile.fs.output.buffer.size";
  /** Default value for TFILE_FS_OUTPUT_BUFFER_SIZE_KEY */
  public static final int     TFILE_FS_OUTPUT_BUFFER_SIZE_DEFAULT = 256*1024;

  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY =
    "ipc.client.connection.maxidletime";
  /** Default value for IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY */
  public static final int     IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT = 10000; // 10s
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IPC_CLIENT_CONNECT_TIMEOUT_KEY =
    "ipc.client.connect.timeout";
  /** Default value for IPC_CLIENT_CONNECT_TIMEOUT_KEY */
  public static final int     IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT = 20000; // 20s
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IPC_CLIENT_CONNECT_MAX_RETRIES_KEY =
    "ipc.client.connect.max.retries";
  /** Default value for IPC_CLIENT_CONNECT_MAX_RETRIES_KEY */
  public static final int     IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT = 10;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY =
      "ipc.client.connect.retry.interval";
  /** Default value for IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY */
  public static final int     IPC_CLIENT_CONNECT_RETRY_INTERVAL_DEFAULT = 1000;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY =
    "ipc.client.connect.max.retries.on.timeouts";
  /** Default value for IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY */
  public static final int  IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT = 45;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IPC_CLIENT_TCPNODELAY_KEY =
    "ipc.client.tcpnodelay";
  /** Defalt value for IPC_CLIENT_TCPNODELAY_KEY */
  public static final boolean IPC_CLIENT_TCPNODELAY_DEFAULT = true;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IPC_SERVER_LISTEN_QUEUE_SIZE_KEY =
    "ipc.server.listen.queue.size";
  /** Default value for IPC_SERVER_LISTEN_QUEUE_SIZE_KEY */
  public static final int     IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT = 128;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IPC_CLIENT_KILL_MAX_KEY = "ipc.client.kill.max";
  /** Default value for IPC_CLIENT_KILL_MAX_KEY */
  public static final int     IPC_CLIENT_KILL_MAX_DEFAULT = 10;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IPC_CLIENT_IDLETHRESHOLD_KEY =
    "ipc.client.idlethreshold";
  /** Default value for IPC_CLIENT_IDLETHRESHOLD_DEFAULT */
  public static final int     IPC_CLIENT_IDLETHRESHOLD_DEFAULT = 4000;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IPC_SERVER_TCPNODELAY_KEY =
    "ipc.server.tcpnodelay";
  /** Default value for IPC_SERVER_TCPNODELAY_KEY */
  public static final boolean IPC_SERVER_TCPNODELAY_DEFAULT = true;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IPC_SERVER_MAX_CONNECTIONS_KEY =
    "ipc.server.max.connections";
  /** Default value for IPC_SERVER_MAX_CONNECTIONS_KEY */
  public static final int     IPC_SERVER_MAX_CONNECTIONS_DEFAULT = 0;

  /** Logs if a RPC is really slow compared to rest of RPCs. */
  public static final String IPC_SERVER_LOG_SLOW_RPC =
                                                "ipc.server.log.slow.rpc";
  public static final boolean IPC_SERVER_LOG_SLOW_RPC_DEFAULT = false;

  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY =
    "hadoop.rpc.socket.factory.class.default";
  public static final String  HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_DEFAULT =
    "org.apache.hadoop.net.StandardSocketFactory";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  HADOOP_SOCKS_SERVER_KEY = "hadoop.socks.server";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  HADOOP_UTIL_HASH_TYPE_KEY =
    "hadoop.util.hash.type";
  /** Default value for HADOOP_UTIL_HASH_TYPE_KEY */
  public static final String  HADOOP_UTIL_HASH_TYPE_DEFAULT = "murmur";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  HADOOP_SECURITY_GROUP_MAPPING =
    "hadoop.security.group.mapping";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  HADOOP_SECURITY_GROUPS_CACHE_SECS =
    "hadoop.security.groups.cache.secs";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final long HADOOP_SECURITY_GROUPS_CACHE_SECS_DEFAULT =
    300;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS =
    "hadoop.security.groups.negative-cache.secs";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final long HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS_DEFAULT =
    30;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String HADOOP_SECURITY_GROUPS_CACHE_WARN_AFTER_MS =
    "hadoop.security.groups.cache.warn.after.ms";
  public static final long HADOOP_SECURITY_GROUPS_CACHE_WARN_AFTER_MS_DEFAULT =
    5000;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String HADOOP_SECURITY_GROUPS_CACHE_BACKGROUND_RELOAD =
      "hadoop.security.groups.cache.background.reload";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a>. */
  public static final boolean
      HADOOP_SECURITY_GROUPS_CACHE_BACKGROUND_RELOAD_DEFAULT = false;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a>. */
  public static final String
      HADOOP_SECURITY_GROUPS_CACHE_BACKGROUND_RELOAD_THREADS =
          "hadoop.security.groups.cache.background.reload.threads";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a>. */
  public static final int
      HADOOP_SECURITY_GROUPS_CACHE_BACKGROUND_RELOAD_THREADS_DEFAULT = 3;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a>.*/
  public static final String  HADOOP_SECURITY_AUTHENTICATION =
    "hadoop.security.authentication";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String HADOOP_SECURITY_AUTHORIZATION =
    "hadoop.security.authorization";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN =
    "hadoop.security.instrumentation.requires.admin";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  HADOOP_SECURITY_SERVICE_USER_NAME_KEY =
    "hadoop.security.service.user.name.key";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  HADOOP_SECURITY_AUTH_TO_LOCAL =
    "hadoop.security.auth_to_local";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS =
      "hadoop.security.sensitive-config-keys";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS_DEFAULT =
      "password$" + "," +
      "fs.s3.*[Ss]ecret.?[Kk]ey" + "," +
      "fs.azure\\.account.key.*" + "," +
      "dfs.webhdfs.oauth2.[a-z]+.token" + "," +
      HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS;

  @Deprecated
  /** Only used by HttpServer. */
  public static final String HADOOP_SSL_ENABLED_KEY = "hadoop.ssl.enabled";
  @Deprecated
  /** Only used by HttpServer. */
  public static final boolean HADOOP_SSL_ENABLED_DEFAULT = false;


  // HTTP policies to be used in configuration
  // Use HttpPolicy.name() instead
  @Deprecated
  public static final String HTTP_POLICY_HTTP_ONLY = "HTTP_ONLY";
  @Deprecated
  public static final String HTTP_POLICY_HTTPS_ONLY = "HTTPS_ONLY";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  HADOOP_RPC_PROTECTION =
    "hadoop.rpc.protection";
  /** Class to override Sasl Properties for a connection */
  public static final String  HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS =
    "hadoop.security.saslproperties.resolver.class";
  public static final String HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX = 
    "hadoop.security.crypto.codec.classes";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY =
    "hadoop.security.crypto.cipher.suite";
  public static final String HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT = 
    "AES/CTR/NoPadding";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY =
    "hadoop.security.crypto.jce.provider";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_CRYPTO_JCEKS_KEY_SERIALFILTER =
      "hadoop.security.crypto.jceks.key.serialfilter";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY = 
    "hadoop.security.crypto.buffer.size";
  /** Defalt value for HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY */
  public static final int HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_DEFAULT = 8192;
  /** Class to override Impersonation provider */
  public static final String  HADOOP_SECURITY_IMPERSONATION_PROVIDER_CLASS =
    "hadoop.security.impersonation.provider.class";

  //  <!-- KMSClientProvider configurations -->
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String KMS_CLIENT_ENC_KEY_CACHE_SIZE =
      "hadoop.security.kms.client.encrypted.key.cache.size";
  /** Default value for KMS_CLIENT_ENC_KEY_CACHE_SIZE */
  public static final int KMS_CLIENT_ENC_KEY_CACHE_SIZE_DEFAULT = 500;

  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK =
      "hadoop.security.kms.client.encrypted.key.cache.low-watermark";
  /** Default value for KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK */
  public static final float KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK_DEFAULT =
      0.3f;

  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String KMS_CLIENT_ENC_KEY_CACHE_NUM_REFILL_THREADS =
      "hadoop.security.kms.client.encrypted.key.cache.num.refill.threads";
  /** Default value for KMS_CLIENT_ENC_KEY_NUM_REFILL_THREADS */
  public static final int KMS_CLIENT_ENC_KEY_CACHE_NUM_REFILL_THREADS_DEFAULT =
      2;

  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_MS =
      "hadoop.security.kms.client.encrypted.key.cache.expiry";
  /** Default value for KMS_CLIENT_ENC_KEY_CACHE_EXPIRY (12 hrs)*/
  public static final int KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_DEFAULT = 43200000;

  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY = 
    "hadoop.security.java.secure.random.algorithm";
  /** Defalt value for HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY */
  public static final String HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT = 
    "SHA1PRNG";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String HADOOP_SECURITY_SECURE_RANDOM_IMPL_KEY = 
    "hadoop.security.secure.random.impl";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY = 
    "hadoop.security.random.device.file.path";
  public static final String HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT = 
    "/dev/urandom";
}

