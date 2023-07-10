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
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.JceAesCtrCryptoCodec;
import org.apache.hadoop.crypto.JceSm4CtrCryptoCodec;
import org.apache.hadoop.crypto.OpensslAesCtrCryptoCodec;
import org.apache.hadoop.crypto.OpensslSm4CtrCryptoCodec;

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
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY =
    "net.topology.script.number.args";
  /** Default value for NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY */
  public static final int     NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_DEFAULT = 100;

  //FS keys
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  FS_DEFAULT_NAME_KEY = "fs.defaultFS";
  /** Default value for FS_DEFAULT_NAME_KEY */
  public static final String  FS_DEFAULT_NAME_DEFAULT = "file:///";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  FS_DF_INTERVAL_KEY = "fs.df.interval"; 
  /** Default value for FS_DF_INTERVAL_KEY */
  public static final long    FS_DF_INTERVAL_DEFAULT = 60000;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  FS_DU_INTERVAL_KEY = "fs.du.interval";
  /** Default value for FS_DU_INTERVAL_KEY */
  public static final long    FS_DU_INTERVAL_DEFAULT = 600000;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String FS_GETSPACEUSED_CLASSNAME =
      "fs.getspaceused.classname";

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String FS_GETSPACEUSED_JITTER_KEY =
      "fs.getspaceused.jitterMillis";
  /** Default value for FS_GETSPACEUSED_JITTER_KEY */
  public static final long FS_GETSPACEUSED_JITTER_DEFAULT = 60000;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY =
    "fs.client.resolve.remote.symlinks";
  /** Default value for FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY */
  public static final boolean FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_DEFAULT = true;


  //Defaults are not specified for following keys
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY =
    "net.topology.script.file.name";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY =
    "net.topology.node.switch.mapping.impl";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  NET_TOPOLOGY_IMPL_KEY =
    "net.topology.impl";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY =
    "net.topology.table.file.name";
  public static final String NET_DEPENDENCY_SCRIPT_FILE_NAME_KEY = 
    "net.topology.dependency.script.file.name";

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  FS_TRASH_CHECKPOINT_INTERVAL_KEY =
    "fs.trash.checkpoint.interval";
  /** Default value for FS_TRASH_CHECKPOINT_INTERVAL_KEY */
  public static final long    FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT = 0;

  /**
   * Directories that cannot be removed unless empty, even by an
   * administrator.
   */
  public static final String FS_PROTECTED_DIRECTORIES =
      "fs.protected.directories";

  // TBD: Code is still using hardcoded values (e.g. "fs.automatic.close")
  // instead of constant (e.g. FS_AUTOMATIC_CLOSE_KEY)
  //
  /** Not used anywhere, looks like default value for FS_LOCAL_BLOCK_SIZE */
  public static final long    FS_LOCAL_BLOCK_SIZE_DEFAULT = 32*1024*1024;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  FS_AUTOMATIC_CLOSE_KEY = "fs.automatic.close";
  /** Default value for FS_AUTOMATIC_CLOSE_KEY */
  public static final boolean FS_AUTOMATIC_CLOSE_DEFAULT = true;

  /**
   * Number of filesystems instances can be created in parallel.
   * <p>
   * A higher number here does not necessarily improve performance, especially
   * for object stores, where multiple threads may be attempting to create an FS
   * instance for the same URI.
   * </p>
   * Default value: {@value}.
   */
  public static final String FS_CREATION_PARALLEL_COUNT =
      "fs.creation.parallel.count";

  /**
   * Default value for {@link #FS_CREATION_PARALLEL_COUNT}.
   * <p>
   * Default value: {@value}.
   * </p>
   */
  public static final int FS_CREATION_PARALLEL_COUNT_DEFAULT =
      64;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  FS_FILE_IMPL_KEY = "fs.file.impl";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  FS_FTP_HOST_KEY = "fs.ftp.host";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  FS_FTP_HOST_PORT_KEY = "fs.ftp.host.port";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  FS_TRASH_INTERVAL_KEY = "fs.trash.interval";
  /** Default value for FS_TRASH_INTERVAL_KEY */
  public static final long    FS_TRASH_INTERVAL_DEFAULT = 0;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  FS_TRASH_CLEAN_TRASHROOT_ENABLE_KEY =
      "fs.trash.clean.trashroot.enable";
  /** Default value for FS_TRASH_CLEAN_TRASHROOT_ENABLE_KEY. */
  public static final boolean FS_TRASH_CLEAN_TRASHROOT_ENABLE_DEFAULT = false;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  FS_CLIENT_TOPOLOGY_RESOLUTION_ENABLED =
      "fs.client.resolve.topology.enabled";
  /** Default value for FS_CLIENT_TOPOLOGY_RESOLUTION_ENABLED. */
  public static final boolean FS_CLIENT_TOPOLOGY_RESOLUTION_ENABLED_DEFAULT =
      false;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IO_MAPFILE_BLOOM_SIZE_KEY =
    "io.mapfile.bloom.size";
  /** Default value for IO_MAPFILE_BLOOM_SIZE_KEY */
  public static final int     IO_MAPFILE_BLOOM_SIZE_DEFAULT = 1024*1024;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IO_MAPFILE_BLOOM_ERROR_RATE_KEY =
    "io.mapfile.bloom.error.rate" ;
  /** Default value for IO_MAPFILE_BLOOM_ERROR_RATE_KEY */
  public static final float   IO_MAPFILE_BLOOM_ERROR_RATE_DEFAULT = 0.005f;
  /** Codec class that implements Lzo compression algorithm */
  public static final String  IO_COMPRESSION_CODEC_LZO_CLASS_KEY =
    "io.compression.codec.lzo.class";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IO_MAP_INDEX_INTERVAL_KEY =
    "io.map.index.interval";
  /** Default value for IO_MAP_INDEX_INTERVAL_DEFAULT */
  public static final int     IO_MAP_INDEX_INTERVAL_DEFAULT = 128;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IO_MAP_INDEX_SKIP_KEY = "io.map.index.skip";
  /** Default value for IO_MAP_INDEX_SKIP_KEY */
  public static final int     IO_MAP_INDEX_SKIP_DEFAULT = 0;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IO_SEQFILE_COMPRESS_BLOCKSIZE_KEY =
    "io.seqfile.compress.blocksize";
  /** Default value for IO_SEQFILE_COMPRESS_BLOCKSIZE_KEY */
  public static final int     IO_SEQFILE_COMPRESS_BLOCKSIZE_DEFAULT = 1000000;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IO_FILE_BUFFER_SIZE_KEY =
    "io.file.buffer.size";
  /** Default value for IO_FILE_BUFFER_SIZE_KEY */
  public static final int     IO_FILE_BUFFER_SIZE_DEFAULT = 4096;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IO_SKIP_CHECKSUM_ERRORS_KEY =
    "io.skip.checksum.errors";
  /** Default value for IO_SKIP_CHECKSUM_ERRORS_KEY */
  public static final boolean IO_SKIP_CHECKSUM_ERRORS_DEFAULT = false;
  /**
   * @deprecated Moved to mapreduce, see mapreduce.task.io.sort.mb
   * in mapred-default.xml
   * See https://issues.apache.org/jira/browse/HADOOP-6801
   *
   * For {@link org.apache.hadoop.io.SequenceFile.Sorter} control
   * instead, see {@link #SEQ_IO_SORT_MB_KEY}.
   */
  public static final String  IO_SORT_MB_KEY = "io.sort.mb";
  /** Default value for {@link #IO_SORT_MB_KEY}. */
  public static final int     IO_SORT_MB_DEFAULT = 100;
  /**
   * @deprecated Moved to mapreduce, see mapreduce.task.io.sort.factor
   * in mapred-default.xml
   * See https://issues.apache.org/jira/browse/HADOOP-6801
   *
   * For {@link org.apache.hadoop.io.SequenceFile.Sorter} control
   * instead, see {@link #SEQ_IO_SORT_FACTOR_KEY}.
   */
  public static final String  IO_SORT_FACTOR_KEY = "io.sort.factor";
  /** Default value for {@link #IO_SORT_FACTOR_KEY}. */
  public static final int     IO_SORT_FACTOR_DEFAULT = 100;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  SEQ_IO_SORT_MB_KEY = "seq.io.sort.mb";
  /** Default value for {@link #SEQ_IO_SORT_MB_KEY}. */
  public static final int     SEQ_IO_SORT_MB_DEFAULT = 100;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  SEQ_IO_SORT_FACTOR_KEY = "seq.io.sort.factor";
  /** Default value for {@link #SEQ_IO_SORT_FACTOR_KEY}. */
  public static final int     SEQ_IO_SORT_FACTOR_DEFAULT = 100;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IO_SERIALIZATIONS_KEY = "io.serializations";

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  TFILE_IO_CHUNK_SIZE_KEY = "tfile.io.chunk.size";
  /** Default value for TFILE_IO_CHUNK_SIZE_DEFAULT */
  public static final int     TFILE_IO_CHUNK_SIZE_DEFAULT = 1024*1024;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  TFILE_FS_INPUT_BUFFER_SIZE_KEY =
    "tfile.fs.input.buffer.size";
  /** Default value for TFILE_FS_INPUT_BUFFER_SIZE_KEY */
  public static final int     TFILE_FS_INPUT_BUFFER_SIZE_DEFAULT = 256*1024;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  TFILE_FS_OUTPUT_BUFFER_SIZE_KEY =
    "tfile.fs.output.buffer.size";
  /** Default value for TFILE_FS_OUTPUT_BUFFER_SIZE_KEY */
  public static final int     TFILE_FS_OUTPUT_BUFFER_SIZE_DEFAULT = 256*1024;

  public static final String  HADOOP_CALLER_CONTEXT_ENABLED_KEY =
      "hadoop.caller.context.enabled";
  public static final boolean HADOOP_CALLER_CONTEXT_ENABLED_DEFAULT = false;
  public static final String  HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY =
      "hadoop.caller.context.max.size";
  public static final int     HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT = 128;
  public static final String  HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_KEY =
      "hadoop.caller.context.signature.max.size";
  public static final int     HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_DEFAULT =
      40;
  public static final String HADOOP_CALLER_CONTEXT_SEPARATOR_KEY =
      "hadoop.caller.context.separator";
  public static final String HADOOP_CALLER_CONTEXT_SEPARATOR_DEFAULT = ",";

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY =
    "ipc.client.connection.maxidletime";
  /** Default value for IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY */
  public static final int     IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT = 10000; // 10s
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IPC_CLIENT_CONNECT_TIMEOUT_KEY =
    "ipc.client.connect.timeout";
  /** Default value for IPC_CLIENT_CONNECT_TIMEOUT_KEY */
  public static final int     IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT = 20000; // 20s
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IPC_CLIENT_CONNECT_MAX_RETRIES_KEY =
    "ipc.client.connect.max.retries";
  /** Default value for IPC_CLIENT_CONNECT_MAX_RETRIES_KEY */
  public static final int     IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT = 10;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY =
      "ipc.client.connect.retry.interval";
  /** Default value for IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY */
  public static final int     IPC_CLIENT_CONNECT_RETRY_INTERVAL_DEFAULT = 1000;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY =
    "ipc.client.connect.max.retries.on.timeouts";
  /** Default value for IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY */
  public static final int  IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT = 45;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IPC_CLIENT_TCPNODELAY_KEY =
    "ipc.client.tcpnodelay";
  /** Default value for IPC_CLIENT_TCPNODELAY_KEY */
  public static final boolean IPC_CLIENT_TCPNODELAY_DEFAULT = true;
  /** Enable low-latency connections from the client */
  public static final String   IPC_CLIENT_LOW_LATENCY = "ipc.client.low-latency";
  /** Default value of IPC_CLIENT_LOW_LATENCY */
  public static final boolean  IPC_CLIENT_LOW_LATENCY_DEFAULT = false;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IPC_SERVER_LISTEN_QUEUE_SIZE_KEY =
    "ipc.server.listen.queue.size";
  /** Default value for IPC_SERVER_LISTEN_QUEUE_SIZE_KEY */
  public static final int     IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT = 256;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IPC_CLIENT_KILL_MAX_KEY = "ipc.client.kill.max";
  /** Default value for IPC_CLIENT_KILL_MAX_KEY */
  public static final int     IPC_CLIENT_KILL_MAX_DEFAULT = 10;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IPC_CLIENT_IDLETHRESHOLD_KEY =
    "ipc.client.idlethreshold";
  /** Default value for IPC_CLIENT_IDLETHRESHOLD_DEFAULT */
  public static final int     IPC_CLIENT_IDLETHRESHOLD_DEFAULT = 4000;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IPC_SERVER_TCPNODELAY_KEY =
    "ipc.server.tcpnodelay";
  /** Default value for IPC_SERVER_TCPNODELAY_KEY */
  public static final boolean IPC_SERVER_TCPNODELAY_DEFAULT = true;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IPC_SERVER_REUSEADDR_KEY =
      "ipc.server.reuseaddr";
  /** Default value for IPC_SERVER_REUSEADDR_KEY. */
  public static final boolean IPC_SERVER_REUSEADDR_DEFAULT = true;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  IPC_SERVER_MAX_CONNECTIONS_KEY =
    "ipc.server.max.connections";
  /** Default value for IPC_SERVER_MAX_CONNECTIONS_KEY */
  public static final int     IPC_SERVER_MAX_CONNECTIONS_DEFAULT = 0;

  /** Logs if a RPC is really slow compared to rest of RPCs. */
  public static final String IPC_SERVER_LOG_SLOW_RPC =
                                                "ipc.server.log.slow.rpc";
  public static final boolean IPC_SERVER_LOG_SLOW_RPC_DEFAULT = false;

  public static final String IPC_SERVER_PURGE_INTERVAL_MINUTES_KEY =
    "ipc.server.purge.interval";
  public static final int IPC_SERVER_PURGE_INTERVAL_MINUTES_DEFAULT = 15;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY =
    "hadoop.rpc.socket.factory.class.default";
  public static final String  HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_DEFAULT =
    "org.apache.hadoop.net.StandardSocketFactory";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  HADOOP_SOCKS_SERVER_KEY = "hadoop.socks.server";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  HADOOP_UTIL_HASH_TYPE_KEY =
    "hadoop.util.hash.type";
  /** Default value for HADOOP_UTIL_HASH_TYPE_KEY */
  public static final String  HADOOP_UTIL_HASH_TYPE_DEFAULT = "murmur";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  HADOOP_SECURITY_GROUP_MAPPING =
    "hadoop.security.group.mapping";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  HADOOP_SECURITY_GROUPS_CACHE_SECS =
    "hadoop.security.groups.cache.secs";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final long HADOOP_SECURITY_GROUPS_CACHE_SECS_DEFAULT =
    300;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS =
    "hadoop.security.groups.negative-cache.secs";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final long HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS_DEFAULT =
    30;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_GROUPS_CACHE_WARN_AFTER_MS =
    "hadoop.security.groups.cache.warn.after.ms";
  public static final long HADOOP_SECURITY_GROUPS_CACHE_WARN_AFTER_MS_DEFAULT =
    5000;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_GROUPS_CACHE_BACKGROUND_RELOAD =
      "hadoop.security.groups.cache.background.reload";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final boolean
      HADOOP_SECURITY_GROUPS_CACHE_BACKGROUND_RELOAD_DEFAULT = false;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String
      HADOOP_SECURITY_GROUPS_CACHE_BACKGROUND_RELOAD_THREADS =
          "hadoop.security.groups.cache.background.reload.threads";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final int
      HADOOP_SECURITY_GROUPS_CACHE_BACKGROUND_RELOAD_THREADS_DEFAULT = 3;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_GROUP_SHELL_COMMAND_TIMEOUT_KEY =
      "hadoop.security.groups.shell.command.timeout";
  /**
   * @deprecated use
   * {@link CommonConfigurationKeysPublic#HADOOP_SECURITY_GROUP_SHELL_COMMAND_TIMEOUT_KEY}
   * instead.
   */
  public static final String HADOOP_SECURITY_GROUP_SHELL_COMMAND_TIMEOUT_SECS =
      HADOOP_SECURITY_GROUP_SHELL_COMMAND_TIMEOUT_KEY;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final long
      HADOOP_SECURITY_GROUP_SHELL_COMMAND_TIMEOUT_DEFAULT =
      0L;
  /**
   * @deprecated use
   * {@link CommonConfigurationKeysPublic#HADOOP_SECURITY_GROUP_SHELL_COMMAND_TIMEOUT_DEFAULT}
   * instead.
   */
  public static final long
      HADOOP_SECURITY_GROUP_SHELL_COMMAND_TIMEOUT_SECS_DEFAULT =
      HADOOP_SECURITY_GROUP_SHELL_COMMAND_TIMEOUT_DEFAULT;
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  HADOOP_SECURITY_AUTHENTICATION =
    "hadoop.security.authentication";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_AUTHORIZATION =
    "hadoop.security.authorization";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN =
    "hadoop.security.instrumentation.requires.admin";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  HADOOP_SECURITY_SERVICE_USER_NAME_KEY =
    "hadoop.security.service.user.name.key";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  HADOOP_SECURITY_AUTH_TO_LOCAL =
    "hadoop.security.auth_to_local";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  HADOOP_SECURITY_AUTH_TO_LOCAL_MECHANISM =
    "hadoop.security.auth_to_local.mechanism";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_DNS_INTERFACE_KEY =
    "hadoop.security.dns.interface";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_DNS_NAMESERVER_KEY =
    "hadoop.security.dns.nameserver";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_TOKEN_FILES =
      "hadoop.token.files";
  public static final String HADOOP_TOKENS =
      "hadoop.tokens";
  public static final String HADOOP_HTTP_AUTHENTICATION_TYPE =
    "hadoop.http.authentication.type";

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN =
          "hadoop.kerberos.min.seconds.before.relogin";
  /** Default value for HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN */
  public static final int HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN_DEFAULT =
          60;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_KERBEROS_KEYTAB_LOGIN_AUTORENEWAL_ENABLED =
          "hadoop.kerberos.keytab.login.autorenewal.enabled";
  /** Default value for HADOOP_KERBEROS_KEYTAB_LOGIN_AUTORENEWAL_ENABLED. */
  public static final boolean
          HADOOP_KERBEROS_KEYTAB_LOGIN_AUTORENEWAL_ENABLED_DEFAULT = false;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  HADOOP_RPC_PROTECTION =
    "hadoop.rpc.protection";
  /** Class to override Sasl Properties for a connection */
  public static final String  HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS =
    "hadoop.security.saslproperties.resolver.class";
  public static final String HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX = 
    "hadoop.security.crypto.codec.classes";
  public static final String
      HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_KEY =
      HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX
          + CipherSuite.AES_CTR_NOPADDING.getConfigSuffix();
  public static final String
      HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_SM4_CTR_NOPADDING_KEY =
      HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX
          + CipherSuite.SM4_CTR_NOPADDING.getConfigSuffix();
  public static final String
      HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_DEFAULT =
      OpensslAesCtrCryptoCodec.class.getName() + "," +
          JceAesCtrCryptoCodec.class.getName();
  public static final String
      HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_SM4_CTR_NOPADDING_DEFAULT =
      OpensslSm4CtrCryptoCodec.class.getName() + "," +
          JceSm4CtrCryptoCodec.class.getName();
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY =
    "hadoop.security.crypto.cipher.suite";
  public static final String HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT = 
    "AES/CTR/NoPadding";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY =
    "hadoop.security.crypto.jce.provider";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_CRYPTO_JCEKS_KEY_SERIALFILTER =
      "hadoop.security.crypto.jceks.key.serialfilter";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY = 
    "hadoop.security.crypto.buffer.size";
  /** Defalt value for HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY */
  public static final int HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_DEFAULT = 8192;
  /** Class to override Impersonation provider */
  public static final String  HADOOP_SECURITY_IMPERSONATION_PROVIDER_CLASS =
    "hadoop.security.impersonation.provider.class";

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_KEY_PROVIDER_PATH =
      "hadoop.security.key.provider.path";

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_KEY_DEFAULT_BITLENGTH_KEY =
      "hadoop.security.key.default.bitlength";
  /** Defalt value for HADOOP_SECURITY_KEY_DEFAULT_BITLENGTH_KEY. */
  public static final int HADOOP_SECURITY_KEY_DEFAULT_BITLENGTH_DEFAULT = 128;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_KEY_DEFAULT_CIPHER_KEY =
      "hadoop.security.key.default.cipher";
  /** Defalt value for HADOOP_SECURITY_KEY_DEFAULT_CIPHER_KEY. */
  public static final String HADOOP_SECURITY_KEY_DEFAULT_CIPHER_DEFAULT =
      "AES/CTR/NoPadding";

  //  <!-- KMSClientProvider configurations -->
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String KMS_CLIENT_ENC_KEY_CACHE_SIZE =
      "hadoop.security.kms.client.encrypted.key.cache.size";
  /** Default value for KMS_CLIENT_ENC_KEY_CACHE_SIZE */
  public static final int KMS_CLIENT_ENC_KEY_CACHE_SIZE_DEFAULT = 500;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK =
      "hadoop.security.kms.client.encrypted.key.cache.low-watermark";
  /** Default value for KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK */
  public static final float KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK_DEFAULT =
      0.3f;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String KMS_CLIENT_ENC_KEY_CACHE_NUM_REFILL_THREADS =
      "hadoop.security.kms.client.encrypted.key.cache.num.refill.threads";
  /** Default value for KMS_CLIENT_ENC_KEY_NUM_REFILL_THREADS */
  public static final int KMS_CLIENT_ENC_KEY_CACHE_NUM_REFILL_THREADS_DEFAULT =
      2;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_MS =
      "hadoop.security.kms.client.encrypted.key.cache.expiry";
  /** Default value for KMS_CLIENT_ENC_KEY_CACHE_EXPIRY (12 hrs)*/
  public static final int KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_DEFAULT = 43200000;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String KMS_CLIENT_TIMEOUT_SECONDS =
      "hadoop.security.kms.client.timeout";
  public static final int KMS_CLIENT_TIMEOUT_DEFAULT = 60;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  /** Default value is the number of providers specified. */
  public static final String KMS_CLIENT_FAILOVER_MAX_RETRIES_KEY =
      "hadoop.security.kms.client.failover.max.retries";

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String KMS_CLIENT_FAILOVER_SLEEP_BASE_MILLIS_KEY =
      "hadoop.security.kms.client.failover.sleep.base.millis";
  /**  Default value is 100 ms. */
  public static final int KMS_CLIENT_FAILOVER_SLEEP_BASE_MILLIS_DEFAULT  = 100;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String KMS_CLIENT_FAILOVER_SLEEP_MAX_MILLIS_KEY =
      "hadoop.security.kms.client.failover.sleep.max.millis";
  /** Default value is 2 secs. */
  public static final int KMS_CLIENT_FAILOVER_SLEEP_MAX_MILLIS_DEFAULT  = 2000;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY = 
    "hadoop.security.java.secure.random.algorithm";
  /** Defalt value for HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY */
  public static final String HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT = 
    "SHA1PRNG";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_SECURE_RANDOM_IMPL_KEY = 
    "hadoop.security.secure.random.impl";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_OPENSSL_ENGINE_ID_KEY =
          "hadoop.security.openssl.engine.id";
  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY = 
    "hadoop.security.random.device.file.path";
  public static final String HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT = 
    "/dev/urandom";

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_KEY =
      "hadoop.shell.missing.defaultFs.warning";
  public static final boolean HADOOP_SHELL_MISSING_DEFAULT_FS_WARNING_DEFAULT =
      false;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SHELL_SAFELY_DELETE_LIMIT_NUM_FILES =
      "hadoop.shell.safely.delete.limit.num.files";
  public static final long HADOOP_SHELL_SAFELY_DELETE_LIMIT_NUM_FILES_DEFAULT =
      100;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_HTTP_LOGS_ENABLED =
      "hadoop.http.logs.enabled";
  /** Defalt value for HADOOP_HTTP_LOGS_ENABLED */
  public static final boolean HADOOP_HTTP_LOGS_ENABLED_DEFAULT = true;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_HTTP_METRICS_ENABLED =
      "hadoop.http.metrics.enabled";
  public static final boolean HADOOP_HTTP_METRICS_ENABLED_DEFAULT = true;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH =
      "hadoop.security.credential.provider.path";

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_CREDENTIAL_CLEAR_TEXT_FALLBACK =
      "hadoop.security.credential.clear-text-fallback";
  public static final boolean
      HADOOP_SECURITY_CREDENTIAL_CLEAR_TEXT_FALLBACK_DEFAULT = true;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String  HADOOP_SECURITY_CREDENTIAL_PASSWORD_FILE_KEY =
      "hadoop.security.credstore.java-keystore-provider.password-file";

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS =
      "hadoop.security.sensitive-config-keys";
  public static final String HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS_DEFAULT =
      String.join(",",
          "secret$",
          "password$",
          "username$",
          "ssl.keystore.pass$",
          "fs.s3.*[Ss]ecret.?[Kk]ey",
          "fs.s3a.*.server-side-encryption.key",
          "fs.s3a.encryption.algorithm",
          "fs.s3a.encryption.key",
          "fs.azure\\.account.key.*",
          "credential$",
          "oauth.*secret",
          "oauth.*password",
          "oauth.*token",
          HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS);

  /**
   * @deprecated Please use
   * {@link CommonConfigurationKeysPublic#HADOOP_TAGS_SYSTEM} instead
   * See https://issues.apache.org/jira/browse/HADOOP-15474
   */
  public static final String HADOOP_SYSTEM_TAGS = "hadoop.system.tags";

  /**
   * @deprecated Please use
   * {@link CommonConfigurationKeysPublic#HADOOP_TAGS_CUSTOM} instead
   * See https://issues.apache.org/jira/browse/HADOOP-15474
   */
  public static final String HADOOP_CUSTOM_TAGS = "hadoop.custom.tags";

  public static final String HADOOP_TAGS_SYSTEM = "hadoop.tags.system";
  public static final String HADOOP_TAGS_CUSTOM = "hadoop.tags.custom";

  /** Configuration option for the shutdown hook manager shutdown time:
   *  {@value}. */
  public static final String SERVICE_SHUTDOWN_TIMEOUT =
      "hadoop.service.shutdown.timeout";

  /** Default shutdown hook timeout: {@value} seconds. */
  public static final long SERVICE_SHUTDOWN_TIMEOUT_DEFAULT = 30;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_PROMETHEUS_ENABLED =
      "hadoop.prometheus.endpoint.enabled";
  public static final boolean HADOOP_PROMETHEUS_ENABLED_DEFAULT = false;

  /**
   * @see
   * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String HADOOP_HTTP_IDLE_TIMEOUT_MS_KEY =
      "hadoop.http.idle_timeout.ms";
  public static final int HADOOP_HTTP_IDLE_TIMEOUT_MS_DEFAULT = 60000;

  /**
   * To configure scheduling of server metrics update thread. This config is used to indicate
   * initial delay and delay between each execution of the metric update runnable thread.
   */
  public static final String IPC_SERVER_METRICS_UPDATE_RUNNER_INTERVAL =
      "ipc.server.metrics.update.runner.interval";
  public static final int IPC_SERVER_METRICS_UPDATE_RUNNER_INTERVAL_DEFAULT = 5000;
}

