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


  //Defaults are not specified for following keys
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY =
    "net.topology.script.file.name";
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY =
    "net.topology.node.switch.mapping.impl";
  
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY =
    "net.topology.table.file.name";

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
  public static final String  IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY =
    "ipc.client.connect.max.retries.on.timeouts";
  /** Default value for IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY */
  public static final int  IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT = 45;
  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  IPC_CLIENT_TCPNODELAY_KEY =
    "ipc.client.tcpnodelay";
  /** Defalt value for IPC_CLIENT_TCPNODELAY_KEY */
  public static final boolean IPC_CLIENT_TCPNODELAY_DEFAULT = false;
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
  public static final boolean IPC_SERVER_TCPNODELAY_DEFAULT = false;

  /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
  public static final String  HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY =
    "hadoop.rpc.socket.factory.class.default";
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

  public static final String HADOOP_SSL_ENABLED_KEY = "hadoop.ssl.enabled";
  public static final boolean HADOOP_SSL_ENABLED_DEFAULT = false;

}

