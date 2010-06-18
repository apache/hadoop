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
import org.apache.hadoop.classification.InterfaceStability;

/** 
 * This class contains constants for configuration keys used
 * in the common code.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CommonConfigurationKeys {
  
  // The Keys
  public static final String  IO_NATIVE_LIB_AVAILABLE_KEY =
                                           "io.native.lib.available";
  public static final boolean IO_NATIVE_LIB_AVAILABLE_DEFAULT = true;
  public static final String  NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY =
                                         "net.topology.script.number.args";
  public static final int     NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_DEFAULT = 100;

  //FS keys
  public static final String  FS_HOME_DIR_KEY = "fs.homeDir";
  public static final String  FS_HOME_DIR_DEFAULT = "/user";
  public static final String  FS_DEFAULT_NAME_KEY = "fs.defaultFS";
  public static final String  FS_DEFAULT_NAME_DEFAULT = "file:///";
  public static final String  FS_PERMISSIONS_UMASK_KEY = "fs.permissions.umask-mode";
  public static final int     FS_PERMISSIONS_UMASK_DEFAULT = 0022;
  public static final String  FS_DF_INTERVAL_KEY = "fs.df.interval"; 
  public static final long    FS_DF_INTERVAL_DEFAULT = 60000;


  //Defaults are not specified for following keys
  public static final String  NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY = 
                                         "net.topology.script.file.name";
  public static final String  NET_TOPOLOGY_CONFIGURED_NODE_MAPPING_KEY =
                                     "net.topology.configured.node.mapping";
  public static final String  NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY =
                                   "net.topology.node.switch.mapping.impl";

  public static final String  FS_CLIENT_BUFFER_DIR_KEY =
                                         "fs.client.buffer.dir";

  //TBD: Code is not updated to use following keys.
  //These keys will be used in later versions
  //
  public static final long    FS_LOCAL_BLOCK_SIZE_DEFAULT = 32*1024*1024;
  public static final String  FS_AUTOMATIC_CLOSE_KEY = "fs.automatic.close";
  public static final boolean FS_AUTOMATIC_CLOSE_DEFAULT = true;
  public static final String  FS_FILE_IMPL_KEY = "fs.file.impl";
  public static final String  FS_FTP_HOST_KEY = "fs.ftp.host";
  public static final String  FS_FTP_HOST_PORT_KEY = "fs.ftp.host.port";
  public static final String  FS_TRASH_INTERVAL_KEY = "fs.trash.interval";
  public static final long    FS_TRASH_INTERVAL_DEFAULT = 0;

  public static final String  IO_MAPFILE_BLOOM_SIZE_KEY = "io.mapfile.bloom.size";
  public static final int     IO_MAPFILE_BLOOM_SIZE_DEFAULT = 1024*1024;
  public static final String  IO_MAPFILE_BLOOM_ERROR_RATE_KEY = 
                                       "io.mapfile.bloom.error.rate" ;
  public static final float   IO_MAPFILE_BLOOM_ERROR_RATE_DEFAULT = 0.005f;
  public static final String  IO_COMPRESSION_CODEC_LZO_CLASS_KEY = "io.compression.codec.lzo.class";
  public static final String  IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY = 
                                       "io.compression.codec.lzo.buffersize";
  public static final int     IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_DEFAULT = 64*1024;
  public static final String  IO_MAP_INDEX_INTERVAL_KEY = "io.map.index.interval";
  public static final int     IO_MAP_INDEX_INTERVAL_DEFAULT = 128;
  public static final String  IO_MAP_INDEX_SKIP_KEY = "io.map.index.skip";
  public static final int     IO_MAP_INDEX_SKIP_DEFAULT = 0;
  public static final String  IO_SEQFILE_COMPRESS_BLOCKSIZE_KEY = "io.seqfile.compress.blocksize";
  public static final int     IO_SEQFILE_COMPRESS_BLOCKSIZE_DEFAULT = 1000000;
  public static final String  IO_SKIP_CHECKSUM_ERRORS_KEY = "io.skip.checksum.errors";
  public static final boolean IO_SKIP_CHECKSUM_ERRORS_DEFAULT = false;
  public static final String  IO_SORT_MB_KEY = "io.sort.mb";
  public static final int     IO_SORT_MB_DEFAULT = 100;
  public static final String  IO_SORT_FACTOR_KEY = "io.sort.factor";
  public static final int     IO_SORT_FACTOR_DEFAULT = 100;
  public static final String  IO_SERIALIZATIONS_KEY = "io.serializations";

  public static final String  TFILE_IO_CHUNK_SIZE_KEY = "tfile.io.chunk.size";
  public static final int     TFILE_IO_CHUNK_SIZE_DEFAULT = 1024*1024;
  public static final String  TFILE_FS_INPUT_BUFFER_SIZE_KEY = "tfile.fs.input.buffer.size";
  public static final int     TFILE_FS_INPUT_BUFFER_SIZE_DEFAULT = 256*1024;
  public static final String  TFILE_FS_OUTPUT_BUFFER_SIZE_KEY = "tfile.fs.output.buffer.size";
  public static final int     TFILE_FS_OUTPUT_BUFFER_SIZE_DEFAULT = 256*1024;

  public static final String  IPC_PING_INTERVAL_KEY = "ipc.ping.interval";
  public static final int     IPC_PING_INTERVAL_DEFAULT = 60000;
  public static final String  IPC_CLIENT_PING_KEY = "ipc.client.ping";
  public static final boolean IPC_CLIENT_PING_DEFAULT = true;
  public static final String  IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY = 
                                       "ipc.client.connection.maxidletime";
  public static final int     IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT = 10000;
  public static final String  IPC_CLIENT_CONNECT_MAX_RETRIES_KEY = 
                                       "ipc.client.connect.max.retries";
  public static final int     IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT = 10;
  public static final String  IPC_CLIENT_TCPNODELAY_KEY = "ipc.client.tcpnodelay";
  public static final boolean IPC_CLIENT_TCPNODELAY_DEFAULT = false;
  public static final String  IPC_SERVER_LISTEN_QUEUE_SIZE_KEY = 
                                       "ipc.server.listen.queue.size";
  public static final int     IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT = 128;
  public static final String  IPC_CLIENT_KILL_MAX_KEY = "ipc.client.kill.max";
  public static final int     IPC_CLIENT_KILL_MAX_DEFAULT = 10;
  public static final String  IPC_CLIENT_IDLETHRESHOLD_KEY = "ipc.client.idlethreshold";
  public static final int     IPC_CLIENT_IDLETHRESHOLD_DEFAULT = 4000;
  public static final String  IPC_SERVER_TCPNODELAY_KEY = "ipc.server.tcpnodelay";
  public static final boolean IPC_SERVER_TCPNODELAY_DEFAULT = false;
  public static final String  IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY = 
                                       "ipc.server.max.response.size";
  public static final int     IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT = 
                                        1024*1024;
  public static final String IPC_SERVER_RPC_READ_THREADS_KEY =
                                        "ipc.server.read.threadpool.size";
  public static final int IPC_SERVER_RPC_READ_THREADS_DEFAULT = 1;
  /**
   * How many calls per handler are allowed in the queue.
   */
  public static final String  IPC_SERVER_HANDLER_QUEUE_SIZE_KEY = 
                                       "ipc.server.handler.queue.size";
  /**
   * The default number of calls per handler in the queue.
   */
  public static final int IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT = 100;

  public static final String  HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY = 
                                       "hadoop.rpc.socket.factory.class.default";
  public static final String  HADOOP_SOCKS_SERVER_KEY = "hadoop.socks.server";
  public static final String  HADOOP_JOB_UGI_KEY = "hadoop.job.ugi";
  public static final String  HADOOP_UTIL_HASH_TYPE_KEY = "hadoop.util.hash.type";
  public static final String  HADOOP_UTIL_HASH_TYPE_DEFAULT = "murmur";
  public static final String  HADOOP_SECURITY_GROUP_MAPPING = "hadoop.security.group.mapping";
  public static final String  HADOOP_SECURITY_GROUPS_CACHE_SECS = "hadoop.security.groups.cache.secs";
  public static final String  HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
  public static final String HADOOP_SECURITY_AUTHORIZATION =
      "hadoop.security.authorization";
}

