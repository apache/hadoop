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

package org.apache.hadoop.fs.kfs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;

/** 
 * This class contains constants for configuration keys used
 * in the kfs file system. 
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class KFSConfigKeys extends CommonConfigurationKeys {
  public static final String  KFS_BLOCK_SIZE_KEY = "kfs.blocksize";
  public static final long    KFS_BLOCK_SIZE_DEFAULT = 64*1024*1024;
  public static final String  KFS_REPLICATION_KEY = "kfs.replication";
  public static final short   KFS_REPLICATION_DEFAULT = 1;
  public static final String  KFS_STREAM_BUFFER_SIZE_KEY = 
                                                    "kfs.stream-buffer-size";
  public static final int     KFS_STREAM_BUFFER_SIZE_DEFAULT = 4096;
  public static final String  KFS_BYTES_PER_CHECKSUM_KEY = 
                                                    "kfs.bytes-per-checksum";
  public static final int     KFS_BYTES_PER_CHECKSUM_DEFAULT = 512;
  public static final String  KFS_CLIENT_WRITE_PACKET_SIZE_KEY =
                                                    "kfs.client-write-packet-size";
  public static final int     KFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64*1024;
}
  
