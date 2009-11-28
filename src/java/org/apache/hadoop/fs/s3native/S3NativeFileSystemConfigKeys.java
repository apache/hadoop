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

package org.apache.hadoop.fs.s3native;

import org.apache.hadoop.fs.CommonConfigurationKeys;

/** 
 * This class contains constants for configuration keys used
 * in the s3 file system. 
 *
 */

public class S3NativeFileSystemConfigKeys extends CommonConfigurationKeys {
  public static final String  S3_NATIVE_BLOCK_SIZE_KEY = "s3native.blocksize";
  public static final long    S3_NATIVE_BLOCK_SIZE_DEFAULT = 64*1024*1024;
  public static final String  S3_NATIVE_REPLICATION_KEY = "s3native.replication";
  public static final short   S3_NATIVE_REPLICATION_DEFAULT = 1;
  public static final String  S3_NATIVE_STREAM_BUFFER_SIZE_KEY = 
                                                    "s3native.stream-buffer-size";
  public static final int     S3_NATIVE_STREAM_BUFFER_SIZE_DEFAULT = 4096;
  public static final String  S3_NATIVE_BYTES_PER_CHECKSUM_KEY = 
                                                    "s3native.bytes-per-checksum";
  public static final int     S3_NATIVE_BYTES_PER_CHECKSUM_DEFAULT = 512;
  public static final String  S3_NATIVE_CLIENT_WRITE_PACKET_SIZE_KEY =
                                                    "s3native.client-write-packet-size";
  public static final int     S3_NATIVE_CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64*1024;
}
  
