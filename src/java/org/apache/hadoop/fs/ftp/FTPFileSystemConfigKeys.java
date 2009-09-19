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

package org.apache.hadoop.fs.ftp;

import org.apache.hadoop.fs.CommonConfigurationKeys;

/** 
 * This class contains constants for configuration keys used
 * in the ftp file system. 
 *
 */

public class FTPFileSystemConfigKeys extends CommonConfigurationKeys {
  public static final String  FTP_BLOCK_SIZE_KEY = "ftp.blocksize";
  public static final long    FTP_BLOCK_SIZE_DEFAULT = 64*1024*1024;
  public static final String  FTP_REPLICATION_KEY = "ftp.replication";
  public static final short   FTP_REPLICATION_DEFAULT = 1;
  public static final String  FTP_STREAM_BUFFER_SIZE_KEY = 
                                                    "ftp.stream-buffer-size";
  public static final int     FTP_STREAM_BUFFER_SIZE_DEFAULT = 4096;
  public static final String  FTP_BYTES_PER_CHECKSUM_KEY = 
                                                    "ftp.bytes-per-checksum";
  public static final int     FTP_BYTES_PER_CHECKSUM_DEFAULT = 512;
  public static final String  FTP_CLIENT_WRITE_PACKET_SIZE_KEY =
                                                    "ftp.client-write-packet-size";
  public static final int     FTP_CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64*1024;
}
  
