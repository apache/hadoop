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

package org.apache.hadoop.fs.bos;

/**
 * Constants used by the BOS FileSystem.
 */
public final class BaiduBosConstants {

  /** User agent string for BOS client. */
  public static final String BOS_FILESYSTEM_USER_AGENT =
      "bos-hadoop-fs-ns/2.0.0.16";

  /** Object type value for directory entries. */
  public static final String BOS_FILE_TYPE_DIRECTORY = "Directory";

  /** Default block size: 128 MB. */
  public static final long DEFAULT_BOS_BLOCK_SIZE = 128 * 1024 * 1024L;

  /** Default read buffer size: 128 KB. */
  public static final int DEFAULT_BOS_READ_BUFFER_SIZE = 128 * 1024;

  /** Maximum number of objects per listing request. */
  public static final int BOS_MAX_LISTING_LENGTH = 1000;

  /** Configuration key for BOS endpoint. */
  public static final String BOS_ENDPOINT = "fs.bos.endpoint";

  /** Configuration key for BOS access key. */
  public static final String BOS_ACCESS_KEY = "fs.bos.access.key";

  /** Configuration key for BOS secret key. */
  public static final String BOS_SECRET_KEY = "fs.bos.secret.access.key";

  /** Configuration key for BOS session token. */
  public static final String BOS_SESSION_TOKEN = "fs.bos.session.token.key";

  /** Environment variable for BOS access key. */
  public static final String BOS_AK_ENV = "FS_BOS_ACCESS_KEY";

  /** Environment variable for BOS secret key. */
  public static final String BOS_SK_ENV = "FS_BOS_SECRET_ACCESS_KEY";

  /** Environment variable for BOS session token. */
  public static final String BOS_STS_TOKEN_ENV = "FS_BOS_SESSION_TOKEN_KEY";

  /** Configuration key for multipart upload block size. */
  public static final String BOS_MULTI_UPLOAD_BLOCK_SIZE =
      "fs.bos.multipart.uploads.block.size";

  /** Default multipart upload block size: 16 MB. */
  public static final int BOS_MULTI_UPLOAD_BLOCK_SIZE_DEFAULT =
      16 * 1024 * 1024;

  /** Configuration key for multipart upload concurrent size. */
  public static final String BOS_MULTI_UPLOAD_CONCURRENT_SIZE =
      "fs.bos.multipart.uploads.concurrent.size";

  /** Default multipart upload concurrent size. */
  public static final int DEFAULT_MULTI_UPLOAD_CONCURRENT_SIZE = 10;

  /** Configuration key for copy large file threshold. */
  public static final String BOS_COPY_LARGE_FILE_THRESHOLD =
      "fs.bos.copy.large.file.threshold";

  /** Default copy large file threshold: 5 GB. */
  public static final long BOS_COPY_LARGE_FILE_THRESHOLD_DEFAULT =
      5 * 1024 * 1024 * 1024L;

  /** Configuration key for maximum connections. */
  public static final String BOS_MAX_CONNECTIONS = "fs.bos.max.connections";

  /** Configuration key for maximum retries. */
  public static final String BOS_MAX_RETRIES = "fs.bos.maxRetries";

  /** Configuration key for sleep between retries. */
  public static final String BOS_SLEEP_MILL_SECONDS =
      "fs.bos.sleep.millSeconds";

  /** Configuration key for stream buffer size. */
  public static final String BOS_STREAM_BUFFER_SIZE =
      "fs.bos.stream.buffer.size";

  /** Configuration key for credentials provider class. */
  public static final String BOS_CREDENTIALS_PROVIDER =
      "fs.bos.credentials.provider";

  /** Default credentials provider class name. */
  public static final String BOS_CONFIG_CREDENTIALS_PROVIDER =
      "org.apache.hadoop.fs.bos.credentials"
          + ".ConfigurationCredentialsProvider";

  /** Configuration key for block size. */
  public static final String BOS_BLOCK_SIZE = "fs.bos.block.size";

  /** Default read ahead size: 4 MB. */
  public static final long DEFAULT_BOS_READ_AHEAD_SIZE = 4 * 1024 * 1024;

  /** Configuration key for read ahead size. */
  public static final String BOS_READ_AHEAD_SIZE = "fs.bos.readahead.size";

  /** Configuration key for read buffer size. */
  public static final String BOS_READ_BUFFER_SIZE =
      "fs.bos.read.buffer.size";

  /** Configuration key for maximum threads. */
  public static final String BOS_THREADS_MAX_NUM = "fs.bos.threads.max.num";

  /** Default maximum thread count. */
  public static final int BOS_THREADS_MAX_NUM_DEFAULT = 64;

  /** User metadata key for HDFS user name. */
  public static final String BOS_FILE_PATH_USER_KEY = "hdfs-user";

  /** User metadata key for HDFS group name. */
  public static final String BOS_FILE_PATH_GROUP_KEY = "hdfs-group";

  /** Configuration key for bucket hierarchy mode. */
  public static final String BOS_BUCKET_HIERARCHY =
      "fs.bos.bucket.hierarchy";

  private BaiduBosConstants() {
    // utility class
  }
}
