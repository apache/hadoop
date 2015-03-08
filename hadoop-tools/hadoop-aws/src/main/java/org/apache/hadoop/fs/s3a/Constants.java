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

public class Constants {
  // s3 access key
  public static final String ACCESS_KEY = "fs.s3a.access.key";

  // s3 secret key
  public static final String SECRET_KEY = "fs.s3a.secret.key";

  // number of simultaneous connections to s3
  public static final String MAXIMUM_CONNECTIONS = "fs.s3a.connection.maximum";
  public static final int DEFAULT_MAXIMUM_CONNECTIONS = 15;
  
  // connect to s3 over ssl?
  public static final String SECURE_CONNECTIONS = "fs.s3a.connection.ssl.enabled";
  public static final boolean DEFAULT_SECURE_CONNECTIONS = true;

  //use a custom endpoint?
  public static final String ENDPOINT = "fs.s3a.endpoint";
  //connect to s3 through a proxy server?
  public static final String PROXY_HOST = "fs.s3a.proxy.host";
  public static final String PROXY_PORT = "fs.s3a.proxy.port";
  public static final String PROXY_USERNAME = "fs.s3a.proxy.username";
  public static final String PROXY_PASSWORD = "fs.s3a.proxy.password";
  public static final String PROXY_DOMAIN = "fs.s3a.proxy.domain";
  public static final String PROXY_WORKSTATION = "fs.s3a.proxy.workstation";

  // number of times we should retry errors
  public static final String MAX_ERROR_RETRIES = "fs.s3a.attempts.maximum";
  public static final int DEFAULT_MAX_ERROR_RETRIES = 10;

  // seconds until we give up trying to establish a connection to s3
  public static final String ESTABLISH_TIMEOUT = "fs.s3a.connection.establish.timeout";
  public static final int DEFAULT_ESTABLISH_TIMEOUT = 50000;
  
  // seconds until we give up on a connection to s3
  public static final String SOCKET_TIMEOUT = "fs.s3a.connection.timeout";
  public static final int DEFAULT_SOCKET_TIMEOUT = 50000;

  // number of records to get while paging through a directory listing
  public static final String MAX_PAGING_KEYS = "fs.s3a.paging.maximum";
  public static final int DEFAULT_MAX_PAGING_KEYS = 5000;

  // the maximum number of threads to allow in the pool used by TransferManager
  public static final String MAX_THREADS = "fs.s3a.threads.max";
  public static final int DEFAULT_MAX_THREADS = 256;

  // the number of threads to keep in the pool used by TransferManager
  public static final String CORE_THREADS = "fs.s3a.threads.core";
  public static final int DEFAULT_CORE_THREADS = DEFAULT_MAXIMUM_CONNECTIONS;

  // when the number of threads is greater than the core, this is the maximum time
  // that excess idle threads will wait for new tasks before terminating.
  public static final String KEEPALIVE_TIME = "fs.s3a.threads.keepalivetime";
  public static final int DEFAULT_KEEPALIVE_TIME = 60;

  // the maximum number of tasks that the LinkedBlockingQueue can hold
  public static final String MAX_TOTAL_TASKS = "fs.s3a.max.total.tasks";
  public static final int DEFAULT_MAX_TOTAL_TASKS = 1000;

  // size of each of or multipart pieces in bytes
  public static final String MULTIPART_SIZE = "fs.s3a.multipart.size";
  public static final long DEFAULT_MULTIPART_SIZE = 104857600; // 100 MB
  
  // minimum size in bytes before we start a multipart uploads or copy
  public static final String MIN_MULTIPART_THRESHOLD = "fs.s3a.multipart.threshold";
  public static final int DEFAULT_MIN_MULTIPART_THRESHOLD = Integer.MAX_VALUE;
  
  // comma separated list of directories
  public static final String BUFFER_DIR = "fs.s3a.buffer.dir";

  // should we upload directly from memory rather than using a file buffer
  public static final String FAST_UPLOAD = "fs.s3a.fast.upload";
  public static final boolean DEFAULT_FAST_UPLOAD = false;

  //initial size of memory buffer for a fast upload
  public static final String FAST_BUFFER_SIZE = "fs.s3a.fast.buffer.size";
  public static final int DEFAULT_FAST_BUFFER_SIZE = 1048576; //1MB

  // private | public-read | public-read-write | authenticated-read | 
  // log-delivery-write | bucket-owner-read | bucket-owner-full-control
  public static final String CANNED_ACL = "fs.s3a.acl.default";
  public static final String DEFAULT_CANNED_ACL = "";

  // should we try to purge old multipart uploads when starting up
  public static final String PURGE_EXISTING_MULTIPART = "fs.s3a.multipart.purge";
  public static final boolean DEFAULT_PURGE_EXISTING_MULTIPART = false;

  // purge any multipart uploads older than this number of seconds
  public static final String PURGE_EXISTING_MULTIPART_AGE = "fs.s3a.multipart.purge.age";
  public static final long DEFAULT_PURGE_EXISTING_MULTIPART_AGE = 14400;

  // s3 server-side encryption
  public static final String SERVER_SIDE_ENCRYPTION_ALGORITHM = 
    "fs.s3a.server-side-encryption-algorithm";
  
  public static final String S3N_FOLDER_SUFFIX = "_$folder$";
  public static final String FS_S3A_BLOCK_SIZE = "fs.s3a.block.size";
  public static final String FS_S3A = "s3a";
}
