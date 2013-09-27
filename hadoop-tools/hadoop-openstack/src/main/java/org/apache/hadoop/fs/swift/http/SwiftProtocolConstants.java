/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift.http;

import org.apache.hadoop.util.VersionInfo;

/**
 * Constants used in the Swift REST protocol,
 * and in the properties used to configure the {@link SwiftRestClient}.
 */
public class SwiftProtocolConstants {
  /**
   * Swift-specific header for authentication: {@value}
   */
  public static final String HEADER_AUTH_KEY = "X-Auth-Token";

  /**
   * Default port used by Swift for HTTP
   */
  public static final int SWIFT_HTTP_PORT = 8080;

  /**
   * Default port used by Swift Auth for HTTPS
   */
  public static final int SWIFT_HTTPS_PORT = 443;

  /** HTTP standard {@value} header */
  public static final String HEADER_RANGE = "Range";

  /** HTTP standard {@value} header */
  public static final String HEADER_DESTINATION = "Destination";

  /** HTTP standard {@value} header */
  public static final String HEADER_LAST_MODIFIED = "Last-Modified";

  /** HTTP standard {@value} header */
  public static final String HEADER_CONTENT_LENGTH = "Content-Length";

  /** HTTP standard {@value} header */
  public static final String HEADER_CONTENT_RANGE = "Content-Range";

  /**
   * Patten for range headers
   */
  public static final String SWIFT_RANGE_HEADER_FORMAT_PATTERN = "bytes=%d-%d";

  /**
   *  section in the JSON catalog provided after auth listing the swift FS:
   *  {@value}
   */
  public static final String SERVICE_CATALOG_SWIFT = "swift";
  /**
   *  section in the JSON catalog provided after auth listing the cloudfiles;
   * this is an alternate catalog entry name
   *  {@value}
   */
  public static final String SERVICE_CATALOG_CLOUD_FILES = "cloudFiles";
  /**
   *  section in the JSON catalog provided after auth listing the object store;
   * this is an alternate catalog entry name
   *  {@value}
   */
  public static final String SERVICE_CATALOG_OBJECT_STORE = "object-store";

  /**
   * entry in the swift catalog defining the prefix used to talk to objects
   *  {@value}
   */
  public static final String SWIFT_OBJECT_AUTH_ENDPOINT =
          "/object_endpoint/";
  /**
   * Swift-specific header: object manifest used in the final upload
   * of a multipart operation: {@value}
   */
  public static final String X_OBJECT_MANIFEST = "X-Object-Manifest";
  /**
   * Swift-specific header -#of objects in a container: {@value}
   */
  public static final String X_CONTAINER_OBJECT_COUNT =
          "X-Container-Object-Count";
  /**
   * Swift-specific header: no. of bytes used in a container {@value}
   */
  public static final String X_CONTAINER_BYTES_USED = "X-Container-Bytes-Used";

  /**
   * Header to set when requesting the latest version of a file: : {@value}
   */
  public static final String X_NEWEST = "X-Newest";

  /**
   * throttled response sent by some endpoints.
   */
  public static final int SC_THROTTLED_498 = 498;
  /**
   * W3C recommended status code for throttled operations
   */
  public static final int SC_TOO_MANY_REQUESTS_429 = 429;

  public static final String FS_SWIFT = "fs.swift";

  /**
   * Prefix for all instance-specific values in the configuration: {@value}
   */
  public static final String SWIFT_SERVICE_PREFIX = FS_SWIFT + ".service.";

  /**
   * timeout for all connections: {@value}
   */
  public static final String SWIFT_CONNECTION_TIMEOUT =
          FS_SWIFT + ".connect.timeout";

  /**
   * timeout for all connections: {@value}
   */
  public static final String SWIFT_SOCKET_TIMEOUT =
          FS_SWIFT + ".socket.timeout";

  /**
   * the default socket timeout in millis {@value}.
   * This controls how long the connection waits for responses from
   * servers.
   */
  public static final int DEFAULT_SOCKET_TIMEOUT = 60000;

  /**
   * connection retry count for all connections: {@value}
   */
  public static final String SWIFT_RETRY_COUNT =
          FS_SWIFT + ".connect.retry.count";

  /**
   * delay in millis between bulk (delete, rename, copy operations: {@value}
   */
  public static final String SWIFT_THROTTLE_DELAY =
          FS_SWIFT + ".connect.throttle.delay";

  /**
   * the default throttle delay in millis {@value}
   */
  public static final int DEFAULT_THROTTLE_DELAY = 0;

  /**
   * blocksize for all filesystems: {@value}
   */
  public static final String SWIFT_BLOCKSIZE =
          FS_SWIFT + ".blocksize";

  /**
   * the default blocksize for filesystems in KB: {@value}
   */
  public static final int DEFAULT_SWIFT_BLOCKSIZE = 32 * 1024;

  /**
   * partition size for all filesystems in KB: {@value}
   */
  public static final String SWIFT_PARTITION_SIZE =
    FS_SWIFT + ".partsize";

  /**
   * The default partition size for uploads: {@value}
   */
  public static final int DEFAULT_SWIFT_PARTITION_SIZE = 4608*1024;

  /**
   * request size for reads in KB: {@value}
   */
  public static final String SWIFT_REQUEST_SIZE =
    FS_SWIFT + ".requestsize";

  /**
   * The default reqeuest size for reads: {@value}
   */
  public static final int DEFAULT_SWIFT_REQUEST_SIZE = 64;


  public static final String HEADER_USER_AGENT="User-Agent";

  /**
   * The user agent sent in requests.
   */
  public static final String SWIFT_USER_AGENT= "Apache Hadoop Swift Client "
                                               + VersionInfo.getBuildVersion();

  /**
   * Key for passing the service name as a property -not read from the
   * configuration : {@value}
   */
  public static final String DOT_SERVICE = ".SERVICE-NAME";

  /**
   * Key for passing the container name as a property -not read from the
   * configuration : {@value}
   */
  public static final String DOT_CONTAINER = ".CONTAINER-NAME";

  public static final String DOT_AUTH_URL = ".auth.url";
  public static final String DOT_TENANT = ".tenant";
  public static final String DOT_USERNAME = ".username";
  public static final String DOT_PASSWORD = ".password";
  public static final String DOT_HTTP_PORT = ".http.port";
  public static final String DOT_HTTPS_PORT = ".https.port";
  public static final String DOT_REGION = ".region";
  public static final String DOT_PROXY_HOST = ".proxy.host";
  public static final String DOT_PROXY_PORT = ".proxy.port";
  public static final String DOT_LOCATION_AWARE = ".location-aware";
  public static final String DOT_APIKEY = ".apikey";
  public static final String DOT_USE_APIKEY = ".useApikey";

  /**
   * flag to say use public URL
   */
  public static final String DOT_PUBLIC = ".public";

  public static final String SWIFT_SERVICE_PROPERTY = FS_SWIFT + DOT_SERVICE;
  public static final String SWIFT_CONTAINER_PROPERTY = FS_SWIFT + DOT_CONTAINER;

  public static final String SWIFT_AUTH_PROPERTY = FS_SWIFT + DOT_AUTH_URL;
  public static final String SWIFT_TENANT_PROPERTY = FS_SWIFT + DOT_TENANT;
  public static final String SWIFT_USERNAME_PROPERTY = FS_SWIFT + DOT_USERNAME;
  public static final String SWIFT_PASSWORD_PROPERTY = FS_SWIFT + DOT_PASSWORD;
  public static final String SWIFT_APIKEY_PROPERTY = FS_SWIFT + DOT_APIKEY;
  public static final String SWIFT_HTTP_PORT_PROPERTY = FS_SWIFT + DOT_HTTP_PORT;
  public static final String SWIFT_HTTPS_PORT_PROPERTY = FS_SWIFT
          + DOT_HTTPS_PORT;
  public static final String SWIFT_REGION_PROPERTY = FS_SWIFT + DOT_REGION;
  public static final String SWIFT_PUBLIC_PROPERTY = FS_SWIFT + DOT_PUBLIC;

  public static final String SWIFT_USE_API_KEY_PROPERTY = FS_SWIFT + DOT_USE_APIKEY;

  public static final String SWIFT_LOCATION_AWARE_PROPERTY = FS_SWIFT +
            DOT_LOCATION_AWARE;

  public static final String SWIFT_PROXY_HOST_PROPERTY = FS_SWIFT + DOT_PROXY_HOST;
  public static final String SWIFT_PROXY_PORT_PROPERTY = FS_SWIFT + DOT_PROXY_PORT;
  public static final String HTTP_ROUTE_DEFAULT_PROXY =
    "http.route.default-proxy";
  /**
   * Topology to return when a block location is requested
   */
  public static final String TOPOLOGY_PATH = "/swift/unknown";
  /**
   * Block location to return when a block location is requested
   */
  public static final String BLOCK_LOCATION = "/default-rack/swift";
  /**
   * Default number of attempts to retry a connect request: {@value}
   */
  static final int DEFAULT_RETRY_COUNT = 3;
  /**
   * Default timeout in milliseconds for connection requests: {@value}
   */
  static final int DEFAULT_CONNECT_TIMEOUT = 15000;
}
