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
package org.apache.hadoop.fs.azurebfs.constants;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Responsible to keep all abfs http headers here.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class HttpHeaderConfigurations {
  public static final String ACCEPT = "Accept";
  public static final String ACCEPT_CHARSET = "Accept-Charset";
  public static final String AUTHORIZATION = "Authorization";
  public static final String IF_MODIFIED_SINCE = "If-Modified-Since";
  public static final String IF_UNMODIFIED_SINCE = "If-Unmodified-Since";
  public static final String IF_MATCH = "If-Match";
  public static final String IF_NONE_MATCH = "If-None-Match";
  public static final String CONTENT_LENGTH = "Content-Length";
  public static final String CONTENT_ENCODING = "Content-Encoding";
  public static final String CONTENT_LANGUAGE = "Content-Language";
  public static final String CONTENT_MD5 = "Content-MD5";
  public static final String CONTENT_TYPE = "Content-Type";
  public static final String RANGE = "Range";
  public static final String TRANSFER_ENCODING = "Transfer-Encoding";
  public static final String USER_AGENT = "User-Agent";
  public static final String X_HTTP_METHOD_OVERRIDE = "X-HTTP-Method-Override";
  public static final String X_MS_CLIENT_REQUEST_ID = "x-ms-client-request-id";
  public static final String X_MS_FECLIENT_METRICS = "x-ms-feclient-metrics";
  public static final String X_MS_EXISTING_RESOURCE_TYPE = "x-ms-existing-resource-type";
  public static final String X_MS_DATE = "x-ms-date";
  public static final String X_MS_REQUEST_ID = "x-ms-request-id";
  public static final String X_MS_VERSION = "x-ms-version";
  public static final String X_MS_RESOURCE_TYPE = "x-ms-resource-type";
  public static final String X_MS_CONTINUATION = "x-ms-continuation";
  public static final String ETAG = "ETag";
  public static final String X_MS_PROPERTIES = "x-ms-properties";
  public static final String X_MS_RENAME_SOURCE = "x-ms-rename-source";
  public static final String LAST_MODIFIED = "Last-Modified";
  public static final String X_MS_OWNER = "x-ms-owner";
  public static final String X_MS_GROUP = "x-ms-group";
  public static final String X_MS_ACL = "x-ms-acl";
  public static final String X_MS_PERMISSIONS = "x-ms-permissions";
  public static final String X_MS_UMASK = "x-ms-umask";
  public static final String X_MS_ABFS_CLIENT_LATENCY = "x-ms-abfs-client-latency";
  public static final String X_MS_ENCRYPTION_KEY = "x-ms-encryption-key";
  public static final String X_MS_ENCRYPTION_KEY_SHA256 = "x-ms-encryption-key-sha256";
  public static final String X_MS_ENCRYPTION_ALGORITHM = "x-ms-encryption-algorithm";
  public static final String X_MS_REQUEST_SERVER_ENCRYPTED = "x-ms-request-server-encrypted";
  public static final String X_MS_SERVER_ENCRYPTED = "x-ms-server-encrypted";
  public static final String X_MS_ENCRYPTION_CONTEXT = "x-ms-encryption-context";
  public static final String X_MS_LEASE_ACTION = "x-ms-lease-action";
  public static final String X_MS_LEASE_DURATION = "x-ms-lease-duration";
  public static final String X_MS_LEASE_ID = "x-ms-lease-id";

  /**
   * Http Request Header for denoting the lease id of source in copy operation.
   * {@value}
   */
  public static final String X_MS_SOURCE_LEASE_ID = "x-ms-source-lease-id";
  public static final String X_MS_PROPOSED_LEASE_ID = "x-ms-proposed-lease-id";
  public static final String X_MS_LEASE_BREAK_PERIOD = "x-ms-lease-break-period";
  public static final String EXPECT = "Expect";
  public static final String X_MS_RANGE_GET_CONTENT_MD5 = "x-ms-range-get-content-md5";

  /**
   * Http Response Header for denoting directory.
   * {@value}
   */
  public static final String X_MS_META_HDI_ISFOLDER = "x-ms-meta-hdi_isfolder";

  /**
   * Http Response Header prefix for user-defined properties.
   * {@value}
   */
  public static final String X_MS_METADATA_PREFIX = "x-ms-meta-";

  /**
   * Http Request Header for denoting the source of copy operation.
   * {@value}
   */
  public static final String X_MS_COPY_SOURCE = "x-ms-copy-source";

  /**
   * Http Request Header for denoting MD5 hash of the blob content.
   * {@value}
   */
  public static final String X_MS_BLOB_CONTENT_MD5 = "x-ms-blob-content-md5";

  /**
   * Http Request Header for denoting blob type.
   * {@value}
   */
  public static final String X_MS_BLOB_TYPE = "x-ms-blob-type";

  /**
   * Http Request Header for copy id.
   * {@value}
   */
  public static final String X_MS_COPY_ID = "x-ms-copy-id";

    /**
     * Http Response Header for denoting the copy status description of the operation.
     * {@value}
     */
  public static final String X_MS_COPY_STATUS_DESCRIPTION = "x-ms-copy-status-description";

    /**
     * Http Response Header for denoting the copy status of the operation.
     * Expected Values:
     * {@link AbfsHttpConstants#COPY_STATUS_SUCCESS}
     * {@link AbfsHttpConstants#COPY_STATUS_FAILED}
     * {@link AbfsHttpConstants#COPY_STATUS_PENDING}
     * {@link AbfsHttpConstants#COPY_STATUS_ABORTED}
     */
  public static final String X_MS_COPY_STATUS = "x-ms-copy-status";

  /**
   * Http Request Header for create rename idempotence.
   * {@value}
   */
  public static final String X_MS_CLIENT_TRANSACTION_ID = "x-ms-client-transaction-id";

  private HttpHeaderConfigurations() {}
}
