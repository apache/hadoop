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
 * Responsible to keep all abfs http headers here
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
  public static final String X_MS_DATE = "x-ms-date";
  public static final String X_MS_REQUEST_ID = "x-ms-request-id";
  public static final String X_MS_VERSION = "x-ms-version";
  public static final String X_MS_RESOURCE_TYPE = "x-ms-resource-type";
  public static final String X_MS_CONTINUATION = "x-ms-continuation";
  public static final String ETAG = "ETag";
  public static final String X_MS_PROPERTIES = "x-ms-properties";
  public static final String X_MS_RENAME_SOURCE = "x-ms-rename-source";
  public static final String LAST_MODIFIED = "Last-Modified";

  private HttpHeaderConfigurations() {}
}
