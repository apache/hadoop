/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web.headers;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * OZONE specific HTTP headers.
 */
@InterfaceAudience.Private
public final class Header {
  public static final String OZONE_QUOTA_BYTES = "BYTES";
  public static final String OZONE_QUOTA_MB = "MB";
  public static final String OZONE_QUOTA_GB = "GB";
  public static final String OZONE_QUOTA_TB = "TB";
  public static final String OZONE_QUOTA_REMOVE = "remove";
  public static final String OZONE_QUOTA_UNDEFINED = "undefined";

  public static final String OZONE_USER = "x-ozone-user";
  public static final String OZONE_SIMPLE_AUTHENTICATION_SCHEME = "OZONE";
  public static final String OZONE_VERSION_HEADER = "x-ozone-version";

  public static final String OZONE_LIST_QUERY_SERVICE = "service";
  public static final String OZONE_LIST_QUERY_VOLUME = "volume";
  public static final String OZONE_LIST_QUERY_BUCKET ="bucket";
  public static final String OZONE_LIST_QUERY_KEY ="key";

  public static final String OZONE_REQUEST_ID = "x-ozone-request-id";
  public static final String OZONE_SERVER_NAME = "x-ozone-server-name";

  private Header() {
    // Never constructed.
  }
}
