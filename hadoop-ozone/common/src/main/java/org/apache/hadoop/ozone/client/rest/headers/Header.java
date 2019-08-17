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

package org.apache.hadoop.ozone.client.rest.headers;

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
  public static final String OZONE_EMPTY_STRING="";
  public static final String OZONE_DEFAULT_LIST_SIZE = "1000";

  public static final String OZONE_USER = "x-ozone-user";
  public static final String OZONE_SIMPLE_AUTHENTICATION_SCHEME = "OZONE";
  public static final String OZONE_VERSION_HEADER = "x-ozone-version";
  public static final String OZONE_V1_VERSION_HEADER ="v1";

  public static final String OZONE_LIST_QUERY_SERVICE = "service";

  public static final String OZONE_INFO_QUERY_VOLUME = "volume";
  public static final String OZONE_INFO_QUERY_BUCKET = "bucket";
  public static final String OZONE_INFO_QUERY_KEY = "key";
  public static final String OZONE_INFO_QUERY_KEY_DETAIL = "key-detail";

  public static final String OZONE_REQUEST_ID = "x-ozone-request-id";
  public static final String OZONE_SERVER_NAME = "x-ozone-server-name";

  public static final String OZONE_STORAGE_TYPE = "x-ozone-storage-type";

  public static final String OZONE_BUCKET_VERSIONING =
      "x-ozone-bucket-versioning";

  public static final String OZONE_ACLS = "x-ozone-acls";
  public static final String OZONE_ACL_ADD = "ADD";
  public static final String OZONE_ACL_REMOVE = "REMOVE";

  public static final String OZONE_INFO_QUERY_TAG ="info";
  public static final String OZONE_QUOTA_QUERY_TAG ="quota";
  public static final String CONTENT_MD5 = "Content-MD5";
  public static final String OZONE_LIST_QUERY_PREFIX="prefix";
  public static final String OZONE_LIST_QUERY_MAXKEYS="max-keys";
  public static final String OZONE_LIST_QUERY_PREVKEY="prev-key";
  public static final String OZONE_LIST_QUERY_ROOTSCAN="root-scan";

  public static final String OZONE_RENAME_TO_KEY_PARAM_NAME = "toKey";

  private Header() {
    // Never constructed.
  }
}
